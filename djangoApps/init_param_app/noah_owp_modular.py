import os
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from .util.utilities import get_hydrofabric_input_attr_file
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

def noah_owp_modular_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    ''' 
    Build initial parameter estimates (IPE) for NOAH-OWP-Modular 

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    module = "Noah-OWP-Modular"
    filename_list = []
 
    # Get list of catchments from gpkg divides layer using geopandas
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
        except:
            # TODO: Replace 'except' with proper catch
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error = error_str) 
            logger.error(error_str)
            return error
    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        logger.error(error_str)
        return error

    #Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
    try:
        attr_file = get_hydrofabric_input_attr_file()
        attr = pq.read_table(attr_file)
    except FileNotFoundError as fnfe:
        logger.error(fnfe)
        error_str = 'Hydrofabric data input directory does not exist'
        error = dict(error=error_str)
        return error
    except Exception as exc:
        error_str = 'Error opening ' + attr_file
        error = dict(error = error_str)
        logger.error(error_str, exc)
        return error
    
    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)
    
    #filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catchments)]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error = error_str) 
        logger.error(error_str)
        return error
    
    #Loop through catchments, get soil type, populate config file template, write config file to temp 
    for index, row in filtered.iterrows():
   
        catchment_id = row['divide_id']
        
        startdate = '202408260000'
        enddate = '202408260000'
        noah_input_dir = 'test'

        # Define namelist template

        tslp = row['slope_mean']
        azimuth = row['aspect_c_mean'] 
        lat = row['Y']
        lon = row['X']
        isltype = row['ISLTYP']
        vegtype = row['IVGTYP']
        if vegtype == 16:
            sfctype = '2'
        else:
            sfctype = '1'
            
        namelist = ['&timing',
                "  " + "dt".ljust(19) +  "= 3600.0" + "                       ! timestep [seconds]",
                "  " + "startdate".ljust(19) + "= " + "'" + startdate + "'" + "               ! UTC time start of simulation (YYYYMMDDhhmm)",
                "  " + "enddate".ljust(19) + "= " + "'" + enddate + "'" + "               ! UTC time end of simulation (YYYYMMDDhhmm)",
                "  " + "forcing_filename".ljust(19) + "= '.'" + "                          ! file containing forcing data",
                "  " + "output_filename".ljust(19) + "= '.'",
                '/',
                "",
                '&parameters',
                "  " + "parameter_dir".ljust(19) + "= " + "'" + noah_input_dir + "'",
                "  " + "general_table".ljust(19) + "= 'GENPARM.TBL'" + "                ! general param tables and misc params",
                "  " + "soil_table".ljust(19) + "= 'SOILPARM.TBL'" + "               ! soil param table",
                "  " + "noahowp_table".ljust(19) + "= 'MPTABLE.TBL'" + "                ! model param tables (includes veg)",
                "  " + "soil_class_name".ljust(19) + "= 'STAS'" + "                       ! soil class data source - 'STAS' or 'STAS-RUC'",
                "  " + "veg_class_name".ljust(19) + "= 'USGS'" + "                       ! vegetation class data source - 'MODIFIED_IGBP_MODIS_NOAH' or 'USGS'",
                '/',
                "",
                '&location',
                "  " + "lat".ljust(19) + "= " + str(lat) + "            ! latitude [degrees]  (-90 to 90)",
                "  " + "lon".ljust(19) + "= " + str(lon) + "          ! longitude [degrees] (-180 to 180)",
                "  " + "terrain_slope".ljust(19) + "= " + str(tslp) + "            ! terrain slope [degrees]",
                "  " + "azimuth".ljust(19) + "= " + str(azimuth) + "           ! terrain azimuth or aspect [degrees clockwise from north]",
                '/',
                "",
                "&forcing",
                "  " + "ZREF".ljust(19) + "= 10.0" + "                         ! measurement height for wind speed (m)",
                "  " + "rain_snow_thresh".ljust(19) + "= 0.5" + "                          ! rain-snow temperature threshold (degrees Celcius)",
                "/",
                "",
                "&model_options",
                "  " + "precip_phase_option".ljust(34) + "= 6",
                "  " + "snow_albedo_option".ljust(34) + "= 1",
                "  " + "dynamic_veg_option".ljust(34) + "= 4",
                "  " + "runoff_option".ljust(34) + "= 3",
                "  " + "drainage_option".ljust(34) + "= 8",
                "  " + "frozen_soil_option".ljust(34) + "= 1",
                "  " + "dynamic_vic_option".ljust(34) + "= 1",
                "  " + "radiative_transfer_option".ljust(34) + "= 3",
                "  " + "sfc_drag_coeff_option".ljust(34) + "= 1",
                "  " + "canopy_stom_resist_option".ljust(34) + "= 1",
                "  " + "crop_model_option".ljust(34) + "= 0",
                "  " + "snowsoil_temp_time_option".ljust(34) + "= 3",
                "  " + "soil_temp_boundary_option".ljust(34) + "= 2",
                "  " + "supercooled_water_option".ljust(34) + "= 1",
                "  " + "stomatal_resistance_option".ljust(34) + "= 1",
                "  " + "evap_srfc_resistance_option".ljust(34) + "= 4",
                "  " + "subsurface_option".ljust(34) + "= 2",
                "/",
                "",
                "&structure",
                "  " + "isltyp".ljust(17) + "= " + str(isltype) + "              ! soil texture class",
                "  " + "nsoil".ljust(17) + "= 4              ! number of soil levels",
                "  " + "nsnow".ljust(17) + "= 3              ! number of snow levels",
                "  " + "nveg".ljust(17) + "= 27             ! number of vegetation type",
                "  " + "vegtyp".ljust(17) + "= " + str(vegtype) + "             ! vegetation type",
                "  " + "croptype".ljust(17) + "= 0              ! crop type (0 = no crops; this option is currently inactive)",
                "  " + "sfctyp".ljust(17) + "= " + str(sfctype) + "              ! land surface type, 1:soil, 2:lake",
                "  " + "soilcolor".ljust(17) + "= 4              ! soil color code",
                "/",
                "",
                "&initial_values",
                "  " + "dzsnso".ljust(10) + "= 0.0, 0.0, 0.0, 0.1, 0.3, 0.6, 1.0      ! level thickness [m]",
                "  " + "sice".ljust(10) + "= 0.0, 0.0, 0.0, 0.0                     ! initial soil ice profile [m3/m3]",
                "  " + "sh2o".ljust(10) + "= 0.3, 0.3, 0.3, 0.3                     ! initial soil liquid profile [m3/m3]",
                "  " + "zwt".ljust(10) + "= -2.0                                   ! initial water table depth below surface [m]",
                "/",
                ]

    
        cfg_filename = "noah-owp-modular-init-" + catchment_id + ".namelist.input"
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(namelist))
                            outfile.write("\n")

    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)
 
    #fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri
    
    # Get default values for calibratable initial parameters.
    for x in range(len(module_metadata[0]["calibrate_parameters"])):
            initial_values = module_metadata[0]["calibrate_parameters"][x]["initial_value"]
            #If initial values are an array, get proper value for vegtype, otherwise use the single value.
            if len(initial_values) > 1:
                 module_metadata["calibrate_parameters"][x]["initial_value"] = initial_values[vegtype - 1]
            else:
                 module_metadata["calibrate_parameters"][x]["initial_value"] = initial_values[0]

    return module_metadata

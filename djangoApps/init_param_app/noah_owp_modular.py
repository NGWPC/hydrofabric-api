import os
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from .utilities import *

def noah_owp_modular_ipe(gage_id, subset_dir, module_metadata):
    ''' 
    Build initial parameter estimates (IPE) for NOAH-OWP-Modular 

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Setup logging
    logger = logging.getLogger(__name__)

    #Get config file
    config = get_config()
    s3url = config['s3url']
    s3bucket = config['s3bucket']
    s3prefix = config['s3prefix']
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = config['hydrofabric_version']
    hydrofabric_type = config['hydrofabric_type']
    
    #setup output dir
    #first save the top level dir for the gpkg
    gpkg_dir = subset_dir
    subset_dir = os.path.join(subset_dir, 'Noah-OWP-Modular')
    if not os.path.exists(subset_dir):
        os.mkdir(subset_dir)


    # Get list of catchments from gpkg divides layer using geopandas
    gpkg_file = "Gage_" + gage_id + ".gpkg"
    gpkg_file = os.path.join(gpkg_dir, gpkg_file)    
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
        except:
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error = error_str) 
            print(error_str)
            logger.error(error_str)
            return error
    except:
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        print(error_str)
        logger.error(error_str)
        return error

    #Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
    attr_file = os.path.join(hydrofabric_dir, hydrofabric_version, hydrofabric_type, 'conus_model-attributes')
    try:
        attr = pq.read_table(attr_file)
    except:
        error_str = 'Error opening ' + attr_file
        error = dict(error = error_str) 
        print(error_str)
        logger.error(error_str)
        return error
    
    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)
    
    #filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catchments)]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error = error_str) 
        print(error_str)
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
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(namelist))
                            outfile.write("\n")

    if s3prefix:
        subset_s3prefix = s3prefix + "/" + gage_id + '/' + 'Noah-OWP-Modular'
    else:
        subset_s3prefix = gage_id  + '/' + 'Noah-OWP-Modular'

    #Get list of .input files in temp directory and copy to s3
    files = Path(subset_dir).glob('*.input')
    for file in files:
        print("writing: " + str(file) + " to s3")
        file_name = os.path.basename(file)
        write_minio(subset_dir, file_name, s3url, s3bucket, subset_s3prefix)

    uri = build_uri(s3bucket, subset_s3prefix)
    status_str = "Config files written to:  " + uri
    print(status_str)
    logger.info(status_str)
 
    #fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri
    
    # Get default values for calibratable initial parameters.
    for x in range(len(module_metadata["calibrate_parameters"])):
            initial_values = module_metadata["calibrate_parameters"][x]["initial_value"]
            #If initial values are an array, get proper value for vegtype, otherwise use the single value.
            if len(initial_values) > 1:
                 module_metadata["calibrate_parameters"][x]["initial_value"] = initial_values[vegtype - 1]
            else:
                 module_metadata["calibrate_parameters"][x]["initial_value"] = initial_values[0]

    return module_metadata

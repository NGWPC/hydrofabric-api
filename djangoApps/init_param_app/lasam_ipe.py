import os
import logging

import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from .util.utilities import get_hydrofabric_input_attr_file, get_subset_dir_file_names
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

def lasam_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    ''' 
    Build initial parameter estimates (IPE) for the LASAM module

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    module = "LASAM"
    filename_list = []
    
    # Calibratable parameters
    soil_param_file = "../../resources/vG_default_params_HYDRUS.dat"        # Van Genuchton soil parameters 
    ponded_depth_max = "1.1[cm]"                                            # Maximum amount of water unavailable for surface drainage
    field_capacity_psi = "340.9[cm]"                                        # Capillary head corresponding to volumetric water content at which gravity drainage becomes slower

    # Skeleton for the config file. Needs layer soil types to be specified per-catchment
    lasam_lst = ['verbosity=none',
                 'soil_params_file=' + soil_param_file,
                 'layer_thickness=200.0[cm]',
                 'initial_psi=2000.0[cm]',
                 'timestep=300[sec]',
                 'endtime=1000[hr]',
                 'forcing_resolution=3600[sec]',
                 'ponded_depth_max=' + ponded_depth_max,
                 'use_closed_form_G=false',
                 'layer_soil_type=',
                 'max_soil_types=15',
                 'wilting_point_psi=15495.0[cm]',
                 'field_capacity_psi=' + field_capacity_psi,
                 'giuh_ordinates=0.06,0.51,0.28,0.12,0.03',
                 'calib_params=true',
                 'adaptive_timestep=true'
                ]
    
    # Make directory and add soil params file there. Also add it to the filelist.
    os.makedirs(subset_dir, exist_ok=True)
    os.system('cp {0} {1}/{2}'.format(soil_param_file, subset_dir, soil_param_file.split("/")[-1]))
    filename_list.append(os.path.join(subset_dir, soil_param_file.split("/")[-1]))

    # Get all the catchments
    filtered = get_catchments(gpkg_file)
    
    # Loop through catchments, get soil type
    for index, row in filtered.iterrows():
        catchment_id = str(row['divide_id'])
        soil_type = str(row['ISLTYP'])
        lasam_lst_catID = lasam_lst.copy()
        lasam_lst_catID[9] = lasam_lst_catID[9] + soil_type
        
        lasam_bmi_file = os.path.join(subset_dir, catchment_id + '_bmi_config_lasam.txt')
        filename_list.append(lasam_bmi_file)
        with open(lasam_bmi_file, "w") as f:
            f.writelines('\n'.join(lasam_lst_catID))
    
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    # Fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri

    return module_metadata

def get_catchments(gpkg_file):
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
    except:# TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        logger.error(error_str)
        return error
    
    # Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
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
    
    # Filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catchments)]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error = error_str) 
        logger.error(error_str)
        return error
    else:
        return filtered

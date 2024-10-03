import copy
import os
import logging
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from init_param_app.utilities import *

logger = logging.getLogger(__name__)

def snow17_ipe(gage_id, subset_dir, module_metadata_list):
    '''
        Build initial parameter estimates (IPE) for Snow17

        Parameters:
        gage_id (str):  The gage ID, e.g., 06710385
        subset_dir (str):  Path to gage id directory where the module directory will be made.
        module_metadata_list (dict):  list dictionary containing URI, initial parameters, output variables

        Returns:
        dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Setup logging
    #logger = logging.getLogger(__name__)

    # Get config file
    config = get_config()
    s3url = config['s3url']
    s3bucket = config['s3bucket']
    s3prefix = config['s3prefix']
    output_dir = config['output_dir']
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = config['hydrofabric_version']
    hydrofabric_type = config['hydrofabric_type']

    # get attrib file
    attr_file = os.path.join(hydrofabric_dir, hydrofabric_version, hydrofabric_type, 'conus_model-attributes')

    # setup output dir
    # first save the top level dir for the gpkg
    gpkg_dir = subset_dir

    # Get list of catchments from gpkg divides layer using geopandas
    #file = 'Gage_6700000.gpkg'
    gpkg_file = "Gage_" + gage_id + ".gpkg"
    gpkg_file = os.path.join(gpkg_dir, gpkg_file)
    divides_layer = gpd.read_file(gpkg_file, layer="divides")
    catchments = divides_layer["divide_id"].tolist()
    areas = divides_layer["areasqkm"].tolist()

    data = gpd.read_file(gpkg_file, layer="divides")
    catch_dict = {}
    for index, row in data.iterrows():
        #print(row['divide_id'], row['areasqkm'])
        catch_dict[str(catchments[index])] = {"areasqkm": str(areas[index])}

    response = create_snow17_input(gage_id, catch_dict, attr_file, output_dir, module_metadata_list)
    logger.info("snow_17::snow17_ipe:returning response as " + str(response))
    return response


def create_snow17_input(gage_id, catch_dict, attr_file, snow17_output_dir: str, module_metadata_list):

    if not os.path.exists(snow17_output_dir):
        os.makedirs(snow17_output_dir, exist_ok=True)

    try:
        attr = pq.read_table(attr_file)
    except:
        error_str = 'Error opening ' + attr_file
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)

    # filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catch_dict.keys())]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    # Read hydrofabric attribute file (THESE ARE NOT USED BY SNOW17)
    #dfa = pd.read_parquet(attr_file)
    #dfa.set_index("divide_id", inplace=True)

    response = []

    #for key in catch_dict.keys(): LOOP CATCH_IDs HERE (from filtered dataframe)!!
    for index, row in filtered.iterrows():
        catchment_id = row['divide_id']
        param_list = ['hru_id ' + str(catchment_id),
                      'hru_area ' + str(catch_dict[str(catchment_id)]['areasqkm']),
                      'latitude ' + str(row['Y']),
                      'elev ' + str(row['elevation_mean']), #elevation_mean[1]
                      'scf 2.15177',
                      'mfmax 0.930472',
                      'mfmin 0.137',
                      'uadj 0.003103',
                      'si 1515.00',
                      'pxtemp 0.713424',
                      'nmf 0.150',
                      'tipm 0.200',
                      'mbase 0.000',
                      'plwhc 0.030',
                      'daygm 0.300',
                      'adc1 0.050',
                      'adc2 0.090',
                      'adc3 0.160',
                      'adc4 0.310',
                      'adc5 0.540',
                      'adc6 0.740',
                      'adc7 0.840',
                      'adc8 0.890',
                      'adc9 0.930',
                      'adc10 0.970',
                      'adc11 1.000']

        # NOTE: use record 0 from module_metadata_list as a template, then append a deep copy to response at end of
        #       this method and return
        module_metadata_rec = set_ipe_json_values(param_list, module_metadata_list)

        input_file = os.path.join(snow17_output_dir, 'snow17-init-' + str(catchment_id) + '.namelist.input')
        param_file = os.path.join(snow17_output_dir, 'snow17_params-' + str(catchment_id) + '.HHWM8.txt')

        with open(param_file, "w") as f:
            f.writelines('\n'.join(param_list))

        input_list = ['&SNOW17_CONTROL',
                '! === run control file for snow17bmi v. 1.x ===',
                '',
                '! -- basin config and path information',
                'main_id             = "' + str(catchment_id) + '"     ! basin label or gage id',
                'n_hrus              = 1            ! number of sub-areas in model',
                'forcing_root        = "extern/snow17/test_cases/ex1/input/forcing/forcing.snow17bmi."',
                'output_root         = "data/output/output.snow17bmi."',
                'snow17_param_file   = "' + param_file + '"',
                'output_hrus         = 1            ! output HRU results? (1=yes; 0=no)',
                '',
                '! -- run period information',
                'start_datehr        = 2017120101   ! start date time, backward looking (check)',
                'end_datehr          = 2017120123   ! end date time',
                'model_timestep      = 3600        ! in seconds (86400 seconds = 1 day)',
                '',
                '! -- state start/write flags and files',
                'warm_start_run      = 0  ! is this run started from a state file?  (no=0 yes=1)',
                "write_states        = 0  ! write restart/state files for 'warm_start' runs (no=0 yes=1)",
                '',
                '! -- filenames only needed if warm_start_run = 1',
                'snow_state_in_root  = "data/state/snow17_states."  ! input state filename root',
                '',
                '! -- filenames only needed if write_states = 1',
                'snow_state_out_root = "data/state/snow17_states."  ! output states filename root',
                '/',
                ''
                ]
        with open(input_file, "w") as f:
            f.writelines('\n'.join(input_list))

        # get config info
        config = get_config()
        output_dir = config['output_dir']
        s3url = config['s3url']
        s3bucket = config['s3bucket']
        s3prefix = config['s3prefix']

        # put all files to write to S3 in a list
        s3_file_list = [os.path.basename(input_file), os.path.basename(param_file)]

        for idx in range(len(s3_file_list)):
            # Write geopackage to s3 bucket
            if s3prefix:
                subset_s3prefix = s3prefix + "/" + gage_id + "/SNOW17"
            else:
                subset_s3prefix = gage_id + "/SNOW17"

            #subset_dir_full = os.path.join(output_dir, s3_file_list[idx])

            write_minio(output_dir, s3_file_list[idx], s3url, s3bucket, subset_s3prefix)
            #uri = build_uri(s3bucket, s3prefix, s3_file_list[idx])  #do not use filename, only dir
            uri = build_uri(s3bucket, subset_s3prefix)
            status_str = "Written to S3 bucket: " + str(uri)
            print(status_str)

            module_metadata_rec['parameter_file']['uri'] = uri
            logger.info(status_str)

        deep_copy_ipe_dict = copy.deepcopy(module_metadata_rec)
        response.append(deep_copy_ipe_dict)
        logger.info("snow17:create_snow17_input:appended deep copy dict to response " + str(deep_copy_ipe_dict))

    return response


def set_ipe_json_values(param_list, module_metadata_rec)-> dict:
    # convert param list to dict to make it searchable by key
    param_list_dict = {}
    for idx in range(len(param_list)):
        key_value_split = str.split(param_list[idx], ' ')
        param_list_dict[key_value_split[0]] = key_value_split[1]

    for idx in range(len(module_metadata_rec['calibrate_parameters'])):
        key_name = module_metadata_rec['calibrate_parameters'][idx]['name']
        module_metadata_rec['calibrate_parameters'][idx]['initial_value'] = param_list_dict[key_name]

    logger.info("snow17::set_ipe_json_values: set the ipe initial values " + str(module_metadata_rec))
    return module_metadata_rec


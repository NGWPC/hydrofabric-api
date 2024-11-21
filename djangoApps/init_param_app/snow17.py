import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import math

from .util.enums import FileTypeEnum
from .util.utilities import *
from .util import utilities


#setup logging
logger = logging.getLogger(__name__)


def snow17_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    '''
    Build initial parameter estimates (IPE) for Sac-SMA

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    source (str):  Gage source, e.g., USGS
    domain (str):  Gage domain, e.g., CONUS
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    gpkg_file (str):  Path and filename of geopackage file 
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    gage_file_mgmt (object):  gage file management object
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # setup input data dir
    config = get_config()
    input_dir = config['input_dir']
    
    #Create empty list for collecting config files
    filename_list = []

    module = 'Snow17'

    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
            area = divides_layer[['divide_id','areasqkm']]
        except:
            # TODO: Replace 'except' with proper catch
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error = error_str) 
            print(error_str)
            logger.error(error_str)
            return error

        try:
            # get path to Hydrofabric VPUID parquet files and read table
            attr_abs_filename = get_hydrofabric_input_attr_file()
            attr_tbl = pq.read_table(attr_abs_filename)

            # drop any nulls and filter on divide_ids to reduce size
            attr_tbl = attr_tbl.drop_null()
            attr_df = pa.Table.to_pandas(attr_tbl)
            att_df_filtered = attr_df[attr_df['divide_id'].isin(catchments)]
        except:
            error_str = f"Error reading loading attr_df from {attr_abs_filename}"
            error = dict(error=error_str)
            print(error_str)
            logger.error(error_str)
            return error

    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        print(error_str)
        logger.error(error_str)
        return error
 
    #Read parameters from ascii created CSV file into a dataframe and filter on divide ids in geopackage.
    parameters_df = pd.read_csv(f'{input_dir}/snow17_params.csv')
    filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

    #Join parameters from csv, area, and attribute file into single dataframe using divide_id as index
    df_all = filtered_parameters.join(area.set_index('divide_id'), on='divide_id').join(att_df_filtered.set_index('divide_id'), on='divide_id')

    # set default values for vars (eventually this will be retrieved from db)
    mfmax = 1.00
    mfmin = 0.20
    uadj = 0.05

    #Loop through divide IDs, get values, and set NA values (represented as NaNs in Pandas) to default values in param_list
    #Create parameter config file.
    for index, row in df_all.iterrows():

        hru_id = row['divide_id']  # need this for filenames as well as parameters
        #hru_area = row['areasqkm']
        if not math.isnan(row['MFMIN']):  mfmin = row['MFMIN']
        if not math.isnan(row['MFMAX']): mfmax = row['MFMAX']
        if not math.isnan(row['UADJ']):  uadj = row['UADJ']

        param_list = ['hru_id ' + str(row['divide_id']),
                      'hru_area ' + str(row['areasqkm']),
                      'latitude ' + str(row['Y']),
                      'elev ' + str(row['elevation_mean']),  # elevation_mean[1]
                      'scf 1.100',
                      'mfmax ' + str(mfmax),
                      'mfmin ' + str(mfmin),
                      'uadj ' + str(uadj),
                      'si 500.00',
                      'pxtemp 1.000',
                      'nmf 0.150',
                      'tipm 0.100',
                      'mbase 0.000',
                      'plwhc 0.030',
                      'daygm 0.000',
                      'adc1 0.050',
                      'adc2 0.100',
                      'adc3 0.200',
                      'adc4 0.300',
                      'adc5 0.400',
                      'adc6 0.500',
                      'adc7 0.600',
                      'adc8 0.700',
                      'adc9 0.800',
                      'adc10 0.900',
                      'adc11 1.000']


        cfg_filename = f'snow17_params-{hru_id}.txt'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
            outfile.writelines('\n'.join(param_list))
            outfile.write("\n")

        # Create Snow17 control file for each catchment
        input_list = ['&SNOW17_CONTROL',
                      '! === run control file for snow17bmi v. 1.x ===',
                      '',
                      '! -- basin config and path information',
                      'main_id             = "' + str(hru_id) + '"     ! basin label or gage id',
                      'n_hrus              = 1            ! number of sub-areas in model',
                      'forcing_root        = "extern/snow17/test_cases/ex1/input/forcing/forcing.snow17bmi."',
                      'output_root         = "data/output/output.snow17bmi."',
                      'snow17_param_file   = "' + cfg_filename.rsplit('/')[-1] + '"',
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

        ctl_filename = f'{hru_id}.namelist.input'
        filename_list.append(ctl_filename)
        cfg_filename_path = os.path.join(subset_dir, ctl_filename)
        with open(cfg_filename_path, 'w') as outfile:
            outfile.writelines('\n'.join(input_list))
            outfile.write("\n")

    
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    #write s3 location and ipe values from one of the parameter config files to output json
    file = os.path.join(subset_dir, cfg_filename)
    with open(file, 'r') as file:
        lines = file.readlines()

    cfg_file_ipes = {}

    for line in lines:
        key, value = line.strip().split(' ')
        cfg_file_ipes[key.strip()] = value.strip()

    for x in range(len(module_metadata["calibrate_parameters"])):
        module_metadata["calibrate_parameters"][x]["initial_value"] = cfg_file_ipes[module_metadata["calibrate_parameters"][x]["name"]]
        
    module_metadata["parameter_file"]["uri"] = uri
    return module_metadata
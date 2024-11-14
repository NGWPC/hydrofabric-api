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


def sac_sma_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
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

    module = 'Sac-SMA'

    #Set default values for parameters per InitialParameterValueSources.xlsx
    uztwm = 75.0
    uzfwm = 30.0
    lztwm = 150.0
    lzfpm = 300.0
    lzfsm = 150.0
    adimp = 0.0
    uzk = 0.3
    lzpk = 0.01
    lzsk = 0.1 
    zperc = 100.0
    rexp = 2.0
    pctim = 0.0
    pfree = 0.1
    riva = 0.0
    side = 0.0
    rserv = 0.3
  
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
    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        print(error_str)
        logger.error(error_str)
        return error
 
    #Read parameters from CSV file into dataframe and filter on divide ids in geopackage. 
    parameters_df = pd.read_csv(f'{input_dir}/sac_sma_params.csv')
    filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

    #Join parameters from csv and area into single dataframe.
    df_all = filtered_parameters.join(area.set_index('divide_id'), on='divide_id')
    
    #Loop through divide IDs, get values and set NA values (represented as NaNs in Pandas) to default values.
    #Create parameter config file.
    for index, row in df_all.iterrows():

        hru_id = row['divide_id']
        hru_area = row['areasqkm']
        if not math.isnan(row['UZTWM']):  uztwm = row['UZTWM']
        if not math.isnan(row['UZFWM']) : uzfwm = row['UZFWM']
        if not math.isnan(row['LZTWM']):  lztwm = row['LZTWM']
        if not math.isnan(row['LZFPM']): lzfpm = row['LZFPM']
        if not math.isnan(row['LZFSM']): lzfsm = row['LZFSM']
        if not math.isnan(row['UZK']): uzk = row['UZK']  
        if not math.isnan(row['LZPK']): lzpk = row['LZPK']
        if not math.isnan(row['LZSK']): lzsk = row['LZSK'] 
        if not math.isnan(row['ZPERC']): zperc = row['ZPERC']
        if not math.isnan(row['REXP']): rexp = row['REXP']
        if not math.isnan(row['PFREE']): pfree = row['PFREE']

        param_list = ['hru_id ' + hru_id,
                      'hru_area ' + str(hru_area),
                      'uztwm ' + str(uztwm),
                      'uzfwm ' + str(uzfwm),
                      'lztwm ' + str(lztwm),
                      'lzfpm ' + str(lzfpm),
                      'lzfsm ' + str(lzfsm),
                       'adimp ' + str(adimp),
                       'uzk ' + str(uzk),
                       'lzpk ' + str(lzpk),
                       'lzsk ' + str(lzsk),
                       'zperc ' + str(zperc),
                       'rexp ' + str(rexp),
                       'pctim ' + str(pctim),
                       'pfree ' + str(pfree),
                       'riva ' + str(riva),
                       'side ' + str(side),
                       'rserv '+ str(rserv)
                       ]
        
        cfg_filename = f'sac_params_{hru_id}.txt'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(param_list))
                            outfile.write("\n")

        # Create Sac-SMA control file for each catchment
        input_list = ['&SAC_CONTROL',
                    '! === run control file for sac17bmi v. 1.x ===',
                    '',
                    '! -- basin config and path information',
                    'main_id             = "' + str(hru_id) + '"     ! basin label or gage id',
                    'n_hrus              = 1                   ! number of sub-areas in model',
                    'forcing_root        = ""',
                    'output_root         = ""',
                    'sac_param_file      = "' + cfg_filename.rsplit('/')[-1] + '"',
                    'output_hrus         = 0            ! output HRU results? (1=yes; 0=no)',
                    '',
                    '! -- run period information',
                    'start_datehr        = 2015120112   ! start date time, backward looking (check)',
                    'end_datehr          = 2015123012   ! end date time',
                    'model_timestep      = 3600        ! in seconds (86400 seconds = 1 day)',
                    '',
                    '! -- state start/write flags and files',
                    'warm_start_run        = 0  ! is this run started from a start file? (no=0 yes=1)',
                    'write_states          = 0  ! write the restart/state files for "warm_start" runs (no=0 yes=1)',
                    '',
                    '! -- filenames only needed if warm_start_run = 1',
                    'sac_state_in_root   = "data/state/sac_states."  ! input state filename root',
                    '',
                    '! -- filenames only needed if write_states = 1',
                    'sac_state_out_root = "data/state/sac_states."  ! output states filename root',
                    '/',
                    ''
                    ]
        ctl_filename = f'sac-init-{hru_id}.namelist.input'
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
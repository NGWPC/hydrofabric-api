'''
Subsets the hydrofabric by gage ID and creates BMI config files with initial parameters.

Inputs:
    gage_id:  a gage_id, e.g., "01123000"
           Look at using the Python rpy2 package for R function calls as the Rscript method is not the best. 

    output_dir: Absolute path to directory where input and output data will be stored.  The directory structure will
    change as the Hydrofabric and NGEN design is refined.  

Outputs:
    Outputs are written to output_dir:  Hydrofabric Subset files (Gage-xxxxxxxx.gpkg) and BMI config files (e.g., cat-10617_bmi_config.ini)
    in CFE-S subdirectory.   
'''
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

from .util.enums import FileTypeEnum
from .util.utilities import *
from .util import utilities


#setup logging
logger = logging.getLogger(__name__)


def sac_sma_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    # setup output dir
    config = get_config()
    #output_dir = config['output_temp_dir']
    #hydrofabric_dir = config['hydrofabric_dir']
    #hydrofabric_version = config['hydrofabric_version']
    #hydrofabric_type = config['hydrofabric_type']
    input_dir = config['input_dir']

    filename_list = []
    filename_list_ctl = []

    module = 'Sac-SMA'

    # get attrib file
    attr_file = get_hydrofabric_input_attr_file()

    # setup output dir
    # first save the top level dir for the gpkg
    #gpkg_dir = subset_dir
  
    #gpkg_file = "Gage_" + gage_id.lstrip("0") + ".gpkg"
    #gpkg_file = os.path.join(gpkg_dir, gpkg_file)
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
            area = divides_layer[['divide_id','areasqkm']]
            print(area)
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

    try:
        attr = pq.read_table(attr_file)
    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + attr_file
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)

    filtered_attr = attr_df[attr_df['divide_id'].isin(catchments)]

    parameters_df = pd.read_csv(f'{input_dir}/sac_sma_params.csv', dtype=str)

    filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

    df_all = filtered_attr.join(filtered_parameters.set_index('divide_id'), on='divide_id')

    df_all = df_all.join(area.set_index('divide_id'), on='divide_id')

    for index, row in df_all.iterrows():

        hru_id = str(row['divide_id'])
        hru_area = str(row['areasqkm'])
        uztwm = row['UZTWM']
        uzfwm = row['UZFWM']
        lztwm = row['LZTWM']
        lzfpm = row['LZFPM']
        lzfsm = row['LZFSM']
        adimp = '0.0'
        uzk = row['UZK']
        lzpk = row['LZPK']
        lzsk = row['LZSK']  
        zperc = row['ZPERC']
        rexp = row['REXP'] 
        pctim = str(row['impervious_mean'])
        pfree = row['PFREE']
        riva = '0.0100'
        side = '0.0000'
        rserv = '0.3000'
        param_list = ['hru_id ' + hru_id,
                      'hru_area ' + hru_area,
                      'uztwm ' + uztwm,
                      'uzfwm ' + uzfwm,
                      'lztwm ' + lztwm,
                      'lzfpm ' + lzfpm,
                      'lzfsm ' + lzfsm,
                       'adimp ' + adimp,
                       'uzk ' + uzk,
                       'lzpk ' + lzpk,
                       'lzsk ' + lzsk,
                       'zperc ' + zperc,
                       'rexp ' + rexp,
                       'pctim ' + pctim,
                       'pfree ' + pfree,
                       'riva ' + riva,
                       'side ' + side,
                       'rserv '+ rserv
                       ]
        
        cfg_filename = f'sac_sma_params-{hru_id}.HHWM8.txt'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(param_list))
                            outfile.write("\n")
    
        input_list = ['&SAC_CONTROL',
                    '! === run control file for sac17bmi v. 1.x ===',
                    '',
                    '! -- basin config and path information',
                    'main_id             = "' + str(hru_id) + '"     ! basin label or gage id',
                    'n_hrus              = 1            ! number of sub-areas in model',
                    'forcing_root        = "extern/sac-sma/sac-sma/test_cases/ex1/input/forcing/forcing.sacbmi."',
                    'output_root         = "data/output/output.sacbmi."',
                    'sac_param_file      = "' + cfg_filename.rsplit('/')[-1] + '"',
                    'output_hrus         = 0            ! output HRU results? (1=yes; 0=no)',
                    '',
                    '! -- run period information',
                    'start_datehr        = 2015120112   ! start date time, backward looking (check)',
                    'end_datehr          = 2015123012   ! end date time',
                    'model_timestep      = 3600        ! in seconds (86400 seconds = 1 day)',
                    '',
                    '! -- state start/write flags and files',
                    'warm_start_run',
                    '! -- filenames only needed if warm_start_run = 1',
                    'sac_state_in_root   = "data/state/sac_states."  ! input state filename root',
                    '',
                    '! -- filenames only needed if write_states = 1',
                    'sac_state_out_root = "data/state/sac_states."  ! output states filename root',
                    '/',
                    ''
                    ]
        ctl_filename = f'sac_sma-init-{hru_id}.namelist.input'
        filename_list_ctl.append(ctl_filename)
        cfg_filename_path = os.path.join(subset_dir, ctl_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(input_list))
                            outfile.write("\n")

    #filename_list = utilities.get_subset_dir_file_names(subset_dir)
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list_ctl, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    #write s3 location and ipe values from one of the subset_dir files to output json
    file = os.path.join(subset_dir, filename_list[0])
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
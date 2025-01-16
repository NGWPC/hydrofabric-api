import subprocess
import json
import pandas as pd

from .models import CfeParams
from .util import utilities
from .util.enums import FileTypeEnum
from .util.utilities import *
from .hf_attributes import *


# Setup logging
logger = logging.getLogger(__name__)


def cfe_ipe(module, version, gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    ''' 
    Build initial parameter estimates (IPE) for CFE-S and CFE-X 

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made. 
    module (str): Module name to specify CFE-S or CFE-X
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''
    cfe_x_params = ('a_Xinanjiang_inflection_point_parameter', 'b_Xinanjiang_shape_parameter', 'x_Xinanjiang_shape_parameter', 'urban_decimal_fraction')

    if module == 'CFE-S':
        scheme = 'Schaake'
    elif module == 'CFE-X':
        scheme = 'Xinanjiang'

    attr_name_key = f"{version}_name"

    # Get config file for paths
    config = get_config()
    input_dir = config['input_dir']

    filename_list = []
        
    from_attr = CfeParams.objects.filter(source_file='attr').values('name', 'nwm_name', 'default_value')
    consts = CfeParams.objects.filter(source_file='const').values('name', 'nwm_name', 'default_value')

    divide_attr = get_hydrofabric_attributes(gpkg_file, version)

    catchments = divide_attr["divide_id"].tolist()

    parameters_df = pd.read_csv(f'{input_dir}/cfe_x.csv')
    filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

    #Join parameters from csv, area, and attribute file into single dataframe using divide_id as index
    df_all = filtered_parameters.join(divide_attr.set_index('divide_id'), on='divide_id')

    if(module == 'CFE-S'):
        for param in cfe_x_params:
           df_all.drop(param, axis=1, inplace=True)
           
    #print(df_all)

    for index, divide in df_all.iterrows():

        divide_id = divide['divide_id']
        cfg_filename = f'{divide_id}_bmi_config_cfe.txt'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
    
        with open(cfg_filename_path, 'w') as outfile:
            #write non-parameter items            
            outfile.write('forcing_file=BMI\n')
            outfile.write('verbosity=0\n')
            outfile.write(f'surface_partitioning_scheme={scheme}\n')
            outfile.write('num_timesteps=0\n')

            #write items from divide attributes and CFE-X csv file
            for param in from_attr:
                param_name = param['name']
                attr_name = json.loads(param['nwm_name'])[attr_name_key]
                attr_value = divide[attr_name]
                cfg_line = f"{param_name}={attr_value}\n"
                outfile.write(cfg_line)

            #write constants
            for param in consts:
                param_name = param['name']
                attr_value = param['default_value']
                cfg_line = f"{param_name}={attr_value}\n"
                outfile.write(cfg_line)
    
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)
    
    #write s3 location and ipe values from one of the subset_dir files to output json
    file = os.path.join(subset_dir, filename_list[0])
    with open(file, 'r') as file:
        lines = file.readlines()

    cfg_file_ipes = {}

    for line in lines:
        key, value = line.strip().split('=')
        cfg_file_ipes[key.strip()] = value.strip()

    for x in range(len(module_metadata["calibrate_parameters"])):
        module_metadata["calibrate_parameters"][x]["initial_value"] = cfg_file_ipes[module_metadata["calibrate_parameters"][x]["name"]]
        
    module_metadata["parameter_file"]["uri"] = uri
    return module_metadata

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
    
    # Get config file for paths
    config = get_config()
    input_dir = config['input_dir']
    
    #List of parameters specific to CFE-X
    cfe_x_params = ('a_Xinanjiang_inflection_point_parameter', 'b_Xinanjiang_shape_parameter', 'x_Xinanjiang_shape_parameter', 'urban_decimal_fraction')

    #Set surface partitioning scheme based on module name
    if module == 'CFE-S':
        scheme = 'Schaake'
    elif module == 'CFE-X':
        scheme = 'Xinanjiang'

    #Set CFE-X parameter CSV filename based on hydrofabric version
    csv_path_filename = f'{input_dir}/CFE-X_params_{version}.csv'

    #Create empty list to store BMI config file names
    filename_list = []

    #Get all parameters from the database    
    from_attr = CfeParams.objects.filter(source_file='attr').values('name', 'nwm_name', 'default_value')
    consts = CfeParams.objects.filter(source_file='const').values('name', 'nwm_name', 'default_value')

    #Get divide attributes from geopackage
    divide_attr = get_hydrofabric_attributes(gpkg_file, version)
    if('error' in divide_attr): return divide_attr
    catchments = divide_attr["divide_id"].tolist()

    #Read CSV file for CFE-X parameters
    if module == 'CFE-X':
        try:
            parameters_df = pd.read_csv(csv_path_filename)
        except FileNotFoundError:
            error_str = f'CFE-X Parameters CSV file not found: {csv_path_filename}'
            error = {'error': error_str}
            logger.error(error_str)
            return error
        except Exception as e:
            error_str = f'CFE-X Parameters CSV Pandas read error: {csv_path_filename}'
            error = {'error': error_str}
            logger.error(error_str)
            return error   
        
        #Make sure that catchements exist in CSV file
        filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]
        if filtered_parameters.empty:
            error_str = f'Catchments in geopackage not found in CFE-X CSV file'
            error = {'error': error_str}
            logger.error(error_str)
            return error
        
        #Make sure that there are a matching number of catchements
        if(len(catchments) != len(filtered_parameters.index)):
            error_str = f'Number of matching catchments found in CFE-X CSV file does not match number of catchments in geopackage'
            error = {'error': error_str}
            logger.error(error_str)
            return error
         
        #Join parameters from csv and attribute file into single dataframe using divide_id as index
        df_all = filtered_parameters.join(divide_attr.set_index('divide_id'), on='divide_id')
    else:
        df_all = divide_attr

    #Loop through catchments and create BMI config files
    for index, divide in df_all.iterrows():

        #empty list for accumulate parameter strings
        params_out = []
        
        #get non-parameter items            
        params_out.append('forcing_file=BMI')
        params_out.append('verbosity=0')
        params_out.append(f'surface_partitioning_scheme={scheme}')
        params_out.append('num_timesteps=0')

        #get items from divide attributes and CFE-X csv file
        for param in from_attr:
            param_name = param['name']
            if module == 'CFE-S' and param_name in cfe_x_params:
                continue
            attr_value = divide.filter(regex=param['nwm_name']).values
            #If attribute has more than 1 layer, use the first.
            if(len(attr_value > 1)):  attr_value = attr_value[0]
            cfg_line = f"{param_name}={attr_value}"
            params_out.append(cfg_line)

        #get constants
        for param in consts:
            param_name = param['name']
            if module == 'CFE-S' and param_name in cfe_x_params:
                continue
            attr_value = param['default_value']
            cfg_line = f"{param_name}={attr_value}"
            params_out.append(cfg_line)

        #join all list items into single string with line breaks
        params_out_all = '\n'.join(params_out)
        
        #write BMI cfg file for catchment to temp dir
        divide_id = divide['divide_id']
        cfg_filename = f'{divide_id}_bmi_config_cfe.txt'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
            outfile.write(params_out_all)
            
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)
    
    #Put data for last catchment into a dictionary and fill in the inital parameter values in the output JSON
    cfg_file_ipes = {}
    for line in params_out:
        key, value = line.strip().split('=')
        cfg_file_ipes[key.strip()] = value.strip()

    for x in range(len(module_metadata["calibrate_parameters"])):
        module_metadata["calibrate_parameters"][x]["initial_value"] = cfg_file_ipes[module_metadata["calibrate_parameters"][x]["name"]]
        
    module_metadata["parameter_file"]["uri"] = uri
    return module_metadata
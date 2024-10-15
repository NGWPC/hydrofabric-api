import subprocess
import json

from .models import CfeParams
from .util import utilities
from .util.enums import FileTypeEnum
from .util.utilities import *

# Setup logging
logger = logging.getLogger(__name__)


def cfe_ipe(module, gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
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
    cfe_s = 'CFE-S'
    # Get config file for paths
    config = get_config()
    input_dir = config['input_dir'] 
    sys_root = os.path.dirname(settings.BASE_DIR)
    app_path = os.path.join(settings.BASE_DIR, 'init_param_app')
    cfe_x_json = 'CFE-X.json'
    cfe_s_json = 'CFE-S.json'
    r_code_file = f"{sys_root}/R/run_create_cfe_init_bmi_config.R"

    #Build input arguments for config file R script
    #create string containing R c (combine) function and gage IDs
    gage_id_string = "'"+gage_id+"'"

    # TODO: This will be replaced with a call to the database when connectivity is available in the container
    #cfe_json = os.path.join(app_path, cfe_x_json) if module == 'CFE-X' else os.path.join(app_path, cfe_s_json)
 
    #try:
    #    import_json = open(cfe_json)
    #    parameters = json.load(import_json)
    #except Exception as e:
    #    logger.error(f"Error reading JSON document {cfe_json} Error is: {e}")
    #    error_str = f"CFE error reading JSON document {cfe_json}"
    #    error = dict(error = error_str)
    #    return error

    # get the cfe parameter from the DB
    queryset = CfeParams.objects.values('name', 'nwm_name', 'default_value')

    # Create lists for passing CFE parameter names and constant values to R code
    cfe_parameters_nwm_name = []
    cfe_parameters_cfe_name =  []
    cfe_parameters_const_name = []
    cfe_parameters_const_value = []

    for row in queryset:
        # We do not need to check for CFE-S or CFE-X on this check because the four
        # parmeter in cfe_x_params do not have default values
        if row['default_value'] is not None:
            cfe_parameters_const_name.append("'" + row['name'] + "'") 
            cfe_parameters_const_value.append("'"  + row['default_value'] + "'")
            
        # We need to check for CFE-S or CFE-X on this check because the three of the four
        # parmeter in cfe_x_params have an nwm name
        if row['nwm_name'] is not None:
            if module == cfe_s and row['name'] in cfe_x_params:
                # skip this row it is a CFE-X parameter
                continue
            else:
                cfe_parameters_nwm_name.append("'" + row["nwm_name"] + "'")
                cfe_parameters_cfe_name.append("'"  + row['name'] + "'")

    cfe_parameters_const_name = '"c(' + ",".join(cfe_parameters_const_name) + ')"' 
    cfe_parameters_const_value = '"c(' + ",".join(cfe_parameters_const_value) + ')"' 
    cfe_parameters_nwm_name = '"c(' + ",".join(cfe_parameters_nwm_name) + ')"' 
    cfe_parameters_cfe_name = '"c(' + ",".join(cfe_parameters_cfe_name) + ')"' 

    #TODO make the Rscript string below a constant
    run_command = [f"/usr/bin/Rscript {r_code_file}", 
    gage_id_string,
    input_dir,
    subset_dir,
    cfe_parameters_const_name,
    cfe_parameters_const_value,
    cfe_parameters_nwm_name,
    cfe_parameters_cfe_name,
    module,
    sys_root,
    gpkg_file]

    run_command_string = " ".join(run_command)

    status_str = "Running CFE IPE R code"
    logger.info(status_str)

    try:
        subprocess.call(run_command_string, shell=True) 
    except:
        error_str = "CFE IPE R code failure"
        error = dict(error = error_str) 
        logger.error(error_str)
        return error
    # Since the R code writes the files to the subset_dir get list of files to send S3
    filename_list = utilities.get_subset_dir_file_names(subset_dir)
    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
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

    for row in range(len(module_metadata[0]["calibrate_parameters"])):
        module_metadata[0]["calibrate_parameters"][row]["initial_value"] = cfg_file_ipes[module_metadata[0]["calibrate_parameters"][row]["name"]]
        
    module_metadata[0]["parameter_file"]["uri"] = uri
    return module_metadata

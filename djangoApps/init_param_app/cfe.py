import os
import subprocess
import json
from pathlib import Path
import logging

from .utilities import *

def cfe_ipe(gage_id, subset_dir, module, module_metadata):
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

    # Setup logging
    logger = logging.getLogger(__name__)

    # Get config file for paths
    config = get_config()
    input_dir = config['input_dir'] 
    s3url = config['s3url']
    s3bucket = config['s3bucket']
    s3prefix = config['s3prefix']

    #Build input arguments for config file R script
    #create string containing R c (combine) function and gage IDs
    gage_id_string = 'c(' + gage_id + ')'
    gage_id_string = "'"+gage_id_string+"'"

    #this will be replaced with a call to the database when connectivity is available in the container
    if module == 'CFE-X':
        importjson = open('init_param_app/CFE-X.json')
    if module == 'CFE-S':
        importjson = open('init_param_app/CFE-S.json')
    parameters = json.load(importjson)

    # Create lists for passing CFE parameter names and constant values to R code
    cfe_parameters_nwm_name = []
    cfe_parameters_cfe_name =  []
    cfe_parameters_const_name = []
    cfe_parameters_const_value = []

    for x in parameters:
        if not x['default_value'] is None:
            cfe_parameters_const_name.append("'" + x['name'] + "'") 
            cfe_parameters_const_value.append("'"  + x['default_value'] + "'")
            
        if not x['nwm_name'] is None:
            cfe_parameters_nwm_name.append("'" + x["nwm_name"] + "'")
            cfe_parameters_cfe_name.append("'"  + x['name'] + "'")

    cfe_parameters_const_name = '"c(' + ",".join(cfe_parameters_const_name) + ')"' 
    cfe_parameters_const_value = '"c(' + ",".join(cfe_parameters_const_value) + ')"' 
    cfe_parameters_nwm_name = '"c(' + ",".join(cfe_parameters_nwm_name) + ')"' 
    cfe_parameters_cfe_name = '"c(' + ",".join(cfe_parameters_cfe_name) + ')"' 

    run_command = ["/usr/bin/Rscript ../R/run_create_cfe_init_bmi_config.R",
    gage_id_string,
    input_dir,
    subset_dir,
    cfe_parameters_const_name,
    cfe_parameters_const_value,
    cfe_parameters_nwm_name,
    cfe_parameters_cfe_name,
    module]

    run_command_string = " ".join(run_command)

    status_str = "Running CFE IPE R code"
    print(status_str)
    logger.info(status_str)

    try:
        subprocess.call(run_command_string, shell=True) 
    except:
        error_str = "CFE IPE R code failure"
        error = dict(error = error_str) 
        print(error_str)
        logger.error(error_str)
        return error

    # CFE IPE R code uses Gage_6719505 format
    gage_id_full = "Gage_" + gage_id.lstrip("0")
    
    if s3prefix:
        s3prefix = s3prefix + "/" + gage_id + "/" + module
    else:
        s3prefix = gage_id + "/" + module        

    files = Path(os.path.join(subset_dir, module, gage_id_full)).glob('*.ini')
    for file in files:
        print("writing: " + str(file) + " to s3")
        file_name = os.path.basename(file)
        write_minio(subset_dir + "/" + module + "/" + gage_id_full, file_name, s3url, s3bucket, s3prefix)

    uri = build_uri(s3bucket, s3prefix)
    status_str = "Config files written to:  " + uri
    print(status_str)
    logger.info(status_str)

    #write s3 location and ipe values to output json
    with open(file, 'r') as file:
        lines = file.readlines()

    cfg_file_ipes = {}

    for line in lines:
        key, value = line.strip().split('=')
        cfg_file_ipes[key.strip()] = value.strip()

    #this will be replaced with a call to the database when the connection is possible in the container
    if module == 'CFE-S':
        importjson = open('init_param_app/calibratable_cfe-s.json')
    if module == 'CFE-X':
        importjson = open('init_param_app/calibratable_cfe-x.json')
    output = json.load(importjson)

    for x in range(len(module_metadata[0]["calibrate_parameters"])):
        module_metadata[0]["calibrate_parameters"][x]["initial_value"] = cfg_file_ipes[module_metadata[0]["calibrate_parameters"][x]["name"]]
        
    uri = build_uri(s3bucket, s3prefix)
    module_metadata[0]["parameter_file"]["url"] = uri
    return module_metadata
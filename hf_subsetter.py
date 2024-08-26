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

import os
import subprocess
import re
import sys
from datetime import datetime
import tarfile
from minio import Minio
import json
from pathlib import Path
from pprint import pprint
import yaml
import logging


def get_geopackage(gage_id):

        #setup logging
        logger = logging.getLogger(__name__)

        #get paths, etc from config.yml
        config = get_config()
        output_dir = config['output_dir'] 
        hydrofabric_dir = config['hydrofabric_dir'] 
        s3url = config['s3url']
        s3bucket = config['s3bucket'] 
        s3prefix = config['s3prefix'] 

        #validate that gage input is in the proper format (8 digits) 
        x = bool(re.search("\d{8}", gage_id))
        if not x:
            
            error = dict(error = "Gage ID is not valid")
            if not run_ipe:  error = json.dumps(error)
            return error

	#setup paths and geopackage name for hydrofabric subsetter R function call

        #append "Gages-" to id per hydrofabric naming convention for hl_uri
        subsetter_gage_id = "Gages-"+gage_id
        status_str = "Create subsetted geopackage file for: " + subsetter_gage_id
        print(status_str)
        logger.info(status_str)
 
        #strip leading zero of gage ID for gpkg filename
        gpkg_filename = "Gage_"+gage_id.lstrip("0") + ".gpkg"

        #create temp directory and s3 prefix for this particular subset
        subset_s3prefix = s3prefix + "/" + gage_id 
        subset_dir_full = os.path.join(output_dir, gage_id)
        if not os.path.exists(subset_dir_full):
            os.mkdir(subset_dir_full)

        status_str = "Calling HF Subsetter R code"
        print(status_str)
        logger.info(status_str)

        #Call R code for subsetter
        run_command = ["/usr/bin/Rscript ../run_subsetter.r", subsetter_gage_id, subset_dir_full, gpkg_filename, hydrofabric_dir]
        run_command_string = " ".join(run_command)
        
        try:
            subprocess.call(run_command_string, shell=True)
        except:
            error_str = "Hydrofabric Subsetter R code failure"
            error = dict(error = error_str)
            if not run_ipe:  error = json.dumps(error)
            print(error_str)
            logger.error(error_str)
            return error

        # Write geopackage to s3 bucket
        write_minio(subset_dir_full, gpkg_filename, s3url, s3bucket, subset_s3prefix)
        uri = build_uri(s3bucket, subset_s3prefix, gpkg_filename)
        status_str = "Written to S3 bucket: " + uri
        print(status_str)
        logger.info(status_str)

        # Build output JSON
        currentDateAndTime = datetime.now()
        currentTime = currentDateAndTime.strftime("%Y%m%d_%H%M%S")
        geopackage_dict = dict(creationDate = currentTime, uri = uri)
        geopackage_json = json.dumps(geopackage_dict)
        return geopackage_json

def get_ipe(gage_id, module, get_gpkg = True):

        # Setup logging
        logger = logging.getLogger(__name__)

        # Read config file for paths
        config = get_config()
        output_dir = config['output_dir']

        # Get geopackage if needed
        if get_gpkg:
            results = get_geopackage(gage_id)
            if 'error' in results: 
                return results 

        # Build path for IPE temp directory
        subset_dir = output_dir + "/" + gage_id + "/"      

        status_str = "Get IPEs for " + module
        print(status_str)
        logger.info(status_str)
 
        # Call function for specific module
        if module == "CFE-S" or module == "CFE-X":
            results = cfe_ipe(gage_id, subset_dir, module)
            return results
        elif module == "Noah-OWP-Modular":
            print("noah-owp")
        elif module == "T-Route":
            print("T-route")
        else:
            error_str = "Module name not valid:" + module
            error = dict(error = error_str)
            error = json.dumps(error)
            print(error_str)
            logger.error(error_str)
            return error 


def cfe_ipe(gage_id, subset_dir, module):

        # Setup logging
        logger = logging.getLogger(__name__)

        # Get config file for paths
        config = get_config()
        input_dir = config['input_dir'] 
        s3url = config['s3url']
        s3bucket = config['s3bucket']
        s3prefix = config['s3prefix']

        #Build input arguments for config file R script
        print("Create BMI config files with initial parameter estimates")
        #create string containing R c (combine) function and gage IDs
        #gage_id_string = ','.join(gage_id)
        gage_id_string = 'c(' + gage_id + ')'
        gage_id_string = "'"+gage_id_string+"'"

        #this will be replaced with a call to the database when connectivity is available in the container
        if module == 'CFE-X':
            importjson = open('../CFE-X.json')
        if module == 'CFE-S':
            importjson = open('../CFE-S.json')
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

        run_command = ["/usr/bin/Rscript ../run_create_cfe_init_bmi_config.R",
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
            if not run_ipe:  error = json.dumps(error)
            print(error_str)
            logger.error(error_str)
            return error

        # CFE IPE R code uses Gage_6719505 format
        gage_id_full = "Gage_" + gage_id.lstrip("0")
        s3prefix = s3prefix + "/" + gage_id + "/" + module
        
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
            importjson = open('../calibratable_cfe-s.json')
        if module == 'CFE-X':
            importjson = open('../calibratable_cfe-x.json')
        output = json.load(importjson)

        #for x in output[0]["calibrate_parameters"]:
            #print(x["name"])
            #x["initial_value"] = cfg_file_ipes[x["name"]]

        #print(range(len(output[0]["calibrate_parameters"])))
        for x in range(len(output[0]["calibrate_parameters"])):
            print(output[0]["calibrate_parameters"][x]["name"])
            output[0]["calibrate_parameters"][x]["initial_value"] = cfg_file_ipes[output[0]["calibrate_parameters"][x]["name"]]
            

        uri = build_uri(s3bucket, s3prefix)
        output[0]["parameter_file"]["url"] = uri
        outjson = json.dumps(output)
        return outjson

def write_minio(path, filename, storage_url, bucket_name, prefix=""):

        #these access keys are for testing only.  This will be updated to use the AWS Secrets Manager
        access_key = os.environ["AWS_ACCESS_KEY_ID"]
        secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        session_token = os.environ["AWS_SESSION_TOKEN"]

        client = Minio(storage_url, access_key, secret_key, session_token, region="us-east-1")

        if not prefix:
            object_name = filename
        else:
            object_name = prefix + '/' + filename

        if client.bucket_exists(bucket_name):
            result = client.fput_object(bucket_name, object_name, path + '/' + filename)
            status_string = "Hydrofabric data  written to " + object_name + " in bucket " + bucket_name
            print(status_string)
        else:
            error_string = "Bucket" + " " + bucket_name + " " + "does not exist"
            print(error_string)

def build_uri(bucket_name, prefix="", filename=""):
    
        if not filename:
            object_name = prefix
        else:
            if not prefix:
                object_name = filename
            else:
                object_name = prefix + '/' + filename
    
        uri = "s3://" + bucket_name + "/" + object_name
        return uri        

def get_config():

    with open('../config.yml', 'r') as file:
        config = yaml.safe_load(file)
    return config    

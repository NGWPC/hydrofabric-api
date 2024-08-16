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

def get_geopackage(gage_id, run_ipe = False):

        output_dir = "/Hydrofabric/data/temp/"
        hydrofabric_dir = "/Hydrofabric/data/hydrofabric" 
        s3url = "s3.amazonaws.com"
        s3bucket = "ngwpc-dev"
        s3prefix = "DanielCumpton"

        #validate that gage input is in the proper format (8 digits) 
        x = bool(re.search("\d{8}", gage_id))
        if not x:
            return dict(error = "Gage ID is not valid")

	#call subsetter R function

        print("Create subsetted geopackage files")
        #append "Gages-" to id per hydrofabric naming convention for hl_uri
        subsetter_gage_id = "Gages-"+gage_id
        #strip leading zero of gage ID for gpkg filename
        subsetter_gage_id_filename = "Gage_"+gage_id.lstrip("0")

        #create directory for this particular subset
        currentDateAndTime = datetime.now()
        currentTime = currentDateAndTime.strftime("%Y%m%d_%H%M%S")        
        subset_dir = subsetter_gage_id_filename + "_" + currentTime + "/"
        subset_s3prefix = s3prefix + "/" + subsetter_gage_id_filename + "_" + currentTime
        subset_dir_full = os.path.join(output_dir, subset_dir)
        os.mkdir(subset_dir_full)
        gpkg_filename = subsetter_gage_id_filename + ".gpkg"

        print("Subsetting:  " + subsetter_gage_id)

        run_command = ["/usr/bin/Rscript /home/NGWPC-3201_GPKG_endpoint/hydrofabric_api/run_subsetter.r", subsetter_gage_id, subset_dir_full, gpkg_filename, hydrofabric_dir]
        run_command_string = " ".join(run_command)
        subprocess.call(run_command_string, shell=True)


        write_minio(subset_dir_full, gpkg_filename, s3url, s3bucket, subset_s3prefix)
        uri = build_uri(s3bucket, subset_s3prefix, gpkg_filename)
        if run_ipe:
            geopackage_dict = dict(creationDate = currentTime, uri = uri, subset_dir_full = subset_dir_full, gpkg_filename = gpkg_filename)
            return geopackage_dict
        else:
            geopackage_dict = dict(creationDate = currentTime, uri = uri)
            #geopackage_json = json.dumps(geopackage_dict)
            #return geopackage_json
            return geopackage_dict 

def get_ipe(gage_id, module):

        geopackage_location = get_geopackage(gage_id, True)
        if geopackage_location.get("error"):
            return geopackage_location
        else:
            subset_dir = geopackage_location["subset_dir_full"]
        
        if module == "CFE-S" or module == "CFE-X":
            cfe_ipe(gage_id, subset_dir, module)

def cfe_ipe(gage_id, subset_dir, module):

        input_dir = "/Hydrofabric/data/input/"

        #Run BMI config file R script
        print("Create BMI config files with initial parameter estimates")
        #create string containing R c (combine) function and gage IDs
        #gage_id_string = ','.join(gage_id)
        gage_id_string = 'c(' + gage_id + ')'
        gage_id_string = "'"+gage_id_string+"'"

        #this will be replaced with a call to the database when connectivity is available in the container
        importjson = open('/home/NGWPC-3201_GPKG_endpoint/hydrofabric_api/CFE-X.json')
        parameters = json.load(importjson)
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

        #print(cfe_parameters_const_name)
        #print(cfe_parameters_const_value)
        #print(cfe_parameters_nwm_name)
        #print(cfe_parameters_cfe_name)

        run_command = ["/usr/bin/Rscript /home/NGWPC-3201_GPKG_endpoint/hydrofabric_api/run_create_cfe_init_bmi_config.R",
        gage_id_string,
        input_dir,
        subset_dir,
        cfe_parameters_const_name,
        cfe_parameters_const_value,
        cfe_parameters_nwm_name,
        cfe_parameters_cfe_name,
        module]

        run_command_string = " ".join(run_command)
        subprocess.call(run_command_string, shell=True) 

        #write_minio(output_dir, tarfilename, s3url, s3bucket, s3prefix)
        s3url = "s3.amazonaws.com"
        s3bucket = "ngwpc-dev"
        s3prefix = "DanielCumpton"

        gage_id_full = "Gage_" + gage_id.lstrip("0")
        s3prefix = s3prefix + "/" + gage_id_full + "/CFE-X"
        
        files = Path(subset_dir + "/" + module + "/" + gage_id_full).glob('*.ini')
        for file in files:
            print("writing: " + str(file) + " to s3")
            file_name = os.path.basename(file)
            write_minio(subset_dir + "/" + module + "/" + gage_id_full, file_name, s3url, s3bucket, s3prefix)

        #write s3 location and ipe values to output json

        with open(file, 'r') as file:
            lines = file.readlines()

        cfg_file_ipes = {}

        for line in lines:
            key, value = line.strip().split('=')
            cfg_file_ipes[key.strip()] = value.strip()

        #this will be replaced with a call to the database when the connection is possible in the container
        importjson = open('/home/NGWPC-3201_GPKG_endpoint/hydrofabric_api/calibratable.json')
        output = json.load(importjson)

        for x in output[0]["calibrate_parameters"]:
            print(x["name"])
            x["initial_value"] = cfg_file_ipes[x["name"]]

        uri = build_uri(s3bucket, s3prefix)
        output[0]["parameter_file"]["url"] = uri

        #outjson = json.dumps(output)
        print(type(output))
        #pprint(output)
        return output


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

#Call function for test.  

gage =  "0671950"

#cfe_ipe(gage, "temp")

#print(get_ipe(gage, "CFE-X"))

#print(get_geopackage(gage, True))

#print(build_uri("ngwpc-dev", filename = "daniel")) 

#write_minio("/home/hydrofabric_api/", "test.txt", "s3.amazonaws.com", "ngwpc-de")

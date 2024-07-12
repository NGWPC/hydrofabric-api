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
import boto3
from minio import Minio

def hf_subsetter(gage_id, output_dir):

        input_dir = "/Hydrofabric/data/input/"
        output_dir = "/Hydrofabric/data/temp/"
        hydrofabric_dir = "/Hydrofabric/data/hydrofabric" 
        s3url = "s3.amazonaws.com"
        s3bucket = "ngwpc-de"
        s3prefix = "DanielCumpton"

        #validate that gage input is in the proper format (8 digits) 
        x = bool(re.search("\d{8}", gage_id))
        if not x:
            print("Gage ID is not valid")
            sys.exit()

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
        tarfilename = subsetter_gage_id_filename + currentTime + ".tar"
        subset_dir_full = os.path.join(output_dir, subset_dir)
        os.mkdir(subset_dir_full)
        gpkg_path_filename = subset_dir_full + subsetter_gage_id_filename + ".gpkg"

        print("Subsetting:  " + subsetter_gage_id)

        #run_command = "/usr/bin/Rscript /home/hydrofabric/R/run_subsetter.r" + " " + subsetter_gage_id + " " + gpkg_path_filename + " " + hydrofabric_dir
        run_command = ["/usr/bin/Rscript run_subsetter.r", subsetter_gage_id, gpkg_path_filename, hydrofabric_dir]
        run_command_string = " ".join(run_command)
        subprocess.call(run_command_string, shell=True)

        #Run BMI config file R script
        print("Create BMI config files with initial parameter estimates")
        #create string containing R c (combine) function and gage IDs
        #gage_id_string = ','.join(gage_id)
        gage_id_string = 'c(' + gage_id + ')'
        gage_id_string = "'"+gage_id_string+"'"

        run_command = ["/usr/bin/Rscript run_create_cfe_init_bmi_config.R", gage_id_string, input_dir, subset_dir_full]
        run_command_string = " ".join(run_command)
        subprocess.call(run_command_string, shell=True) 

        with tarfile.open(output_dir + tarfilename, "w") as tarhandle:
            for root, dirs, files in os.walk(subset_dir_full):
                for f in files:
                    tarhandle.add(os.path.join(root, f))


        write_minio(output_dir, tarfilename, s3url, s3bucket, s3prefix)


def write_s3(path, filename, bucket_name, prefix=""):

    session = boto3.Session(profile_name="218573839066_SoftwareEngineersFull")
    s3 = session.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    if not prefix:
       s3_filename = filename
    else:
       s3_filename = prefix + '/' + filename
    try:
        my_bucket.upload_file(path + filename, s3_filename)
#   except client.meta.client.exceptions as error:
    except:
        print(error)

def write_minio(path, filename, storage_url, bucket_name, prefix=""):

    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    session_token = os.environ["AWS_SESSION_TOKEN"]

    client = Minio(storage_url, access_key, secret_key, session_token, region="us-east-1")

    if not prefix:
       object_name = filename
    else:
       object_name = prefix + '/' + filename

    if client.bucket_exists(bucket_name):
       result = client.fput_object(bucket_name, object_name, path + filename)
       status_string = "Hydrofabric data  written to " + object_name + " in bucket " + bucket_name
       print(status_string)
    else:
       error_string = "Bucket" + " " + bucket_name + " " + "does not exist"
       print(error_string)
    
        
#Call function for test.  

gage =  "01123000"

hf_subsetter(gage, "/Hydrofabric/data/output/")

#write_minio("/home/hydrofabric_api/", "test.txt", "s3.amazonaws.com", "ngwpc-de")

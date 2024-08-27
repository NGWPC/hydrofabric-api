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
            noah_owp_modular_cfe(gage_id, subset_dir)
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

def noah_owp_modular_cfe(gage_id, subset_dir):

    # Setup logging
    logger = logging.getLogger(__name__)

    #Get config file
    config = get_config()
    s3url = config['s3url']
    s3bucket = config['s3bucket']
    s3prefix = config['s3prefix']
    
    #setup output dir
    subset_dir = os.path.join(subset_dir, 'Noah-OWP-Modular')
    if not os.path.exists(subset_dir):
        os.mkdir(subset_dir)


    # Get list of catchments from gpkg

    catchments = ['Cat-2885656', 'Cat-2885658', 'Cat-2885666']
    
    for catchment in catchments:
        
        startdate = '202408260000'
        enddate = '202408260000'
        noah_input_dir = 'test'

        # Define namelist template

        tslp = 'slope'
        azimuth = 'azimuth' 
        lat = 'lat'
        lon = 'lon'
        isltype = 'soil type'
        vegtype = 'veg type'
        sfctype = '1'

        namelist = ['&timing',
                "  " + "dt".ljust(19) +  "= 3600.0" + "                       ! timestep [seconds]",
                "  " + "startdate".ljust(19) + "= " + "'" + startdate + "'" + "               ! UTC time start of simulation (YYYYMMDDhhmm)",
                "  " + "enddate".ljust(19) + "= " + "'" + enddate + "'" + "               ! UTC time end of simulation (YYYYMMDDhhmm)",
                "  " + "forcing_filename".ljust(19) + "= '.'" + "                          ! file containing forcing data",
                "  " + "output_filename".ljust(19) + "= '.'",
                '/',
                "",
                '&parameters',
                "  " + "parameter_dir".ljust(19) + "= " + "'" + noah_input_dir + "'",
                "  " + "general_table".ljust(19) + "= 'GENPARM.TBL'" + "                ! general param tables and misc params",
                "  " + "soil_table".ljust(19) + "= 'SOILPARM.TBL'" + "               ! soil param table",
                "  " + "noahowp_table".ljust(19) + "= 'MPTABLE.TBL'" + "                ! model param tables (includes veg)",
                "  " + "soil_class_name".ljust(19) + "= 'STAS'" + "                       ! soil class data source - 'STAS' or 'STAS-RUC'",
                "  " + "veg_class_name".ljust(19) + "= 'USGS'" + "                       ! vegetation class data source - 'MODIFIED_IGBP_MODIS_NOAH' or 'USGS'",
                '/',
                "",
                '&location',
                "  " + "lat".ljust(19) + "= " + str(lat) + "            ! latitude [degrees]  (-90 to 90)",
                "  " + "lon".ljust(19) + "= " + str(lon) + "           ! longitude [degrees] (-180 to 180)",
                "  " + "terrain_slope".ljust(19) + "= " + str(tslp) + "           ! terrain slope [degrees]",
                "  " + "azimuth".ljust(19) + "= " + str(azimuth) + "           ! terrain azimuth or aspect [degrees clockwise from north]",
                '/',
                "",
                "&forcing",
                "  " + "ZREF".ljust(19) + "= 10.0" + "                         ! measurement height for wind speed (m)",
                "  " + "rain_snow_thresh".ljust(19) + "= 0.5" + "                          ! rain-snow temperature threshold (degrees Celcius)",
                "/",
                "",
                "&model_options",
                "  " + "precip_phase_option".ljust(34) + "= 6",
                "  " + "snow_albedo_option".ljust(34) + "= 1",
                "  " + "dynamic_veg_option".ljust(34) + "= 4",
                "  " + "runoff_option".ljust(34) + "= 3",
                "  " + "drainage_option".ljust(34) + "= 8",
                "  " + "frozen_soil_option".ljust(34) + "= 1",
                "  " + "dynamic_vic_option".ljust(34) + "= 1",
                "  " + "radiative_transfer_option".ljust(34) + "= 3",
                "  " + "sfc_drag_coeff_option".ljust(34) + "= 1",
                "  " + "canopy_stom_resist_option".ljust(34) + "= 1",
                "  " + "crop_model_option".ljust(34) + "= 0",
                "  " + "snowsoil_temp_time_option".ljust(34) + "= 3",
                "  " + "soil_temp_boundary_option".ljust(34) + "= 2",
                "  " + "supercooled_water_option".ljust(34) + "= 1",
                "  " + "stomatal_resistance_option".ljust(34) + "= 1",
                "  " + "evap_srfc_resistance_option".ljust(34) + "= 4",
                "  " + "subsurface_option".ljust(34) + "= 2",
                "/",
                "",
                "&structure",
                "  " + "isltyp".ljust(17) + "= " + str(isltype) + "              ! soil texture class",
                "  " + "nsoil".ljust(17) + "= 4              ! number of soil levels",
                "  " + "nsnow".ljust(17) + "= 3              ! number of snow levels",
                "  " + "nveg".ljust(17) + "= 27             ! number of vegetation type",
                "  " + "vegtyp".ljust(17) + "= " + str(vegtype) + "             ! vegetation type",
                "  " + "croptype".ljust(17) + "= 0              ! crop type (0 = no crops; this option is currently inactive)",
                "  " + "sfctyp".ljust(17) + "= " + str(sfctype) + "              ! land surface type, 1:soil, 2:lake",
                "  " + "soilcolor".ljust(17) + "= 4              ! soil color code",
                "/",
                "",
                "&initial_values",
                "  " + "dzsnso".ljust(10) + "= 0.0, 0.0, 0.0, 0.1, 0.3, 0.6, 1.0      ! level thickness [m]",
                "  " + "sice".ljust(10) + "= 0.0, 0.0, 0.0, 0.0                     ! initial soil ice profile [m3/m3]",
                "  " + "sh2o".ljust(10) + "= 0.3, 0.3, 0.3, 0.3                     ! initial soil liquid profile [m3/m3]",
                "  " + "zwt".ljust(10) + "= -2.0                                   ! initial water table depth below surface [m]",
                "/",
                ]


        cfg_filename = "noah-owp-modular-init-" + catchment + ".namelist.input"
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(namelist))
                            outfile.write("\n")

    s3prefix = s3prefix + '/' + gage_id + '/' + 'NOAH-OWP-Modular' 
    files = Path(subset_dir).glob('*.input')
    for file in files:
        print("writing: " + str(file) + " to s3")
        file_name = os.path.basename(file)
        write_minio(subset_dir, file_name, s3url, s3bucket, s3prefix)

    uri = build_uri(s3bucket, s3prefix)
    status_str = "Config files written to:  " + uri
    print(status_str)
    logger.info(status_str)

    importjson = open('NOAH-OWP-Modular.json')
    output = json.load(importjson)

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

    with open('config.yml', 'r') as file:
        config = yaml.safe_load(file)
    return config

gage_id = '06719505'
dir = '/Hydrofabric/data/temp/06719505'
print(noah_owp_modular_cfe(gage_id, dir))


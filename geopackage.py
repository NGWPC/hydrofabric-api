import os
import subprocess
import re
import logging

import utilities

def get_geopackage(gage_id):     
    '''
    Creates a geopackage containing a subset of the hydrofabric  
    
    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    
    Returns:
    dict: The URI of the geopackage.
    '''

    #setup logging
    logger = logging.getLogger(__name__)

    #get paths, etc from config.yml
    config = utilities.get_config()
    output_dir = config['output_dir'] 
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = config['hydrofabric_version']
    hydrofabric_type = config['hydrofabric_type']
    s3url = config['s3url']
    s3bucket = config['s3bucket'] 
    s3prefix = config['s3prefix'] 

    #validate that gage input is in the proper format (8 digits) 
    x = bool(re.search("\d{8}", gage_id))
    if not x:
        
        error = dict(error = "Gage ID is not valid")
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
    if s3prefix:
        subset_s3prefix = s3prefix + "/" + gage_id
    else:
        subset_s3prefix = gage_id

    subset_dir_full = os.path.join(output_dir, gage_id)
    if not os.path.exists(subset_dir_full):
        os.mkdir(subset_dir_full)

    status_str = "Calling HF Subsetter R code"
    print(status_str)
    logger.info(status_str)

    #Call R code for subsetter
    run_command = ["/usr/bin/Rscript ../R/run_subsetter.R",
                    subsetter_gage_id,
                    subset_dir_full, 
                    gpkg_filename, 
                    hydrofabric_dir, 
                    hydrofabric_version.lstrip('v'),
                    hydrofabric_type]
    run_command_string = " ".join(run_command)
    
    try:
        subprocess.call(run_command_string, shell=True)
    except:
        error_str = "Hydrofabric Subsetter R code failure"
        error = dict(error = error_str)
        print(error_str)
        logger.error(error_str)
        return error

    # Write geopackage to s3 bucket
    utilities.write_minio(subset_dir_full, gpkg_filename, s3url, s3bucket, subset_s3prefix)
    uri = utilities.build_uri(s3bucket, subset_s3prefix, gpkg_filename)
    status_str = "Written to S3 bucket: " + uri
    print(status_str)
    logger.info(status_str)

    # Build output 
    geopackage_output = dict(uri = uri)
    return geopackage_output
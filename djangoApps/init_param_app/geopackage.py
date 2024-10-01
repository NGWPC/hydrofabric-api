import os
import inspect
#import subprocess
from subprocess import run
import logging

#import utilities
from .utilities import *

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
    config = get_config()
    output_dir = config['output_dir'] 
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = config['hydrofabric_version']
    hydrofabric_type = config['hydrofabric_type']
    s3url = config['s3url']
    s3bucket = config['s3bucket'] 
    s3prefix = config['s3prefix'] 

    #setup paths and geopackage name for hydrofabric subsetter R function call

    #append "Gages-" to id per hydrofabric naming convention for hl_uri
    subsetter_gage_id = "Gages-"+gage_id
    status_str = "Create subsetted geopackage file for: " + subsetter_gage_id
    print(status_str)
    logger.info(status_str)

    #strip leading zero of gage ID for gpkg filename
    #gpkg_filename = "Gage_"+gage_id.lstrip("0") + ".gpkg"
    gpkg_filename = "Gage_" + gage_id + ".gpkg"

    #create temp directory and s3 prefix for this particular subset
    if s3prefix:
        subset_s3prefix = s3prefix + "/" + gage_id
    else:
        subset_s3prefix = gage_id

    subset_dir_full = os.path.join(output_dir, gage_id)

    current_filename = __file__
    try:
        if not os.path.exists(subset_dir_full):
            os.mkdir(subset_dir_full)
            current_line = inspect.currentframe().f_lineno - 1
            status_str = f"geopkg dir {subset_dir_full} not found, creating directory. {current_filename}::{current_line}"
            logger.info(status_str)
    except Exception as e:
        current_line = inspect.currentframe().f_lineno
        error_str = f"error creating directory {subset_dir_full}, {current_filename}::{current_line}-{e}"
        logger.error(error_str)
        return error_str

    status_str = "Calling HF Subsetter R code"
    print(status_str)
    logger.info(status_str)

    #Call R code for subsetter
    run_command = ["/usr/bin/Rscript", "../R/run_subsetter.R",
                    subsetter_gage_id,
                    subset_dir_full, 
                    gpkg_filename, 
                    hydrofabric_dir, 
                    hydrofabric_version.lstrip('v'),
                    hydrofabric_type]

    result = run(run_command, capture_output=True)

    if 'error' in str(result.stderr):
        error_str = 'Hydrofabric subsetting failed; check gage id.'
        error = {'error':  error_str}
        logger.error(error_str)
        return error
    
    # Write geopackage to s3 bucket
    write_minio(subset_dir_full, gpkg_filename, s3url, s3bucket, subset_s3prefix)
    uri = build_uri(s3bucket, subset_s3prefix, gpkg_filename)
    status_str = "Written to S3 bucket: " + str(uri)
    print(status_str)
    logger.info(status_str)

    # Build output 
    geopackage_output = dict(uri = uri)
    return geopackage_output

import os
from pathlib import Path
import inspect
#import subprocess
from subprocess import run
import logging

import psycopg2
from minio import S3Error

from .util.gage_file_management import GageFileManagement
#import utilities
from .util.utilities import *


def get_geopackage(gage_id, source, domain, keep_file=False):
    """
    Creates a geopackage containing a subset of the hydrofabric

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385

    Returns:
    dict: The URI of the geopackage.
    """
    gage_file_mgmt = GageFileManagement()
    data_type = 'GEOPACKAGE'

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

        # hydrofabric input data directory
        hydrofabric_dir = config['hydrofabric_dir']
        # hydrofabric input data version
        hydrofabric_version = config['hydrofabric_version']
        # hydrofabric input data version type
        hydrofabric_type = config['hydrofabric_type']
        # Tell the subsetter what to retrieve
        subsetter_gage_id = f"Gages-{gage_id}"
        
        gpkg_filename = gage_file_mgmt.get_geopackage_filename(gage_id)
        status_str = "Calling HF Subsetter R code"
        
        #Call R code for subsetter
        logger.debug(status_str)
        
        path_string = "R/run_subsetter.R"
        grandparent_dir = os.path.dirname(settings.BASE_DIR)
        directory = os.path.join(grandparent_dir, path_string)
        logger.debug(f"R code file = {directory}")

        run_command = ["/usr/bin/Rscript", "../R/run_subsetter.R",
                       subsetter_gage_id,
                       loc_temp_dir,
                       gpkg_filename,
                       hydrofabric_dir,
                       hydrofabric_version.lstrip('v'),
                       hydrofabric_type]
        
        result = run(run_command, capture_output=True)
        if 'error' in str(result.stderr):
            error_str = 'Hydrofabric subsetting failed; check gage id.'
            error = {'error': error_str}
            logger.error(error_str)
            return error
    except OSError as ose:
        current_filename = __file__
        current_line = inspect.currentframe().f_lineno
        error_str = f"error creating local temp directory for {data_type}, and gage_id = {gage_id} - {current_filename}::{current_line}-{ose}"
        logger.error(error_str)
        return error_str

    # Write geopackage to s3 bucket
    try:
        uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, data_type, source, loc_temp_dir,
                                                                   gpkg_filename)
    # TODO PROPERLY HANDEL LOGGING "RESPONSE" FOR CAUGHT ERRORS
    except psycopg2.DatabaseError as psycopg2_error:
        logging.error(psycopg2_error)
    except S3Error as s3_error:
        logging.error(f"AWS Credentials have failed; Log into AWS and retrieve new credentials. Exception = {s3_error}")
    except Exception as error1:
        logging.error(f"General exception error = {error1}")

    # Clean up temp files
    if not keep_file:
        # remove temp file
        gage_file_mgmt.delete_local_temp_directory(loc_temp_dir)

    # TODO PROPERLY HANDEL LOGGING "RESPONSE" FOR CAUGHT ERRORS
    return uri

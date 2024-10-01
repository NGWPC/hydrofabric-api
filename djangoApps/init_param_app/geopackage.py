import os
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
    #temp dir for files
    loc_temp_dir = None
    try:
        loc_temp_dir = gage_file_mgmt.get_local_temp_directory(data_type, gage_id)

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
        logger.debug(status_str)

        #Call R code for subsetter
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
        result, insert_time, uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, data_type, source, loc_temp_dir,
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
        local_file = loc_temp_dir + gpkg_filename
        try:
            os.remove(local_file)
            logging.debug(f"Deleted: {local_file}")
        except Exception as e:
            print(f"Error deleting {local_file}: {e}")
            logging.warning(f"Error deleting {local_file}: {e}")

    # TODO PROPERLY HANDEL LOGGING "RESPONSE" FOR CAUGHT ERRORS
    geopackage_output = dict(uri=uri)
    return geopackage_output

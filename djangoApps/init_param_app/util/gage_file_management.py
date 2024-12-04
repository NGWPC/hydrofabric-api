"""
Performs file management for data stored on the NGWPC S3 including managing the DB with the file metadata
"""
import os
from datetime import datetime
from os.path import join
import shutil
from django.conf import settings
from django.utils import timezone
import logging

from .enums import FileTypeEnum
from ..models import HFFiles

logger = logging.getLogger(__name__)

from .file_management import FileManagement


class GageFileManagement(FileManagement):

    def __init__(self):
        """
        gageid: The gage the data was requested for
        data_type: The type of data retrieved (Ex. Observational, Forcing)
        source: Source of the dat (Ex USGS, USARC, Tx DOT, AORC, Legacy_AORC)
        domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        """
        super().__init__()
        self.gage_id = None
        self.domain = None
        self.data_type = None
        self.source = None
        self.module = None
        self.formatted_datetime = None
        self.input_path = None
        self.db_object = None

    def __build_s3_param_path(self):
        """
        Private method to build a S3 path for a param data file
        """
        self.s3_path = join(self.hydro_version, self.domain, self.gage_id, self.data_type, self.source, self.module, self.formatted_datetime)
        logger.debug(self.s3_path)

    def __build_s3_data_path(self):
        """
        Private method to build a S3 path for a data file
        """
        self.s3_path = join(self.hydro_version, self.domain, self.gage_id, self.data_type, self.source, self.formatted_datetime)

    def get_local_temp_directory(self, data_type, gage_id=None):
        """
        Builds a local directory to put created data files into, prior to being transferred S3 and the HFFILES table.
        Creates directory if not present
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param gage_id: The gage the directory was requested for
        :return: String of a local directory to use for temp file storage
        """
        path_string = f"data/{data_type}/" if gage_id is None else f"data/{data_type}/{gage_id}/"
        
        grandparent_dir = os.path.dirname(settings.BASE_DIR)
        directory = os.path.join(grandparent_dir, path_string)
        logger.debug(f"local temp directory = {directory}")
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def delete_local_temp_directory(self, directory):
        """
        Deletes a local directory used to put created data files into, prior to being transferred S3 and the HFFILES table.
        Checks if directory exists and removes

        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param gage_id: The gage the directory was requested for
        :return: String of a local directory to use for temp file storage
        """
        if os.path.exists(directory):
            try:
                shutil.rmtree(directory)
                logger.debug(f"Directory '{directory}' deleted successfully (non-empty).")
            except Exception as e:
                logger.error(f"Error deleting directory '{directory}': {e}")       

    def param_files_exists(self, gage_id, domain, source, data_type, modules):
        """
        Go through the list of modules and determine if files have already been calculated. Return list of modules to 
        be computed
        Example of return ["CFE-S", "CFE-X"]
        TODO Flush this method out for each module return dict of found data this might return the pre-calculated JSON from DB
        
        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param modules: List of modules to check against (Ex. CFE-S, CFE-X, NOAH-OWP-MODULAR, T-Route ... etc)
        :return: The existence of param files
        """
        # For now return modules until the function is flushed out
        #result = {}
        return modules

    def file_exists(self, gage_id, domain, source, data_type):
        """
        Determines if a data file exists in S3 and in HFFILES table
        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :return: If file is found and the S3 URI
        """
        file_found = False
        results = None
        my_data = HFFiles.objects.filter(gage_id=gage_id, source=source, domain=domain, data_type=data_type).values()

        if not my_data:
            log_string = f"Database missing entry for gage_id - {gage_id}, data type - {data_type}, source -  {source}, domain - {domain}."
            logger.debug(log_string)
        else:
            # Check S3 for file from DB call.
            # Return file URL in schema dict
            uri = my_data[0].get('uri')
            # start MinIO client if not started
            self.start_minio_client()
            if not self.s3_file_exists(uri) and data_type == FileTypeEnum.OBSERVATIONAL:
                # Observational streamflow data is precomputed for pre-defined gages if missing file then this is error
                log_string = f"S3 bucket missing gage_id - {gage_id}, data type - {data_type}, source -  {source}, domain - {domain}. Database entry uri is {uri}. Also might be an AWS S3 Credentials issue"
                logger.error(log_string)
            else:
                file_found = True
                results = dict(uri=uri)
        return file_found, results

    def ipe_files_exists(self, gage_id, domain, source, module):
        """
        Determines if a ipe data files exists in S3 and in HFFILES table
        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)

        :return: If files exists in DB and S3 and return the ipe json document
        """
        file_found = False
        results = None
        data_type = FileTypeEnum.PARAMS
        my_data = HFFiles.objects.filter(gage_id=gage_id, source=source, domain=domain, module_id=module, data_type=FileTypeEnum.PARAMS).values()

        if not my_data:
            log_string = f"Database missing entry for gage_id - {gage_id}, module - {module}, data type - {data_type}, source -  {source}, domain - {domain}."
            logger.debug(log_string)
        else:
            # Check S3 for file from DB call.
            # Return file URL in schema dict
            uri = my_data[0].get('uri')
            ipe_json = my_data[0].get('ipe_json')
            # start MinIO client if not started
            self.start_minio_client()
                        # Check S3 for file from DB call.
            # Return file URL in schema dict
            uri_stripped = uri.split(self.s3_bucket)[1].lstrip('/')
            # start MinIO client if not started
            self.start_minio_client()
            
            objects = self.client.list_objects(self.s3_bucket, prefix=uri_stripped, recursive=False)
            for obj in objects:
                file_found = True  # Found at least one object in the folder
                break

            if not file_found:
                log_string = f"S3 bucket missing gage_id - {gage_id}, data type - {data_type}, module - {module}, source -  {source}, domain - {domain}. Database entry uri is {uri}. Also might be an AWS S3 Credentials issue"
                logger.error(log_string)
            else:
                file_found = True
                results = ipe_json
        return file_found, results

    def write_file_to_s3(self, gage_id, domain, data_type, source, input_directory, input_filenames, module=None):
        """

        :param module:
        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param input_directory: Directory where local files are stored
        :param input_filenames:  List of filenames of one or more locally created files
        :return:
        :raises  S3Exception:

        """
        self.gage_id = gage_id
        self.domain = domain
        self.data_type = data_type
        self.source = source
        self.input_path = input_directory
        self.module = module

        # Get the current date and time
        now = timezone.now().replace(microsecond=0)
        # Format the date and time as a string
        self.formatted_datetime = now.strftime("%Y_%b_%d_%H_%M_%S")

        #start MinIO client if not started
        self.start_minio_client()

        # Build the S3 Path
        if self.module is not None:
            self.__build_s3_param_path()
        else:
            self.__build_s3_data_path()

        # Write files to S3
        for self.input_filename in input_filenames:
            self.write_minio()

        # Create a new HFFILES row.
        try:
            if self.module is not None:
                # PARAM files are a group of files. Set filename to blank string, and remove filename from self.full_s3_path
                blank = ""
                self.full_s3_path = self.full_s3_path.rsplit("/", 1)[0]
                new_hffiles = HFFiles(gage_id=self.gage_id, hydrofabric_version=self.hydro_version,
                                    filename=blank,
                                    uri=self.full_s3_path, domain=self.domain, data_type=self.data_type,
                                    source=self.source,
                                    module_id=self.module,
                                    update_time=now)
                
            else:
                new_hffiles = HFFiles(gage_id=self.gage_id, hydrofabric_version=self.hydro_version,
                                    filename=self.input_filename,
                                    uri=self.full_s3_path, domain=self.domain, data_type=self.data_type,
                                    source=self.source,
                                    update_time=now)
            new_hffiles.save()
            # Save off the db object to add the IPE json document to
            if self.module is not None:
                self.db_object = new_hffiles

        except Exception as exception:
            logger.error(f"Unhandled exception caught - {exception}")

        return self.full_s3_path

    def get_file_from_s3(self, gage_id, domain, source, data_type):
        #Find file in HFFles table
        try:
            file_found, results = self.file_exists(gage_id, domain, source, data_type)

            #Create the local temp directory to put the file into
            loc_temp_dir = self.get_local_temp_directory(data_type, gage_id)
            #Get and store the S3 file to local storage
            self.retrieve_minio(results['uri'], loc_temp_dir)
        except Exception as exception:
            logger.error(f"Unhandled exception caught - {exception}")

    def get_observational_filename(self, gage_id):
        return gage_id + "_hourly_discharge.csv"

    def get_geopackage_filename(self, gage_id):
        return 'gage_' + gage_id + ".gpkg"
    
    def get_db_object(self):
        return self.db_object

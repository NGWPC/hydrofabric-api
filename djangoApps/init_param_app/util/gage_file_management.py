"""
Performs file management for data stored on the NGWPC S3 including managing the DB with the file metadata
"""
import os
from datetime import datetime
from os.path import join
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
        self.formatted_datetime = None
        self.input_path = None

    def __build_s3_param_estimate_path(self):
        #TODO - Flush out
        self.input_path = join(str(self.input_path), self.data_type, self.source)
        # 2885302_bmi_config.ini

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
        path_string = f"/data/{data_type}/" if gage_id is None else f"/data/{data_type}/{gage_id}/"
        cwd = os.getcwd() + path_string
        if not os.path.exists(cwd):
            os.makedirs(cwd)
        return cwd

    def param_files_exists(self, gage_id, domain, source, data_type, modules):
        """
        TODO Flush this method out for each module return dict of found data this might return the pre-calculated JSON from DB
        Example of return [{"CFE-S": "some_S3_URL_to_module_param_files"},
                           {"CFE-X": "some_S3_URL_to_module_param_files"}

        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param modules: List of modules to check against (Ex. CFE-S, CFE-X, NOAH-OWP-MODULAR, T-Route ... etc)
        :return: The existence of param files
        """
        # For now return empty dict
        result = {}
        return result

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

    def write_file_to_s3(self, gage_id, domain, data_type, source, input_directory, input_filename):
        """

        :param gage_id: The gage the data was requested for
        :param domain: Domain of the gage (CONUS, Alaska, Hawaii, Puerto Rico, American Virgin Islands)
        :param source: Source or Agency owning the gage (Ex USGS, USARC, Env Canada ... etc)
        :param data_type: The type of data retrieved (Ex. GEOPACKAGE, Observational, Forcing ... etc)
        :param input_directory: Directory where local files are stored
        :param input_filename:  Filename of locally created file
        :return:
        :raises  S3Exception:

        """
        self.gage_id = gage_id
        self.domain = domain
        self.data_type = data_type
        self.source = source
        self.input_path = input_directory
        self.input_filename = input_filename

        # Get the current date and time
        now = datetime.now()
        # Format the date and time as a string
        self.formatted_datetime = now.strftime("%Y_%b_%d_%H_%M_%S")
        # Build the S3 Path
        self.__build_s3_data_path()
        #start MinIO client if not started
        self.start_minio_client()
        # TODO Need to have write_minio return result
        result = self.write_minio()
        # TODO Need to test this with geopackage and config files
        # Create a new HFFILES row
        new_hffiles = HFFiles(gage_id=self.gage_id, hydrofabric_version=self.hydro_version, filename=self.input_filename,
                              uri=self.full_s3_path, domain=self.domain, data_type=self.data_type, source=self.source,
                              update_time=now)
        new_hffiles.save()
        # Remove local file
        local_file = os.path.expanduser(self.input_path + self.input_filename)
        try:
            os.remove(local_file)
        except Exception as e:
            logging.warning(f"Error deleting {local_file}: {e}")

        return result, self.full_s3_path

    def get_file_from_s3(self, gage_id, domain, source, data_type):
        #Find file in HFFles table
        uri = None
        file_found, results = self.file_exists(gage_id, domain, source, data_type)

        #Create the local temp directory to put the file into
        loc_temp_dir = self.get_local_temp_directory(data_type, gage_id)
        #Get and store the S3 file to local storage
        self.retrieve_minio(results['uri'], loc_temp_dir)


    def get_observational_filename(self, gage_id):
        return gage_id + "_hourly_discharge.csv"

    def get_geopackage_filename(self, gage_id):
        return 'gage_' + self.gage_id + ".gpkg"

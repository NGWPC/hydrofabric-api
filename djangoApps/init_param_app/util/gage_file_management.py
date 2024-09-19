"""
Performs file management for data stored on the NGWPC S3 including managing the DB with the file metadata
"""

from datetime import datetime
from os.path import join

from ..enums import FileTypeEnum
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

    def __build_param_estimate_path(self):
        self.input_path = join(str(self.input_path), self.data_type, self.source)
        # 2885302_bmi_config.ini

    def __build_geopackage_path(self):
        """

        :return:
        """
        # input_path already built in ctor/initializer
        # build file_name

        if self.data_type == FileTypeEnum.GEOPACKAGE:
            self.s3_path = join(self.domain, self.gage_id)

    def __build_data_path(self):
        self.s3_path = join(self.domain, self.gage_id, self.data_type, self.source, self.formatted_datetime)
        print(self.s3_path)

    def get_directory_for_file(self) -> str:
        """
        Returns from the database the location of preexisting file for this instance
        Note a call/check to the method file_exists() will prevent NONE returns

        :raises  FileException:
        :return: S3 URI file location or None
        """

        # This code is mocked out until the actual db table and DAL objects are complete
        return False

    def write_file_to_s3(self, gage_id, domain, data_type, source, input_directory, input_filename):
        """
        This function will perform the following functions
          1. Generate a S3 URI to store the file
          2. Create a row in the file_management table representing the metadata for this file
          3. Stream or Move the file from temp/local storage to the S3 URI

        :raises  FileException:
        """
        self.gage_id = gage_id
        self.domain = domain
        self.data_type = data_type
        self.source = source
        self.input_path = join(self.domain, self.gage_id)

        # Get the current date and time
        now = datetime.now()

        # Format the date and time as a string
        self.formatted_datetime = now.strftime("%Y_%b_%d_%H_%M_%S")

        self.input_path = input_directory
        self.input_filename = input_filename
        # Build the S3 Path and filenames
        self.__build_data_path()

        result = self.write_minio()

        return result, now, self.full_s3_path

    def get_observational_filename(self, gage_id):
        return gage_id + "_hourly_discharge.csv"

    def get_geopackage_filename(self, start_date, end_date):
        return 'gage_' + self.gage_id + ".gpkg"

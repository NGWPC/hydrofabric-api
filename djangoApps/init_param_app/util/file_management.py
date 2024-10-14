"""
This module manages files that have a gage dependency for CRUD DB operations and R/W to S3
"""
import os
import logging
from minio import Minio, S3Error
from .utilities import get_config

logger = logging.getLogger(__name__)

class FileManagement:
    # these access keys are for testing only.  This will be updated to use the AWS Secrets Manager

    def __init__(self):
        self.access_key = os.environ["AWS_ACCESS_KEY_ID"]
        self.secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        self.session_token = os.environ["AWS_SESSION_TOKEN"]
        config = get_config()
        self.s3_url = config['s3url']
        self.s3_bucket = config["s3bucket"]
        self.s3_uri = config['s3uri']
        self.hydro_version = config['hydrofabric_output_version']
        self.s3_path = None
        self.full_s3_path = None
        self.input_filename = None
        self.input_path = None
        self.client = None

    def start_minio_client(self):
        if self.client is None:
            self.client = Minio(self.s3_url, self.access_key, self.secret_key, self.session_token, region="us-east-1")

    def check_s3_bucket(self):
        self.start_minio_client()
        bucket_exists = True
        if not self.client.bucket_exists(self.s3_bucket):
            logger.error(f"Bucket {self.s3_bucket} does not exist")
            bucket_exists = False
        return bucket_exists

    def s3_file_exists(self, object_name):
        try:
            object_name = object_name.removeprefix(self.s3_uri)

            self.client.stat_object(self.s3_bucket, object_name)
            return True
        except S3Error as s3_error:
            if s3_error.code == 'NoSuchKey':
                return False
            else:
                logger.error(
                    f"AWS Credentials have failed; Log into AWS and retrieve new credentials. Exception = {s3_error}")
        except Exception as exception:
            logger.error(f"Unhandled exception caught - {exception}")

    def write_minio(self):
        s3_path_output = self.s3_path + '/' + self.input_filename
        try:
            self.client.fput_object(self.s3_bucket, s3_path_output, self.input_path + self.input_filename)
            self.full_s3_path = "s3://" + self.s3_bucket + "/" + s3_path_output
            status_string = "Hydrofabric data written to " + s3_path_output
            logger.info(status_string)
        except Exception as exception:
            logger.error(f"Unhandled exception caught - {exception}")

    def retrieve_minio(self, object_name, local_dir):
        self.start_minio_client()
        try:
            object_name = object_name.removeprefix(self.s3_uri)
            file_name = os.path.basename(object_name)
            local_dir = os.path.join(local_dir, file_name)
            self.client.fget_object(self.s3_bucket, object_name, local_dir)
            logger.debug(f"File '{object_name}' successfully downloaded to '{local_dir}'.")
        except Exception as e:
            logger.error(f"Error downloading file: {e}")


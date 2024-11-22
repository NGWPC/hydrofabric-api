"""
This module manages files that have a gage dependency for CRUD DB operations and R/W to S3
"""
import os
import logging
import requests
import json
from datetime import datetime, timezone
from minio import Minio, S3Error
from .utilities import get_config

logger = logging.getLogger(__name__)

class FileManagement:
    def __init__(self):
        config = get_config()
        self.s3_url = config['s3url']
        self.s3_bucket = config["s3bucket"]
        self.s3_uri = config['s3uri']
        self.hydro_version = config['hydrofabric_output_version']
        # Get region from config or environment, with a fallback default of us-east-1
        self.region = (
            config.get('region') or
            os.environ.get('AWS_REGION') or
            os.environ.get('AWS_DEFAULT_REGION') or
            'us-east-1'
        )
        self.s3_path = None
        self.full_s3_path = None
        self.input_filename = None
        self.input_path = None
        self.client = None
        self._credentials = None
        self._credentials_expiry = None

    def _get_imds_token(self):
        """Get IMDSv2 token for subsequent requests"""
        try:
            response = requests.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
                timeout=2
            )
            return response.text if response.ok else None
        except requests.RequestException:
            logger.debug("Unable to fetch IMDSv2 token - not running on EC2?")
            return None

    def _get_instance_credentials(self):
        """Fetch credentials from IMDSv2"""
        token = self._get_imds_token()
        if not token:
            return None

        try:
            # Get role name
            role_response = requests.get(
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
                headers={"X-aws-ec2-metadata-token": token},
                timeout=2
            )
            if not role_response.ok:
                return None
            
            role_name = role_response.text

            # Get credentials
            creds_response = requests.get(
                f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}",
                headers={"X-aws-ec2-metadata-token": token},
                timeout=2
            )
            if not creds_response.ok:
                return None

            creds = creds_response.json()
            return {
                'access_key': creds['AccessKeyId'],
                'secret_key': creds['SecretAccessKey'],
                'session_token': creds['Token'],
                'expiry': datetime.strptime(creds['Expiration'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            }
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            logger.debug(f"Error fetching instance credentials: {e}")
            return None

    def _get_credentials(self):
        """Get credentials from environment or instance metadata"""
        # First check environment variables
        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        session_token = os.environ.get("AWS_SESSION_TOKEN")

        if access_key and secret_key:
            logger.debug("Using AWS credentials from environment variables")
            return {
                'access_key': access_key,
                'secret_key': secret_key,
                'session_token': session_token
            }

        # Then try instance metadata
        logger.debug("Attempting to fetch credentials from instance metadata")
        return self._get_instance_credentials()

    def _should_refresh_credentials(self):
        """Check if credentials need refreshing"""
        if not self._credentials or not self._credentials_expiry:
            return True
        # Refresh if within 5 minutes of expiry
        return (self._credentials_expiry - datetime.now(timezone.utc)).total_seconds() < 300

    def start_minio_client(self):
        if self.client is None or (self._credentials and self._should_refresh_credentials()):
            credentials = self._get_credentials()
            
            if credentials:
                logger.debug("Creating Minio client with credentials")
                self.client = Minio(
                    self.s3_url,
                    access_key=credentials['access_key'],
                    secret_key=credentials['secret_key'],
                    session_token=credentials.get('session_token'),
                    region=self.region
                )
                self._credentials = credentials
                self._credentials_expiry = credentials.get('expiry')
            else:
                logger.warning("No credentials available - operations may fail")
                self.client = Minio(
                    self.s3_url,
                    region=self.region
                )

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
                logger.error(f"Error checking if file exists: {s3_error}")
                return False
        except Exception as exception:
            logger.error(f"Unhandled exception caught - {exception}")
            return False

    def write_minio(self):
        # Ensure credentials are fresh before writing
        self.start_minio_client()
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

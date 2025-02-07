import pytest
import os
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import requests
from djangoApps.init_param_app.util.file_management import FileManagement
from django.test import override_settings


@pytest.fixture
def mock_config():
    return {
        's3url': 's3.us-east-1.amazonaws.com',
        's3uri': 's3://test-bucket',
        'hydrofabric_output_version': '1.0',
        'region': 'us-east-1'
    }


@pytest.fixture
@override_settings(S3_BUCKET='test-bucket')
def file_management(mock_config):
    with patch('djangoApps.init_param_app.util.file_management.get_config', return_value=mock_config):
        return FileManagement()


@pytest.fixture
def mock_env_credentials():
    env_vars = {
        'AWS_ACCESS_KEY_ID': 'test-access-key',
        'AWS_SECRET_ACCESS_KEY': 'test-secret-key',
        'AWS_SESSION_TOKEN': 'test-session-token'
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


@pytest.fixture
def mock_imds_credentials():
    expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    return {
        'AccessKeyId': 'imds-access-key',
        'SecretAccessKey': 'imds-secret-key',
        'Token': 'imds-session-token',
        'Expiration': expiry.strftime('%Y-%m-%dT%H:%M:%SZ')
    }


class TestFileManagement:
    @override_settings(S3_BUCKET='test-bucket')
    def test_init(self, file_management, mock_config):
        """Test initialization of FileManagement"""
        assert file_management.s3_url == mock_config['s3url']
        assert file_management.s3_bucket == 'test-bucket'
        assert file_management.region == mock_config['region']
        assert file_management.client is None

    @override_settings(S3_BUCKET='test-bucket')
    @patch('requests.put')
    def test_get_imds_token_success(self, mock_put, file_management):
        """Test successful IMDSv2 token retrieval"""
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.text = 'test-token'
        mock_put.return_value = mock_response

        token = file_management._get_imds_token()

        assert token == 'test-token'
        mock_put.assert_called_once_with(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            timeout=2
        )

    @override_settings(S3_BUCKET='test-bucket')
    @patch('requests.put')
    def test_get_imds_token_failure(self, mock_put, file_management):
        """Test failed IMDSv2 token retrieval"""
        mock_put.side_effect = requests.RequestException()

        token = file_management._get_imds_token()

        assert token is None
        mock_put.assert_called_once()

    @override_settings(S3_BUCKET='test-bucket')
    @patch('requests.get')
    @patch('requests.put')
    def test_get_instance_credentials_success(self, mock_put, mock_get, file_management, mock_imds_credentials):
        """Test successful instance credential retrieval"""
        # Mock token response
        mock_token_response = MagicMock()
        mock_token_response.ok = True
        mock_token_response.text = 'test-token'
        mock_put.return_value = mock_token_response

        # Mock role name response
        mock_role_response = MagicMock()
        mock_role_response.ok = True
        mock_role_response.text = 'test-role'

        # Mock credentials response
        mock_creds_response = MagicMock()
        mock_creds_response.ok = True
        mock_creds_response.json.return_value = mock_imds_credentials

        mock_get.side_effect = [mock_role_response, mock_creds_response]

        creds = file_management._get_instance_credentials()

        assert creds['access_key'] == mock_imds_credentials['AccessKeyId']
        assert creds['secret_key'] == mock_imds_credentials['SecretAccessKey']
        assert creds['session_token'] == mock_imds_credentials['Token']
        assert isinstance(creds['expiry'], datetime)

    @override_settings(S3_BUCKET='test-bucket')
    def test_get_credentials_from_env(self, file_management, mock_env_credentials):
        """Test getting credentials from environment variables"""
        creds = file_management._get_credentials()

        assert creds['access_key'] == mock_env_credentials['AWS_ACCESS_KEY_ID']
        assert creds['secret_key'] == mock_env_credentials['AWS_SECRET_ACCESS_KEY']
        assert creds['session_token'] == mock_env_credentials['AWS_SESSION_TOKEN']

    @override_settings(S3_BUCKET='test-bucket')
    @patch('djangoApps.init_param_app.util.file_management.FileManagement._get_instance_credentials')
    def test_get_credentials_fallback_to_imds(self, mock_get_instance_creds, file_management, mock_imds_credentials):
        """Test fallback to IMDS when no environment variables"""
        instance_creds = {
            'access_key': mock_imds_credentials['AccessKeyId'],
            'secret_key': mock_imds_credentials['SecretAccessKey'],
            'session_token': mock_imds_credentials['Token'],
            'expiry': datetime.now(timezone.utc) + timedelta(hours=1)
        }
        mock_get_instance_creds.return_value = instance_creds

        with patch.dict(os.environ, {}, clear=True):
            creds = file_management._get_credentials()

        assert creds == instance_creds
        mock_get_instance_creds.assert_called_once()

    @override_settings(S3_BUCKET='test-bucket')
    def test_should_refresh_credentials(self, file_management):
        """Test credential refresh logic"""
        # Test with no credentials
        assert file_management._should_refresh_credentials() is True

        # Test with expired credentials
        file_management._credentials = {'some': 'credentials'}
        file_management._credentials_expiry = datetime.now(timezone.utc) - timedelta(minutes=1)
        assert file_management._should_refresh_credentials() is True

        # Test with valid credentials
        file_management._credentials_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        assert file_management._should_refresh_credentials() is False

        # Test with credentials about to expire
        file_management._credentials_expiry = datetime.now(timezone.utc) + timedelta(minutes=4)
        assert file_management._should_refresh_credentials() is True

    @override_settings(S3_BUCKET='test-bucket')
    @patch('djangoApps.init_param_app.util.file_management.Minio')
    def test_start_minio_client_with_env_creds(self, mock_minio, file_management, mock_env_credentials):
        """Test Minio client initialization with environment credentials"""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        file_management.start_minio_client()

        mock_minio.assert_called_once_with(
            file_management.s3_url,
            access_key=mock_env_credentials['AWS_ACCESS_KEY_ID'],
            secret_key=mock_env_credentials['AWS_SECRET_ACCESS_KEY'],
            session_token=mock_env_credentials['AWS_SESSION_TOKEN'],
            region=file_management.region
        )

    @override_settings(S3_BUCKET='test-bucket')
    @patch('djangoApps.init_param_app.util.file_management.Minio')
    @patch('djangoApps.init_param_app.util.file_management.FileManagement._get_instance_credentials')
    def test_start_minio_client_with_imds_creds(
        self, mock_get_instance_creds, mock_minio, file_management, mock_imds_credentials
    ):
        """Test Minio client initialization with IMDS credentials"""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        instance_creds = {
            'access_key': mock_imds_credentials['AccessKeyId'],
            'secret_key': mock_imds_credentials['SecretAccessKey'],
            'session_token': mock_imds_credentials['Token'],
            'expiry': datetime.now(timezone.utc) + timedelta(hours=1)
        }
        mock_get_instance_creds.return_value = instance_creds

        with patch.dict(os.environ, {}, clear=True):
            file_management.start_minio_client()

        mock_minio.assert_called_once_with(
            file_management.s3_url,
            access_key=instance_creds['access_key'],
            secret_key=instance_creds['secret_key'],
            session_token=instance_creds['session_token'],
            region=file_management.region
        )

    @override_settings(S3_BUCKET='test-bucket')
    @patch('minio.Minio')
    def test_check_s3_bucket(self, mock_minio, file_management):
        """Test bucket existence check"""
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True
        mock_minio.return_value = mock_client

        file_management.client = mock_client
        result = file_management.check_s3_bucket()

        assert result is True
        mock_client.bucket_exists.assert_called_once_with(file_management.s3_bucket)

    @override_settings(S3_BUCKET='test-bucket')
    @patch('minio.Minio')
    def test_s3_file_exists(self, mock_minio, file_management):
        """Test S3 file existence check"""
        mock_client = MagicMock()
        mock_client.stat_object.return_value = True
        mock_minio.return_value = mock_client

        file_management.client = mock_client
        result = file_management.s3_file_exists("test-file.txt")

        assert result is True
        mock_client.stat_object.assert_called_once()

    @override_settings(S3_BUCKET='test-bucket')
    @patch('minio.Minio')
    def test_write_minio(self, mock_minio, file_management):
        """Test writing file to S3"""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        file_management.client = mock_client
        file_management.s3_path = "test-path"
        file_management.input_filename = "test.txt"
        file_management.input_path = "/tmp/"

        file_management.write_minio()

        mock_client.fput_object.assert_called_once_with(
            file_management.s3_bucket,
            "test-path/test.txt",
            "/tmp/test.txt"
        )

    @override_settings(S3_BUCKET='test-bucket')
    @patch('minio.Minio')
    def test_retrieve_minio(self, mock_minio, file_management):
        """Test retrieving file from S3"""
        mock_client = MagicMock()
        mock_minio.return_value = mock_client

        file_management.client = mock_client
        file_management.retrieve_minio("test-file.txt", "/tmp")

        mock_client.fget_object.assert_called_once()

import os
from minio import Minio
import yaml

def write_minio(path, filename, storage_url, bucket_name, prefix=""):
    '''
    Write a file to cloud storage 

    Parameters:
    path (str): Path to file to be copied  
    filename (str):  Name of file to to be copied.
    storage_url (str): URL for cloud storage.
    bucket_name (str):  Name of bucket where the file is to be written.
    prefix str):  Optional prefix for S3 path, e.g., gage id in s3://ngwpc-hydrofabric/06710385 
    
    Returns:  nothing
    '''

    #these access keys are for testing only.  This will be updated to use the AWS Secrets Manager
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    session_token = os.environ["AWS_SESSION_TOKEN"]

    client = Minio(storage_url, access_key, secret_key, session_token, region="us-east-1")

    if not prefix:
        object_name = filename
    else:
        object_name = prefix + '/' + filename

    if client.bucket_exists(bucket_name):
        result = client.fput_object(bucket_name, object_name, path + '/' + filename)
        status_string = "Hydrofabric data  written to " + object_name + " in bucket " + bucket_name
        print(status_string)
    else:
        error_string = "Bucket" + " " + bucket_name + " " + "does not exist"
        print(error_string)

def build_uri(bucket_name, prefix="", filename=""):
    ''' 
    Builds URI path from bucket name, prefix, and filename 

    Parameters:
    bucket_name (str):  Name of bucket where the file is to be written.
    prefix (str):  Optional prefix for S3 path, e.g., gage id in s3://ngwpc-hydrofabric/06710385 
    filename (str):  Optional filename

    Returns:  
    str:  URI for file
    '''      
   
    if not filename:
        object_name = prefix
    else:
        if not prefix:
            object_name = filename
        else:
            object_name = prefix + '/' + filename

    uri = "s3://" + bucket_name + "/" + object_name
    return uri        

def get_config():
    ''' 
    Load yaml config file 

    Parameters:
    None
    
    Returns:
    yaml object
    '''
    directory = os.getcwd() + "/config.yml"
    with open(directory, 'r') as file:
        config = yaml.safe_load(file)
    return config

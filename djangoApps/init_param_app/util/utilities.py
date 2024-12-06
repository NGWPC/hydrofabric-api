import os
from os.path import isfile, join

from minio import Minio
from django.conf import settings
import yaml
import logging

logger = logging.getLogger(__name__)

def get_config():
    ''' 
    Load yaml config file 

    Parameters:
    None
    
    Returns:
    yaml object
    '''
    # Get the grandparent directory of BASE_DIR
    grandparent_dir = os.path.dirname(settings.BASE_DIR)
    directory = os.path.join(grandparent_dir, "config.yml")

    with open(directory, 'r') as file:
        config = yaml.safe_load(file)
    return config

def get_hydrofabric_input_attr_file(version):
    config = get_config()
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = version
    hydrofabric_type = config['hydrofabric_type']

    grandparent_dir = os.path.dirname(settings.BASE_DIR)
    attr_file = os.path.join(grandparent_dir, hydrofabric_dir, hydrofabric_version, hydrofabric_type, 'conus_model-attributes')
    if not os.path.exists(attr_file):
        raise FileNotFoundError(f"Directory '{attr_file}' does not exist.")
    return attr_file


def get_subset_dir_file_names(subset_dir):
    """
    Lists only files in directory

    :param subset_dir:
    :return:
    """
    filename_list = [f for f in os.listdir(subset_dir) if isfile(join(subset_dir, f))]
    return filename_list

def get_hydrus_data():
    soil_param_name = "resources/vG_default_params_HYDRUS.dat"
    grandparent_dir = os.path.dirname(settings.BASE_DIR)
    soil_param_file = os.path.join(grandparent_dir, soil_param_name)
    return soil_param_file
    

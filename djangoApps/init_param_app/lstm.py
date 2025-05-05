import os
import logging

from .util.utilities import get_hydrofabric_input_attr_file, get_subset_dir_file_names
from .util.enums import FileTypeEnum
from .hf_attributes import *

logger = logging.getLogger(__name__)

def lstm_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    ''' 
    Build initial parameter estimates (IPE) for LSTM 

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    module = 'LSTM'
    filename_list = []

    divide_attr = get_hydrofabric_attributes(gpkg_file, version, domain)

    attr22 = {'divide_id':'divide_id', 'slope':'mean.slope', 'elevation_mean':'mean.elevation', 
              'lat':'centroid_y', 'lon':'centroid_x', 'area':'areasqkm'}

    attr21 =  {'divide_id':'divide_id', 'slope':'slope_mean', 'elevation_mean':'elevation_mean',
              'lat':'Y', 'lon':'X', 'area':'areasqkm'}

    if version == '2.2':
        attr = attr22
    elif version == '2.1':
        attr = attr21


    #Loop through catchments, get soil type, populate config file template, write config file to temp 
    for index, row in divide_attr.iterrows():

        catchment_id = row[attr['divide_id']]
        area = str(row[attr['area']])
        slope = str(row[attr['slope']])
        elev = str(row[attr['elevation_mean']])
        lat = str(row[attr['lat']])
        lon = str(row[attr['lon']])

        namelist = ['area_sqkm: ' + area,
                    'basin_id: ' + gage_id,
                    'basin_name:',
                    'elev_mean: ' + elev,
                    'initial_state: zero',
                    'lat: ' + lat,
                    'lon: ' + lon,
                    'slope_mean: ' + slope,
                    'timestep: 1 hour',
                    'train_cfg_file:',
                    'verbose: 0'
                    ]

        cfg_filename = f'{catchment_id}.yml'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.writelines('\n'.join(namelist))
                            outfile.write("\n")

    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list,
                                          module=module)
    status_str = 'Config files written to: ' + uri
    logger.info(status_str)

    #fill in parameter files uri 
    module_metadata['parameter_file']['uri'] = uri

    return module_metadata
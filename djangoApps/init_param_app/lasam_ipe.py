import os
import logging

import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from .util.utilities import get_hydrofabric_input_attr_file, get_subset_dir_file_names, get_hydrus_data
from .util.enums import FileTypeEnum
from .hf_attributes import *

logger = logging.getLogger(__name__)

def lasam_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt, dep_modules_included):
    ''' 
    Build initial parameter estimates (IPE) for the LASAM module

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    module = "LASAM"
    filename_list = []
    
    # Calibratable parameters
    # TODO - Use the system to get the "base" directory for uri below
    soil_param_file =  get_hydrus_data()       # Van Genuchton soil parameters 
    ponded_depth_max = "1.1[cm]"                                            # Maximum amount of water unavailable for surface drainage
    field_capacity_psi = "340.9[cm]"                                        # Capillary head corresponding to volumetric water content at which gravity drainage becomes slower
    soil_z = '10,30,100.0,200.0[cm]'

    # Skeleton for the config file. Needs layer soil types to be specified per-catchment
    lasam_lst = ['verbosity=none',
                 'soil_params_file=' + soil_param_file.split("/")[-1],
                 'layer_thickness=200.0[cm]',
                 'initial_psi=2000.0[cm]',
                 'timestep=300[sec]',
                 'endtime=1000[hr]',
                 'forcing_resolution=3600[sec]',
                 'ponded_depth_max=' + ponded_depth_max,
                 'use_closed_form_G=false',
                 'layer_soil_type=',
                 'max_soil_types=15',
                 'wilting_point_psi=15495.0[cm]',
                 'field_capacity_psi=' + field_capacity_psi,
                 'giuh_ordinates=0.06,0.51,0.28,0.12,0.03',
                 'calib_params=true',
                 'adaptive_timestep=true',
                 'sft_coupled=' + str('SFT' in dep_modules_included).lower(),
                 'soil_z=' + soil_z
                ]
    
    # Make directory
    if not os.path.isdir(subset_dir):
        os.makedirs(subset_dir)
    
    # Get divide attributes
    divide_attr = get_hydrofabric_attributes(gpkg_file, version)
    attr21 = {'soil_type':'ISLTYP'}
    attr22 = {'soil_type':'mode.ISLTYP'}

    if version == '2.1':
        attr = attr21
    elif version == '2.2':
        attr=attr22
    
    # Loop through catchments, get soil type
    for index, row in divide_attr.iterrows():
        catchment_id = str(row['divide_id'])
        soil_type = str(row[attr['soil_type']])
        lasam_lst_catID = lasam_lst.copy()
        lasam_lst_catID[9] = lasam_lst_catID[9] + soil_type
        
        lasam_bmi_file = os.path.join(subset_dir, catchment_id + '_bmi_config_lasam.txt')
        with open(lasam_bmi_file, "w") as f:
            f.writelines('\n'.join(lasam_lst_catID))
        filename_list.append(lasam_bmi_file.split('/')[-1])

    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    # Fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri

    return module_metadata

import os
import logging
import json

import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from collections import OrderedDict
from .util.utilities import get_hydrofabric_input_attr_file, get_subset_dir_file_names, get_config
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

def topmodel_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    ''' 
    Build initial parameter estimates (IPE) for TopModel

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    source (str):  Gage source, e.g., USGS
    domain (str):  Gage domain, e.g., CONUS
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    gpkg_file (str):  Path and filename of geopackage file 
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    gage_file_mgmt (object):  gage file management object
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    module = "TopModel"
    filename_list = []
 
    # Get list of catchments from gpkg divides layer using geopandas
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
            divides_geojson = divides_layer.to_json(to_wgs84 = True)
        except:
            # TODO: Replace 'except' with proper catch
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error = error_str) 
            logger.error(error_str)
            return error
    except:# TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        logger.error(error_str)
        return error
    
    #Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
    try:
        attr_file = get_hydrofabric_input_attr_file()
        attr = pq.read_table(attr_file)
    except FileNotFoundError as fnfe:
        logger.error(fnfe)
        error_str = 'Hydrofabric data input directory does not exist'
        error = dict(error=error_str)
        return error
    except Exception as exc:
        error_str = 'Error opening ' + attr_file
        error = dict(error = error_str)
        logger.error(error_str, exc)
        return error
    
    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)
    
    #filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catchments)]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error = error_str) 
        logger.error(error_str)
        return error

    for index, row in filtered.iterrows():

        #build subcatchment data
        #TWI values are from Hydrofabric divide attributes, num_channels, cum_dist_area_with_dist,
        #dist_from_outlet set per InitialParametersValueSources.xlsx
        num_sub_catchments = 1
        imap = 1
        yes_print_output = 1
        divide_id = row['divide_id']
        area = 1
        twi = json.loads(row['twi_dist_4'])
        num_topodex_values = len(twi)
        num_channels = 1
        cum_dist_area_with_dist = 1
        dist_from_outlet = 0
    
        subcat_line1 = f"{num_sub_catchments} {imap} {yes_print_output} \n"
        subcat_line2 = f"Extracted study basin:  {divide_id} \n"
        subcat_line3 = f"{num_topodex_values} {area} \n"
        subcat_line5 = f"{num_channels}\n"
        subcat_line6 = f"{cum_dist_area_with_dist} {dist_from_outlet}\n"

        df = pd.DataFrame(twi)

        cfg_filename_subcat = f'{divide_id}_subcat.dat'
        filename_list.append(cfg_filename_subcat)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename_subcat)
    
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.write(subcat_line1)
                            outfile.write(subcat_line2)
                            outfile.write(subcat_line3)
        df.to_csv(cfg_filename_path, mode='a', sep=' ', columns=['frequency', 'v'], index=False, header=False)
        with open(cfg_filename_path, 'a') as outfile:
            outfile.write(subcat_line5)
            outfile.write(subcat_line6)

        #Parameter values are set per InitialParametersValueSources.xlsx
        #Q0 and sr0 set to 0 recommended in email with Deltares
        params = OrderedDict()
        params['szm'] = "0.0125"
        params['t0'] = "0.000075"
        params['td'] = "20"
        params['chv'] = "1000"
        params['rv'] = "1000"
        params['srmax'] = "0.04"
        params['Q0'] = "0"
        params['sr0'] = "0"
        params['infex'] = "0"
        params['xk0'] = "2"
        params['hf'] = "0.1"
        params['dth'] = "0.1"

        line1 = divide_id + '\n'
        line2 = " ".join(f'{v}' for k,v in params.items())

        cfg_filename = f'{divide_id}_params.dat'
        filename_list.append(cfg_filename)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.write(line1)
                            outfile.write(line2)

        # Create primary configuration file
        stand_alone = '0\n'  #  Set to false for BMI
        title = f'{divide_id}\n'
        input_fptr = 'input.dat\n'
        subcat_fptr = f'data/{cfg_filename_subcat}\n'
        params_fptr = f'data/{cfg_filename}\n'
        output_fptr = f'{divide_id}_topmod.out\n'
        out_hyd_fptr = f'{divide_id}_hyd.out\n'

        cfg_filename_run = f'{divide_id}_topmodel.run'
        filename_list.append(cfg_filename_run)
        cfg_filename_path = os.path.join(subset_dir, cfg_filename_run)
        with open(cfg_filename_path, 'w') as outfile:
                            outfile.write(stand_alone)
                            outfile.write(title)
                            outfile.write(input_fptr)
                            outfile.write(subcat_fptr)
                            outfile.write(params_fptr)
                            outfile.write(output_fptr)
                            outfile.write(out_hyd_fptr)
         
    # Write files to DB and S3
    print(FileTypeEnum.PARAMS)
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list, module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)
 
    #fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri
    
    # Get default values for calibratable initial parameters.
    for x in range(len(module_metadata["calibrate_parameters"])):

            module_metadata["calibrate_parameters"][x]["initial_value"] = params[module_metadata["calibrate_parameters"][x]["name"]]
    
    return module_metadata
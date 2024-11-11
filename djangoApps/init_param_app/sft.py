import copy
import os
import logging
import json
from pathlib import Path
import fnmatch
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from .util.utilities import *
#from utilities import *
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

#def sft_ipe(gage_id, subset_dir, module_metadata_list, module_metadata, gpkg_file):
def sft_ipe(module, gage_id, source, domain, subset_dir, gpkg_file, modules, module_metadata, gage_file_mgmt):
    '''
        Build initial parameter estimates (IPE) for snow freeze thaw (SFT)

        Parameters:
        gage_id (str):  The gage ID, e.g., 06710385
        subset_dir (str):  Path to gage id directory where the module directory will be made.
        module_metadata_list (dict):  list dictionary containing URI, initial parameters, output variables

        Returns:
        dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Setup logging
    #logger = logging.getLogger(__name__)

    # get attrib file
    attr_file = get_hydrofabric_input_attr_file()

    # setup output dir
    # first save the top level dir for the gpkg
    #gpkg_dir = subset_dir

    # Get list of catchments from gpkg divides layer using geopandas
    ##file = 'Gage_6700000.gpkg'
    #gpkg_file = "Gage_" + gage_id.lstrip("0") + ".gpkg"
    #gpkg_file = os.path.join(gpkg_dir, gpkg_file)
    ##divides_layer = gpd.read_file(gpkg_file, layer="divides")
    ##catchments = divides_layer["divide_id"].tolist()
    ##areas = divides_layer["areasqkm"].tolist()

    try:
        divides_layer = gpd.read_file(gpkg_file, layer="divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
            areas = divides_layer["areasqkm"].tolist()
        except:
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error=error_str)
            print(error_str)
            logger.error(error_str)
            return error
    except:
        error_str = 'Error opening ' + gpkg_file
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    # data = gpd.read_file(gpkg_file, layer="divides") #This info is a dupe of divides_layer above
    catch_dict = {}
    #for index, row in data.iterrows():
    for index, row in divides_layer.iterrows():
        #print(row['divide_id'], row['areasqkm'])
        catch_dict[str(catchments[index])] = {"areasqkm": str(areas[index])}

    response = create_sft_input(gage_id, source, domain, catch_dict, attr_file, subset_dir, modules, module_metadata, gage_file_mgmt)
    logger.info("sft::sft_ipe:returning response as " + str(response))

    # TODO Returning just the "first" record, does not match Swagger docs. Verify!
    # TODO if returning the full list, it will break json.JSONDecoder in initial_parameters as it expects a dict not a list of dicts
    return response[0]


def create_sft_input(gage_id, source, domain, catch_dict, attr_file, output_dir, modules, module_metadata, gage_file_mgmt):
    #os.makedirs(sft_dir, exist_ok=True)
    #os.makedirs(smp_dir, exist_ok=True)
    #this dir should exist here already, but just in case...
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    try:
        attr = pq.read_table(attr_file)
    except:
        error_str = 'Error opening ' + attr_file
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)

    # filter rows with catchments in gpkg
    filtered = attr_df[attr_df['divide_id'].isin(catch_dict.keys())]

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

    # Read attribute file to obtain quartz
    #dfa = pd.read_parquet(attr_file)
    #dfa.set_index('divide_id', inplace=True)

    # Ice fraction scheme
    #icefscheme = 'Schaake'
    #if ('cfe.xaj' in modules):
    #    icefscheme = 'Xinanjiang'
    if 'CFE-S' in modules:
        icefscheme = 'Schaake'
    else:
        icefscheme = 'Xinanjiang'

    response = []

    # for key in catch_dict.keys(): LOOP CATCH_IDs HERE (from filtered dataframe)!!
    for index, row in filtered.iterrows():

        catID = row['divide_id']

        # Read cfe BMI files to obtain annual mean surface temperature as proxy for initial soil temperature
        # OBE - This is now just being set as reasonable estimate (from Edwin)
        ##cfe_bmi_file = os.path.join(cfe_dir, fnmatch.filter(os.listdir(cfe_dir), '*' + catID + '*.txt')[0])
        #cfe_bmi_file = f"{output_dir}/{catID}_bmi_config.ini"    ##'/home/james.matera/DATA/Hydrofabric/data/temp/cat-1562743_bmi_config_sft.txt'
        #df = pd.read_table(cfe_bmi_file, delimiter='=', names=["Params", "Values"], index_col=0)

        # Obtain annual mean surface temperature as proxy for initial soil temperature
        ##csv_file = f"lump_forcing_csv_dir-{catID}.csv"
        ##fdf = pd.read_table(os.path.join(lump_forcing_csv_dir, catID + '.csv'), delimiter=',') # orig code from YL
        ##fdf = pd.read_table(csv_file)
        ##mtemp = round(fdf['T2D'].mean(), 2)
        # This value is just a reasonable estimate per new direction (Edwin)
        mtemp = (45 - 32) * 5/9 + 273.15  ##this is avg soil temp of 45 degrees F converted to Kelvin

        # loop items in row to set/calc sft values
        smcmax_val = bexp_val = psisat_val = quartz_val = 0.0
        smcmax_count = bexp_count = psisat_count = quartz_count = 0
        for key, value in row.items():
            if key.startswith('smcmax'):
                smcmax_val += value
                smcmax_count += 1
            elif key.startswith('bexp'):
                bexp_val += value
                bexp_count += 1
            elif key.startswith('psisat'):
                psisat_val += value
                psisat_count += 1
            elif key.startswith('quartz'):
                quartz_val += value
                quartz_count += 1

        # get avgs for loop items
        smcmax_avg = smcmax_val / smcmax_count
        bexp_avg = bexp_val / bexp_count
        psisat_avg = psisat_val / psisat_count
        quartz_avg = quartz_val / quartz_count

        # Create sft param list
        param_list = ['verbosity=none',
                      'soil_moisture_bmi=1',
                      'end_time=1.[d]',
                      'dt=1.0[h]',
                      'soil_params.smcmax=' + str(smcmax_avg),
                      'soil_params.b=' + str(bexp_avg),
                      'soil_params.satpsi=' + str(psisat_avg),
                      'soil_params.quartz=' + str(quartz_avg),
                      'ice_fraction_scheme=' + icefscheme,
                      'soil_z=0.1,0.3,1.0,2.0[m]',
                      #'soil_temperature=' + ','.join([str(mtemp)] * 4) + '[K]',
                      'soil_temperature=' + str(mtemp) + '[K]'
                      ]

        module_metadata_rec = set_ipe_json_values(param_list, module_metadata, sep="=")

        sft_bmi_file = os.path.join(output_dir, catID + '_bmi_config_sft.txt')
        with open(sft_bmi_file, "w") as f:
            f.writelines('\n'.join(param_list))

        # put all files to write to S3 in a list
        s3_file_list = [os.path.basename(sft_bmi_file)]

        # Write files to DB and S3
        module_name = module_metadata["module_name"]
        uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source,
                                              output_dir, s3_file_list, module=module_name)

        # for idx in range(len(s3_file_list)):
        #     # Write geopackage to s3 bucket
        #     if s3prefix:
        #         subset_s3prefix = s3prefix + "/" + gage_id + "/SFT"
        #     else:
        #         subset_s3prefix = gage_id + "/SFT"
        #
        #     # subset_dir_full = os.path.join(output_dir, s3_file_list[idx])
        #
        #     write_minio(output_dir, s3_file_list[idx], s3url, s3bucket, subset_s3prefix)
        #     # uri = build_uri(s3bucket, s3prefix, s3_file_list[idx])  #do not use filename, only dir
        #     uri = build_uri(s3bucket, subset_s3prefix)
        #     status_str = "Written to S3 bucket: " + str(uri)
        #     print(status_str)

        module_metadata_rec['parameter_file']['uri'] = uri
        logger.info("Written to S3 bucket: " + str(uri))

        deep_copy_ipe_dict = copy.deepcopy(module_metadata_rec)
        response.append(deep_copy_ipe_dict)
        logger.info("sft:create_sft_input:appended deep copy dict to response " + str(deep_copy_ipe_dict))

    return response


def set_ipe_json_values(param_list, module_metadata_rec, sep=' ')-> dict:
    # convert param list to dict to make it searchable by key
    param_list_dict = {}
    for idx in range(len(param_list)):
        #key_value_split = str.split(param_list[idx], ' ')
        key_value_split = str.split(param_list[idx], sep)
        param_list_dict[key_value_split[0]] = key_value_split[1]

    for idx in range(len(module_metadata_rec['calibrate_parameters'])):
        key_name = module_metadata_rec['calibrate_parameters'][idx]['name']
        module_metadata_rec['calibrate_parameters'][idx]['initial_value'] = param_list_dict[key_name]

    logger.info(f"{module_metadata_rec['module_name']}::set_ipe_json_values: set the ipe initial values " + str(module_metadata_rec))
    return module_metadata_rec


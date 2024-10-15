import copy
import geopandas as gpd
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from .util.enums import FileTypeEnum
from .util.utilities import *


logger = logging.getLogger(__name__)


def snow17_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    '''
        Build initial parameter estimates (IPE) for Snow17

        Parameters:
        gage_id (str):  The gage ID, e.g., 06710385
        subset_dir (str):  Path to gage id directory where the module directory will be made.
        module_metadata (dict):  list dictionary containing URI, initial parameters, output variables

        Returns:
        dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # get attrib file
    attr_file = get_hydrofabric_input_attr_file()

    # setup output dir
    # first save the top level dir for the gpkg
    #gpkg_dir = subset_dir

    # Get list of catchments from gpkg divides layer using geopandas
    divides_layer = gpd.read_file(gpkg_file, layer="divides")
    catchments = divides_layer["divide_id"].tolist()
    areas = divides_layer["areasqkm"].tolist()

    data = gpd.read_file(gpkg_file, layer="divides")
    catch_dict = {}
    for index, row in data.iterrows():
        #print(row['divide_id'], row['areasqkm'])
        catch_dict[str(catchments[index])] = {"areasqkm": str(areas[index])}

    response = create_snow17_input(gage_id, source, domain, catch_dict, attr_file, subset_dir, module_metadata, gage_file_mgmt)
    logger.info("snow_17::snow17_ipe:returning response as " + str(response))
    return response


def create_snow17_input(gage_id, source, domain, catch_dict, attr_file, snow17_output_dir: str, module_metadata, gage_file_mgmt):
    #if not os.path.exists(snow17_output_dir):
    #    os.makedirs(snow17_output_dir, exist_ok=True)
    # TODO: Make Constant or StrEnum
    module = 'SNOW17'
    try:
        attr = pq.read_table(attr_file)
    except:
        # TODO: Replace 'except' with proper catch
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

    # Read hydrofabric attribute file (THESE ARE NOT USED BY SNOW17)
    #dfa = pd.read_parquet(attr_file)
    #dfa.set_index("divide_id", inplace=True)

    response = []
    filename_list =[]
    #for key in catch_dict.keys(): LOOP CATCH_IDs HERE (from filtered dataframe)!!
    for index, row in filtered.iterrows():
        catchment_id = row['divide_id']
        param_list = ['hru_id ' + str(catchment_id),
                      'hru_area ' + str(catch_dict[str(catchment_id)]['areasqkm']),
                      'latitude ' + str(row['Y']),
                      'elev ' + str(row['elevation_mean']),  #elevation_mean[1]
                      'scf 2.15177',
                      'mfmax 0.930472',
                      'mfmin 0.137',
                      'uadj 0.003103',
                      'si 1515.00',
                      'pxtemp 0.713424',
                      'nmf 0.150',
                      'tipm 0.200',
                      'mbase 0.000',
                      'plwhc 0.030',
                      'daygm 0.300',
                      'adc1 0.050',
                      'adc2 0.090',
                      'adc3 0.160',
                      'adc4 0.310',
                      'adc5 0.540',
                      'adc6 0.740',
                      'adc7 0.840',
                      'adc8 0.890',
                      'adc9 0.930',
                      'adc10 0.970',
                      'adc11 1.000']

        # NOTE: use record 0 from module_metadata as a template, then append a deep copy to response at end of
        #       this method and return
        module_metadata_rec = set_ipe_json_values(param_list, module_metadata[0])
        input_file = os.path.join(snow17_output_dir, 'snow17-init-' + str(catchment_id) + '.namelist.input')
        param_file = os.path.join(snow17_output_dir, 'snow17_params-' + str(catchment_id) + '.HHWM8.txt')

        with open(param_file, "w") as f:
            f.writelines('\n'.join(param_list))

        input_list = ['&SNOW17_CONTROL',
                      '! === run control file for snow17bmi v. 1.x ===',
                      '',
                      '! -- basin config and path information',
                      'main_id             = "' + str(catchment_id) + '"     ! basin label or gage id',
                      'n_hrus              = 1            ! number of sub-areas in model',
                      'forcing_root        = "extern/snow17/test_cases/ex1/input/forcing/forcing.snow17bmi."',
                      'output_root         = "data/output/output.snow17bmi."',
                      'snow17_param_file   = "' + param_file.rsplit('/')[-1] + '"',
                      'output_hrus         = 1            ! output HRU results? (1=yes; 0=no)',
                      '',
                      '! -- run period information',
                      'start_datehr        = 2017120101   ! start date time, backward looking (check)',
                      'end_datehr          = 2017120123   ! end date time',
                      'model_timestep      = 3600        ! in seconds (86400 seconds = 1 day)',
                      '',
                      '! -- state start/write flags and files',
                      'warm_start_run      = 0  ! is this run started from a state file?  (no=0 yes=1)',
                      "write_states        = 0  ! write restart/state files for 'warm_start' runs (no=0 yes=1)",
                      '',
                      '! -- filenames only needed if warm_start_run = 1',
                      'snow_state_in_root  = "data/state/snow17_states."  ! input state filename root',
                      '',
                      '! -- filenames only needed if write_states = 1',
                      'snow_state_out_root = "data/state/snow17_states."  ! output states filename root',
                      '/',
                      ''
                      ]
        with open(input_file, "w") as f:
            f.writelines('\n'.join(input_list))

        # get config info

        # put all file names to write to S3 in a list
        filename_list.extend((input_file.rsplit('/')[-1], param_file.rsplit('/')[-1]))

        deep_copy_ipe_dict = copy.deepcopy(module_metadata_rec)
        response.append(deep_copy_ipe_dict)
        logger.info("snow17:create_snow17_input:appended deep copy dict to response " + str(deep_copy_ipe_dict))

    # Write files to DB and S3
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, snow17_output_dir, filename_list,
                                              module=module)
    response[0]['parameter_file']['uri'] = uri
    return response


def set_ipe_json_values(param_list, module_metadata_rec) -> dict:
    # convert param list to dict to make it searchable by key
    param_list_dict = {}
    for idx in range(len(param_list)):
        key_value_split = str.split(param_list[idx], ' ')
        param_list_dict[key_value_split[0]] = key_value_split[1]

    for idx in range(len(module_metadata_rec['calibrate_parameters'])):
        key_name = module_metadata_rec['calibrate_parameters'][idx]['name']
        module_metadata_rec['calibrate_parameters'][idx]['initial_value'] = param_list_dict[key_name]

    logger.info("snow17::set_ipe_json_values: set the ipe initial values " + str(module_metadata_rec))
    return module_metadata_rec

'''
Subsets the hydrofabric by gage ID and creates BMI config files with initial parameters.

Inputs:
    gage_id:  a gage_id, e.g., "01123000"
           Look at using the Python rpy2 package for R function calls as the Rscript method is not the best. 

    output_dir: Absolute path to directory where input and output data will be stored.  The directory structure will
    change as the Hydrofabric and NGEN design is refined.  

Outputs:
    Outputs are written to output_dir:  Hydrofabric Subset files (Gage-xxxxxxxx.gpkg) and BMI config files (e.g., cat-10617_bmi_config.ini)
    in CFE-S subdirectory.   
'''

import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa

from .util.enums import FileTypeEnum
from init_param_app.util.utilities_transform import *
from .util.utilities import *


#setup logging
logger = logging.getLogger(__name__)


def sac_sma_ipe(gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    # setup output dir
    #config = get_config()
    #output_dir = config['output_temp_dir']
    #hydrofabric_dir = config['hydrofabric_dir']
    #hydrofabric_version = config['hydrofabric_version']
    #hydrofabric_type = config['hydrofabric_type']

    # get attrib file
    attr_file = get_hydrofabric_input_attr_file()

    # setup output dir
    # first save the top level dir for the gpkg
    #gpkg_dir = subset_dir
  
    #gpkg_file = "Gage_" + gage_id.lstrip("0") + ".gpkg"
    #gpkg_file = os.path.join(gpkg_dir, gpkg_file)
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = "divides")
        try:
            catchments = divides_layer["divide_id"].tolist()
        except:
            # TODO: Replace 'except' with proper catch
            error_str = 'Error reading divides layer in ' + gpkg_file
            error = dict(error = error_str)
            logger.error(error_str)
            return error
    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str) 
        logger.error(error_str)
        return error

 
    areas = divides_layer["areasqkm"].tolist()

    data = gpd.read_file(gpkg_file, layer="divides")
    catch_dict = {}
    for index, row in data.iterrows():
        catch_dict[str(catchments[index])] = {"areasqkm": str(areas[index])}

    response = create_sac_sma_input(gage_id, source, domain, catch_dict, attr_file, subset_dir, module_metadata, gage_file_mgmt)

    return response

# Provide date
#def create_snow17_input(catids: List[str], snow17_input_dir: str)->None:
def create_sac_sma_input(gage_id, source, domain, catch_dict, attr_file, subset_dir: str, module_metadata, gage_file_mgmt):
    # TODO: Make Constant or StrEnum
    module = "Sac-SMA"
    #Get s3 bucket configurations
    #config = get_config()
    #s3url = config['s3url']
    #s3bucket = config['s3bucket']
    #s3prefix = config['s3prefix']

    #subset_dir = os.path.join(subset_dir, 'Sac-SMA')
    #if not os.path.exists(subset_dir):
    #    os.makedirs(subset_dir, exist_ok=True)
    try:
        attr = pq.read_table(attr_file)
    except:
        # TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + attr_file
        error = dict(error=error_str)
        logger.error(error_str)
        return error

    attr = attr.drop_null()
    attr_df = pa.Table.to_pandas(attr)

    logger.debug(catch_dict)
    #exit()
    # filter rows with catchments in gpkg
    #for idx in catchment_list =
    filtered = attr_df[attr_df['divide_id'].isin(catch_dict.keys())]
    logger.debug(filtered)

    if len(filtered) == 0:
        error_str = 'No matching catchments in attribute file'
        error = dict(error=error_str)
        logger.error(error_str)
        return error

    # Read hydrofabric attribute file (THESE ARE NOT USED BY SAC_SMA)
    #dfa = pd.read_parquet(attr_file)
    #dfa.set_index("divide_id", inplace=True)

    '''REFERENCE param_list
    param_list = ['hru_id HHWM8IL HHWM8IU',
            'hru_area 2994.7 1271.3',
            'uztwm 29.7257 31.9842',
            'uzfwm 22.8335 86.7465',
            'lztwm 18.6968 105.763',
            'lzfpm 419.418 956.052',
            'lzfsm 215.932 212.664',
            'adimp 0.0000 0.0000',
            'uzk 0.8910 0.9266',
            'lzpk 0.0032 0.0037',
            'lzsk 0.2551 0.2633',
            'zperc 281.8200 267.7290',
            'rexp 5.2353 5.0608',
            'pctim 0.0000 0.0000',
            'pfree 0.3142 0.2880',
            'riva 0.0100 0.0100',
            'side 0.0000 0.0000',
            'rserv 0.3000 0.3000']
    '''

    #  Get the initial params names from DB
    #for key in catch_dict.keys():
    # In the future the constant values will be replaced by catchment ID computation as suggested by Marl's email
    #Loop through catchments, get soil type, populate config file template, write config file to temp
    filename_list = []
    param_list = []
    for index, row in filtered.iterrows():
        catchment_id = row['divide_id']
        param_list = ['hru_id ' + str(catchment_id),
                      'hru_area ' + str(catch_dict[str(catchment_id)]['areasqkm']),
                      'latitude ' + str(row['Y']),
                      'longtitude ' + str(row['X']),                     
                      'uztwm 29.7257',
                      'uzfwm 22.8335',
                      'lztwm 18.6968',
                      'lzfpm 419.418',
                      'lzfsm 215.932',
                      'adimp 0.0000',
                      'uzk 0.8910',
                      'lzpk 0.0032',
                      'lzsk 0.2551',
                      'zperc 281.8200',
                      'rexp 5.2353',
                      'pctim 0.0000',
                      'pfree 0.3142',
                      'riva 0.0100',
                      'side 0.0000',
                      'rserv 0.3000']
        # Loop through the list and check for the desired parameters
        for i, param in enumerate(param_list):
            if param.startswith('latitude'):
                latitude = param.split()[1]  # Extract the value after 'latitude'
                latitude = float(latitude)
            elif param.startswith('longtitude'):
                longitude = param.split()[1]  # Extract the value after 'longtitude'
                longitude =  float(longitude)
            elif param.startswith('uztwm'):
                uztwm_default = param.split()[1]  # Extract the value after 'uztwm'
                uztwm = getValueForLatLon_point(latitude, longitude, 'uztwm')
                if uztwm == -1:
                    uztwm = uztwm_default
                param_list[i] = f'uztwm {uztwm}'
            elif param.startswith('uzfwm'):
                uzfwm_default = param.split()[1]  # Extract the value after 'uzfwm'
                uzfwm = getValueForLatLon_point(latitude, longitude, 'uzfwm')
                if uzfwm == -1:
                    uzfwm = uzfwm_default
                param_list[i] = f'uzfwm {uzfwm}'
            elif param.startswith('lztwm'):
                lztwm_default = param.split()[1]  # Extract the value after 'lztwm'
                lztwm = getValueForLatLon_point(latitude, longitude, 'lztwm')
                if lztwm == -1:
                    lztwm = lztwm_default
                param_list[i] = f'lztwm {lztwm}'
            elif param.startswith('lzfpm'):
                lzfpm_default = param.split()[1]  # Extract the value after 'lzfpm'
                logger.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                logger.debug("lzfpm_default: " + lzfpm_default)
                lzfpm = getValueForLatLon_point(latitude, longitude, 'lzfpm')
                logger.debug("lzfpm: " + str(lzfpm))
                logger.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                if lzfpm == -1:
                    lzfpm = lzfpm_default
                param_list[i] = f'lzfpm {lzfpm}'
                logger.debug("lzfpm: " + str(lzfpm))
                logger.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            elif param.startswith('lzfsm'):
                lzfsm_default = param.split()[1]  # Extract the value after 'lzfsm'
                lzfsm = getValueForLatLon_point(latitude, longitude, 'lzfsm')
                if lzfsm == -1:
                    lzfsm = lzfsm_default
                param_list[i] = f'lzfsm {lzfsm}'
            elif param.startswith('adimp'):
                adimp_default = param.split()[1]  # Extract the value after 'adimp'
                adimp = getValueForLatLon_point(latitude, longitude, 'adimp')
                if adimp == -1:
                    adimp = adimp_default
                param_list[i] = f'adimp {adimp}'
            elif param.startswith('uzk'):
                uzk_default = param.split()[1]  # Extract the value after 'uzk'
                uzk = getValueForLatLon_point(latitude, longitude, 'uzk')
                if uzk == -1:
                    uzk = uzk_default
                param_list[i] = f'uzk {uzk}'
            elif param.startswith('lzpk'):
                lzpk_default = param.split()[1]  # Extract the value after 'lzpk'
                lzpk = getValueForLatLon_point(latitude, longitude, 'lzpk')
                if lzpk == -1:
                    lzpk = lzpk_default
                param_list[i] = f'lzpk {lzpk}'
            elif param.startswith('lzsk'):
                lzsk_default = param.split()[1]  # Extract the value after 'lzsk'
                lzsk = getValueForLatLon_point(latitude, longitude, 'lzsk')
                if lzsk == -1:
                    lzsk = lzsk_default
                param_list[i] = f'lzsk {lzsk}'
            elif param.startswith('zperc'):
                zperc_default = param.split()[1]  # Extract the value after 'zperc'
                zperc = getValueForLatLon_point(latitude, longitude, 'zperc')
                if zperc == -1:
                    zperc = zperc_default
                param_list[i] = f'zperc {zperc}'
            elif param.startswith('rexp'):
                rexp_default = param.split()[1]  # Extract the value after 'rexp'
                rexp = getValueForLatLon_point(latitude, longitude, 'rexp')
                if rexp == -1:
                    rexp = rexp_default
                param_list[i] = f'rexp {rexp}'
            elif param.startswith('pctim'):
                pctim_default = param.split()[1]  # Extract the value after 'pctim'
                pctim = getValueForLatLon_point(latitude, longitude, 'pctim')
                if pctim == -1:
                    pctim = pctim_default
                param_list[i] = f'pctim {pctim}'
            elif param.startswith('pfree'):
                pfree_default = param.split()[1]  # Extract the value after 'pfree'
                pfree = getValueForLatLon_point(latitude, longitude, 'pfree')
                if pfree == -1:
                    pfree = pfree_default
                param_list[i] = f'pfree {pfree}'
            elif param.startswith('riva'):
                riva_default = param.split()[1]  # Extract the value after 'riva'
                riva = getValueForLatLon_point(latitude, longitude, 'riva')
                if riva == -1:
                    riva = riva_default
                param_list[i] = f'riva {riva}'
            elif param.startswith('side'):
                side_default = param.split()[1]  # Extract the value after 'side'
                side = getValueForLatLon_point(latitude, longitude, 'side')
                if side == -1:
                    side = side_default
                param_list[i] = f'side {side}'
            elif param.startswith('rserv'):
                rserv_default = param.split()[1]  # Extract the value after 'rserv'
                rserv = getValueForLatLon_point(latitude, longitude, 'rserv')
                if rserv == -1:
                    rserv = rserv_default
                param_list[i] = f'rserv {rserv}'


        #for catID in catids:

        param_file = os.path.join(subset_dir, 'sac_sma_params-' + str(catchment_id) + '.HHWM8.txt')

        with open(param_file, "w") as f:
            f.writelines('\n'.join(param_list))

        input_list = ['&SAC_CONTROL',
                '! === run control file for sac17bmi v. 1.x ===',
                '',
                '! -- basin config and path information',
                'main_id             = "' + str(catchment_id) + '"     ! basin label or gage id',
                'n_hrus              = 1            ! number of sub-areas in model',
                'forcing_root        = "extern/sac-sma/sac-sma/test_cases/ex1/input/forcing/forcing.sacbmi."',
                'output_root         = "data/output/output.sacbmi."',
                'sac_param_file      = "' + param_file.rsplit('/')[-1] + '"',
                'output_hrus         = 0            ! output HRU results? (1=yes; 0=no)',
                '',
                '! -- run period information',
                'start_datehr        = 2015120112   ! start date time, backward looking (check)',
                'end_datehr          = 2015123012   ! end date time',
                'model_timestep      = 3600        ! in seconds (86400 seconds = 1 day)',
                '',
                '! -- state start/write flags and files',
                'warm_start_run uztwm 29.7257',
                      'uzfwm 22.8335',
                      'lztwm 18.6968',
                      'lzfpm 419.418',
                      'lzfsm 215.932',
                      'adimp 0.0000',
                      'uzk 0.8910',
                      'lzpk 0.0032',
                      'lzsk 0.2551',
                      'zperc 281.8200',
                      'rexp 5.2353',
                      'pctim 0.0000',
                      'pfree 0.3142',
                      'riva 0.0100',
                      'side 0.0000',
                      'rserv 0.3000'
                '! -- filenames only needed if warm_start_run = 1',
                'sac_state_in_root   = "data/state/sac_states."  ! input state filename root',
                '',
                '! -- filenames only needed if write_states = 1',
                'sac_state_out_root = "data/state/sac_states."  ! output states filename root',
                '/',
                ''
                ]
        input_file = os.path.join(subset_dir, 'sac_sma-init-' + str(catchment_id) + '.namelist.input')
        with open(input_file, "w") as f:
            f.writelines('\n'.join(input_list))

        # Collect all the file names to save to DB and S3
        filename_list.extend((input_file.rsplit('/')[-1], param_file.rsplit('/')[-1]))

    #if s3prefix:
    #    subset_s3prefix = s3prefix + "/" + gage_id + 'Sac_SMA'
    #else:
    #    subset_s3prefix = gage_id  + '/' + 'Sac_SMA'

    #Get list of .input files in temp directory and copy to s3
    #files = Path(subset_dir).glob('*.input')
    
    #for file in files:
    #    logger.debug("writing: " + str(file) + " to s3")
    #    file_name = os.path.basename(file)
    #    write_minio(subset_dir, file_name, s3url, s3bucket, subset_s3prefix)

    #uri = build_uri(s3bucket, subset_s3prefix)
    uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list,
                                              module=module)
    status_str = "Config files written to:  " + uri
    logger.info(status_str)

    #fill in parameter files uri 
    module_metadata[0]["parameter_file"]["uri"] = uri

    # Temp Hack till we get the initial values based on Mark's email and list of documents
    # Create an empty dictionary
    # initial_value_dict={'uztwm' : '29.7257',
    #                   'uzfwm' :  '22.8335',
    #                   'lztwm' :  '18.6968',
    #                   'lzfpm' :  '419.418',
    #                   'lzfsm' :  '215.932',
    #                   'adimp' :  ' 0.0000',
    #                   'uzk'   :  ' 0.8910',
    #                   'lzpk'  :  ' 0.0032',
    #                   'lzsk'  :  ' 0.2551',
    #                   'zperc' :  ' 281.8200',
    #                   'rexp'  :  ' 5.2353',
    #                   'pctim' :  ' 0.0000',
    #                   'pfree' :  ' 0.3142',
    #                   'riva'  :  ' 0.0100',
    #                   'side'  :  ' 0.0000',
    #                   'rserv' :  ' 0.3000'
    #                   } 
    initial_value_dict = {}
    for item in param_list:
        key, value = item.split(" ", 1)  # Split on the first space
        initial_value_dict[key] = float(value) if value.replace('.', '', 1).isdigit() else value  # Convert to float if numeric

    # Output the resulting dictionary
    logger.debug(initial_value_dict)

    # Get default values for calibratable initial parameters from initial_value_dict.

    # Check if "calibrate_parameters" exists in combined_data
    logger.debug(type(module_metadata[0]["calibrate_parameters"]))
    if "calibrate_parameters" in module_metadata[0]:
        logger.debug("Yes calibrate_parameters is in module_metadata[0]")
        for idx in range(len(module_metadata[0]["calibrate_parameters"])):
            param = module_metadata[0]["calibrate_parameters"][idx]  # Access the list element by index
            # Now access dictionary keys inside the parameter
            key_name = param.get("name")  # Assuming each list element is a dictionary
            logger.debug(f"Parameter {idx}: {key_name}")
            #key_name = module_metadata['calibrate_parameters'][idx]['name']
            module_metadata[0]["calibrate_parameters"][idx]["initial_value"] = initial_value_dict[key_name]        
        
    return module_metadata

    
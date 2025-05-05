import collections
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
from collections import OrderedDict
from .DatabaseManager import DatabaseManager
from .cfe import *
from .noah_owp_modular import *
from .t_route import *
from .sac_sma import *
from .snow17 import *
from .topmodel import *
from .sft import *
from .smp import *
from .ueb import UEB
from .lasam_ipe import *
from .topoflow import TopoFlow
from .pet_ipe import *
from .lstm import *

# Setup logging
logger = logging.getLogger(__name__)


def get_ipe(gage_id, version, source, domain, modules, gage_file_mgmt):
    '''
    Build initial parameter estimates (IPE) for a module.  

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    modules (str): Module names

    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Build path for IPE temp directory
    subset_dir = gage_file_mgmt.get_local_temp_directory(FileTypeEnum.PARAMS, gage_id)
    gpkg_dir = gage_file_mgmt.get_local_temp_directory(FileTypeEnum.GEOPACKAGE, gage_id)
    gpkg_file = gage_file_mgmt.get_geopackage_filename(gage_id)
    gpkg_file = os.path.join(gpkg_dir, gpkg_file)
    module_results = None

    dependent_module_list = ["SFT","SMP"]
    dep_modules_included = list(set(modules).intersection(set(dependent_module_list)))

    module_output_list = []
    for module in modules:
        found, ipe_json = gage_file_mgmt.ipe_files_exists(gage_id, version, domain, source, module)
        if not found:
            if module in dependent_module_list:
                module_results = calculate_dependent_module_params(gage_id, version, source, domain, module, modules,
                                                                   subset_dir, gpkg_file, gage_file_mgmt)
            else:
                module_results = calculate_module_params(gage_id, version, source, domain, module, subset_dir, gpkg_file, gage_file_mgmt, dep_modules_included)

            if 'error' not in module_results:
                # TODO: Remove PET module stipulation when the module is implemented
                if module != "PET":
                    # add ipe_json to Database
                    hffiles_row = gage_file_mgmt.get_db_object()
                    hffiles_row.ipe_json = json.dumps(module_results)
                    hffiles_row.save()
            else:
                error_str = module_results['error']
                logger.error(error_str)
        else:
            # Found IPE data file, clean and add to response list
            decoder = json.JSONDecoder(object_pairs_hook=collections.OrderedDict)
            module_results = decoder.decode(ipe_json)

        module_output_list.append(module_results)

    module_output_list = {"modules": module_output_list}

    gage_file_mgmt.delete_local_temp_directory(subset_dir)
    gage_file_mgmt.delete_local_temp_directory(gpkg_dir)    
    return Response(module_output_list, status=status.HTTP_200_OK)


def calculate_dependent_module_params(gage_id, version, source, domain, module, modules, subset_dir, gpkg_file, gage_file_mgmt):
    subset_dir = os.path.join(subset_dir, module)
    if not os.path.exists(subset_dir):
        os.mkdir(subset_dir)
    #Add the trailing /
    subset_dir += "/"
    module_metadata = get_module_metadata(module)
    logger.debug(module_metadata)
    logger.info(f"Get IPEs for {module} module")

    if module == "SFT":
        results = sft_ipe(module, gage_id, version, source, domain, subset_dir,
                          gpkg_file, modules, module_metadata, gage_file_mgmt)
    elif module == "SMP":
        results = smp_ipe(module, gage_id, version, source, domain, subset_dir,
                          gpkg_file, modules, module_metadata, gage_file_mgmt)

    else:
        results = module_json(module, [], [], error=f"module '{module}' does not exist")

    return results


def calculate_module_params(gage_id, version, source, domain, module, subset_dir, gpkg_file, gage_file_mgmt, dep_modules_included):
    subset_dir = os.path.join(subset_dir, module)
    if not os.path.exists(subset_dir):
        os.mkdir(subset_dir)
    #Add the trailing /
    subset_dir += "/"
    # TODO: Remove this exception for PET once it's no longer returning empty an IPE list as a placeholder
    if module == "PET":
        module_metadata = OrderedDict()
    else:
        module_metadata = get_module_metadata(module)
    logger.debug(module_metadata)
    logger.info(f"Get IPEs for {module} module")

    """
    TODO Create a Base class for all modules below
    TODO Replace with SWITCH or dict of module and function call
    TODO Validate module name to a Enum to prevent string/case corruption

    TODO: TopoFlow is not ready for implementation/integration at the is time. However the TopoFlow class is created as 
          as a Stub and will output a "fake" JSON Response document with the information on parameters and the output 
          variables the HF team has at this time. This work to date also includes the creation/update of database tables 
          necessary for Stub to produce the BMI Files and the JSON Response list document.
          Once the module is ready for implementation/integration, and the coding necessary to complete the actually 
          BMI files and JSON Response List Document, just put the code snippet below back into else-if chain below      
    elif module == "TopoFlow":
        topoflow = TopoFlow()
        results = topoflow.initial_parameters(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    """
    if module == "CFE-S" or module == "CFE-X":
        results = cfe_ipe(module, version, gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt, dep_modules_included)
    elif module == "Noah-OWP-Modular":
        results = noah_owp_modular_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "T-Route":
        results = t_route_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "Snow-17":
        results = snow17_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "Sac-SMA":
        results = sac_sma_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "TopModel":
        results = topmodel_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == 'UEB':
        ueb = UEB()
        results = ueb.initial_parameters(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "LASAM":
        results = lasam_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt, dep_modules_included)
    elif module == "PET":
        results = pet_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    elif module == "LSTM":
        results = lstm_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt)
    else:
         results = module_json(module, [], [], error=f"module '{module}' does not exist")
    return results

def get_initial_parameters(model_type):
    if not isinstance(model_type, str) or len(model_type) > 20:
        error_str = {"error": "Invalid model type"}
        logger.error(error_str)

    # Execute the query
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectInitialParameters(model_type)
            if column_names and rows:
                # Replace " " with None (null)
                cleaned_rows = [
                    [None if isinstance(value, str) and value.strip() == "" else value for value in row]
                    for row in rows
                ]
                results = [OrderedDict(zip(column_names, row)) for row in cleaned_rows]
                return results
            else:
                error_str = {"error": "No initial parameter data found"}
                logger.error(error_str)
                return error_str

    except Exception as e:
        # TODO: Replace 'except' with proper catch
        logger.error(f"Error executing query: {e}")
        # TODO Send back a proper response
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

def get_module_metadata(module_name):

    # Get initial parameter data
    calibrate_data_response = module_calibrate_data(module_name)

    # Get the output variables data
    out_variables_data_response = module_out_variables_data(module_name)

    # Write JSON for module
    module_output = module_json(module_name, calibrate_data_response, out_variables_data_response)

    return module_output

def module_json(module_name, calibrate_parameters, output_variables, error=''):
    combined_data = OrderedDict()
    combined_data["module_name"] = module_name
    combined_data["parameter_file"] = {"uri": None}
    combined_data["calibrate_parameters"] = calibrate_parameters
    combined_data["output_variables"] = output_variables
    if error:
        combined_data["error"] = error
    return combined_data

def module_calibrate_data(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            if model_type == "SFT" or model_type == "SMP":
                column_names, rows = db.selectDependentModuleCalibrateData(model_type)
            else:
                column_names, rows = db.selectModuleCalibrateData(model_type)

            if column_names and rows:

                module_data = []
                for row in rows:
                    param_data = {
                        "name": row[column_names.index("name")],
                        "initial_value": row[column_names.index("default_value")],
                        "description": row[column_names.index("description")],
                        "min": row[column_names.index("min")],
                        "max": row[column_names.index("max")],
                        "data_type": row[column_names.index("data_type")],
                        "units": row[column_names.index("units")]
                    }
                    module_data.append(param_data)
                return  module_data
            else:
                module_data = []
                return module_data
    except Exception as e:
        # TODO: Replace 'except' with proper catch
        error_str = {"Error": "Error executing selectModuleCalibrateData query: {e}"}
        logger.error(error_str)
        return error_str


def module_out_variables_data(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectModuleOutVariablesData(model_type)

            module_data = []
            if column_names and rows:
                for row in rows:
                    output_var_data = {
                        "variable": row[column_names.index("name")],
                        "description": row[column_names.index("description")]
                    }
                    module_data.append(output_var_data)

            return module_data
    except Exception as e:
        # TODO: Replace 'except' with proper catch
        error_str = {"Error": "Error executing moduleOutVariablesData query: {e}"}
        logger.error(error_str)
        return error_str




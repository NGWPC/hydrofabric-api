from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
from collections import OrderedDict
from .serializers import ModelSerializer, InitialParameterSerializer
import logging
from .DatabaseManager import DatabaseManager
import json
import sys

from .geopackage import *
from .initial_parameters import *
#sys.path.append("/home/hydrofabric_api")

logger = logging.getLogger(__name__)
logging.basicConfig(filename='hf.log', level=logging.INFO)

# Execute the query  to fetch all models and model_ids.
@api_view(['GET'])
def get_modules(request):
    
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            # cursor.execute("SELECT model_id, name FROM public.models ORDER BY model_id ASC")
            # rows = cursor.fetchall()
            rows = db.selectAllModules()
            if rows:
                results = [OrderedDict({"model_id": row[0], "name": row[1]}) for row in rows]
                return Response(results, status=status.HTTP_200_OK)
            else:
                return Response({"error": "No data found"}, status=status.HTTP_404_NOT_FOUND)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    
# Execute the query  to fetch all models details .
@api_view(['GET'])
def modules(request):
    
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectAllModulesDetail()
            if column_names and rows:
                results = []
                for row in rows:
                    result = OrderedDict()
                    result["description"] = row[column_names.index("description")]
                    result["groups"] = row[column_names.index("groups")]
                    result["module_name"] = row[column_names.index("name")]
                    result["module_version"] = {
                        "version_url": row[column_names.index("version_url")],
                        "commit_hash": row[column_names.index("commit_hash")],
                        "version_number": row[column_names.index("version_number")]
                    }
                    results.append(result)
                
                # Wrap results in the "modules" key
                return Response({"modules": results}, status=status.HTTP_200_OK)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# @api_view(['GET'])
# def moduleMetaDataT(request):
#         try:
#             with connection.cursor() as cursor:
#                 db = DatabaseManager(cursor)
#                 column_names, rows = db.selectModuleMetaData()

#                 if column_names and rows:
#                     module_data = OrderedDict()
#                     module_data["module_name"] = 'NOAH-OWP-MODULAR'
#                     module_data["parameter_file"] = {"url": ""}
#                     module_data["calibrate_parameters"] = []
#                     module_data["module_output_variables"] = []

#                     # Separate out calibratable parameters and output variables
#                     calibrate_params_set = set()
#                     output_vars_set = set()

#                     for row in rows:
#                         param_name = row[column_names.index("param_name")]
#                         if param_name and param_name not in calibrate_params_set:
#                             param_data = {
#                                 "name": param_name,
#                                 "initial_value": "",
#                                 "description": row[column_names.index("param_description")],
#                                 "min": row[column_names.index("param_min")],
#                                 "max": row[column_names.index("param_max")],
#                                 "data_type": row[column_names.index("param_data_type")],
#                                 "units": row[column_names.index("param_units")]
#                             }
#                             module_data["calibrate_parameters"].append(param_data)
#                             calibrate_params_set.add(param_name)

#                         output_var_name = row[column_names.index("output_var_name")]
#                         if output_var_name and output_var_name not in output_vars_set:
#                             output_var_data = {
#                                 "name": output_var_name,
#                                 "description": row[column_names.index("output_var_description")]
#                             }
#                             module_data["module_output_variables"].append(output_var_data)
#                             output_vars_set.add(output_var_name)

#                     return Response([module_data], status=200)
#                 else:
#                     return Response({"error": "No data found"}, status=404)
            
#         except Exception as e:
#             print(f"Error executing selectModuleMetaData query: {e}")
#             return Response({"error": str(e)}, status=500)



@api_view(['GET'])
def moduleMetaData(request, model_type):
    try:
        # Get the calibration parameters data
        calibrate_data_response = moduleCalibrateData(model_type)
        #calibrate_data = calibrate_data_response.data[0]  # Assuming the structure is consistent

        # Get the output variables data
        out_variables_data_response = moduleOutVariablesData( model_type)
        #out_variables_data = out_variables_data_response.data[0]  # Assuming the structure is consistent

        # Combine the data
        combined_data = OrderedDict()
        combined_data["module_name"] = model_type
        combined_data["parameter_file"] = {"url": None}
        combined_data["calibrate_parameters"] = calibrate_data_response["calibrate_parameters"]
        combined_data["module_output_variables"] = out_variables_data_response["module_output_variables"]

        return Response([combined_data], status=200)

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return Response({"error": str(e)}, status=500)

@api_view(['GET'])
def get_model_parameters_total_count(request, model_type):
    if not isinstance(model_type, str) or len(model_type) > 20:
        return Response({"error": "Invalid model type"}, status=status.HTTP_400_BAD_REQUEST)

    # Execute the query
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            total_count = db.getModelParametersTotalCount(model_type)
            if total_count is not None:
                return Response({"total_count": total_count}, status=status.HTTP_200_OK)
            else:
                return Response({"error": "Model not found"}, status=status.HTTP_404_NOT_FOUND)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



@api_view(['GET'])
def get_initial_parameters(request, model_type):
    if not isinstance(model_type, str) or len(model_type) > 20:
        return Response({"error": "Invalid model type"}, status=status.HTTP_400_BAD_REQUEST)

    # Execute the query
    try:
        with connection.cursor() as cursor:
            # cursor.execute("SELECT model_id FROM public.models WHERE name = %s", [model_type])
            # model_id = cursor.fetchone()
            # if not model_id:
            #     return Response({"error": "Model not found"}, status=status.HTTP_404_NOT_FOUND)

            # cursor.execute("""
            #     SELECT sp.name, sp.units, sp.limits, sp.role 
            #     FROM public.soil_params sp
            #     WHERE sp.soil_id IN (
            #         SELECT mpm.paramtry:_field_id_fk
            #         FROM public.model_params_map mpm
            #         JOIN public.models mdl ON mdl.model_id = mpm.model_id_fk
            #         WHERE mdl.model_id = %s
            #     )
            # """, [model_id[0]])
            # rows = cursor.fetchall()
            # column_names = [desc[0] for desc in cursor.description]
            db = DatabaseManager(cursor)
            column_names, rows = db.selectInitialParameters(model_type)
            if column_names and rows:
                # Replace " " with None (null)
                cleaned_rows = [
                    [None if isinstance(value, str) and value.strip() == "" else value for value in row]
                    for row in rows
                ]
                results = [OrderedDict(zip(column_names, row)) for row in cleaned_rows]
                return Response(results, status=status.HTTP_200_OK)
            else:
                return Response({"error": "No data found"}, status=status.HTTP_404_NOT_FOUND)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
 
def moduleCalibrateData(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectModuleCalibrateData(model_type)

            if column_names and rows:
                module_data = OrderedDict()
                module_data["module_name"] = model_type
                module_data["parameter_file"] = {"url": ""}
                module_data["calibrate_parameters"] = []

                for row in rows:
                    param_data = {
                        "name": row[column_names.index("name")],
                        "initial_value": None, 
                        "description": row[column_names.index("description")],
                        "min": row[column_names.index("min")],
                        "max": row[column_names.index("max")],
                        "data_type": row[column_names.index("data_type")],
                        "units": row[column_names.index("units")],
                        "calibratable": row[column_names.index("calibratable")]
                    }
                    module_data["calibrate_parameters"].append(param_data)

                return  module_data 
            else:
                return Response({"error": "No calibratable parameters found"}, status=404)
    except Exception as e:
        print(f"Error executing selectModuleCalibrateData query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"error": str(e)}, status=500)
    

#@api_view(['GET'])
def moduleOutVariablesData(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectModuleOutVariablesData(model_type)

            if column_names and rows:
                module_data = OrderedDict()
                module_data["module_name"] = model_type
                module_data["parameter_file"] = {"url": ""}
                module_data["module_output_variables"] = []

                for row in rows:
                    output_var_data = {
                        "name": row[column_names.index("name")],
                        "description": row[column_names.index("description")]
                    }
                    module_data["module_output_variables"].append(output_var_data)

                return module_data
            else:
                return Response({"error": "No data found"}, status=404)

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return Response({"error": str(e)}, status=500)

@api_view(['GET'])
def return_geopackage(request, gage_id):
    #import geopackage
    results = get_geopackage(gage_id)
    if 'error' not in results:
        return Response(results, status=status.HTTP_200_OK)
    else:
        return Response(results, status=status.HTTP_404_NOT_FOUND)

@api_view(['POST'])
def return_ipe(request):
    #import initial_parameters
    
    gage_id = request.data.get("gage_id")
    modules = request.data.get("modules")

    results = []
    for module in enumerate(modules):

        if module[0] > 0:
            module_results = get_ipe(gage_id, module[1], get_gpkg = False)
        else:
            module_results = get_ipe(gage_id, module[1])

        if 'error' not in module_results:
            results.append(module_results[0])
        else:
            results = module_results 
            print(results)
            return Response(results, status=status.HTTP_404_NOT_FOUND)

    return Response(results, status=status.HTTP_200_OK)

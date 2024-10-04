from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from rest_framework import generics
from django.db import connection
from collections import OrderedDict

from rest_framework.views import APIView

from .models import HFFiles
from .util.enums import FileTypeEnum
from .util.gage_file_management import GageFileManagement
from .serializers import HFFilesSerializers
import logging
from .DatabaseManager import DatabaseManager
import json
import sys

from .geopackage import get_geopackage
from .initial_parameters import get_ipe

logger = logging.getLogger(__name__)
logging.basicConfig(filename='hf.log', level=logging.DEBUG)


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
                    result["module_name"] = row[column_names.index("name")]
                    result["groups"] = row[column_names.index("groups")]
                    results.append(result)

                # Wrap results in the "modules" key
                return Response({"modules": results}, status=status.HTTP_200_OK)

    except Exception as e:
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
def return_geopackage(request):
    http_200 = status.HTTP_200_OK
    http_422 = status.HTTP_422_UNPROCESSABLE_ENTITY
    gage_id = request.query_params.get('gage_id')
    source = request.query_params.get('source')
    domain = request.query_params.get('domain')
    gage_file_mgmt = GageFileManagement()
    results = None
    loc_status = http_200

    # Determine if this has already been computed, Check DB HFFiles table and S3 for pre-existing data
    file_found, results = gage_file_mgmt.file_exists(gage_id, domain, source, FileTypeEnum.GEOPACKAGE)
    if not file_found:
        results = get_geopackage(gage_id, source, domain)
        if 'error' in results:
            loc_status = http_422
    else:
        logger.debug(f"Prexisting Geopackage found for gage_id - {gage_id}, domain - {domain}, source - {source}")

    return Response(results, status=loc_status)


@api_view(['POST'])
def return_ipe(request):
    gage_id = request.data.get("gage_id")
    source = request.data.get("source")
    domain = request.data.get("domain")
    modules = request.data.get("modules")
    gage_file_mgmt = GageFileManagement()

    # Determine if IPE files already exists for this module and gage
    params_exists = gage_file_mgmt.param_files_exists(gage_id, domain, source, FileTypeEnum.PARAMS, modules)
    #Determine if GEOPACKAGE is necessary and file for this gage exists
    if len(params_exists) != len(modules):
        # Geopackage file needed
        file_found, results = gage_file_mgmt.file_exists(gage_id, domain, source, FileTypeEnum.GEOPACKAGE)
        if file_found:
            # Get the Geopackage file from S3 and put into local directory
            gage_file_mgmt.get_file_from_s3(gage_id, domain, source, FileTypeEnum.GEOPACKAGE)
        else:
            # Build the Geopackage file from scratch
            results = get_geopackage(gage_id, source, domain, keep_file=True)

    results = []
    for module in enumerate(modules):
        metadata = get_module_metadata(module[1])
        module_results = get_ipe(gage_id, module[1], metadata)

        if 'error' not in module_results:
            results.append(module_results[0])
        else:
            results = module_results
            print(results)
            return Response(results, status=status.HTTP_404_NOT_FOUND)
    """
    TODO:
        Remove all temp files
    """
    return Response(results, status=status.HTTP_200_OK)


class GetObservationalData(APIView):

    def get(self, request):
        loc_status = status.HTTP_200_OK
        gage_id = request.query_params.get('gage_id')
        source = request.query_params.get('source')
        domain = request.query_params.get('domain')
        gage_file_mgmt = GageFileManagement()

        # Check DB and HFFiles table for pre-existing data
        file_found, results = gage_file_mgmt.file_exists(gage_id, domain, source, FileTypeEnum.OBSERVATIONAL)
        if not file_found:
            loc_status = status.HTTP_422_UNPROCESSABLE_ENTITY
            results = f"Non-Headwater Basin gage or missing data for gage_id - {gage_id}, source -  {source}, domain - {domain}"
            log_string = f"Database or S3 bucket missing gage_id - {gage_id}, data type - {FileTypeEnum.OBSERVATIONAL}, source -  {source}, domain - {domain}."
            logger.error(log_string)

        return Response(results, status=loc_status)


class HFFilesCreate(generics.CreateAPIView):
    # API endpoint that allows creation of a new HFFiles
    queryset = HFFiles.objects.all(),
    serializer_class = HFFilesSerializers


class HFFilesList(generics.ListAPIView):
    # API endpoint that allows HFFiles to be viewed.
    queryset = HFFiles.objects.all()
    serializer_class = HFFilesSerializers


class HFFilesDetail(generics.RetrieveAPIView):
    # API endpoint that returns a single HFFiles by pk.
    queryset = HFFiles.objects.all()
    serializer_class = HFFilesSerializers


class HFFilesUpdate(generics.RetrieveUpdateAPIView):
    # API endpoint that allows a HFFiles record to be updated.
    queryset = HFFiles.objects.all()
    serializer_class = HFFilesSerializers


class HFFilesDelete(generics.RetrieveDestroyAPIView):
    # API endpoint that allows a HFFiles record to be deleted.
    queryset = HFFiles.objects.all()
    serializer_class = HFFilesSerializers

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
        print(f"Error executing query: {e}")
        logger.error(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


def module_calibrate_data(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
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
        error_str = {"Error": "Error executing selectModuleCalibrateData query: {e}"}
        logger.error(error_str)
        return error_str


def module_out_variables_data(model_type):
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectModuleOutVariablesData(model_type)

            if column_names and rows:
                module_data = []

                for row in rows:
                    output_var_data = {
                        "variable": row[column_names.index("name")],
                        "description": row[column_names.index("description")]
                    }
                    module_data.append(output_var_data)

                return module_data
            else:
                error_str = {"error": "No data found for model outputs"}
                logger.error(error_str)
                return error_str

    except Exception as e:
        error_str = {"Error": "Error executing moduleOutVariablesData query: {e}"}
        logger.error(error_str)
        return error_str

def get_module_metadata(module_name):

    # Get initial parameter data
    calibrate_data_response = module_calibrate_data(module_name)

    # Get the output variables data
    out_variables_data_response = module_out_variables_data(module_name)

    # Combine the data
    combined_data = OrderedDict()
    combined_data["module_name"] = module_name
    combined_data["parameter_file"] = {"uri": None}
    if not calibrate_data_response:
        combined_data["calibrate_parameters"] = []
    else:
        combined_data["calibrate_parameters"] = calibrate_data_response

    combined_data["output_variables"] = out_variables_data_response

    return [combined_data]



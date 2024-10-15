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

from .geopackage import get_geopackage
from .initial_parameters import get_ipe

logger = logging.getLogger(__name__)
logging.basicConfig(filename='hf.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)



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

    # TODO: Determine if IPE files already exists for this module and gage
    modules_to_calculate = gage_file_mgmt.param_files_exists(gage_id, domain, source, FileTypeEnum.PARAMS, modules)
    #Determine if GEOPACKAGE is necessary and file for this gage exists
    if len(modules_to_calculate) != 0:
        # Geopackage file needed
        geopackage_file_found, results = gage_file_mgmt.file_exists(gage_id, domain, source, FileTypeEnum.GEOPACKAGE)
        if geopackage_file_found:
            # Get the Geopackage file from S3 and put into local directory
            gage_file_mgmt.get_file_from_s3(gage_id, domain, source, FileTypeEnum.GEOPACKAGE)
        else:
            # Build the Geopackage file from scratch
            results = get_geopackage(gage_id, source, domain, keep_file=True)

    results = get_ipe(gage_id, source, domain, modules, gage_file_mgmt)

    return results


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





from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from django.db import connection
from collections import OrderedDict
from .serializers import ModelSerializer, InitialParameterSerializer
from .DatabaseManager import DatabaseManager


# Execute the query  to fetch all models and model_ids.
@api_view(['GET'])
def get_models(request):
    
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            # cursor.execute("SELECT model_id, name FROM public.models ORDER BY model_id ASC")
            # rows = cursor.fetchall()
            rows = db.selectAllModels()
            if rows:
                results = [OrderedDict({"model_id": row[0], "name": row[1]}) for row in rows]
                return Response(results, status=status.HTTP_200_OK)
            else:
                return Response({"error": "No data found"}, status=status.HTTP_404_NOT_FOUND)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    
# Execute the query  to fetch all models details .
@api_view(['GET'])
def modules(request):
    
    try:
        with connection.cursor() as cursor:
            db = DatabaseManager(cursor)
            column_names, rows = db.selectAllModelsDeatil()
            if column_names and rows:
                results = []
                for row in rows:
                    result = OrderedDict()
                    result["description"] = row[column_names.index("description")]
                    result["groups"] = row[column_names.index("groups")]
                    result["module_name"] = row[column_names.index("module_name")]
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
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
            #         SELECT mpm.param_field_id_fk
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
                results = [OrderedDict(zip(column_names, row)) for row in rows]
                return Response(results, status=status.HTTP_200_OK)
            else:
                return Response({"error": "No data found"}, status=status.HTTP_404_NOT_FOUND)
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return Response({"Error executing query": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
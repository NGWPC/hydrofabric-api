from django.urls import path
from .views import get_modules, get_initial_parameters,get_model_parameters_total_count,modules,moduleCalibrateData,moduleMetaData,moduleOutVariablesData,return_geopackage,return_ipe

urlpatterns = [
    path('api/initial_parameters/get_modules', get_modules, name='get_modules'),
    path('api/initial_parameters/modules', modules, name='modules'),
    path('api/initial_parameters/moduleMetaData/<str:model_type>', moduleMetaData, name='moduleMetaData'),
    path('api/initial_parameters/<str:model_type>', get_initial_parameters, name='get_initial_parameters'),
    path('api/initial_parameters/count/<str:model_type>', get_model_parameters_total_count, name='get_model_parameters_total_count'),
    path('api/initial_parameters/moduleCalibrateData/<str:model_type>', moduleCalibrateData, name='moduleCalibrateData'),
    path('api/initial_parameters/moduleOutVariablesData/<str:model_type>', moduleOutVariablesData, name='moduleOutVariablesData'),
    path("api/get_geopackage/geopackage/<str:gage_id>", return_geopackage, name='return_geopackage'),
    path("api/get_geopackage/get_parameters/", return_ipe, name='return_ipe')
    
]

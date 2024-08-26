from django.urls import path
from .views import get_modules, get_initial_parameters,get_model_parameters_total_count,modules,moduleCalibrateData,moduleMetaData,moduleOutVariablesData


urlpatterns = [
    path('api/initial_parameters/get_modules', get_modules, name='get_modules'),
    path('api/initial_parameters/modules', modules, name='modules'),
    path('api/initial_parameters/moduleMetaData/<str:model_type>', moduleMetaData, name='moduleMetaData'),
    path('api/initial_parameters/<str:model_type>', get_initial_parameters, name='get_initial_parameters'),
    path('api/initial_parameters/count/<str:model_type>', get_model_parameters_total_count, name='get_model_parameters_total_count'),
    path('api/initial_parameters/moduleCalibrateData/<str:model_type>', moduleCalibrateData, name='moduleCalibrateData'),
    path('api/initial_parameters/moduleOutVariablesData/<str:model_type>', moduleOutVariablesData, name='moduleOutVariablesData')
]
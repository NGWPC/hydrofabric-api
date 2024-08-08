from django.urls import path
from .views import get_models, get_initial_parameters,get_model_parameters_total_count,modules

urlpatterns = [
    path('api/initial_parameters/getModels', get_models, name='get_models'),
    path('api/initial_parameters/modules', modules, name='modules'),
    path('api/initial_parameters/<str:model_type>', get_initial_parameters, name='get_initial_parameters'),
    path('api/initial_parameters/count/<str:model_type>', get_model_parameters_total_count, name='get_model_parameters_total_count')
]
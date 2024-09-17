from django.urls import path
from .views import get_modules, return_geopackage, return_ipe

urlpatterns = [
    path('hydrofabric/2.1/modules/', get_modules, name='get_modules'),
    path("hydrofabric/2.1/modules/parameters/", return_ipe, name='return_ipe'),
    path("hydrofabric/2.1/geopackages/<str:gage_id>", return_geopackage, name='return_geopackage'),
]

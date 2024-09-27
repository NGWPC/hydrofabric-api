from django.urls import path
from .views import modules, return_geopackage, return_ipe, GetObservationalData, HFFilesCreate, HFFilesList, \
    HFFilesDetail, HFFilesUpdate, HFFilesDelete

urlpatterns = [
    path('hydrofabric/2.1/modules/', modules, name='modules'),
    path("hydrofabric/2.1/modules/parameters/", return_ipe, name='return_ipe'),
    path("hydrofabric/2.1/geopackages", return_geopackage, name='return_geopackage'),
    path('hydrofabric/2.1/observational', GetObservationalData.as_view(), name='observationalDataQuery'),
    path('create/', HFFilesCreate.as_view(), name='create-HFFiles'),
    path('list/', HFFilesList.as_view()),
    path('<int:pk>/', HFFilesDetail.as_view(), name='retrieve-HFFiles'),
    path('update/<int:pk>/', HFFilesUpdate.as_view(), name='update-HFFiles'),
    path('delete/<int:pk>/', HFFilesDelete.as_view(), name='delete-HFFiles'),
]

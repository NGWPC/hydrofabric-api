import os
import logging

import geopandas as gpd
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

def get_hydrofabric_attributes(gpkg_file,version):

    attr_layer = 'divide-attributes'
    if version == '2.1':
        attr_layer = 'model-attributes'

    # Get list of catchments from gpkg divides layer using geopandas
    try:
        divides_layer = gpd.read_file(gpkg_file, layer = attr_layer)
    except:# TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str)
        logger.error(error_str)
        return error
    #Soil and vegetation types are read from the gpkg as floats, but need to be ints 
    if version == '2.1':
        divides_layer = divides_layer.astype({'ISLTYP':'int'})
        divides_layer = divides_layer.astype({'IVGTYP':'int'})
    elif version == '2.2':
        divides_layer = divides_layer.astype({'mode.ISLTYP':'int'})
        divides_layer = divides_layer.astype({'mode.IVGTYP':'int'})
    return divides_layer

    
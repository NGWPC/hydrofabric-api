import os
import logging

import geopandas as gpd
from pyproj import Transformer
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

    #Zmax/max_gw_storage units are mm in the hydrofabric but CFE expects m.
    #Elevation in 2.2 is in cm, convert to m
    if version == '2.1':
        divides_layer['gw_Zmax'] = divides_layer['gw_Zmax'].apply(lambda x: x/1000)

    elif version == '2.2':
        divides_layer['mean.Zmax'] = divides_layer['mean.Zmax'].apply(lambda x: x/1000)
        divides_layer['mean.elevation'] = divides_layer['mean.elevation'].apply(lambda x: x/100)
        
    
    #Convert centroid_x and centroid_y (lat/lon) from CONUS Albers to WGS84 for decimal degrees for 2.2.
    if version == '2.2':
        transformer = Transformer.from_crs(5070, 4326)
        for index, row in divides_layer.iterrows():
            y = row['centroid_y']
            x = row['centroid_x']
            wgs84_latlon = transformer.transform(x,y)
            divides_layer.loc[index, 'centroid_y'] = wgs84_latlon[0] #latitude
            divides_layer.loc[index, 'centroid_x'] = wgs84_latlon[1] #longitude
    
    return divides_layer

    
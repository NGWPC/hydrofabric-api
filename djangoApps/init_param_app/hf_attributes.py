import os
import logging

import geopandas as gpd
from pyproj import Transformer
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

def get_hydrofabric_attributes(gpkg_file,version,domain):

    attr_layer = 'divide-attributes'
    if version == '2.1':
        attr_layer = 'model-attributes'

    #Map oCONUS lat/lon attribute names to CONUS names.
    column_names_ak_hi = {'X':'centroid_x',
                          'Y':'centroid_y',
                          'mode.bexp_soil_layers_stag.1':'mode.bexp_soil_layers_stag=1',
                          'mode.bexp_soil_layers_stag.2':'mode.bexp_soil_layers_stag=2',
                          'mode.bexp_soil_layers_stag.3':'mode.bexp_soil_layers_stag=3',
                          'mode.bexp_soil_layers_stag.4':'mode.bexp_soil_layers_stag=4',
                          'geom_mean.dksat_soil_layers_stag.1':'geom_mean.dksat_soil_layers_stag=1',
                          'geom_mean.dksat_soil_layers_stag.2':'geom_mean.dksat_soil_layers_stag=2',
                          'geom_mean.dksat_soil_layers_stag.3':'geom_mean.dksat_soil_layers_stag=3',
                          'geom_mean.dksat_soil_layers_stag.4':'geom_mean.dksat_soil_layers_stag=4',
                          'mean.smcmax_soil_layers_stag.1':'mean.smcmax_soil_layers_stag=1',
                          'mean.smcmax_soil_layers_stag.2':'mean.smcmax_soil_layers_stag=2',
                          'mean.smcmax_soil_layers_stag.3':'mean.smcmax_soil_layers_stag=3',
                          'mean.smcmax_soil_layers_stag.4':'mean.smcmax_soil_layers_stag=4',
                          'geom_mean.psisat_soil_layers_stag.1':'geom_mean.psisat_soil_layers_stag=1',
                          'geom_mean.psisat_soil_layers_stag.2':'geom_mean.psisat_soil_layers_stag=2',
                          'geom_mean.psisat_soil_layers_stag.3':'geom_mean.psisat_soil_layers_stag=3',
                          'geom_mean.psisat_soil_layers_stag.4':'geom_mean.psisat_soil_layers_stag=4',
                          'mean.smcwlt_soil_layers_stag.1':'mean.smcwlt_soil_layers_stag=1',
                          'mean.smcwlt_soil_layers_stag.2':'mean.smcwlt_soil_layers_stag=2',
                          'mean.smcwlt_soil_layers_stag.3':'mean.smcwlt_soil_layers_stag=3',
                          'mean.smcwlt_soil_layers_stag.4':'mean.smcwlt_soil_layers_stag=4'
                       }
    
    #Puerto Rico has a number of attribute names that don't match the other domains in HF v2.2
    column_names_pr = {'dksat_Time._soil_layers_stag.1': 'geom_mean.dksat_soil_layers_stag=1',
                       'dksat_Time._soil_layers_stag.2': 'geom_mean.dksat_soil_layers_stag=2',
                       'dksat_Time._soil_layers_stag.3': 'geom_mean.dksat_soil_layers_stag=3',
                       'dksat_Time._soil_layers_stag.4': 'geom_mean.dksat_soil_layers_stag=4',
                       'mean.cwpvt_Time.': 'mean.cwpvt',
                       'mean.mfsno_Time.': 'mean.mfsno',
                       'mean.mp_Time.': 'mean.mp',
                       'mean.refkdt_Time.': 'mean.refkdt',
                       'mean.slope_Time.': 'mean.slope_1km',
                       'mean.smcmax_Time._soil_layers_stag.1': 'mean.smcmax_soil_layers_stag=1',
                       'mean.smcmax_Time._soil_layers_stag.2': 'mean.smcmax_soil_layers_stag=2',
                       'mean.smcmax_Time._soil_layers_stag.3': 'mean.smcmax_soil_layers_stag=3',
                       'mean.smcmax_Time._soil_layers_stag.4': 'mean.smcmax_soil_layers_stag=4',
                       'mean.smcwlt_Time._soil_layers_stag.1': 'mean.smcwlt_soil_layers_stag=1',
                       'mean.smcwlt_Time._soil_layers_stag.2': 'mean.smcwlt_soil_layers_stag=2',
                       'mean.smcwlt_Time._soil_layers_stag.3': 'mean.smcwlt_soil_layers_stag=3',
                       'mean.smcwlt_Time._soil_layers_stag.4': 'mean.smcwlt_soil_layers_stag=4',
                       'mean.vcmx25_Time.': 'mean.vcmx25',
                       'mode.bexp_Time._soil_layers_stag.1': 'mode.bexp_soil_layers_stag=1',
                       'mode.bexp_Time._soil_layers_stag.2': 'mode.bexp_soil_layers_stag=2',
                       'mode.bexp_Time._soil_layers_stag.3': 'mode.bexp_soil_layers_stag=3',
                       'mode.bexp_Time._soil_layers_stag.4': 'mode.bexp_soil_layers_stag=4',
                       'psisat_Time._soil_layers_stag.1': 'geom_mean.psisat_soil_layers_stag=1',
                       'psisat_Time._soil_layers_stag.2': 'geom_mean.psisat_soil_layers_stag=2',
                       'psisat_Time._soil_layers_stag.3': 'geom_mean.psisat_soil_layers_stag=3',
                       'psisat_Time._soil_layers_stag.4': 'geom_mean.psisat_soil_layers_stag=4'}

    # Get list of catchments from gpkg divides layer using geopandas
    try:
        divide_attr = gpd.read_file(gpkg_file, layer = attr_layer)
        divide_layer = gpd.read_file(gpkg_file, layer = 'divides')
    except:# TODO: Replace 'except' with proper catch
        error_str = 'Error opening ' + gpkg_file
        error = dict(error = error_str)
        logger.error(error_str)
        return error
    
    #Get catchement area from divides layer and append to attributes data frame
    area = divide_layer[['divide_id','areasqkm']]
    divide_attr = divide_attr.join(area.set_index('divide_id'), on='divide_id')
    
    #Account for differences in column names between CONUS and oCONUS
    if version == '2.2' and domain != 'CONUS':
        divide_attr.rename(columns=column_names_ak_hi, inplace=True)
    if version == '2.2' and domain == 'Puerto_Rico':
        divide_attr.rename(columns=column_names_pr, inplace=True)

    #Soil and vegetation types are read from the gpkg as floats, but need to be ints 
    if version == '2.1':
        divide_attr = divide_attr.astype({'ISLTYP':'int'})
        divide_attr = divide_attr.astype({'IVGTYP':'int'})
    elif version == '2.2':
        divide_attr = divide_attr.astype({'mode.ISLTYP':'int'})
        divide_attr = divide_attr.astype({'mode.IVGTYP':'int'})

    #Zmax/max_gw_storage units are mm in the hydrofabric but CFE expects m.
    if version == '2.1':
        divide_attr['gw_Zmax'] = divide_attr['gw_Zmax'].apply(lambda x: x/1000)

    elif version == '2.2':
        divide_attr['mean.Zmax'] = divide_attr['mean.Zmax'].apply(lambda x: x/1000)
    
     #Elevation in 2.2 is in cm, convert to m.  Except for AK, which is still in m.
    if version == '2.2' and domain != 'Alaska':
        divide_attr['mean.elevation'] = divide_attr['mean.elevation'].apply(lambda x: x/100)
        
    #Convert centroid_x and centroid_y (lat/lon) from the domain's CRS to WGS84 for decimal degrees for 2.2.
    if version == '2.2':
        crs = divide_layer.crs
        transformer = Transformer.from_crs(crs, 4326)
        for index, row in divide_attr.iterrows():
            y = row['centroid_y']
            x = row['centroid_x']
            wgs84_latlon = transformer.transform(x,y)
            divide_attr.loc[index, 'centroid_y'] = wgs84_latlon[0] #latitude
            divide_attr.loc[index, 'centroid_x'] = wgs84_latlon[1] #longitude

    #If a soil divide attribute less than the min value or greater than the max value, 
    #reset to min or max.
    
    soil_attr = [{"name": "mode.bexp_soil_layers_stag=1", "min": 2, "max": 15},
    {"name": "geom_mean.dksat_soil_layers_stag=1", "min": 0.0000000195, "max": 0.000141},
    {"name": "geom_mean.psisat_soil_layers_stag=1", "min": 0.036, "max": 0.955},
    {"name": "mean.smcmax_soil_layers_stag=1", "min": 0.16, "max": 0.58},
    {"name": "mean.smcwlt_soil_layers_stag=1", "min": 0.05, "max": 0.30}]
    
    for attr in soil_attr:
        divide_attr.loc[divide_attr[attr['name']] > attr['max'], attr['name']] = attr['max']
        divide_attr.loc[divide_attr[attr['name']] < attr['min'], attr['name']] = attr['min']

    return divide_attr

    
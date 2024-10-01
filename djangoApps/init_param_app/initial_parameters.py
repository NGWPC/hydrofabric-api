import logging

#import geopackage

from .geopackage import *
from .cfe import *
from .noah_owp_modular import *
from .t_route import *
from .sac_sma import *
from .utilities import *
from .snow17 import *

def get_ipe(gage_id, module, module_metadata, get_gpkg = True):     
    '''
    Build initial parameter estimates (IPE) for a module.  

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    module (str): Module name
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    get_gpkg (bool):  Option to not create a geopackage if false

    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Setup logging
    logger = logging.getLogger(__name__)

    # Read config file for paths
    config = get_config()
    output_dir = config['output_dir']

    # Get geopackage if needed
    if get_gpkg:
        results = get_geopackage(gage_id)
        if 'error' in results: 
            return results 

    # Build path for IPE temp directory
    subset_dir = output_dir + "/" + gage_id + "/"      

    status_str = "Get IPEs for " + module
    print(status_str)
    logger.info(status_str)

    # Call function for specific module
    if module == "CFE-S" or module == "CFE-X":
        results = cfe_ipe(gage_id, subset_dir, module, module_metadata)
        return results
    elif module == "Noah-OWP-Modular":
        results = noah_owp_modular_ipe(gage_id, subset_dir, module_metadata)
        return results
    elif module == "T-Route":
        results = t_route_ipe(gage_id, subset_dir, module_metadata)
        return results
    elif module.upper() == "SNOW17":
        results = snow17_ipe(gage_id, subset_dir, module_metadata)
        return results
    elif module == "Sac-SMA":
        results = sac_sma_ipe(gage_id, subset_dir, module_metadata)
        return results
    else:
        error_str = "Module name not valid:" + module
        error = dict(error=error_str)
        print(error_str)
        logger.error(error_str)
        return error

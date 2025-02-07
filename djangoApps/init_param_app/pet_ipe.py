import logging

logger = logging.getLogger(__name__)

def pet_ipe(gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
    """
    Build initial parameter estimates (IPE) for the PET module

    Parameters:
        gage_id (str):  The gage ID, e.g., 06710385
        subset_dir (str):  Path to gage id directory where the module directory will be made.
        module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
        dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    """

    module = "PET"

    # TODO: Fill this file out once PET is fully implemented.
    module_metadata["module_name"] = module
    module_metadata["parameter_file"] = {"uri" : "s3://ngwpc-hydrofabric/2.2/CONUS/06710385/PARAMS/USGS/PET/Placeholder-Date"}
    module_metadata["calibrate_parameters"] = []
    module_metadata["output_variables"] = []
    return module_metadata
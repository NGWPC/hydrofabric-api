import os
import logging

import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from ambiance import Atmosphere
from .util.utilities import get_config, get_hydrofabric_input_attr_file, get_subset_dir_file_names
from .util.enums import FileTypeEnum
from .hf_attributes import get_hydrofabric_attributes

logger = logging.getLogger(__name__)

class TopoFlow:
    """
    Represents the TopoFlow Module for parameters initial values, output variables etc.
    Note: TopoFlow has no output variables
    """

    def __init__(self):
        self.gage_id = None
        self.module = 'TopoFlow'
        self.config = get_config()
        self.input_dir = self.config['input_dir']

    def initial_parameters(self, gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
        """
        Builds initial parameter estimates (IPE) for UEB (Utah Energy Balance) Module
        :param gage_id: The gage ID, e.g., 06710385
        :param version: The hydrofabric version
        :param source: The gage provider or agency
        :param domain: The NWM region the gage belongs to (Ex CONUS)
        :param subset_dir: Path to gage id directory where the module directory will be made.
        :param gpkg_file:
        :param module_metadata: Dictionary containing URI, initial parameters, output variables
        :param gage_file_mgmt:
        :return: JSON output with cfg file URI, calibratable parameters initial values, output variables.
        """
        self.gage_id = gage_id
        filename_list = []

        # Get list of catchments from gpkg divides layer using geopandas
        # TODO: This code needs to be moved to a geopackage file utility it is duplicated all over
        try:
            divides_layer = gpd.read_file(gpkg_file, layer="divides")
            try:
                catchments = divides_layer["divide_id"].tolist()
            except:
                # TODO: Replace 'except' with proper catch
                error_str = 'Error reading divides layer in ' + gpkg_file
                error = dict(error=error_str)
                logger.error(error_str)
                return error
        except:  # TODO: Replace 'except' with proper catch
            error_str = 'Error opening ' + gpkg_file
            error = dict(error=error_str)
            logger.error(error_str)
            return error

        # Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
        try:
            # Read parameters from CSV file into dataframe and filter on divide ids in geopackage.
            parameters_df = pd.read_csv(f'{self.input_dir}/deltat.csv')
            filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

        except FileNotFoundError as fnfe:
            logger.error(fnfe)
            error_str = 'Hydrofabric data input directory does not exist'
            error = dict(error=error_str)
            return error
        except Exception as exc:
            error_str = 'Error opening ' + attr_file
            error = dict(error=error_str)
            logger.error(error_str, exc)
            return error

        divide_attr = get_hydrofabric_attributes(gpkg_file, version)

        attr21 = {'slope':'slope_mean', 'aspect':'aspect_c_mean', 'elevation':'elevation_mean', 'lat':'Y','lon':'X'}
        attr22 = {'slope':'mean.slope', 'aspect':'circ_mean.aspect', 'elevation':'mean.elevation', 'lat':'centroid_y','lon':'centroid_y'}

        if version == '2.1':
            attr = attr21
        elif version == '2.2':
            attr=attr22

        # Begin Build/Calc IPE files for topoflow
        # End   Build/Calc IPE files for topoflow

        # Write files to DB and S3
        # uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list,
        #                                    module=self.module)
        uri = "s3://ngwpc-hydrofabric/2.2/CONUS/06710385_fake/PARAMS/USGS/TopoFlow_fake/2025_Jan_23_19_28_32"
        
        status_str = "Config files written to:  " + uri
        logger.info(status_str)

        #fill in parameter files uri 
        module_metadata["parameter_file"]["uri"] = uri
        return module_metadata
         

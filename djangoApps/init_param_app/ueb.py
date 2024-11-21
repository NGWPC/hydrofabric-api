import os
import logging

import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from ambiance import Atmosphere
from .util.utilities import get_config, get_hydrofabric_input_attr_file, get_subset_dir_file_names
from .util.enums import FileTypeEnum

logger = logging.getLogger(__name__)

class UEB:
    """
    Represents the UEB (Utah Energy Balance) Module for parameters initial values, output variables etc.
    """

    def __init__(self):
        self.gage_id = None
        self.module = 'UEB'
        # UEB requires 5 input files that have to be generated for each catchment, listed below. The first four are
        # constants and will not be produced by HF (Per Confluence page
        # https://confluence.nextgenwaterprediction.com/display/NGWPC/Interface+to+Enterprise+Hydrofabric+Services+Details+and+Timeline
        # Table Module BMI Initial Parameter File Naming Convention)
        # The last file is all constants except for the parameters HF needs to calculate
        # 1. "ueb-init-cat-{catchment}_calib.dat"
        # 2. "ueb_params-cat-{catchment}_calib.dat"
        # 3. "ueb_outputctr-cat-{catchment}_calib.dat"
        # 4. "ueb_inputctr-cat-{catchment}_calib.dat"
        # 5. "ueb_sitevars-cat-{catchment}_calib.dat"

        # to use: filename = self.init_template.format(catchment = '012345')
        self.config = get_config()
        self.input_dir = self.config['input_dir']
        self.sitevar_filename_template = "ueb_sitevars-{catchment}_calib.dat"
        self.sitevar_file_template = ("Site and Initial Condition Input Variables\n"
                                      "USic:  Energy content initial condition (kg m-3)\n"
                                      "0\n"
                                      "0.0\n"
                                      "WSis:  Snow water equivalent initial condition (m)\n"
                                      "0\n"
                                      "0.0\n"
                                      "Tic:  Snow surface dimensionless age initial condition\n"
                                      "0\n"
                                      "0.0\n"
                                      "WCic:  Snow water equivalent of canopy condition(m)\n"
                                      "0\n"
                                      "0.0\n"
                                      "df: Drift factor multiplier\n"
                                      "0\n"
                                      "1.0\n"
                                      "apr: Average atmospheric pressure\n"
                                      "0\n"
                                      "{std_atm_pressure}\n"
                                      "Aep: Albedo extinction coefficient\n"
                                      "0\n"
                                      "0.1\n"
                                      "cc: Canopy coverage fraction\n"
                                      "0\n"
                                      "0.7\n"
                                      "hcan: Canopy height\n"
                                      "0\n"
                                      "12.0\n"
                                      "lai: Leaf area index\n"
                                      "0\n"
                                      "7.5\n"
                                      "Sbar: Maximum snow load held per unit branch area\n"
                                      "0\n"
                                      "6.6\n"
                                      "ycage: Forest age flag for wind speed profile parameterization\n"
                                      "0\n"
                                      "1.00\n"
                                      "slope: A 2-D grid that contains the slope at each grid point\n"
                                      "0\n"
                                      "{slope}\n"
                                      "aspect: A 2-D grid that contains the aspect at each grid point\n"
                                      "0\n"
                                      "{aspect}\n"
                                      "latitude: A 2-D grid that contains the latitude at each grid point\n"
                                      "0\n"
                                      "{latitude}\n"
                                      "subalb: Albedo (fraction 0-1) of the substrate beneath the snow (ground, or glacier)\n"
                                      "0\n"
                                      "0.25\n"
                                      "subtype: Type of beneath snow substrate encoded as (0 = Ground/Non Glacier, 1=Clean Ice/glacier, 2= Debris covered ice/glacier, 3= Glacier snow accumulation zone)\n"
                                      "0\n"
                                      "0.0\n"
                                      "gsurf: The fraction of surface melt that runs off (e.g. from a glacier)\n"
                                      "0\n"
                                      "0.0\n"
                                      "b01: Bristow-Campbell B for January (1)\n"
                                      "0\n"
                                      "{jan_temp_range}\n"
                                      "b02: Bristow-Campbell B for February (2)\n"
                                      "0\n"
                                      "{feb_temp_range}\n"
                                      "b03: Bristow-Campbell B for March(3)\n"
                                      "0\n"
                                      "{mar_temp_range}\n"
                                      "b04: Bristow-Campbell B for April (4)\n"
                                      "0\n"
                                      "{apr_temp_range}\n"
                                      "b05: Bristow-Campbell B for may (5)\n"
                                      "0\n"
                                      "{may_temp_range}\n"
                                      "b06: Bristow-Campbell B for June (6)\n"
                                      "0\n"
                                      "{jun_temp_range}\n"
                                      "b07: Bristow-Campbell B for July (7)\n"
                                      "0\n"
                                      "{jul_temp_range}\n"
                                      "b08:  Bristow-Campbell B for August (8)\n"
                                      "0\n"
                                      "{aug_temp_range}\n"
                                      "b09: Bristow-Campbell B for September (9)\n"
                                      "0\n"
                                      "{sep_temp_range}\n"
                                      "b10: Bristow-Campbell B for October (10)\n"
                                      "0\n"
                                      "{oct_temp_range}\n"
                                      "b11: Bristow-Campbell B for November (11)\n"
                                      "0\n"
                                      "{nov_temp_range}\n"
                                      "b12: Bristow-Campbell B for December (12)\n"
                                      "0\n"
                                      "{dec_temp_range}\n"
                                      "ts_last:  degree celsius\n"
                                      "0\n"
                                      "-9999\n"
                                      "longitude: A 2-D grid that contains the longitude at each grid\n"
                                      "0\n"
                                      "{longitude}")

    def initial_parameters(self, gage_id, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
        """
        Builds initial parameter estimates (IPE) for UEB (Utah Energy Balance) Module
        :param gage_id: The gage ID, e.g., 06710385
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

        #Read model attributes Hive partitioned Parquet dataset using pyarrow, remove rows containing null, convert to pandas dataframe
        try:
            attr_file = get_hydrofabric_input_attr_file()
            attr = pq.read_table(attr_file)
            #Read parameters from CSV file into dataframe and filter on divide ids in geopackage. 
            parameters_df = pd.read_csv(f'{self.input_dir}/deltat.csv')
            filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]

        except FileNotFoundError as fnfe:
            logger.error(fnfe)
            error_str = 'Hydrofabric data input directory does not exist'
            error = dict(error=error_str)
            return error
        except Exception as exc:
            error_str = 'Error opening ' + attr_file
            error = dict(error = error_str)
            logger.error(error_str, exc)
            return error

        attr = attr.drop_null()
        attr_df = pa.Table.to_pandas(attr)

        #filter rows with catchments in gpkg
        filtered = attr_df[attr_df['divide_id'].isin(catchments)]
        #Join parameters from csv and area into single dataframe.
        df_all = filtered_parameters.join(filtered.set_index('divide_id'), on='divide_id')
        
        if len(filtered) == 0:
            error_str = 'No matching catchments in attribute file'
            error = dict(error = error_str)
            logger.error(error_str)
            return error

        #Loop through catchments, get soil type, populate config file template, write config file to temp 
        for index, row in df_all.iterrows():

            catchment_id = row['divide_id']

            temp_ranges = self.get_monthly_temp_ranges(row)
            
            slpe = round(row['slope_mean'], 4)
            aspct = round(row['aspect_c_mean'], 4)
            lat = round(row['Y'], 4)
            lon = round(row['X'], 4)
            elevation = round(row['elevation_mean'], 4)
            standard_atm_pressure = round(Atmosphere(elevation).pressure[0], 4)
            
            filename = self.sitevar_filename_template.format(catchment = catchment_id)
            file_string = self.sitevar_file_template.format(std_atm_pressure = standard_atm_pressure, 
                                                            slope = slpe,
                                                            aspect = aspct, 
                                                            latitude = lat, 
                                                            longitude = lon, 
                                                            jan_temp_range = temp_ranges['jan'],
                                                            feb_temp_range = temp_ranges['feb'],
                                                            mar_temp_range = temp_ranges['mar'],
                                                            apr_temp_range = temp_ranges['apr'],
                                                            may_temp_range = temp_ranges['may'],
                                                            jun_temp_range = temp_ranges['jun'],
                                                            jul_temp_range = temp_ranges['jul'],
                                                            aug_temp_range = temp_ranges['aug'],
                                                            sep_temp_range = temp_ranges['sep'],
                                                            oct_temp_range = temp_ranges['oct'],
                                                            nov_temp_range = temp_ranges['nov'],
                                                            dec_temp_range = temp_ranges['dec'],
                                                            )
            cfg_filename_path = os.path.join(subset_dir, filename)
            with open(cfg_filename_path, 'w') as outfile:
                outfile.write(file_string)
            filename_list.append(filename)
            
        # Write files to DB and S3
        uri = gage_file_mgmt.write_file_to_s3(gage_id, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list,
                                            module=self.module)
        status_str = "Config files written to:  " + uri
        logger.info(status_str)

        #fill in parameter files uri 
        module_metadata["parameter_file"]["uri"] = uri
        return module_metadata
        
            
    def get_monthly_temp_ranges(self, row):
        temp_ranges = {}
        temp_ranges['jan'] = round(row['january'], 4)
        temp_ranges['feb'] = round(row['february'], 4)
        temp_ranges['mar'] = round(row['march'], 4)
        temp_ranges['apr'] = round(row['april'], 4)
        temp_ranges['may'] = round(row['may'], 4)
        temp_ranges['jun'] = round(row['june'], 4)
        temp_ranges['jul'] = round(row['july'], 4)
        temp_ranges['aug'] = round(row['august'], 4)
        temp_ranges['sep'] = round(row['september'], 4)
        temp_ranges['oct'] = round(row['october'], 4)
        temp_ranges['nov'] = round(row['november'], 4)
        temp_ranges['dec'] = round(row['december'], 4)

        return temp_ranges
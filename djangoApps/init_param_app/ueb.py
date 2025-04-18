import os
import logging
import math

import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
import pyarrow as pa
from ambiance import Atmosphere
from .util.utilities import get_config, get_hydrofabric_input_attr_file, get_subset_dir_file_names
from .util.enums import FileTypeEnum
from .hf_attributes import *

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

    def initial_parameters(self, gage_id, version, source, domain, subset_dir, gpkg_file, module_metadata, gage_file_mgmt):
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

        csv_path_filename = f'{self.input_dir}/ueb_deltat_{version}.csv'

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

        divide_attr = get_hydrofabric_attributes(gpkg_file, version, domain)

        if len(divide_attr) == 0:
            error_str = 'No matching catchments in attribute file'
            error = dict(error = error_str)
            logger.error(error_str)
            return error

        attr21 = {'slope':'slope_mean', 'aspect':'aspect_c_mean', 'elevation':'elevation_mean', 'lat':'Y','lon':'X'}
        attr22 = {'slope':'mean.slope', 'aspect':'circ_mean.aspect', 'elevation':'mean.elevation', 'lat':'centroid_y','lon':'centroid_x'}

        if version == '2.1':
            attr = attr21
        elif version == '2.2':
            attr=attr22

        #Read parameters from CSV file into dataframe and filter on divide ids in geopackage.
        #Temperature deltas are only available for CONUS (except for ENVCA).  Use defaults otherwise.
        if domain == 'CONUS' and source != 'ENVCA':
            try: 
                parameters_df = pd.read_csv(csv_path_filename)
            except FileNotFoundError:
                error_str = f'Temperature delta CSV file not found: {csv_path_filename}'
                error = {'error': error_str}
                logger.error(error_str)
                return error
            except Exception as e:
                error_str = f'Temperature delta CSV read error: {csv_path_filename}'
                error = {'error': error_str}
                logger.error(error_str)
                return error   

            filtered_parameters = parameters_df[parameters_df['divide_id'].isin(catchments)]
            if filtered_parameters.empty:
                error_str = f'Catchments in geopackage not found in temperature delta CSV file'
                error = {'error': error_str}
                logger.error(error_str)
                return error
            
            #Make sure that there are a matching number of catchements
            if(len(catchments) != len(filtered_parameters.index)):
                error_str = f'Number of matching catchments found in temperature delta CSV file does not match number of catchments in geopackage'
                error = {'error': error_str}
                logger.error(error_str)
                return error

            #Join parameters from csv and area into single dataframe.
            df_all = filtered_parameters.join(divide_attr.set_index('divide_id'), on='divide_id')
        else:
            df_all = divide_attr
        
        #Set month temperature delta to the average if a catchment is NA.  The average
        #is taken monthy for all catchments in the csv file.
        jan_temp_range = 11.04395
        feb_temp_range = 11.79382
        mar_temp_range = 12.72711
        apr_temp_range = 13.67701
        may_temp_range = 13.70334
        jun_temp_range = 13.76782
        jul_temp_range = 13.90212
        aug_temp_range = 13.9958
        sep_temp_range = 14.04895
        oct_temp_range = 13.44001
        nov_temp_range = 11.90162
        dec_temp_range = 10.71597

        #Loop through catchments, get soil type, populate config file template, write config file to temp 
        for index, row in df_all.iterrows():

            catchment_id = row['divide_id']
            slpe = round(row[attr['slope']], 4)
            aspct = round(row[attr['aspect']], 4)
            lat = round(row[attr['lat']], 4)
            lon = round(row[attr['lon']], 4)
            elevation = round(row[attr['elevation']], 4)
            standard_atm_pressure = round(Atmosphere(elevation).pressure[0], 4)
            
            #If not CONUS or ENVCA, use defaults
            if domain == 'CONUS' and source != 'ENVCA':
                temp_ranges = self.get_monthly_temp_ranges(row)
                if not math.isnan(temp_ranges['jan']): jan_temp_range = temp_ranges['jan']
                if not math.isnan(temp_ranges['feb']): feb_temp_range = temp_ranges['feb']
                if not math.isnan(temp_ranges['mar']): mar_temp_range = temp_ranges['mar']
                if not math.isnan(temp_ranges['apr']): apr_temp_range = temp_ranges['apr']
                if not math.isnan(temp_ranges['may']): may_temp_range = temp_ranges['may']
                if not math.isnan(temp_ranges['jun']): jun_temp_range = temp_ranges['jun']
                if not math.isnan(temp_ranges['jul']): jul_temp_range = temp_ranges['jul']
                if not math.isnan(temp_ranges['aug']): aug_temp_range = temp_ranges['aug']
                if not math.isnan(temp_ranges['sep']): sep_temp_range = temp_ranges['sep']
                if not math.isnan(temp_ranges['oct']): oct_temp_range = temp_ranges['oct']
                if not math.isnan(temp_ranges['nov']): nov_temp_range = temp_ranges['nov']
                if not math.isnan(temp_ranges['dec']): dec_temp_range = temp_ranges['dec']

            filename = self.sitevar_filename_template.format(catchment = catchment_id)
            file_string = self.sitevar_file_template.format(std_atm_pressure = standard_atm_pressure, 
                                                            slope = slpe,
                                                            aspect = aspct, 
                                                            latitude = lat, 
                                                            longitude = lon, 
                                                            jan_temp_range = jan_temp_range,
                                                            feb_temp_range = feb_temp_range,
                                                            mar_temp_range = mar_temp_range,
                                                            apr_temp_range = apr_temp_range,
                                                            may_temp_range = may_temp_range,
                                                            jun_temp_range = jun_temp_range,
                                                            jul_temp_range = jul_temp_range,
                                                            aug_temp_range = aug_temp_range,
                                                            sep_temp_range = sep_temp_range,
                                                            oct_temp_range = oct_temp_range,
                                                            nov_temp_range = nov_temp_range,
                                                            dec_temp_range = dec_temp_range
                                                            )
            cfg_filename_path = os.path.join(subset_dir, filename)
            with open(cfg_filename_path, 'w') as outfile:
                outfile.write(file_string)
            filename_list.append(filename)
            
        # Write files to DB and S3
        uri = gage_file_mgmt.write_file_to_s3(gage_id, version, domain, FileTypeEnum.PARAMS, source, subset_dir, filename_list,
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
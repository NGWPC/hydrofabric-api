""" 
This module contains a variety of functions to create different input files. 

@author: Raghav Vadhera
"""

import pytest
from typing import Union
from pathlib import Path
import yaml
import logging



# Configure Django logging
logger = logging.getLogger(__name__)

class TRouteConfigCreator:
    def create_troute_config(
        self,
        gpkg_file: Union[str, Path],
        rt_cfg_file: Union[str, Path],
        start_date: str,
        nts: int,
    ) -> None:
        logger.info(f"Starting the creation of TRoute config for {gpkg_file}.")

        try:
            # The content of your create_troute_config function
            # Same as the function you provided earlier

            # bmi_parameters 
            bmi_param = {"flowpath_columns": ["id", "toid", "lengthkm"],
                         "attributes_columns": ['attributes_id', 
                                                'rl_gages',
                                                'rl_NHDWaterbodyComID',
                                                'MusK',
                                                'MusX',
                                                'n',
                                                'So',
                                                'ChSlp',
                                                'BtmWdth',
                                                'nCC',
                                                'TopWdthCC',
                                                'TopWdth'],
                         "waterbody_columns": ['hl_link', 
                                               'ifd',
                                               'LkArea',
                                               'LkMxE',
                                               'OrificeA',
                                               'OrificeC',
                                               'OrificeE',
                                               'WeirC',
                                               'WeirE',
                                               'WeirL'],
                         "network_columns": ['network_id', 'hydroseq', 'hl_uri'],
                        }

            # log_parameters
            log_param = {"showtiming": True, "log_level": 'DEBUG'}

            # network_topology_parameters
            columns = {"key": "id",  
                       "downstream": "toid",
                       "dx": "lengthkm",
                       "n": "n",
                       "ncc": "nCC",
                       "s0": "So",
                       "bw": "BtmWdth",
                       "waterbody": "rl_NHDWaterbodyComID",
                       "gages": "rl_gages",
                       "tw": "TopWdth",
                       "twcc": "TopWdthCC",
                       "musk": "MusK",
                       "musx": "MusX",
                       "cs": "ChSlp",
                       "alt": "alt",
                      }

            dupseg = ["717696", "1311881", "3133581", "1010832", "1023120", "1813525", 
                      "1531545", "1304859", "1320604", "1233435", "11816", "1312051",
                      "2723765", "2613174", "846266", "1304891", "1233595", "1996602", 
                      "2822462", "2384576", "1021504", "2360642", "1326659", "1826754",
                      "572364", "1336910", "1332558", "1023054", "3133527", "3053788",  
                      "3101661", "2043487", "3056866", "1296744", "1233515", "2045165", 
                      "1230577", "1010164", "1031669", "1291638", "1637751",
                     ]

            nwtopo_param = {"supernetwork_parameters": {"network_type": "HYFeaturesNetwork",
                                                        "geo_file_path": gpkg_file, 
                                                        "columns": columns, 
                                                        "duplicate_wb_segments": dupseg},
                            "waterbody_parameters": {"break_network_at_waterbodies": True,
                                                     "level_pool": {"level_pool_waterbody_parameter_file_path": gpkg_file}},
                           }

            # compute_parameters
            res_da = {"reservoir_persistence_da":{"reservoir_persistence_usgs": False,
                                                   "reservoir_persistence_usace": False},
                      "reservoir_rfc_da": {"reservoir_rfc_forecasts": False,
                                           "reservoir_rfc_forecasts_time_series_path": None,
                                           "reservoir_rfc_forecasts_lookback_hours": 28,
                                           "reservoir_rfc_forecasts_offset_hours": 28,
                                           "reservoir_rfc_forecast_persist_days": 11},
                      "reservoir_parameter_file": None,
                     }

            stream_da = {"streamflow_nudging": False,
                         "diffusive_streamflow_nudging": False,
                         "gage_segID_crosswalk_file": None,
                        }

            comp_param = {"parallel_compute_method": "by-subnetwork-jit-clustered",
                         "subnetwork_target_size": 10000,
                         "cpu_pool": 16,
                         "compute_kernel": "V02-structured",
                         "assume_short_ts": True,
                         "restart_parameters": {"start_datetime": start_date},
                         "forcing_parameters": {"qts_subdivisions": 12,
                                                "dt": 300,
                                                "qlat_input_folder": ".",
                                                "qlat_file_pattern_filter": "nex-*", 
                                                "nts": nts, 
                                                "max_loop_size": divmod(nts*300, 3600)[0]+1},
                         "data_assimilation_parameters": {"usgs_timeslices_folder": None,
                                                          "usace_timeslices_folder": None,
                                                          "timeslice_lookback_hours": 48, 
                                                          "qc_threshold": 1, 
                                                          "streamflow_da": stream_da,
                                                          "reservoir_da": res_da},  
                         }

            # output_parameters
            output_param = {'stream_output': {'stream_output_directory': ".",
                                              'stream_output_time': divmod(nts*300, 3600)[0]+1,
                                              'stream_output_type': '.nc',
                                              'stream_output_internal_frequency': 60, 
                                               },
                           }

            # Combine all parameters
            config = {"bmi_parameters": bmi_param, 
                      "log_parameters": log_param,
                      "network_topology_parameters": nwtopo_param,
                      "compute_parameters": comp_param,
                      "output_parameters": output_param,
                     }

            # Save configuration into yaml file
            with open(rt_cfg_file, 'w') as file:
                yaml.dump(config, file, sort_keys=False, default_flow_style=False, indent=4)

            logger.info(f"TRoute config successfully created at {rt_cfg_file}.")
        
        except Exception as e:
            logger.error(f"An error occurred while creating the TRoute config: {e}")
            raise






    # # Use case test # Convert into pytest checkin the test file
    # def test_create_troute_config(self):
    #     # The path of the gpkg_file
    #     gpkg_file = "../test/testdata/Gage_6719505.gpkg"
    #     rt_cfg_file = "t_route_6719505.yml"
    #     start_date = "April 1, 2024"
    #     nts = 100

        
    #     # Call the method with these inputs
    #     self.create_troute_config(gpkg_file, rt_cfg_file, start_date, nts)
        
    #     # You would add assertions here to check if the file was created
    #     # and possibly verify its content. Example:
    #     assert Path(rt_cfg_file).exists(), f"{rt_cfg_file} does not exist!"
        
    #     # Further assertions can be done on the contents of the YAML file if needed
    #     with open(rt_cfg_file, 'r') as file:
    #         content = yaml.safe_load(file)
    #         assert content is not None, "The YAML content is empty or invalid."
    #         assert content["compute_parameters"]["restart_parameters"]["start_datetime"] == start_date, "Start date does not match."
    #         assert content["compute_parameters"]["forcing_parameters"]["nts"] == nts, "Number of timesteps does not match."

# # Example usage in a script:
# if __name__ == "__main__":
#     creator = TRouteConfigCreator()
#     creator.test_create_troute_config()
#     print("Done testing")

# Pytest
def test_create_troute_config():
    # Setup the input parameters
    gpkg_file = "./s3/Gage_6719505.gpkg"
    rt_cfg_file = "t_route_6719505.yml"  # Use a temporary directory for the test file
    start_date = "April 1, 2024"
    nts = 100

    # Create an instance of the class
    creator = TRouteConfigCreator()
    
    # Call the method with these inputs
    creator.create_troute_config(gpkg_file, rt_cfg_file, start_date, nts)
    
    # Assert that the YAML file was created
    assert Path(rt_cfg_file).exists(), f"{rt_cfg_file} does not exist!"
    
    # Further assertions can be done on the contents of the YAML file
    with open(rt_cfg_file, 'r') as file:
        content = yaml.safe_load(file)
        assert content is not None, "The YAML content is empty or invalid."
        assert content["compute_parameters"]["restart_parameters"]["start_datetime"] == start_date, "Start date does not match."
        assert content["compute_parameters"]["forcing_parameters"]["nts"] == nts, "Number of timesteps does not match."


# Example usage in a script:
if __name__ == "__main__":
    # Directly run pytest when the script is executed
    pytest.main(["-v"])
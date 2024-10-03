import os
import json
import yaml
import logging

from .utilities import *

def t_route_ipe(gage_id, subset_dir, module_metadata):
    '''
    Build initial parameter estimates (IPE) for T-Route 

    Parameters:
    gage_id (str):  The gage ID, e.g., 06710385
    subset_dir (str):  Path to gage id directory where the module directory will be made.
    module_metadata (dict):  dictionary containing URI, initial parameters, output variables
    
    Returns:
    dict: JSON output with cfg file URI, calibratable parameters initial values, output variables.
    '''

    # Configure logging
    logger = logging.getLogger(__name__) 

    #Get config file
    config = get_config()
    s3url = config['s3url']
    s3bucket = config['s3bucket']
    s3prefix = config['s3prefix']
    hydrofabric_dir = config['hydrofabric_dir']
    hydrofabric_version = config['hydrofabric_version']
    hydrofabric_type = config['hydrofabric_type']
    
    #setup output dir
    #first save the top level dir for the gpkg
    gpkg_dir = subset_dir
    subset_dir = os.path.join(subset_dir, 'T-Route')
    if not os.path.exists(subset_dir):
        os.mkdir(subset_dir)

    gpkg_file = "Gage_" + gage_id + ".gpkg"
    gpkg_file = os.path.join(gpkg_dir, gpkg_file)

    start_date = ''
    nts = 5

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
    output_filename = 'troute.yml'
    output_full_path = os.path.join(subset_dir, output_filename)
    with open(output_full_path, 'w') as file:
        yaml.dump(config, file, sort_keys=False, default_flow_style=False, indent=4)

    if s3prefix:
        subset_s3prefix = s3prefix + "/" + gage_id + '/' + 'T-Route'
    else:
        subset_s3prefix = gage_id  + '/' + 'T-Route'

    write_minio(subset_dir, output_filename, s3url, s3bucket, subset_s3prefix)

    uri = build_uri(s3bucket, subset_s3prefix)
    status_str = "Config files written to:  " + uri
    print(status_str)
    logger.info(status_str)

    #fill in parameter files uri 
    module_metadata["parameter_file"]["uri"] = uri

    return module_metadata

    logger.info(f"TRoute config successfully created at {output_filename}.")

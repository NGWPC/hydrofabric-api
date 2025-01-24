import os
import requests
import json
import pytest
import unicodedata

# API server base URL - pulls from environment variables
# Default is "http://127.0.0.1:8000"
API_IP = os.environ.get("API_IP", "127.0.0.1")
API_PORT = os.environ.get("API_PORT", "8000")
BASE_URL = f"http://{API_IP}:{API_PORT}"

# Parameters for each IPE module call:
# gage_id, module and json filename
IPE_PARAMS = [
    ("06710385", ["CFE-S", "CFE-X"], "ipe_cfe.json"),
    ("06710385", ["Noah-OWP-Modular"], "ipe_noah_owp_modular.json"),
    ("01123000", ["SFT"], "ipe_sft.json"),
    ("06710385", ["SMP"], "ipe_smp.json"),
    ("06710385", ["Sac-SMA"], "ipe_sac_sma.json"),
    ("06710385", ["Snow-17"], "ipe_snow_17.json"),
    ("06710385", ["T-Route"], "ipe_t_route.json"),
    ("06710385", ["TopModel"], "ipe_topmodel.json"),
    ("06710385", ["LASAM"], "ipe_lasam.json"),
    ("01123000", ["UEB"], "ipe_ueb.json")
]


def test_version():
    """ Check that the version endpoint is active """
    print(BASE_URL)
    results = requests.get(BASE_URL + "/version")
    assert results.ok

def test_modules():
    """ Check the list of modules returned by the API """
    results = requests.get(BASE_URL + "/hydrofabric/2.1/modules/")
    module_list = [mod["module_name"] for mod in json.loads(results.text)["modules"]]
    expected = ["CFE-S", "CFE-X", "Noah-OWP-Modular", "SMP", "Sac-SMA", "Snow-17", "T-Route", "TopModel"]
    assert module_list == expected


class TestGeopackage:
    """ Tests concerning making API calls to return geopackages """
    def test_gpkg_good(self):
        """ Check geopackage query results when a well-formed call is made """
        results = make_api_call("geopackage", "06710385", "2.2", "USGS", "CONUS")
        expected = {"uri": "s3://ngwpc-hydrofabric/2.2/CONUS/06710385/GEOPACKAGE/USGS/2025_Jan_17_21_02_53/gauge_06710385.gpkg"}
        assert results == expected

    def test_gpkg_bad_gage_id(self):
        """ Check geopackage query results when a bad gage id is supplied """
        results = make_api_call("geopackage", "00000000", "2.2", "USGS", "CONUS")
        expected = {"error": "Hydrofabric subsetting failed: Error in find_origin(network = query_source_layer(query$source, \"network\"),  : \n  No origin found\nExecution halted\n"}
        assert results == expected

    def test_gpkg_bad_version(self):
        """ Check geopackage query results when a bad version is supplied """
        results = make_api_call("geopackage", "06710385", "2.0", "USGS", "CONUS")
        expected = {"error": "Hydrofabric version must be 2.2 or 2.1"}
        assert results == expected

    def test_gpkg_bad_domain(self):
        """ Check geopackage query results when a bad domain is supplied """
        results = make_api_call("geopackage", "06710385", "2.1", "USGS", "Alaska")
        expected = {"error": "oCONUS domains not availiable in Hydrofabric version 2.1"}
        assert results == expected


class TestInitialParameterEstimates:
    """ Tests concering API calls for IPE results, across different modules """
    @pytest.mark.parametrize("gage_id, modules, expected_result_file", IPE_PARAMS)
    def test_ipe_good(self, gage_id, modules, expected_result_file):
        """ Check the IPE results for every module. Runs once for each module """
        results = make_api_call("ipe", gage_id, "2.2", "USGS", "CONUS", modules)
        expected = get_json_resource(expected_result_file)
        if (results != expected):
            # Create json files under .actual/ for debugging. They contain the actual responses.
            # Only kept if there's a mismatch between the return and expected
            os.makedirs(f"{os.path.dirname(__file__)}/resources/.actual/", exist_ok=True)
            with open(f"{os.path.dirname(__file__)}/resources/.actual/.{expected_result_file}", "w") as f:
                f.write(json.dumps(results, indent=4))
        assert results == expected

    def test_ipe_bad_gage_id(self):
        """ Check for an error when a bad gage id is supplied for an IPE call """
        results = make_api_call("ipe", "00000000", "2.2", "USGS", "CONUS", ["CFE-S", "CFE-X"])
        expected = {"error": "Hydrofabric subsetting failed: Error in find_origin(network = query_source_layer(query$source, \"network\"),  : \n  No origin found\nExecution halted\n"} 
        assert results == expected

    def test_ipe_bad_module(self):
        """ Check for an error when a bad module is supplied for an IPE call """
        results = make_api_call("ipe", "06710385", "2.2", "USGS", "CONUS", ["CFE"])
        expected = {"error": "Module name not valid: CFE"}
        assert results == expected

    def test_ipe_bad_version(self):
        """ Check for an error when a bad version is supplied for an IPE call """
        results = make_api_call("ipe", "06710385", "2.0", "USGS", "CONUS", ["CFE-S", "CFE-X"])
        expected = {"error": "Hydrofabric version must be 2.2 or 2.1"}
        assert results == expected

    def test_ipe_bad_domain(self):
        """ Check for an error when a bad domain is supplied for an IPE call """
        results = make_api_call("ipe", "06710385", "2.1", "USGS", "ALASKA", ["CFE-S", "CFE-X"])
        expected = {"error": "oCONUS domains not availiable in Hydrofabric version 2.1"}
        assert results == expected


class TestObservationalData:
    """ Tests concerning API calls for observational data """
    def test_observ_data_good(self):
        """ Check observational data query results when a well-formed call is made  """
        results = make_api_call("observational", "06710385", "", "USGS", "CONUS")
        expected = {"uri": "s3://ngwpc-hydrofabric/2.1/CONUS/06710385/OBSERVATIONAL/USGS/2024_Sep_19_09_12_20/06710385_hourly_discharge.csv"}
        assert results == expected

    def test_observ_data_bad_gage_id(self):
        """ Check observational data query results when a bad gage id is supplied  """
        results = make_api_call("observational", "00000000", "", "USGS", "CONUS")
        expected = {"error": "Non-Headwater Basin gage or missing data for gage_id - 00000000, source -  USGS, domain - CONUS"}
        assert results == expected


def get_json_resource(filename: str):
    """ Helper function to streamline pulling in contents of JSON files

        Takes the base filename of a JSON file used to compare expected vs actual output. Assumes the
        file is inside the /resources directory.

        Returns:
            A dict object representing the JSON file
    """
    with open(f"{os.path.dirname(__file__)}/resources/{filename}") as f:
        json_contents = json.load(f)
    return json_contents

def make_api_call(method: str, gage_id: str, version: str, source: str, domain: str, modules: list=[]):
    """ Helper function to simplify API calls for getting geopackages, IPEs and observational data

        When given a geopackage call, query the API for a specific geopackage
        When given an ipe call, poll the API for the IPE results for a given geopackage and module(s)
        When given an observational data call, query the API for observational data from a specific geopackage

        Returns:
            A dict object representing the JSON response that was returned
    """
    if method == "geopackage":
        # Getting a geopackage
        api_url = f"{BASE_URL}/hydrofabric/geopackages?gage_id={gage_id}&source={source}&domain={domain}&version={version}"
        response = requests.get(api_url)
    elif method == "ipe":
        # Getting IPE results
        api_url = f"{BASE_URL}/hydrofabric/modules/parameters/"
        payload = json.dumps({"gage_id": gage_id, "version": version, "source": source, "domain": domain, "modules": modules})
        headers = {"Content-Type": "application/json"}
        response = requests.post(api_url, data=payload, headers=headers)
        print(response.text)
    elif method == "observational":
        # Getting observational data
        api_url = f"{BASE_URL}/hydrofabric/2.1/observational?gage_id={gage_id}&source={source}&domain={domain}"
        response = requests.get(api_url)
    
    return json.loads(unicodedata.normalize("NFKD", response.text))

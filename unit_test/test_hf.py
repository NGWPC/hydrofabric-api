import sys
sys.path.append("/home/NGWPC-3201_3145/hydrofabric_api/")
from hf_subsetter import get_geopackage
import requests
import json


# nominal geopackage
def test_gpkg():
    gage_id = "06719505"
    output = '{"uri":"s3://ngwpc-hydrofabric/06719505/Gage_6719505.gpkg"}'
    url = 'http://127.0.0.1:8000/api/get_geopackage/geopackage/' + gage_id
    results = requests.get(url)
    results = results.text
    assert results == output

# non-nominal geopackage:  bad gage ID
def test_gpkg_bad_gage_id():
    
    gage_id = "0671950"
    output = {"error":"Gage ID is not valid"} 
    url = 'http://127.0.0.1:8000/api/get_geopackage/geopackage/' + gage_id
    results = requests.get(url)
    results = results.text
    assert results == output

# nominal CFE-S and CFE-X IPE
def test_good_ipe():

    f = open('good_ipe.json')
    output = json.load(f)

    url = 'http://127.0.0.1:8000/api/get_geopackage/get_parameters/'
    payload = '{"gage_id": "06719505", "modules": ["CFE-S", "CFE-X"]}'
    headers = {'Content-Type': 'application/json'}
    results = requests.post(url, data=payload, headers=headers)
    results = results.text
    assert results == output

# non-nominal get ipe bad gage id
def test_get_ipe_bad_gage_id():

    url = 'http://127.0.0.1:8000/api/get_geopackage/get_parameters/'
    payload = '{"gage_id": "0671950", "modules": ["CFE-S", "CFE-X"]}'
    headers = {'Content-Type': 'application/json'}
    results = requests.post(url, data=payload, headers=headers)
    results = results.text

    output = {"error":"Gage ID is not valid"} 
    assert results == output

def test_get_ipe_bad_module():

    url = 'http://127.0.0.1:8000/api/get_geopackage/get_parameters/'
    payload = '{"gage_id": "06719505", "modules": ["CFE-S", "CFE"]}' 
    headers = {'Content-Type': 'application/json'}
    results = requests.post(url, data=payload, headers=headers)
    results = results.text
    output = {"error":"Module name not valid:CFE"} 
    assert results == output

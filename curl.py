import requests

url = 'http://127.0.0.1:8000/hydrofabric/2.1/modules/parameters/'
#payload = '{"gage_id": "06710385", "domain":"CONUS", "source":"USGS", "modules": ["Noah-OWP-Modular"]}'
#payload = '{"gage_id": "01123000", "domain":"CONUS", "source":"USGS", "modules": ["Noah-OWP-Modular"]}'
#payload = '{"gage_id": "01027200", "domain":"CONUS", "source":"USGS", "modules": ["SNOW17"]}'
#payload = '{"gage_id": "01027200", "domain":"CONUS", "source":"USGS", "modules": ["T-Route"]}'
#payload = '{"gage_id": "01027200", "domain":"CONUS", "source":"USGS", "modules": ["Noah-OWP-Modular"]}'
#payload = '{"gage_id": "01027200", "domain":"CONUS", "source":"USGS", "modules": ["SFT, CFE-S"]}'
#payload = '{"gage_id": "01011000", "domain":"CONUS", "source":"USGS", "modules": ["CFE-S"]}'
payload = '{"gage_id": "01011000", "domain":"CONUS", "source":"USGS", "modules": ["CFE-S"]}'
"""
NOTE: THE BELOW PAYLOADS DO NOT WORK ANY MORE
    THESE ARE KEPT FOR REFERENCE
    TO USE, FOLLOW THE SIGNATURE ABOVE.
#payload = '{"gage_id": "01123000", "modules": ["CFE-S", "CFE-X"]}'
#payload = '{"gage_id": "06710385", "modules": ["T-Route"]}'
#payload = '{"gage_id": "06710385", "modules": ["CFE-S", "Noah-OWP-Modular", "T-Route"]}'
#payload = '{"gage_id": "06710385", "modules": ["SNOW-17"]}'
"""
headers = {'Content-Type': 'application/json'}
results = requests.post(url, data=payload, headers=headers)
print(results.text)
'''
gage_id = "06710385"
source = 'USGS'
domain = 'CONUS'
rest_of_url = f"?gage_id={gage_id}&source={source}&domain={domain}"
url = 'http://127.0.0.1:8000/hydrofabric/2.1/geopackages' + rest_of_url
'''
results = requests.get(url)
results = results.text
print(results)


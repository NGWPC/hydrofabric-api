import requests

url = 'http://127.0.0.1:8000/api/initial_parameters/get_parameters/'
#payload = '{"gage_id": "06710385", "modules": ["Noah-OWP-Modular"]}'
#payload = '{"gage_id": "06710385", "modules": ["CFE-S", "CFE-X"]}'
payload = '{"gage_id": "06710385", "modules": ["CFE-S"]}'
#payload = '{"gage_id": "06710385", "modules": ["T-Route"]}'
headers = {'Content-Type': 'application/json'}
results = requests.post(url, data=payload, headers=headers)
print(results.text)
'''
gage_id = "06710385"
url = 'http://127.0.0.1:8000/api/get_geopackage/geopackage/' + gage_id
results = requests.get(url)
results = results.text
print(results)
'''

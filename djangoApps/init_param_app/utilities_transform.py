import unittest
import cartopy.crs as ccrs
import math
import csv
import os
from .utilities import *



def save_lat_lon_dict_to_text(lat_lon_dict, filename="lat_lon_dict.txt"):
    with open(filename, 'w') as file:
        # Iterate through the dictionary and write each key-value pair
        for (lat, lon), value in lat_lon_dict.items():
            file.write(f"Latitude: {lat}, Longitude: {lon}, Value: {value}\n")
    print(f"Dictionary saved to {filename}")



def save_transformed_dict_to_csv(lat_lon_dict, filename="lat_lon_center_dict.csv"):

    # Open the CSV file in write mode
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        
        # Write the header row
        writer.writerow(['Longitude(Meters)', 'Latitude(meters)', 'Value'])
        
        # Write each (latitude, longitude, value) tuple to the file
        for (lon,lat), value in lat_lon_dict.items():
            writer.writerow([ lon, lat, value])
    
    print(f"Dictionary saved to {filename}")



def get_closest_key(lat, lon, lat_lon_dict):
    # Function to find the closest key based on Euclidean distance
    closest_key = min(lat_lon_dict.keys(), key=lambda k: math.sqrt((k[0] - lat)**2 + (k[1] - lon)**2))
    return closest_key

# Function to find the closest key in lat_lon_dict
def find_closest_key(lat_lon_dict, lon, lat):
    closest_key = None
    min_distance = float('inf')

    for key in lat_lon_dict.keys():
        lon_x, lat_y = key
        # Calculate the Euclidean distance between the two points
        distance = math.sqrt((lat - lat_y)**2 + (lon - lon_x)**2)
        
        if distance < min_distance:
            min_distance = distance
            closest_key = key

    return closest_key

# Import the function to be tested (assuming it is in the same file)
# Read  attached ascii file and the very first top row of this file has entry as "ncols 1080" which means number of columns in this file for the data is 1080. Save this first row second column value as COLUMNS.
#     # `The second row from  the top of this file has entry as "nrows 841" which means number of rows in this file for the data is 841. Save this second column value of the Second row from the top as ROWS.
#     # Now ignore top six rows and read the data from row 7 the from the top onward in following order.
#     # Start reading this file from bottom-up where last row and first column is in this file is marked or tagged as first column and first row that is (1,1) and  last row and second column is  
#     # marked or tagged as second column and first row that is (2,1) and so on.
#     # Similarly Second last row and first column in this Ascii file is marked or tagged as first column and second row  (1,2)  and  Second last row and second column is  marked or tagged as second column 
#     # and first row that is (2,2) and so on.
#     #
#     # Continue this all the way from bottom up till you reach 7th row from the top of this file.

def getValueForLatLon_point(lat, lon, paramName):

    paramName = paramName.upper()
    # setup input dir
    config = get_config()
    input_dir = config['input_dir']
    filename = input_dir + "sac_sma/asc_data/sac_" + paramName + ".asc"
    
    # filename = "sac_" + paramName + ".asc"
    if os.path.exists(filename):
        print(f"The file '{filename}' exists.")
    else:
        print(f"The file '{filename}' does not exist.")
        return -1   #use default value of the paramName


    # Open and read the file
    with open(filename, "r") as file:
        lines = file.readlines()

    # Extract the number of columns and rows from the first two lines
    COLUMNS = int(lines[0].split()[1])  # Extracting value of "ncols"
    ROWS = int(lines[1].split()[1])     # Extracting value of "nrows"
    
    # Extracting the corner and NODATA values
    xllcorner = float(lines[2].split()[1])
    yllcorner = float(lines[3].split()[1])
    NODATA_value = float(lines[5].split()[1])

    # Skip the top 6 lines (metadata) and keep only the data part from row 7 onward
    data_lines = lines[6:]

    # Initialize an empty dictionary to store the values
    data_dict = {}

    # Process the data lines in reverse (bottom-up)
    for row_idx in range(ROWS):
        line = data_lines[ROWS - 1 - row_idx].strip()
        values = line.split()

        for col_idx in range(COLUMNS):
            column_number = col_idx + 1  # Column numbers start at 1
            row_number = row_idx + 1     # Row numbers also start at 1
            data_dict[(column_number, row_number)] = float(values[col_idx])
    
    # Example: Accessing a value at position (1,1)
    print(f"Value at (1,1): {data_dict[(1, 1)]}")

    # Example: Accessing a value at position (2,1)
    print(f"Value at (2,1): {data_dict[(2, 1)]}")

    # Example: Accessing a value at position (1,2)
    print(f"Value at (1,2): {data_dict[(1, 2)]}")

    # Example: Accessing a value at position (2,2)
    print(f"Value at (2,2): {data_dict[(2, 2)]}")

    # Transformation of these data_dict keys into meters.
    transformed_dict = {}
    for (x, y), value in data_dict.items():
        xster = x * 4762.5 - 401 * 4762.5
        yster = y * 4762.5 - 1601 * 4762.5
        #center value to calculate and save in the dictionary
        xster_center = (xster+4762.5)/2
        yster_enter =  (yster+4762.5)/2
        transformed_dict[(xster_center, yster_enter)] = value

    # Example: Accessing some transformed key-value pairs
    for key, value in list(transformed_dict.items())[:5]:  # Display first 5 entries
        print(f"Transformed coordinates: {key}, Value: {value}")

    # Save the transformed_dict to a CSV file
    save_transformed_dict_to_csv(transformed_dict)

    # Now Transform the lat,lon points into meters from degree
    proj=ccrs.Stereographic(central_latitude=90, central_longitude=-105, false_easting=0.0, false_northing=0.0, \
            true_scale_latitude=60, scale_factor=None, globe=None)

    geo_coord = proj.as_geodetic()
 
    #convert given lat , lon  from degree to meter 
    ur_lcc = proj.transform_point( lon, lat, geo_coord )
    lon_meter = ur_lcc[0]
    lat_meter = ur_lcc[1]
    # lon_meter = 314500
    # lat_meter = -2488766.25
    # lon_meter = 557593.75
    # lat_meter = -2795087.5
    # lon_meter = 818150
    # lat_meter = -3543700
    # Example: Accessing some lat_lon  pairs
    print(f"lat_lon coordinates in degree: lon = {lon}, lat =  {lat}")
    print(f"lat_lon coordinates in meters: lon_meter = {lon_meter}, lat_meter =  {lat_meter}")

    closest_key = find_closest_key(transformed_dict, lon_meter, lat_meter)
    closest_value = transformed_dict[closest_key]
    
    print(f"Closest point: {closest_key}, Value: {closest_value}")

    return closest_value




# class TestLatLonFunction(unittest.TestCase):

#     def test_getValueForLatLon_point(self):
#         # lat = -82.0
#         # lon = 27.5
#         #  Value for lon lat below is 78.60714
#         lon = -84.8700980486998
#         lat = 31.4747516112045
        
#         #three diff example
#         paramName = "LZFPM"

#         result = getValueForLatLon_point(lat, lon, paramName)

#         return result
        
#         # Check if the returned dictionary contains expected values


# if __name__ == "__main__":
#     unittest.main()

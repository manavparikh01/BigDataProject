
import json

import numpy as np

MILES_TO_KM = 1.60934
EARTH_RADIUS_KM = 6371.0
DEGREE_TO_KM = np.pi / 180 * EARTH_RADIUS_KM

def miles_to_latitude_degrees(miles):
    return miles / MILES_TO_KM / DEGREE_TO_KM

def miles_to_longitude_degrees(miles, latitude):
    return miles / MILES_TO_KM / (DEGREE_TO_KM * np.cos(latitude * np.pi / 180))

side_length_miles = 0.25

min_latitude, max_latitude = 40.49, 40.92
min_longitude, max_longitude = -74.27, -73.68

side_length_lat_deg = miles_to_latitude_degrees(side_length_miles)
side_length_lon_deg = miles_to_longitude_degrees(side_length_miles, (min_latitude + max_latitude) / 2)

latitudes = np.arange(min_latitude, max_latitude, side_length_lat_deg)
longitudes = np.arange(min_longitude, max_longitude, side_length_lon_deg)

def create_feature(coordinates):
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [coordinates]
        }
    }

features = [create_feature([(lon, lat), (lon, lat + side_length_lat_deg), 
                            (lon + side_length_lon_deg, lat + side_length_lat_deg), 
                            (lon + side_length_lon_deg, lat), (lon, lat)])
            for lat in latitudes for lon in longitudes]

geojson = {
    "type": "FeatureCollection",
    "features": features
}

geojson_str = json.dumps(geojson, indent=2)

geojson_half_file_path = '/grid_coordinates_half_size.geojson'
with open(geojson_half_file_path, 'w') as file:
    json.dump(geojson_str, file)
import csv
import numpy as np

min_latitude, max_latitude = 40.49, 40.92
min_longitude, max_longitude = -74.27, -73.68

grid_size_miles = 0.25

MILES_TO_KM = 1.60934
EARTH_RADIUS_KM = 6371.0
DEGREE_TO_KM = np.pi / 180 * EARTH_RADIUS_KM

def miles_to_latitude_degrees(miles):
    return miles / MILES_TO_KM / DEGREE_TO_KM

def miles_to_longitude_degrees(miles, latitude):
    return miles / MILES_TO_KM / (DEGREE_TO_KM * np.cos(latitude * np.pi / 180))

grid_size_lat = miles_to_latitude_degrees(grid_size_miles)
grid_size_lon = miles_to_longitude_degrees(grid_size_miles)

grid = []
id = 0
current_lat = min_latitude
while current_lat + grid_size_lat <= max_latitude:
    current_lon = min_longitude
    while current_lon + grid_size_lon <= max_longitude:
        grid.append([id, current_lat, current_lon, current_lat + grid_size_lat, current_lon + grid_size_lon])
        id += 1
        current_lon += grid_size_lon
    current_lat += grid_size_lat

with open("grid_coordinates_half_size.csv", mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ID', 'Min Latitude', 'Min Longitude', 'Max Latitude', 'Max Longitude'])
    for row in grid:
        writer.writerow(row)
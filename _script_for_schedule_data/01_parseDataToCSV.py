import os
import json
import pandas as pd
from geopy.distance import geodesic
import numpy as np

# Initialize a list to store the dataframes
dfs = []

# Directory containing the JSON files
json_dir = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_odFilipa\\output"

# Loop through each JSON file in the directory
for filename in os.listdir(json_dir):
    if filename.endswith(".json"):
        filepath = os.path.join(json_dir, filename)

        # Load the JSON file
        with open(filepath, "r") as f:
            data = json.load(f)

        # Convert to DataFrame
        df = pd.DataFrame(data['positions'])

        # Convert timestamp to datetime object
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Store dataframe in list
        dfs.append(df)

# Combine all the dataframes
all_data = pd.concat(dfs, ignore_index=True)

# Sort by trip_id and timestamp
all_data.sort_values(['trip_id', 'timestamp'], inplace=True)

# Calculate time difference in seconds
all_data['time_diff'] = all_data.groupby('trip_id')['timestamp'].diff().dt.total_seconds()

# Calculate Haversine distance
def haversine_distance(row):
    lon1, lat1, lon2, lat2 = row
    if pd.isna(lon1) or pd.isna(lat1) or pd.isna(lon2) or pd.isna(lat2):
        return np.nan
    coord1 = (lat1, lon1)
    coord2 = (lat2, lon2)
    return geodesic(coord1, coord2).meters

all_data['lat_shifted'] = all_data.groupby('trip_id')['lat'].shift(-1)
all_data['lon_shifted'] = all_data.groupby('trip_id')['lon'].shift(-1)

all_data['haversine_distance'] = all_data[['lon', 'lat', 'lon_shifted', 'lat_shifted']].apply(haversine_distance, axis=1)

# Calculate speed in km/h
all_data['speed'] = (all_data['haversine_distance'] / 1000) / (all_data['time_diff'] / 3600)

# Remove unnecessary columns
all_data.drop(['lat_shifted', 'lon_shifted', 'haversine_distance', 'time_diff'], axis=1, inplace=True)

# Save to CSV
all_data.to_csv("C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_odFilipa\\output\\processed_data.csv", index=False)

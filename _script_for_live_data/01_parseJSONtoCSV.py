import os
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

# Initialize an empty DataFrame to store all data
all_data = pd.DataFrame()

# Directory containing the JSON files
json_dir = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\odFilipa\\output"

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

        # Sort by trip_id and timestamp
        df.sort_values(['trip_id', 'timestamp'], inplace=True)

        # Calculate speed (Placeholder: You'll need to fill in this logic)
        # ...

        # Append to the master DataFrame
        all_data = all_data.append(df, ignore_index=True)

# Convert master DataFrame to GeoDataFrame
gdf = gpd.GeoDataFrame(all_data, geometry=[Point(xy) for xy in zip(all_data['lon'], all_data['lat'])])

# Save to a GeoJSON file to visualize in QGIS
gdf.to_file("combined_output_file.geojson", driver='GeoJSON')

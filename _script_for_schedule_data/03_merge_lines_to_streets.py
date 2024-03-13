import geopandas as gpd
from shapely.geometry import LineString
from tqdm import tqdm
import os

# Load the GeoDataFrame
segments_gdf = gpd.read_file(
    r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\2024_03_08\individual_segments.shp")

# Reproject to EPSG:2180 for accurate distance calculations
segments_gdf = segments_gdf.to_crs("EPSG:2180")

# Ensure there's a 'trip_sum' column initialized with 'trip_count' values
segments_gdf['trip_sum'] = segments_gdf['trip_count']

# Filter out segments shorter than 30 meters and where 'vehicle' is not 'Unknown'
segments_gdf = segments_gdf[segments_gdf.geometry.length > 10]
segments_gdf = segments_gdf[segments_gdf['vehicle'] != 'Unknown'].copy()

# Limit the GeoDataFrame to the first 1000 elements for testing
segments_gdf = segments_gdf.iloc[:5000].copy()

# Initialize a dictionary for updates to 'trip_sum'
trip_count_updates = {}

# Spatial index for efficiency
spatial_index = segments_gdf.sindex

for index, segment in tqdm(segments_gdf.iterrows(), total=segments_gdf.shape[0], desc="Processing segments"):
    buffer = segment.geometry.buffer(5)  # Adjust buffer distance as needed

    possible_matches_index = list(spatial_index.intersection(buffer.bounds))
    trip_sum = segment['trip_sum']

    for idx in possible_matches_index:
        if idx == index:
            continue  # Skip the segment itself

        possible_segment = segments_gdf.iloc[idx]
        if possible_segment.geometry.intersects(buffer):
            intersection = segment.geometry.intersection(possible_segment.geometry)
            # Check if the intersection is significant (at least 30m long)
            if isinstance(intersection, LineString) and intersection.length >= 30:
                trip_sum += possible_segment['trip_sum']
                # Consider marking for removal if needed
                # trip_count_updates[idx] = 0  # Uncomment if removing segments

    trip_count_updates[index] = trip_sum

# Apply the updates
for idx, sum_val in trip_count_updates.items():
    segments_gdf.at[idx, 'trip_sum'] = sum_val

# Optionally, remove segments marked with 'trip_sum' as 0 if they were overlapped and aggregated
segments_gdf = segments_gdf[segments_gdf['trip_sum'] > 0]  # Uncomment if removing segments

# Save the updated GeoDataFrame
output_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\2024_03_08\aggregated_segments.shp"
segments_gdf.to_file(output_path)

print("Segments updated successfully.")
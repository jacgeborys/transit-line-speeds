import geopandas as gpd
from tqdm import tqdm
import os
from shapely.geometry import LineString, MultiLineString

def split_linestring(linestring, segment_length):
    num_segments = int(round(linestring.length / segment_length))
    if num_segments == 0:
        num_segments = 1
    points = [linestring.interpolate(float(n) / num_segments, normalized=True) for n in range(num_segments + 1)]
    return [LineString([points[n], points[n + 1]]) for n in range(num_segments)]

# Load and reproject the GeoDataFrame
segments_gdf = gpd.read_file(
    r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\NY_2024_03_20\individual_segments.shp")
segments_gdf = segments_gdf.to_crs("EPSG:2180")

# Initialize 'trip_sum' and filter segments
segments_gdf['trip_sum'] = segments_gdf['trip_count']

# Drop rows where 'trip_sum' is 0
filtered_gdf = segments_gdf[(segments_gdf['trip_sum'] > 0) & (segments_gdf['length'] > 10) & (segments_gdf['vehicle'] != 'Unknown')].copy()

# Split segments into approximately 10m long segments
split_segments = []
for _, row in filtered_gdf.iterrows():
    if row.geometry.length > 10:
        for segment in split_linestring(row.geometry, 10):
            row_copy = row.copy()
            row_copy.geometry = segment
            row_copy['length'] = segment.length
            split_segments.append(row_copy)
    else:
        split_segments.append(row)

split_gdf = gpd.GeoDataFrame(split_segments, geometry='geometry', crs="EPSG:2180")

# Group by 'vehicle' type
grouped = split_gdf.groupby('vehicle')

output_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\NY_2024_03_20"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

for vehicle, df in grouped:
    df.sort_values(by='length', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    spatial_index = df.sindex

    trip_sum_updates = {}
    processed_shape_ids = {}  # Keep track of processed shape_ids for each longer segment

    for index, longer_segment in tqdm(df.iterrows(), total=df.shape[0], desc=f"Processing {vehicle} segments"):
        if index in processed_shape_ids:  # Skip if already processed
            continue

        buffer = longer_segment.geometry.buffer(4)
        possible_matches_index = list(spatial_index.intersection(buffer.bounds))
        processed_shape_ids[index] = {longer_segment['shape_id']}  # Initialize with its own shape_id

        for idx in possible_matches_index:
            if idx == index or df.at[idx, 'trip_sum'] == 0 or df.at[idx, 'shape_id'] in processed_shape_ids[index]:
                continue  # Skip self, processed, or same shape_id segments

            shorter_segment = df.iloc[idx]
            direction_difference = abs(shorter_segment['direction'] - longer_segment['direction']) % 360
            if direction_difference > 180:
                direction_difference = 360 - direction_difference

            if direction_difference <= 10:
                processed_shape_ids[index].add(shorter_segment['shape_id'])
                trip_sum_updates[index] = trip_sum_updates.get(index, longer_segment['trip_sum']) + shorter_segment['trip_sum']
                trip_sum_updates[idx] = 0  # Mark the shorter segment for removal

    for idx, sum_val in trip_sum_updates.items():
        if idx in df.index:
            df.at[idx, 'trip_sum'] = sum_val

    df = df[df['trip_sum'] > 0]
    df.to_file(os.path.join(output_dir, f'aggregated_segments_{vehicle}.shp'))

print("Aggregated segments for all vehicle types saved successfully.")
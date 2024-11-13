import geopandas as gpd
from tqdm import tqdm
import os
from shapely.geometry import LineString, MultiLineString
import numpy as np

# Define input and output directories
folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Warszawa_2024_11_10\\"
input_dir = folder_path
output_dir = folder_path

def split_linestring(linestring, segment_length):
    num_segments = int(round(linestring.length / segment_length))
    if num_segments == 0:
        num_segments = 1
    points = [linestring.interpolate(float(n) / num_segments, normalized=True) for n in range(num_segments + 1)]
    return [LineString([points[n], points[n + 1]]) for n in range(num_segments)]

def process_vehicle_segments(df, batch_size=10000):
    """Process segments in batches for better performance"""
    df = df.copy()
    df.sort_values(by='length', ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)
    spatial_index = df.sindex

    results = []
    processed_indices = set()  # Track processed indices instead of shape_ids

    for start_idx in tqdm(range(0, len(df), batch_size), desc="Processing batches"):
        batch = df.iloc[start_idx:min(start_idx + batch_size, len(df))]
        
        for idx, longer_segment in batch.iterrows():
            if idx in processed_indices:
                continue

            buffer = longer_segment.geometry.buffer(4)
            possible_matches_index = list(spatial_index.intersection(buffer.bounds))
            
            trip_sum = longer_segment['trip_count']
            matched_indices = {idx}

            for match_idx in possible_matches_index:
                if match_idx == idx or match_idx in processed_indices:
                    continue

                shorter_segment = df.iloc[match_idx]
                
                direction_difference = abs(shorter_segment['direction'] - longer_segment['direction']) % 360
                if direction_difference > 180:
                    direction_difference = 360 - direction_difference

                if direction_difference <= 10:
                    trip_sum += shorter_segment['trip_count']
                    matched_indices.add(match_idx)

            if trip_sum > 0:
                longer_segment = longer_segment.copy()
                longer_segment['trip_sum'] = trip_sum
                results.append(longer_segment)
            
            processed_indices.update(matched_indices)

    return gpd.GeoDataFrame(results, geometry='geometry', crs=df.crs)

def calculate_direction(line):
    """Calculate direction of a line segment in degrees"""
    if isinstance(line, LineString) and len(line.coords) > 1:
        start, end = line.coords[0], line.coords[-1]
        angle = np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))
        return angle % 360  # Normalize angle to be within [0, 360] degrees
    return None

def main():
    # Load and reproject the GeoDataFrame
    segments_gdf = gpd.read_file(os.path.join(input_dir, "individual_segments.shp"))
    segments_gdf = segments_gdf.to_crs("EPSG:2180")

    # Filter segments
    filtered_gdf = segments_gdf[
        (segments_gdf['trip_count'] > 0) & 
        (segments_gdf['length'] > 0) &  # Changed from 10 to 0
        (segments_gdf['vehicle'] != 'Unknown')
    ].copy()

    # Split lines into smaller segments
    print("Splitting lines into segments...")
    split_segments = []
    for idx, row in tqdm(filtered_gdf.iterrows(), total=len(filtered_gdf)):
        # Split each line into 10-meter segments (changed from 50m)
        segments = split_linestring(row.geometry, 10)
        for segment in segments:
            new_row = row.copy()
            new_row.geometry = segment
            new_row['length'] = segment.length
            new_row['direction'] = calculate_direction(segment)
            split_segments.append(new_row)
    
    split_gdf = gpd.GeoDataFrame(split_segments, crs=filtered_gdf.crs)
    print(f"Created {len(split_gdf)} segments from {len(filtered_gdf)} original lines")

    # Process each vehicle type
    for vehicle, df in split_gdf.groupby('vehicle'):
        print(f"\nProcessing {vehicle} segments...")
        result_gdf = process_vehicle_segments(df)
        
        output_file = os.path.join(output_dir, f'aggregated_segments_{vehicle}.shp')
        result_gdf.to_file(output_file)
        print(f"Saved {len(result_gdf)} segments for {vehicle}")

if __name__ == "__main__":
    main()

print("Aggregated segments for all vehicle types saved successfully.")
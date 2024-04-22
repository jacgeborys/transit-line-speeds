import geopandas as gpd
from tqdm import tqdm
import os
import pickle
import numpy as np
from shapely.geometry import LineString, MultiLineString

def calculate_direction(line):
    """Calculate the azimuth of the line segment (in degrees)."""
    start, end = line.coords[:2]
    return np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))


def split_linestring(linestring, segment_length):
    """Splits a linestring into multiple segments of approximately equal length."""
    num_segments = int(round(linestring.length / segment_length))
    if num_segments == 0:
        num_segments = 1
    points = [linestring.interpolate(float(n) / num_segments, normalized=True) for n in range(num_segments + 1)]
    return [LineString([points[n], points[n + 1]]) for n in range(num_segments)]

def main():
    print("Loading and reprojecting GeoDataFrame...")
    segments_gdf = gpd.read_file(
        r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_03_27\speed_processed_to_lines.shp")
    segments_gdf = segments_gdf.to_crs("EPSG:2180")

    print("Calculating segment lengths...")
    if 'length' not in segments_gdf.columns:
        segments_gdf['length'] = segments_gdf.geometry.length

    print("Filtering segments longer than 10m and known vehicle types...")
    filtered_gdf = segments_gdf[(segments_gdf['length'] > 10) & (segments_gdf['vehicle'] != 'Unknown')].copy()

    print("Splitting segments and calculating directions...")
    split_segments = []
    for _, row in tqdm(filtered_gdf.iterrows(), total=len(filtered_gdf), desc="Processing Segments"):
        for segment in split_linestring(row.geometry, 10):
            row_copy = row.copy()
            row_copy.geometry = segment
            row_copy['length'] = segment.length
            row_copy['direction'] = calculate_direction(segment)
            split_segments.append(row_copy)

    split_gdf = gpd.GeoDataFrame(split_segments, geometry='geometry', crs="EPSG:2180")

    # Save the split data to a pickle file
    with open('split_data.pkl', 'wb') as f:
        pickle.dump(split_gdf, f)
    print("Split data saved to pickle.")

    # Load the split data from the pickle file for further processing
    with open('split_data.pkl', 'rb') as f:
        split_gdf = pickle.load(f)
    print("Split data loaded from pickle.")

    output_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_03_27"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print("Processing each vehicle type...")
    grouped = split_gdf.groupby('vehicle')
    for vehicle, df in grouped:
        print(f"Processing vehicle type: {vehicle}")
        df.sort_values(by='length', ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        spatial_index = df.sindex

        average_speed_updates = {}
        processed_shape_ids = {}

        for index, longer_segment in tqdm(df.iterrows(), total=df.shape[0], desc=f"Aggregating speeds for {vehicle}"):
            buffer = longer_segment.geometry.buffer(4)
            possible_matches_index = list(spatial_index.intersection(buffer.bounds))
            processed_shape_ids[index] = {longer_segment['shape_id']}

            for idx in possible_matches_index:
                if idx == index or df.at[idx, 'shape_id'] in processed_shape_ids[index]:
                    continue
                shorter_segment = df.iloc[idx]
                direction_difference = abs(shorter_segment['direction'] - longer_segment['direction']) % 360
                if direction_difference > 180:
                    direction_difference = 360 - direction_difference
                if direction_difference <= 10:
                    processed_shape_ids[index].add(shorter_segment['shape_id'])
                    speeds = average_speed_updates.get(index, [])
                    speeds.append(shorter_segment['speed'])
                    average_speed_updates[index] = speeds

        for idx, speeds in average_speed_updates.items():
            if speeds:
                df.at[idx, 'average_speed'] = np.mean(speeds)

        df.drop(columns=['speed'], inplace=True)
        df.rename(columns={'average_speed': 'speed'}, inplace=True)
        df = df.dropna(subset=['speed'])
        df.to_file(os.path.join(output_dir, f'average_speed_segments_{vehicle}.shp'))
        print(f"Data saved for vehicle type: {vehicle}")

    print("All processes complete and data saved successfully.")

if __name__ == "__main__":
    main()
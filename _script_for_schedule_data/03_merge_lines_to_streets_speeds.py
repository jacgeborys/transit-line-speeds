import geopandas as gpd
from tqdm import tqdm
import os
import numpy as np
from shapely.geometry import LineString

def calculate_direction(line):
    """Calculate the azimuth of the line segment in degrees."""
    if not line.is_empty and len(line.coords) > 1:
        start, end = line.coords[0], line.coords[-1]
        return np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))
    return None

def split_linestring(linestring, segment_length):
    """Splits a linestring into multiple segments of approximately equal length."""
    num_segments = max(1, int(round(linestring.length / segment_length)))
    points = [linestring.interpolate(float(n) / num_segments, normalized=True) for n in range(num_segments + 1)]
    return [LineString([points[n], points[n + 1]]) for n in range(num_segments)]

def main():
    print("Loading and reprojecting GeoDataFrame...")
    file_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_11_04\speed_processed_to_lines.shp"
    segments_gdf = gpd.read_file(file_path)
    segments_gdf = segments_gdf.to_crs("EPSG:2180")

    segments_gdf['direction'] = segments_gdf['geometry'].apply(calculate_direction)
    segments_gdf['processed'] = False  # Initialize 'processed' column

    filtered_gdf = segments_gdf[(segments_gdf['length'] > 10) & (segments_gdf['vehicle'] != 'Unknown')]

    split_segments = []
    for _, row in tqdm(filtered_gdf.iterrows(), total=len(filtered_gdf), desc="Processing Segments"):
        for segment in split_linestring(row.geometry, 10):
            row_copy = row.copy()
            row_copy.geometry = segment
            row_copy['length'] = segment.length
            row_copy['direction'] = calculate_direction(segment)
            split_segments.append(row_copy)

    split_gdf = gpd.GeoDataFrame(split_segments, geometry='geometry', crs="EPSG:2180")

    output_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_11_04"
    os.makedirs(output_dir, exist_ok=True)

    grouped = split_gdf.groupby('vehicle')
    for vehicle, df in grouped:
        df = df.reset_index(drop=True)
        spatial_index = df.sindex

        results = []
        for idx, segment in df.iterrows():
            if not segment['processed']:
                buffer = segment.geometry.buffer(4)
                possible_matches_index = list(spatial_index.intersection(buffer.bounds))
                possible_matches = df.iloc[possible_matches_index]
                compatible = possible_matches[np.abs(possible_matches['direction'] - segment['direction']) % 360 <= 10]
                compatible = compatible[compatible.index != idx]  # Exclude self

                if not compatible.empty:
                    mean_speed = compatible['speed'].mean()
                    segment['speed'] = mean_speed
                    segment['processed'] = True  # Mark current and compatible segments as processed
                    df.loc[compatible.index, 'processed'] = True
                    results.append(segment)

        # Exclude 'shape_id' and 'processed' from output
        final_gdf = gpd.GeoDataFrame(results, columns=[col for col in df.columns if col not in ['shape_id', 'processed']], geometry='geometry', crs=df.crs)
        final_gdf.to_file(os.path.join(output_dir, f'average_speed_segments_{vehicle}.shp'))
        print(f"Data saved for vehicle type: {vehicle}")

    print("All processes complete and data saved successfully.")

if __name__ == "__main__":
    main()

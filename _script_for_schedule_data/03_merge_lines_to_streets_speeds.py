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

def process_vehicle_segments(df, spatial_index, batch_size=10000):
    """Process segments in batches for better performance"""
    results = []
    total_segments = len(df)
    
    # Process in batches
    for start_idx in tqdm(range(0, total_segments, batch_size), desc=f"Processing batches"):
        end_idx = min(start_idx + batch_size, total_segments)
        batch = df.iloc[start_idx:end_idx]
        
        # Process only unprocessed segments in the batch
        unprocessed = batch[~batch['processed']]
        
        for idx, segment in unprocessed.iterrows():
            buffer = segment.geometry.buffer(4)
            possible_matches_index = list(spatial_index.intersection(buffer.bounds))
            
            if possible_matches_index:
                possible_matches = df.iloc[possible_matches_index]
                direction_diff = np.abs(possible_matches['direction'] - segment['direction']) % 360
                compatible = possible_matches[(direction_diff <= 10) & (possible_matches.index != idx)]
                
                if not compatible.empty:
                    mean_speed = compatible['speed'].mean()
                    segment['speed'] = mean_speed
                    segment['processed'] = True
                    df.loc[compatible.index, 'processed'] = True
                    results.append(segment)
        
        if len(results) % 10000 == 0:
            print(f"Found {len(results)} compatible segments")
    
    return results

def main():
    print("Loading and reprojecting GeoDataFrame...")
    # File paths
    input_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_11_10"
    file_path = os.path.join(input_dir, "speed_processed_to_lines.shp")
    output_dir = input_dir  # Set output directory same as input
    segments_gdf = gpd.read_file(file_path)
    segments_gdf = segments_gdf.to_crs("EPSG:2180")

    segments_gdf['direction'] = segments_gdf['geometry'].apply(calculate_direction)
    segments_gdf['processed'] = False

    filtered_gdf = segments_gdf[(segments_gdf['length'] > 10) & (segments_gdf['vehicle'] != 'Unknown')]
    print(f"Number of segments after filtering: {len(filtered_gdf)}")

    # Process each vehicle type separately
    for vehicle, df in filtered_gdf.groupby('vehicle'):
        print(f"\nProcessing vehicle type: {vehicle}")
        print(f"Number of segments: {len(df)}")
        
        df = df.reset_index(drop=True)
        spatial_index = df.sindex
        
        results = process_vehicle_segments(df, spatial_index)
        
        if results:
            final_gdf = gpd.GeoDataFrame(
                results, 
                columns=[col for col in df.columns if col not in ['shape_id', 'processed']], 
                geometry='geometry', 
                crs=df.crs
            )
            output_file = os.path.join(output_dir, f'average_speed_segments_{vehicle}.shp')
            final_gdf.to_file(output_file)
            print(f"Saved {len(final_gdf)} segments for {vehicle}")
        else:
            print(f"No compatible segments found for {vehicle}")

    print("Processing complete!")

if __name__ == "__main__":
    main()

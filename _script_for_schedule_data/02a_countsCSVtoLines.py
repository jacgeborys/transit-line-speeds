import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point
import numpy as np
import os

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Warszawa_2024_11_10\\"

def calculate_direction(line):
    """Calculate direction of a line segment in degrees"""
    if isinstance(line, LineString) and len(line.coords) > 1:
        start, end = line.coords[0], line.coords[-1]
        angle = np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))
        return angle % 360  # Normalize angle to be within [0, 360] degrees
    return None

def create_segments(group):
    """Create line segments from points"""
    segments = []
    points = list(group.geometry)
    for i in range(len(points) - 1):
        segment = LineString([points[i], points[i + 1]])
        length = segment.length
        direction = calculate_direction(segment)
        segments.append({
            'geometry': segment,
            'shape_id': group['shape_id'].iloc[0],
            'trip_count': group['trip_count'].iloc[0],
            'vehicle': group['vehicle'].iloc[0],  # Changed from vehicle_type to vehicle
            'length': length,
            'direction': direction
        })
    return segments

def main():
    # Read the CSV file produced by 01_parseDataToCSV_counts.py
    print("Reading shapes_processed.csv...")
    df = pd.read_csv(os.path.join(folder_path, "shapes_processed.csv"))
    
    # Create GeoDataFrame from points
    print("Creating GeoDataFrame from points...")
    gdf = gpd.GeoDataFrame(
        df, 
        geometry=[Point(xy) for xy in zip(df.shape_pt_lon, df.shape_pt_lat)],
        crs="EPSG:4326"
    )
    
    # Reproject to EPSG:2180 for accurate distance calculations
    print("Reprojecting to EPSG:2180...")
    gdf = gdf.to_crs("EPSG:2180")
    
    # Sort by shape_id and sequence
    print("Sorting points...")
    gdf_sorted = gdf.sort_values(['shape_id', 'shape_pt_sequence'])
    
    # Create segments
    print("Creating line segments...")
    segments_list = []
    for name, group in gdf_sorted.groupby('shape_id'):
        segments_list.extend(create_segments(group))
    
    # Create GeoDataFrame from segments
    print("Creating final GeoDataFrame...")
    segments_gdf = gpd.GeoDataFrame(segments_list, geometry='geometry', crs="EPSG:2180")
    
    # Save to shapefile
    output_path = os.path.join(folder_path, "individual_segments.shp")
    print(f"Saving to {output_path}...")
    segments_gdf.to_file(output_path)
    
    print("Summary:")
    print(f"Number of segments created: {len(segments_gdf)}")
    print(f"Number of unique shape_ids: {segments_gdf['shape_id'].nunique()}")
    print(f"Vehicle types: {segments_gdf['vehicle'].unique()}")
    print("Individual segments shapefile saved successfully.")

if __name__ == "__main__":
    main() 
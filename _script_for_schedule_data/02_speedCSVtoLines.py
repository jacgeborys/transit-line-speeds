import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
import numpy as np
import math

# Set the folder path for ease of access
folder_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\Warszawa_2024_11_10\\"

def calculate_direction(line):
    """Calculate the azimuth of the line segment (in degrees)."""
    if isinstance(line, LineString) and len(line.coords) > 1:
        start, end = line.coords[0], line.coords[-1]
        angle = np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))
        return angle % 360  # Normalize angle to be within [0, 360] degrees
    return None

def offset_segment_right(segment, distance=10):
    """Offset a line segment to the right by a given distance."""
    if isinstance(segment, LineString):
        start, end = segment.coords[:2]
        azimuth = np.arctan2(end[1] - start[1], end[0] - start[0])
        perp_azimuth = azimuth - np.pi / 2
        start_offset = (start[0] + distance * np.cos(perp_azimuth), start[1] + distance * np.sin(perp_azimuth))
        end_offset = (end[0] + distance * np.cos(perp_azimuth), end[1] + distance * np.sin(perp_azimuth))
        return LineString([start_offset, end_offset])
    return segment

def process_data():
    # Load the processed shapes CSV
    shapes_csv_path = folder_path + "shapes_processed.csv"
    shapes_df = pd.read_csv(shapes_csv_path)
    gdf = gpd.GeoDataFrame(
        shapes_df,
        geometry=gpd.points_from_xy(shapes_df.shape_pt_lon, shapes_df.shape_pt_lat),
        crs="EPSG:4326"
    ).to_crs(epsg=2180)  # Direct conversion to EPSG:2180 for metric calculation

    # Sort and create line segments
    gdf = gdf.sort_values(['shape_id', 'shape_pt_sequence'])
    segments = []

    for shape_id, group in gdf.groupby('shape_id'):
        prev_point = None
        for idx, row in group.iterrows():
            if prev_point is not None:
                segment = LineString([prev_point, row['geometry']])
                segments.append({
                    'shape_id': shape_id,
                    'vehicle': row['vehicle'],
                    'geometry': segment,
                    'speed': row['speed'],
                    'length': segment.length,
                    'direction': calculate_direction(segment)
                })
            prev_point = row['geometry']

    # Create a GeoDataFrame from the segments
    segment_gdf = gpd.GeoDataFrame(segments, crs="EPSG:2180")
    segment_gdf['geometry'] = segment_gdf['geometry'].apply(offset_segment_right)

    # Set the geometry column explicitly
    segment_gdf = segment_gdf.set_geometry('geometry')
    segment_gdf['length'] = segment_gdf['geometry'].length
    segment_gdf['direction'] = segment_gdf['geometry'].apply(calculate_direction)

    # Save to a shapefile
    output_path = folder_path + "speed_processed_to_lines.shp"
    segment_gdf.to_file(output_path, driver='ESRI Shapefile')

if __name__ == "__main__":
    process_data()
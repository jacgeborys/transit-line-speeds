import geopandas as gpd
from tqdm import tqdm

# Load the GeoDataFrame
segments_gdf = gpd.read_file(
    r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\2024_01_19\individual_segments_2180.shp")

# Spatial index for efficiency
spatial_index = segments_gdf.sindex

# Initialize a list to keep track of indices to remove
indices_to_remove = set()

for index, segment in tqdm(segments_gdf.iterrows(), total=segments_gdf.shape[0], desc="Checking segments"):
    if index in indices_to_remove:
        # If the segment is already marked for removal, skip it
        continue

    # Create a 30m buffer around the segment
    buffer = segment.geometry.buffer(30)

    # Find potential segments within the buffer bounds
    possible_matches_index = list(spatial_index.intersection(buffer.bounds))
    for idx in possible_matches_index:
        # Skip the segment itself
        if idx == index:
            continue

        # If another segment is fully within the buffer, mark it for removal
        if segments_gdf.iloc[idx].geometry.within(buffer):
            indices_to_remove.add(idx)

# Exclude the segments marked for removal
indices_to_keep = [idx for idx in range(len(segments_gdf)) if idx not in indices_to_remove]
unique_segments_gdf = segments_gdf.iloc[indices_to_keep]

# Save to a shapefile
output_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\2024_01_19\unique_segments.shp"
unique_segments_gdf.to_file(output_path)

print("Unique segments saved successfully.")
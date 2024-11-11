import pandas as pd
from tqdm import tqdm
import os
import numpy as np
import re

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Warszawa_2024_11_10\\"
MAX_SPEED = 80  # Set the maximum allowable speed

def parse_time(t):
    """
    Converts a string in the format HH:MM:SS into a pd.Timestamp object.
    If the hour is 24 or more, it returns None.
    Only the time information is kept. The date part is discarded.
    """
    t_split = t.split(':')
    hours = int(t_split[0])

    # If the hour is 24 or more, return None.
    if hours >= 24:
        return None

    return pd.to_datetime(t).time()

def is_within_time_range(t, start, end):
    """
    Checks if a given time is within the range [start, end].
    All inputs should be pd.Timestamp.time objects.
    """
    if start <= end:
        return start <= t <= end
    else:  # Wraps around midnight.
        return start <= t or t <= end

def vehicle_type(route_id):
    """
    Determines vehicle type based on route_id pattern:
    - 1-2 digits: Tram
    - 3 digits: Bus
    - Starts with S or R: Train
    """
    route_id = str(route_id).strip()
    
    if route_id.isdigit():
        if len(route_id) <= 2:
            return 'Tram'
        elif len(route_id) == 3:
            return 'Bus'
    elif route_id.startswith(('S', 'R')):
        return 'Train'
    return 'Bus'  # Default to Bus for other cases like 'E-1'

def load_and_filter_data():
    # Load dataframes
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"))
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"))

    # Add vehicle type based on route_id instead of shape_id
    trips['vehicle'] = trips['route_id'].apply(vehicle_type)

    # Load stop_times from pickle if available or from csv
    pickle_path = os.path.join(folder_path, "stop_times_processed.pkl")
    if os.path.exists(pickle_path):
        stop_times = pd.read_pickle(pickle_path)
    else:
        stop_times = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'stop_id': str})

        # Convert and filter time
        tqdm.pandas(desc="Parsing times")
        stop_times['arrival_time'] = stop_times['arrival_time'].progress_apply(parse_time)
        stop_times.dropna(subset=['arrival_time'], inplace=True)

        # Add departure time parsing and filtering
        stop_times['departure_time'] = stop_times['departure_time'].progress_apply(parse_time)
        stop_times.dropna(subset=['departure_time'], inplace=True)

        # Save processed stop_times to pickle
        stop_times.to_pickle(pickle_path)

    # Filter by time range
    range_starts = [pd.to_datetime('06:00:00').time(), pd.to_datetime('14:00:00').time()]
    range_ends = [pd.to_datetime('11:00:00').time(), pd.to_datetime('19:00:00').time()]

    # Create masks for times within the specified ranges
    in_range_masks = []

    for start, end in zip(range_starts, range_ends):
        in_range_mask = stop_times['arrival_time'].apply(is_within_time_range, args=(start, end))
        in_range_masks.append(in_range_mask)

    # Combine the masks to identify rows with times within any range
    combined_in_range_mask = pd.concat(in_range_masks, axis=1).any(axis=1)

    # Filter rows by those within the timeframes
    valid_stops = stop_times[combined_in_range_mask]

    # Identify valid trip_ids
    valid_trips = valid_stops['trip_id'].unique()

    # Filter stop_times and trips dataframe by the valid trips
    stop_times = stop_times[stop_times['trip_id'].isin(valid_trips)]
    trips = trips[trips['trip_id'].isin(valid_trips)]

    return shapes, trips, stop_times

def calculate_differences(filtered_stop_times):
    filtered_stop_times['arrival_time'] = pd.to_datetime(filtered_stop_times['arrival_time'], format='%H:%M:%S')
    filtered_stop_times['departure_time'] = pd.to_datetime(filtered_stop_times['departure_time'], format='%H:%M:%S')
    filtered_stop_times.sort_values(by=['trip_id', 'arrival_time'], inplace=True)

    # Calculate differences in time and distance for general cases
    filtered_stop_times['time_diff'] = filtered_stop_times.groupby('trip_id')['arrival_time'].diff().dt.total_seconds()
    filtered_stop_times['dist_diff'] = filtered_stop_times.groupby('trip_id')['shape_dist_traveled'].diff()

    # Handle the first segment for each trip
    filtered_stop_times['prev_departure_time'] = filtered_stop_times.groupby('trip_id')['departure_time'].shift(1)
    mask = filtered_stop_times['time_diff'].isna()
    filtered_stop_times.loc[mask, 'time_diff'] = \
    (filtered_stop_times['arrival_time'] - filtered_stop_times['prev_departure_time']).dt.total_seconds().loc[mask]
    filtered_stop_times.loc[mask, 'dist_diff'] = filtered_stop_times['shape_dist_traveled'].loc[mask]

    # Filter out any NaN values, rows where time_diff is 0, and rows with speed < 1
    filtered_stop_times = filtered_stop_times.dropna(subset=['time_diff', 'dist_diff']).loc[filtered_stop_times['time_diff'] > 0]
    filtered_stop_times['speed'] = (filtered_stop_times['dist_diff'] / filtered_stop_times['time_diff']) * 3600  # Convert speed to km/h
    filtered_stop_times = filtered_stop_times.loc[filtered_stop_times['speed'] >= 1]
    filtered_stop_times = filtered_stop_times[filtered_stop_times['speed'] <= MAX_SPEED]

    print(f"Number of records after calculating differences: {filtered_stop_times.shape[0]}")
    return filtered_stop_times

def merge_and_save(shapes, trips, stop_times):
    # Ensure vehicle type is determined before merging
    trips['vehicle'] = trips['route_id'].apply(vehicle_type)

    # Merge stop_times with trips to get shape_id and vehicle type
    stop_times = pd.merge(stop_times, trips[['trip_id', 'shape_id', 'vehicle']], on='trip_id', how='inner')

    # Calculate average speed for each shape_id and shape_dist_traveled
    average_speed_shape = stop_times.groupby(['shape_id', 'shape_dist_traveled', 'vehicle'])['speed'].mean().reset_index()
    print("\nAverage speeds per shape_id and distance:")
    print(average_speed_shape.head())

    # Create a mapping of shape_id to vehicle type
    vehicle_mapping = stop_times[['shape_id', 'vehicle']].drop_duplicates()
    
    # First, merge shapes with vehicle type
    shapes = pd.merge(shapes, vehicle_mapping, on='shape_id', how='left')
    
    # Then assign speeds to points based on their position in the sequence
    shapes_with_speeds = []
    
    for shape_id, group in shapes.groupby('shape_id'):
        shape_speeds = average_speed_shape[average_speed_shape['shape_id'] == shape_id]
        if not shape_speeds.empty:
            # Sort by sequence
            group = group.sort_values('shape_pt_sequence')
            
            for idx, row in group.iterrows():
                # Find the closest shape_dist_traveled value
                if pd.isna(row['shape_dist_traveled']):
                    # Estimate position based on sequence number
                    position = row['shape_pt_sequence'] / group['shape_pt_sequence'].max()
                    max_dist = shape_speeds['shape_dist_traveled'].max()
                    estimated_dist = position * max_dist
                    
                    # Find the closest speed measurement
                    closest_idx = (shape_speeds['shape_dist_traveled'] - estimated_dist).abs().idxmin()
                    row['speed'] = shape_speeds.loc[closest_idx, 'speed']
                else:
                    # Use exact distance if available
                    closest_idx = (shape_speeds['shape_dist_traveled'] - row['shape_dist_traveled']).abs().idxmin()
                    row['speed'] = shape_speeds.loc[closest_idx, 'speed']
                
                shapes_with_speeds.append(row)
    
    shapes = pd.DataFrame(shapes_with_speeds)

    # Filter out invalid speeds
    shapes = shapes.dropna(subset=['speed'])
    shapes = shapes[shapes['speed'] >= 1]

    # Save result
    output_path = os.path.join(folder_path, "shapes_processed.csv")
    shapes.to_csv(output_path, index=False)

    # Print summary
    print("\nFinal Summary:")
    print(f"Number of unique shape_ids: {shapes['shape_id'].nunique()}")
    print(f"Number of rows with valid speeds: {len(shapes)}")
    if len(shapes) > 0:
        print(f"Speed range: {shapes['speed'].min():.2f} to {shapes['speed'].max():.2f}")

def main():
    shapes, trips, stop_times = load_and_filter_data()
    stop_times = calculate_differences(stop_times)
    merge_and_save(shapes, trips, stop_times)

if __name__ == "__main__":
    main()
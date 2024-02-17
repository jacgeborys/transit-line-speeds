import pandas as pd
from tqdm import tqdm
import os
import numpy as np

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\Oslo_2024_01_24\\"
MAX_SPEED = 80  # Set the maximum allowable speed

# Set display options
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)        # Use maximum width of the console
pd.set_option('display.max_colwidth', None) # Show full width of each column

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

def load_and_filter_data():
    # Load dataframes
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"), dtype={'shape_id': str})
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"), dtype={'trip_id': str, 'shape_id': str})

    print("Unique 'shape_id's in 'shapes':", shapes['shape_id'].nunique())
    print("Unique 'shape_id's in 'trips':", trips['shape_id'].nunique())

    # Convert 'shape_dist_traveled' from meters to kilometers
    shapes['shape_dist_traveled'] /= 1000

    # Load stop_times from pickle if available or from csv
    pickle_path = os.path.join(folder_path, "stop_times_processed.pkl")
    if os.path.exists(pickle_path):
        stop_times = pd.read_pickle(pickle_path)
    else:
        stop_times = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'stop_id': str})

        stop_times['shape_dist_traveled'] = pd.to_numeric(stop_times['shape_dist_traveled'], errors='coerce') / 1000

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

    print("Number of records in stop_times after filtering:", len(stop_times))

    return shapes, trips, stop_times

def calculate_differences(filtered_stop_times):
    filtered_stop_times['arrival_time'] = pd.to_datetime(filtered_stop_times['arrival_time'], format='%H:%M:%S')
    filtered_stop_times['departure_time'] = pd.to_datetime(filtered_stop_times['departure_time'], format='%H:%M:%S')
    filtered_stop_times.sort_values(by=['trip_id', 'arrival_time'], inplace=True)

    # Calculate differences in time and distance for general cases
    filtered_stop_times['time_diff'] = filtered_stop_times.groupby('trip_id')['arrival_time'].diff().dt.total_seconds()
    filtered_stop_times['dist_diff'] = filtered_stop_times.groupby('trip_id')['shape_dist_traveled'].diff()

    # Debugging: Check distributions and sample values
    print("Sample time differences (in seconds):")
    print(filtered_stop_times['time_diff'].sample(10))
    print("Sample distance differences (in meters):")
    print(filtered_stop_times['dist_diff'].sample(10))

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

    # Debugging: Check sample speed values after calculation
    print("Sample speed values (in km/h):")
    print(filtered_stop_times['speed'].sample(10))

    print("First few records in filtered_stop_times:")
    print(filtered_stop_times.head())
    print("Last few records in filtered_stop_times:")
    print(filtered_stop_times.tail())

    return filtered_stop_times

    print(f"Number of records after calculating differences: {filtered_stop_times.shape[0]}")
    return filtered_stop_times

def merge_and_save(shapes, trips, filtered_stop_times):
    # Select necessary columns and merge dataframes
    filtered_stop_times = filtered_stop_times[
        ['trip_id', 'arrival_time', 'stop_id', 'shape_dist_traveled', 'time_diff', 'dist_diff', 'speed']]

    print("Unique shape_ids in filtered_stop_times before merging with trips:",
          filtered_stop_times['shape_id'].nunique())
    print("Sample data from filtered_stop_times before merging with trips:")
    print(filtered_stop_times.head())

    # Merge with trips to get shape_id
    filtered_stop_times = pd.merge(filtered_stop_times, trips[['trip_id', 'shape_id']], on='trip_id', how='inner')

    print("Unique shape_ids in filtered_stop_times after merging with trips:",
          filtered_stop_times['shape_id'].nunique())
    print("Sample data from filtered_stop_times after merging with trips:")
    print(filtered_stop_times.head())

    # Average speed for each shape_id and shape_dist_traveled
    average_speed_shape = filtered_stop_times.groupby(['shape_id', 'shape_dist_traveled'])['speed'].mean().reset_index()

    # Merge shapes with average_speed_shape to get the speed information
    shapes = pd.merge(shapes, average_speed_shape, on=['shape_id', 'shape_dist_traveled'], how='left')

    # Backfill the speeds within each shape_id group
    shapes['speed'] = shapes.groupby('shape_id')['speed'].fillna(method='bfill')

    # Removing rows where speed is null or less than 1 km/h
    shapes = shapes.dropna(subset=['speed'])  # <-- New line to remove rows with null speed
    shapes = shapes[~(shapes['speed'] < 1)]

    # Save the result
    shapes.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)

    unique_shapes_before_merge = shapes['shape_id'].nunique()
    unique_shapes_with_speeds = shapes.dropna(subset=['speed'])['shape_id'].nunique()
    print(f"Number of unique shape_ids before merging: {unique_shapes_before_merge}")
    print(f"Number of unique shape_ids with speeds after merging: {unique_shapes_with_speeds}")
    print("Unique shape_ids in shapes after all operations:", shapes['shape_id'].nunique())


def main():
    shapes, trips, filtered_stop_times = load_and_filter_data()


    # Debugging: Count unique shape_ids in shapes and trips
    print("Unique shape_ids in shapes:", shapes['shape_id'].nunique())
    print("Unique shape_ids in trips:", trips['shape_id'].nunique())

    filtered_stop_times = calculate_differences(filtered_stop_times)

    # Debugging: Print a few rows from filtered_stop_times before merging
    print("Sample data from filtered_stop_times before merging with trips:")
    print(filtered_stop_times.head())

    # Merge with trips to get shape_id
    filtered_stop_times = pd.merge(filtered_stop_times, trips[['trip_id', 'shape_id']], on='trip_id', how='inner')

    # Debugging: Count unique shape_ids after merging with trips
    print("Unique shape_ids in filtered_stop_times after merging with trips:", filtered_stop_times['shape_id'].nunique())

    # Debugging: Print a few rows from filtered_stop_times after merging
    print("Sample data from filtered_stop_times after merging with trips:")
    print(filtered_stop_times.head())

    average_speed_shape = filtered_stop_times.groupby(['shape_id', 'shape_dist_traveled'])['speed'].mean().reset_index()

    shapes = pd.merge(shapes, average_speed_shape, on=['shape_id', 'shape_dist_traveled'], how='left')
    shapes['speed'] = shapes.groupby('shape_id')['speed'].fillna(method='bfill')
    shapes = shapes.dropna(subset=['speed'])
    shapes = shapes[~(shapes['speed'] < 1)]

    # Debugging: Count unique shape_ids after all operations
    print("Unique shape_ids in shapes after all operations:", shapes['shape_id'].nunique())

    shapes.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)

if __name__ == "__main__":
    main()

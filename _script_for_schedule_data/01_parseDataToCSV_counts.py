import pandas as pd
from tqdm import tqdm
import os
import re

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\2024_03_08\\"
MAX_SPEED = 80  # Maximum speed in km/h
DATE_FILTER = "RA240308"  # Date to filter by

def parse_time(t):
    t_split = t.split(':')
    if int(t_split[0]) >= 24:  # Filter out invalid times
        return None
    return pd.to_datetime(t).time()

def is_within_time_range(t, start, end):
    return start <= t <= end

def vehicle_type(shape_id):
    if re.search(r'/\d{1,2}/', shape_id):
        return 'Tram'
    elif re.search(r'/\d{3}/', shape_id):
        return 'Bus'
    elif re.search(r'/S\d{1,2}/', shape_id):
        return 'Train'
    return 'Unknown'

def load_and_filter_data():
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"), dtype={'shape_id': str})
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"), dtype={'trip_id': str, 'shape_id': str})
    stop_times = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'stop_id': str})

    # Filter for specific date and within time range
    shapes = shapes[shapes['shape_id'].str.contains(DATE_FILTER)]
    trips = trips[trips['shape_id'].str.contains(DATE_FILTER)]
    stop_times = stop_times[stop_times['trip_id'].isin(trips['trip_id'])]

    tqdm.pandas(desc="Parsing times")
    stop_times['arrival_time'] = stop_times['arrival_time'].progress_apply(parse_time)
    stop_times['departure_time'] = stop_times['departure_time'].progress_apply(parse_time)
    stop_times.dropna(subset=['arrival_time', 'departure_time'], inplace=True)

    # Apply time range filter (6:00 to 22:00)
    start_time = pd.to_datetime('06:00:00').time()
    end_time = pd.to_datetime('22:00:00').time()
    stop_times = stop_times[stop_times['arrival_time'].apply(lambda x: is_within_time_range(x, start_time, end_time))]

    # Add vehicle type to shapes
    shapes['vehicle_type'] = shapes['shape_id'].apply(vehicle_type)

    # Count trips per shape_id and merge
    trip_counts = trips.groupby('shape_id').size().reset_index(name='trip_count')
    shapes_merged = pd.merge(shapes, trip_counts, on='shape_id', how='left')

    # Save to CSV
    shapes_merged.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)
    print("Filtered shapes saved to CSV file.")

    # Save to pickle
    shapes_merged.to_pickle(os.path.join(folder_path, "shapes_processed.pkl"))
    print("Filtered shapes saved to pickle file.")

def main():
    load_and_filter_data()

if __name__ == "__main__":
    main()
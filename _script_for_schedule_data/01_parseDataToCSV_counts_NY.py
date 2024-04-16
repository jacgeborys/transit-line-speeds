import pandas as pd
from tqdm import tqdm
import os

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\NY_2024_03_21\\"

def parse_time(t):
    t_split = t.split(':')
    if int(t_split[0]) >= 24:  # Adjust for times beyond 24:00:00
        adjusted_hour = int(t_split[0]) - 24
        adjusted_time = f"{adjusted_hour:02}:{t_split[1]}:{t_split[2]}"
        return pd.to_datetime(adjusted_time, format='%H:%M:%S').time()
    return pd.to_datetime(t, format='%H:%M:%S').time()

def is_within_time_range(t, start, end):
    return start <= t <= end

def load_and_filter_data():
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"), dtype={'shape_id': str})
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"), dtype={'trip_id': str, 'shape_id': str})
    stop_times = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'stop_id': str})

    # Assuming all data is for buses, so no need to filter by DATE_FILTER or vehicle type
    shapes['vehicle_type'] = 'Bus'

    tqdm.pandas(desc="Parsing times")
    stop_times['arrival_time'] = stop_times['arrival_time'].progress_apply(parse_time)
    stop_times['departure_time'] = stop_times['departure_time'].progress_apply(parse_time)
    stop_times.dropna(subset=['arrival_time', 'departure_time'], inplace=True)

    # Apply time range filter (6:00 to 22:00)
    start_time = pd.to_datetime('06:00:00').time()
    end_time = pd.to_datetime('22:00:00').time()
    stop_times = stop_times[stop_times['arrival_time'].apply(lambda x: is_within_time_range(x, start_time, end_time))]

    # Merge shapes with trips to include trip_count
    trip_counts = trips.groupby('shape_id').size().reset_index(name='trip_count')
    shapes_merged = pd.merge(shapes, trip_counts, on='shape_id', how='left')

    # Save to CSV
    shapes_merged.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)
    print("Filtered shapes saved to CSV file.")

def main():
    load_and_filter_data()

if __name__ == "__main__":
    main()
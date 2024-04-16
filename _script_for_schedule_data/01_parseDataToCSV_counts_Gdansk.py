import pandas as pd
from tqdm import tqdm
import os
import re

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Gdansk_2024_03_20\\"
MAX_SPEED = 80  # Maximum speed in km/h
DATE_FILTER = "20240320"  # Date to filter by

def is_within_time_range(t, start, end):
    """Check if time t is within the range specified by start and end."""
    return start <= t <= end

def parse_time(t):
    """Parses and adjusts times beyond 24:00:00."""
    hours, minutes, seconds = map(int, t.split(':'))
    adjusted_hour = hours % 24
    adjusted_time = f"{adjusted_hour:02}:{minutes:02}:{seconds:02}"
    return pd.to_datetime(adjusted_time, format='%H:%M:%S').time()

def vehicle_type(shape_id):
    if re.search(r'^\d{1,2}_', shape_id):
        return 'Tram'
    elif re.search(r'^\d{3}_', shape_id):
        return 'Bus'
    else:
        return 'Unknown'

def load_and_filter_data():
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"), dtype={'shape_id': str})
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"), dtype={'trip_id': str, 'shape_id': str})

    # Filter trips DataFrame directly without relying on DATE_FILTER in 'shape_id'
    trips['is_in_range'] = trips['trip_id'].apply(lambda x: '20240320' in x)
    trips_filtered = trips[trips['is_in_range']]

    # Correct vehicle_type assignment
    shapes['vehicle_type'] = shapes['shape_id'].apply(vehicle_type)

    # Count trips per shape_id and merge
    trip_counts = trips_filtered.groupby('shape_id').size().reset_index(name='trip_count')
    shapes_merged = pd.merge(shapes, trip_counts, on='shape_id', how='left').fillna(0)

    shapes_merged.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)
    print("Filtered shapes saved to CSV file.")

if __name__ == "__main__":
    load_and_filter_data()

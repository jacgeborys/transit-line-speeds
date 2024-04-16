import pandas as pd
from tqdm import tqdm
import os

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Gdynia_2024_03_20\\"

def parse_time(t):
    """Parses time strings, adjusting those beyond 24:00:00."""
    try:
        hours, minutes, seconds = map(int, t.split(':'))
        hours = hours % 24  # Adjust hours beyond 24 to the correct 24-hour format
        return pd.to_datetime(f"{hours:02d}:{minutes:02d}:{seconds:02d}", format='%H:%M:%S').time()
    except ValueError:
        return None

def load_and_filter_data():
    shapes_df = pd.read_csv(os.path.join(folder_path, "shapes.txt"), dtype={'shape_id': str})
    trips_df = pd.read_csv(os.path.join(folder_path, "trips.txt"), dtype={'trip_id': str, 'shape_id': str})
    stop_times_df = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'trip_id': str})

    tqdm.pandas(desc="Parsing times")
    stop_times_df['arrival_time'] = stop_times_df['arrival_time'].progress_apply(parse_time)
    stop_times_df['departure_time'] = stop_times_df['departure_time'].progress_apply(parse_time)
    stop_times_df.dropna(subset=['arrival_time', 'departure_time'], inplace=True)

    # Assuming all data pertains to buses, so directly assign 'Bus' to the 'vehicle_type' column
    shapes_df['vehicle_type'] = 'Bus'

    # Adjust trip_ids in trips_df to match the shape_ids in shapes_df
    trips_df['adjusted_shape_id'] = trips_df['shape_id'].apply(lambda x: x.split(',')[0])

    # Count trips per shape_id and merge
    trip_counts = trips_df.groupby('adjusted_shape_id').size().reset_index(name='trip_count')
    shapes_merged = pd.merge(shapes_df, trip_counts, left_on='shape_id', right_on='adjusted_shape_id', how='left').fillna(0)

    shapes_merged.to_csv(os.path.join(folder_path, "shapes_processed.csv"), index=False)
    print("Filtered shapes saved to CSV file.")

if __name__ == "__main__":
    load_and_filter_data()

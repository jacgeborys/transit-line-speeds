import pandas as pd
from tqdm import tqdm
import os

folder_path = "C:\\Users\\Asus\\OneDrive\\Pulpit\\Rozne\\QGIS\\TransitLineSpeeds\\_schedule_data\\Warszawa_2024_11_10\\"

def parse_time(t):
    t_split = t.split(':')
    if int(t_split[0]) >= 24:  # Filter out invalid times
        return None
    return pd.to_datetime(t).time()

def is_within_time_range(t, start, end):
    if start <= end:
        return start <= t <= end
    else:  # Wraps around midnight
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

def load_and_filter_data(target_date='20241113'):
    # Load dataframes
    shapes = pd.read_csv(os.path.join(folder_path, "shapes.txt"))
    trips = pd.read_csv(os.path.join(folder_path, "trips.txt"))
    stop_times = pd.read_csv(os.path.join(folder_path, "stop_times.txt"), dtype={'stop_id': str})
    calendar = pd.read_csv(os.path.join(folder_path, "calendar.txt"))

    # Find service_id for target date
    calendar_service = calendar[calendar['start_date'] == int(target_date)]['service_id'].iloc[0]  # e.g., '5_2'
    print(f"\nFound calendar service for {target_date}:", calendar_service)

    # Extract service pattern from trips (part after colon)
    trips['service_pattern'] = trips['service_id'].str.split(':').str[1]
    print("\nSample service patterns:", trips['service_pattern'].head())

    # Add vehicle type based on route_id
    trips['vehicle'] = trips['route_id'].apply(vehicle_type)

    # Debug prints
    print("\nBefore filtering:")
    print(f"Total trips: {len(trips)}")
    
    # Filter trips for the target service pattern
    trips = trips[trips['service_pattern'] == calendar_service]
    print(f"\nAfter service filtering:")
    print(f"Filtered to {len(trips)} trips for service {calendar_service}")

    # Process times
    tqdm.pandas(desc="Parsing times")
    stop_times['arrival_time'] = stop_times['arrival_time'].progress_apply(parse_time)
    stop_times['departure_time'] = stop_times['departure_time'].progress_apply(parse_time)
    stop_times.dropna(subset=['arrival_time', 'departure_time'], inplace=True)

    # Filter by time ranges (peak hours)
    range_starts = [pd.to_datetime('06:00:00').time(), pd.to_datetime('14:00:00').time()]
    range_ends = [pd.to_datetime('11:00:00').time(), pd.to_datetime('19:00:00').time()]

    in_range_masks = []
    for start, end in zip(range_starts, range_ends):
        in_range_mask = stop_times['arrival_time'].apply(is_within_time_range, args=(start, end))
        in_range_masks.append(in_range_mask)

    combined_in_range_mask = pd.concat(in_range_masks, axis=1).any(axis=1)
    stop_times = stop_times[combined_in_range_mask]

    # Get valid trips and filter
    valid_trips = stop_times['trip_id'].unique()
    trips = trips[trips['trip_id'].isin(valid_trips)]

    # Count trips per shape_id
    trip_counts = trips.groupby('shape_id').agg({
        'trip_id': 'count',
        'vehicle': 'first'
    }).reset_index()
    trip_counts.columns = ['shape_id', 'trip_count', 'vehicle']

    # Merge with shapes
    shapes_merged = pd.merge(shapes, trip_counts[['shape_id', 'trip_count', 'vehicle']], 
                           on='shape_id', how='left')
    shapes_merged['trip_count'] = shapes_merged['trip_count'].fillna(0)

    # Save results
    output_path = os.path.join(folder_path, "shapes_processed.csv")
    shapes_merged.to_csv(output_path, index=False)
    print(f"\nFinal Summary:")
    print(f"Saved processed shapes with {shapes_merged['shape_id'].nunique()} unique shape_ids")
    print(f"Total trips counted: {shapes_merged['trip_count'].sum()}")

def main():
    # For November 13th (Wednesday)
    load_and_filter_data('20241113')  # This will look for service_id '5_2'

if __name__ == "__main__":
    main()
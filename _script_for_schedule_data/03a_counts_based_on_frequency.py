import pandas as pd
from datetime import timedelta, datetime

# Read the frequencies file
df = pd.read_csv(r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitLineSpeeds\_schedule_data\2024_03_08\frequencies.txt")

# Function to adjust "24:xx:xx" times and convert to datetime
def adjust_time(time_str):
    if time_str.startswith("24:"):
        adjusted_str = "00" + time_str[2:]
        time_obj = datetime.strptime(adjusted_str, '%H:%M:%S') + timedelta(days=1)  # Adjust for "next day"
    elif time_str.startswith("25:"):
        adjusted_str = "01" + time_str[2:]
        time_obj = datetime.strptime(adjusted_str, '%H:%M:%S')
        time_obj += timedelta(days=1)  # Adding a day to account for the "next day"
    elif time_str.startswith("26:"):
        adjusted_str = "01" + time_str[2:]
        time_obj = datetime.strptime(adjusted_str, '%H:%M:%S')
        time_obj += timedelta(days=1)  # Adding a day to account for the "next day"
    else:
        time_obj = datetime.strptime(time_str, '%H:%M:%S')
    return time_obj

# Function to compute trips considering only hours between 6 AM and 10 PM
def calculate_trips(row):
    start_time = max(row['start_time'], row['start_time'].replace(hour=6, minute=0, second=0))
    end_time = min(row['end_time'], row['start_time'].replace(hour=22, minute=0, second=0))
    if start_time >= end_time:
        return 0  # No trips if start time is after the end of the considered interval
    duration = (end_time - start_time).total_seconds() / 3600  # Duration in hours
    return round(duration * 3600 / row['headway_secs'])

# Apply adjustment to start and end times
df['start_time'] = df['start_time'].apply(adjust_time)
df['end_time'] = df['end_time'].apply(adjust_time)

# Calculate trips considering only the specified hours
df['trips'] = df.apply(calculate_trips, axis=1)

# Aggregate total trips per day for M1 and M2
trips_per_day = df.groupby(df['trip_id'].str[:2])['trips'].sum().reset_index()

print(trips_per_day)
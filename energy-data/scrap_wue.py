import requests
import pandas as pd
import time
from datetime import datetime, timedelta, UTC

COORDINATES_BY_REGION = {
    "mixed": {"lat": 50.1109, "lon": 8.6821},  # Frankfurt - Germany
    "clean_energy":  {"lat": 48.8566, "lon": 2.3522},  # Paris - France
    "fossil_heavy":  {"lat": 52.2297, "lon": 21.0122}  # Warsaw - Poland
}

start_date = datetime(year=2026, month=1, day=11, tzinfo=UTC)
end_date = start_date + timedelta(days=7)

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

def estimate_wue(wet_bulb_temp):
    """
    Empirical model for WUE based on Wet-Bulb Temperature (WBT).
    If WBT < 10°C, assume WUE = 0.2 (Free Cooling).
    If WBT >= 10°C, water consumption grows quadratically.
    """
    if pd.isna(wet_bulb_temp):
        return None
    if wet_bulb_temp < 10.0:
        return 0.2
    else:
        return 0.2 + 0.005 * ((wet_bulb_temp - 10.0) ** 2)

def fetch_historical_data(coordinates_by_region: dict[str, dict[str, float]], start_date: datetime, end_date: datetime, export_to_file: bool = False):
    dataframe_by_region = {}
    for host_id, coordinates in coordinates_by_region.items():
        print(f"Fetching historical data for {host_id} (Lat: {coordinates['lat']}, Lon: {coordinates['lon']})...")
        
        params = {
            "latitude": coordinates['lat'],
            "longitude": coordinates['lon'],
            "hourly": "wet_bulb_temperature_2m",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": "wet_bulb_temperature_2m",
            "timezone": "UTC"
        }

        try:
            response = requests.get(OPEN_METEO_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Create the raw DataFrame with the actual date strings
            df = pd.DataFrame({
                "datetime": pd.to_datetime(data["hourly"]["time"]),
                "wet_bulb_temp_C": data["hourly"]["wet_bulb_temperature_2m"]
            })
            
            # Interpolate to 15 minutes
            df.set_index("datetime", inplace=True)
            df = df.resample("15min").interpolate(method="linear")
            df = df.reset_index()
            local_start_time = df["datetime"].min()
            df["timestamp"] = (df["datetime"] - local_start_time).dt.total_seconds().astype(int)

            # Format columns to match our project standard
            df["host_id"] = host_id
            df["property_name"] = "wue"
            df["new_value"] = df["wet_bulb_temp_C"].apply(estimate_wue).round(4)

            # Reorganize columns for better readability
            df = df[["timestamp", "host_id", "property_name", "new_value"]]
            dataframe_by_region[host_id] = df
            
            if export_to_file:
                filename = f"{host_id}_wue_trace.csv"
                df.to_csv(filename, index=False)
            
            # Wait 1 second to avoid overloading the free public API
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {host_id}. Details: {e}")
        
    return dataframe_by_region


def main():
    _ = fetch_historical_data(COORDINATES_BY_REGION, start_date, end_date, export_to_file=True)

if __name__ == "__main__":
    main()
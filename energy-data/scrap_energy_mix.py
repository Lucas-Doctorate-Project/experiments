import csv
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd


entsoe_info_url = "https://transparency.entsoe.eu/enum/list"
entsoe_source_data_url = "https://transparency.entsoe.eu/generation/actual/perType/generation/load"

headers = {"content-type": "application/json; charset=utf-8", "accept": "application/json"}

start_date = datetime(year=2026, month=1, day=11, tzinfo=timezone.utc)
end_date = start_date + timedelta(days=7)

REGIONS = ["CTY|10YFR-RTE------C", "CTY|10YPL-AREA-----S", "CTY|10Y1001A1001A83F"]
GRID_COMPOSITION_BY_REGION = {
    "CTY|10YFR-RTE------C": "clean_energy",
    "CTY|10YPL-AREA-----S": "fossil_heavy",
    "CTY|10Y1001A1001A83F": "mixed"
}

# Per-source intensity factors (UNECE 2020 defaults where available, else IPCC 2014,
# from energy-data/intensities.json). Keys must match source names returned by ENTSO-E.
CARBON_INTENSITY = {
    "Biomass": 230, "Fossil Gas": 280, "Fossil Hard coal": 630, "Fossil Oil": 280,
    "Hydro Pumped Storage": 81, "Hydro Run-of-river and pondage": 81, "Hydro Water Reservoir": 81,
    "Nuclear": 5.1, "Solar": 21, "Waste": 230,
    "Wind Offshore": 13, "Wind Onshore": 12, "Energy storage": 21,
}
WATER_INTENSITY = {
    "Biomass": 1.147, "Fossil Gas": 1.086, "Fossil Hard coal": 1.802, "Fossil Oil": 1.086,
    "Hydro Pumped Storage": 17.0, "Hydro Run-of-river and pondage": 17.0, "Hydro Water Reservoir": 17.0,
    "Nuclear": 1.957, "Solar": 0.004, "Waste": 1.147,
    "Wind Offshore": 0, "Wind Onshore": 0, "Energy storage": 0.004,
}

def get_sources_map():
    info_response = requests.post(entsoe_info_url, data='{"attributeList":[{"useCase":"generation/installed/perType","code":"PRODUCTION_TYPE","strict":false}]}', headers=headers)
    info_data = info_response.json().get("enumList", [])
    
    if len(info_data) == 0:
        print("No valid response from entsoe.")
    sources = info_data[0].get("attributeEnum", [])
    
    if len(sources) == 0:
        print("No sources from entsoe.")
    
    return {source["code"]: source["name"] for source in sources}


def format_point_value(value: str | dict | None):
    if value is None:
        return 0 
    
    if isinstance(value, dict) and 'alt' in value and value['alt'] == 'n/e':
        return 0
    
    return float(value)

def get_data_from_region(region: str, source_by_code: dict[str, str]):
    raw_data = f'{{"dateTimeRange":{{"from":"{start_date.isoformat(timespec="seconds")}","to":"{end_date.isoformat(timespec="seconds")}"}},"areaList":["{region}"],"timeZone":"CET","sorterList":[],"filterMap":{{}}}}'
    raw_data = raw_data.replace("+00:00", "Z")
    sources_data_response = requests.post(
        entsoe_source_data_url, 
        data=raw_data,
        headers=headers
    )
    sources_data = sources_data_response.json().get("instanceList", [])
    if len(sources_data) == 0:
        print("No valid response from entsoe.")

    values_by_source = {}
    for source_info in sources_data:
        source_name = source_by_code.get(source_info["businessDimensionMap"]["PRODUCTION_TYPE"])
        time_period_values_list = source_info["curveData"]["periodList"]
        if len(time_period_values_list) == 0:
            print("No source values structure from entsoe.")

        points = time_period_values_list[0]["pointMap"]
        if len(points) == 0:
            print("No data points for source from entsoe.")
        
        points = [points[point][0] for point in points]
        values = [format_point_value(point_value) for point_value in points]
        values_by_source[source_name] = values
    
    return values_by_source


def calculate_grid_intensities(energy_values_by_source: dict) -> pd.DataFrame:
    """
    Calculates the weighted average grid intensities (Carbon and Water) 
    at each time step based on the energy generation of each source.
    """
    energy_df = pd.DataFrame(energy_values_by_source)

    # 1. Cleanup: Remove sources that generated zero energy during the entire period
    energy_df = energy_df.loc[:, (energy_df.sum(axis=0) > 0)]
    
    # 2. Weights: Calculate the fraction (0.0 to 1.0) of each source in the grid at that instant
    total_energy_by_instant = energy_df.sum(axis=1)
    weights_df = energy_df.div(total_energy_by_instant, axis=0).fillna(0)
    
    # 3. Intensity Alignment: Fetch the exact intensities matching the active columns
    c_intensities = [CARBON_INTENSITY.get(col, 0) for col in weights_df.columns]
    w_intensities = [WATER_INTENSITY.get(col, 0) for col in weights_df.columns]

    # 4. Math: Instantaneous weighted average (Fraction * Intensity)
    # We create a new DataFrame just for the results to avoid shape mismatch issues
    results_df = pd.DataFrame()
    results_df['timestamp'] = (weights_df.index * 900).astype(int)
    results_df['carbon_intensity'] = weights_df.dot(c_intensities).round(4)
    results_df['water_intensity'] = weights_df.dot(w_intensities).round(4)
    
    return results_df


def format_trace_for_batsim(intensities_df: pd.DataFrame, host_id: str = "AS0") -> pd.DataFrame:
    """
    Transforms the wide intensity DataFrame (columns) into the long format 
    (events) required by the Batsim/SimGrid simulator.
    """
    # 1. Unpivot: Flatten the DataFrame (transforms intensity columns into event rows)
    final_df = intensities_df.melt(
        id_vars=['timestamp'], 
        value_vars=['carbon_intensity', 'water_intensity'],
        var_name='property_name', 
        value_name='new_value'
    )
    
    # 2. Add the Host ID
    final_df['host_id'] = host_id

    # 3. Final Formatting: Order columns and sort rows chronologically
    final_df = final_df[['timestamp', 'host_id', 'property_name', 'new_value']]
    final_df = final_df.sort_values(by=['timestamp', 'property_name']).reset_index(drop=True)
    
    return final_df

def export_energy_data(energy_values_by_region: dict[str, dict[str, list[float]]]):
    for region, energy_values_by_source in energy_values_by_region.items():
        # Step 1: Calculate the mathematical intensities
        intensities_df = calculate_grid_intensities(energy_values_by_source)
        
        # Step 2: Format to the simulator's standards
        batsim_trace_df = format_trace_for_batsim(intensities_df, host_id="AS0")

        batsim_trace_df.to_csv(f"{GRID_COMPOSITION_BY_REGION[region]}_trace.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')


def main():
    source_by_code = get_sources_map()
    energy_generation_by_region = {region: get_data_from_region(region, source_by_code) for region in REGIONS}
    export_energy_data(energy_generation_by_region)

if __name__ == "__main__":
    main()
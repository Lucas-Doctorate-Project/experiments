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
# from intensity-factors/intensities.json). Keys must match source names returned by ENTSO-E.
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


def format_df_row_into_energy_mix_str(row) -> str:
    items = [(source, val) for source, val in row.items() if source != 'timestamp']
    if not items:
        return ""
    # Round all but the last source to 2 decimal places, then set the last one
    # to 100 - sum(others) so the total is exactly 100.00. This avoids the
    # per-value rounding error that causes the plugin's validation to reject
    # mixes summing to 99.99% instead of 100%.
    rounded = [round(val, 2) for _, val in items[:-1]]
    rounded.append(round(100.0 - sum(rounded), 2))
    return ";".join(f"{source}:{val:.2f}" for (source, _), val in zip(items, rounded))

def export_energy_data(energy_values_by_region: dict[str, dict[str, list[float]]]):
    for region, energy_values_by_source in energy_values_by_region.items():
        energy_df = pd.DataFrame(energy_values_by_source)

        # Cleaning sources with all values equal to 0
        energy_df = energy_df.loc[:, (energy_df.sum(axis=0) > 0)]
        
        # Calculating total of generated energy at each time point in region, and after that the source percentage.
        total_energy_by_instant = energy_df.sum(axis=1)
        percentage_df = energy_df.div(total_energy_by_instant, axis=0).fillna(0) * 100

        # Adding timestamps (900s = 15min)
        percentage_df['timestamp'] = [i * 900 for i in range(len(percentage_df))]

        sources = [col for col in percentage_df.columns if col != 'timestamp']
        carbon_str = ";".join(f"{s}:{CARBON_INTENSITY.get(s, 0)}" for s in sources)
        water_str  = ";".join(f"{s}:{WATER_INTENSITY.get(s, 0)}" for s in sources)

        rows = []
        for _, row in percentage_df.iterrows():
            rows.append({
                "timestamp": int(row['timestamp']),
                "host_id": "AS0",
                "property_name": "energy_mix",
                "new_value": format_df_row_into_energy_mix_str(row)
            })

        # Insert static intensity rows at t=0, after the first energy_mix row.
        # Order matters: carbon/water intensities are only applied to sources that
        # already exist in the mix, so energy_mix must be processed first.
        rows.insert(1, {"timestamp": 0, "host_id": "AS0", "property_name": "water_intensity", "new_value": water_str})
        rows.insert(1, {"timestamp": 0, "host_id": "AS0", "property_name": "carbon_intensity", "new_value": carbon_str})

        final_df = pd.DataFrame(rows)
        final_df.to_csv(f"{GRID_COMPOSITION_BY_REGION[region]}_trace.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')


def main():
    source_by_code = get_sources_map()
    energy_generation_by_region = {region: get_data_from_region(region, source_by_code) for region in REGIONS}
    export_energy_data(energy_generation_by_region)

if __name__ == "__main__":
    main()
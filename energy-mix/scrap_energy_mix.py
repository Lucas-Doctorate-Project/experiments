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
    "CTY|10YFR-RTE------C": "low_carbon",
    "CTY|10YPL-AREA-----S": "high_carbon",
    "CTY|10Y1001A1001A83F": "volatile_carbon"
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
    sources = [f"{source}:{percentage:.2f}" for source, percentage in row.items() if source != 'timestamp']
    return ";".join(sources)

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

        rows = []
        for _, row in percentage_df.iterrows():
            rows.append({
                "timestamp": int(row['timestamp']),
                "host_id": f"{GRID_COMPOSITION_BY_REGION[region]}_host",
                "property_name": "energy_mix",
                "new_value": format_df_row_into_energy_mix_str(row)
            })

        final_df = pd.DataFrame(rows)
        final_df.to_csv(f"{GRID_COMPOSITION_BY_REGION[region]}_trace.csv", index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')


def main():
    source_by_code = get_sources_map()
    energy_generation_by_region = {region: get_data_from_region(region, source_by_code) for region in REGIONS}
    export_energy_data(energy_generation_by_region)

if __name__ == "__main__":
    main()
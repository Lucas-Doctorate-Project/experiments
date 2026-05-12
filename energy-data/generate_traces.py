import csv

import pandas as pd

import scrap_energy_mix
import scrap_wue

def merge_and_filter_events(wue_df: pd.DataFrame, energy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges the DataFrames and removes events where the property value 
    has not changed compared to the previous timestamp.
    """
    # 1. Standardize the host_id (WUE script uses "mixed" etc., Energy script uses "AS0")
    # For Batsim, we will standardize everything as "AS0" in the final trace.
    wue_df['host_id'] = "AS0"
    energy_df['host_id'] = "AS0"

    unified_df = pd.concat([wue_df, energy_df], ignore_index=True)
    unified_df = unified_df.sort_values(by=["timestamp", "property_name"]).reset_index(drop=True)
    
    # 4. The Event Filter: Group by property and shift the values down by 1 row
    # This aligns the PREVIOUS value of that specific property next to its CURRENT value.
    unified_df['prev_value'] = unified_df.groupby('property_name')['new_value'].shift(1)
    
    filtered_df = unified_df[unified_df['new_value'] != unified_df['prev_value']]
    filtered_df = filtered_df.drop(columns=['prev_value']).reset_index(drop=True)
    
    return filtered_df

def main():
    print("Starting Orchestrated Extraction...\n")

    print(">> Fetching Energy data (ENTSO-E)...")
    source_by_code = scrap_energy_mix.get_sources_map()
    energy_generation_by_region = {
        region: scrap_energy_mix.get_data_from_region(region, source_by_code) 
        for region in scrap_energy_mix.REGIONS
    }
    energy_dfs = scrap_energy_mix.get_energy_dataframes(energy_generation_by_region)

    print("\n>> Fetching WUE data (Open-Meteo)...")
    wue_dfs = scrap_wue.fetch_historical_data(
        scrap_wue.COORDINATES_BY_REGION, 
        scrap_wue.start_date, 
        scrap_wue.end_date
    )

    print("\n>> Applying filters and generating final trace files...")
    
    for region in wue_dfs.keys():
        wue_df = wue_dfs[region]
        energy_df = energy_dfs[region]
        
        final_trace_df = merge_and_filter_events(wue_df, energy_df)
        
        filename = f"{region}_trace.csv"
        
        final_trace_df.to_csv(filename, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        print(f" ✅ '{filename}' successfully generated! (Reduced to {len(final_trace_df)} essential events)")

if __name__ == "__main__":
    main()
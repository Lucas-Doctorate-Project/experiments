"""
Data exploration script for Batsim/Batsched experiment results.

Builds a single summary dataframe by joining experiments.csv with the
per-experiment batsim_output_schedule.csv files, then prints an overview.

Output: analysis/summary.csv
"""

import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
RESULTS_DIR = BASE_DIR / "results"
ANALYSIS_DIR = BASE_DIR / "analysis"

# Columns to keep from batsim_output_schedule.csv
SCHEDULE_COLS = [
    "makespan",
    "mean_slowdown",
    "max_slowdown",
    "mean_waiting_time",
    "max_waiting_time",
    "mean_turnaround_time",
    "consumed_joules",
    "total_carbon_footprint",
    "total_water_footprint",
    "nb_jobs",
]

# Columns to keep from experiments.csv
MANIFEST_COLS = [
    "id",
    "workload",
    "energy_grid",
    "algorithm",
    "queue_order",
    "output_dir",
]


def build_summary() -> pd.DataFrame:
    manifest = pd.read_csv(RESULTS_DIR / "experiments.csv", usecols=MANIFEST_COLS)

    rows = []
    for _, exp in manifest.iterrows():
        schedule_path = RESULTS_DIR / exp["output_dir"] / "batsim_output_schedule.csv"
        schedule = pd.read_csv(schedule_path, usecols=SCHEDULE_COLS)
        row = exp.to_dict()
        row.update(schedule.iloc[0].to_dict())
        rows.append(row)

    df = pd.DataFrame(rows)

    # Convert joules → kWh for readability
    df["consumed_kwh"] = df["consumed_joules"] / 3_600_000

    # Carbon in kgCO2e (Batsim outputs gCO2e)
    df["carbon_kg"] = df["total_carbon_footprint"] / 1_000

    return df


def print_overview(df: pd.DataFrame):
    print(f"Shape: {df.shape[0]} experiments × {df.shape[1]} columns\n")

    print("── Experiment dimensions ──────────────────────────────")
    for col in ["workload", "energy_grid", "algorithm", "queue_order"]:
        print(f"  {col}: {sorted(df[col].unique().tolist())}")

    metrics = {
        "makespan (s)":          "makespan",
        "mean_slowdown":         "mean_slowdown",
        "mean_waiting_time (s)": "mean_waiting_time",
        "consumed_kwh":          "consumed_kwh",
        "carbon_kg (kgCO2e)":   "carbon_kg",
        "water (L)":             "total_water_footprint",
    }

    print("\n── Metric ranges (across all 72 experiments) ──────────")
    for label, col in metrics.items():
        print(f"  {label:25s}  min={df[col].min():.4g}  max={df[col].max():.4g}  mean={df[col].mean():.4g}")

    print("\n── Mean metrics by algorithm ───────────────────────────")
    print(df.groupby("algorithm")[[
        "makespan", "mean_slowdown", "consumed_kwh", "carbon_kg", "total_water_footprint"
    ]].mean().to_string())

    print("\n── Mean metrics by energy_grid ─────────────────────────")
    print(df.groupby("energy_grid")[[
        "makespan", "mean_slowdown", "consumed_kwh", "carbon_kg", "total_water_footprint"
    ]].mean().to_string())

    print("\n── Mean metrics by workload ─────────────────────────────")
    print(df.groupby("workload")[[
        "makespan", "mean_slowdown", "consumed_kwh", "carbon_kg", "total_water_footprint"
    ]].mean().to_string())


if __name__ == "__main__":
    ANALYSIS_DIR.mkdir(exist_ok=True)

    print("Building summary dataframe...")
    df = build_summary()

    output_path = ANALYSIS_DIR / "summary.csv"
    df.to_csv(output_path, index=False)
    print(f"Saved: {output_path}\n")

    print_overview(df)

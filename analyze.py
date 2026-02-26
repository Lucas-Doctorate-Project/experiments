"""
Comparison analysis: greenfilling vs easy_bf.

Builds an in-memory summary from experiment outputs, computes time-normalized
environmental metrics (divided by makespan), then prints % change relative
to the easy_bf baseline for each workload x energy_grid x alpha combination.
"""

import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).parent.resolve()
RESULTS_DIR = BASE_DIR / "outputs" / "20260225_160755_alphas"

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
    "variant_options",
]

# Metrics and their display labels
RAW_METRICS = {
    "makespan":          "Makespan (s)",
    "mean_slowdown":     "Mean slowdown",
    "max_slowdown":      "Max slowdown",
    "mean_waiting_time": "Mean wait (s)",
    "max_waiting_time":  "Max wait (s)",
    "consumed_kwh":      "Energy (kWh)",
    "carbon_kg":         "Carbon (kgCO2e)",
    "total_water_footprint": "Water (L)",
}

NORMALIZED_METRICS = {
    # per-second rates, reveal true environmental efficiency
    "carbon_per_s":  ("carbon_kg",              "Carbon rate (kgCO2e/s)"),
    "kwh_per_s":     ("consumed_kwh",            "Energy rate (kWh/s)"),
    "water_per_s":   ("total_water_footprint",   "Water rate (L/s)"),
}

# Semantic color mapping for known energy-grid scenarios.
# Unknown grid names fall back to a stable tab10 color.
GRID_COLOR_HINTS = [
    (("hydro", "renew", "clean", "low"), "#2ca02c"),   # green
    (("wind",), "#17becf"),                             # cyan
    (("solar",), "#ffbf00"),                            # amber
    (("mixed", "medium"), "#1f77b4"),                   # blue
    (("fossil", "coal", "gas", "high", "dirty"), "#d62728"),  # red
]


def pct_change(new, base):
    return 100.0 * (new - base) / base


def fmt(val):
    """Human-readable number: comma-separated for large, fixed decimals for small."""
    a = abs(val)
    if a >= 1_000:
        return f"{val:,.0f}"
    elif a >= 10:
        return f"{val:.1f}"
    elif a >= 0.001:
        return f"{val:.6f}"
    else:
        return f"{val:.2e}"


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

    # Convert joules -> kWh for readability
    df["consumed_kwh"] = df["consumed_joules"] / 3_600_000

    # Carbon in kgCO2e (Batsim outputs gCO2e)
    df["carbon_kg"] = df["total_carbon_footprint"] / 1_000

    return df


def build_percent_change_df(df: pd.DataFrame) -> pd.DataFrame:
    metric_cols = {
        "makespan": "Makespan",
        "mean_slowdown": "Mean slowdown",
        "total_water_footprint": "Water",
        "carbon_kg": "Carbon",
    }

    rows = []
    for (workload, energy_grid), grp in df.groupby(["workload", "energy_grid"]):
        baseline_rows = grp[grp["algorithm"] == "easy_bf"]
        gf_rows = grp[grp["algorithm"] == "greenfilling"].sort_values("alpha")
        if baseline_rows.empty or gf_rows.empty:
            continue

        baseline = baseline_rows.iloc[0]
        for _, gf in gf_rows.iterrows():
            row = {
                "workload": workload,
                "energy_grid": energy_grid,
                "alpha": gf["alpha"],
            }
            for col, label in metric_cols.items():
                row[label] = pct_change(gf[col], baseline[col])
            rows.append(row)

    return pd.DataFrame(rows)


def plot_percent_changes(pct_df: pd.DataFrame) -> None:
    if pct_df.empty:
        print("No complete data for plotting.")
        return

    metrics = ["Mean slowdown", "Makespan", "Water", "Carbon"]
    alpha_ticks = sorted(pct_df["alpha"].dropna().unique().tolist())
    alpha_pos = list(range(len(alpha_ticks)))
    workloads = sorted(pct_df["workload"].dropna().unique().tolist())
    n_rows = len(workloads)
    n_cols = len(metrics)

    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=(4.2 * n_cols, 3.2 * n_rows),
        sharex=True,
    )
    if n_rows == 1:
        axes = [axes]

    fallback_colors = list(plt.get_cmap("tab10").colors)

    def color_for_grid(grid_name: str, idx: int) -> str:
        lowered = grid_name.lower()
        for keywords, color in GRID_COLOR_HINTS:
            if any(k in lowered for k in keywords):
                return color
        return fallback_colors[idx % len(fallback_colors)]

    for row_idx, workload in enumerate(workloads):
        workload_df = pct_df[pct_df["workload"] == workload]
        grid_keys = (
            workload_df["energy_grid"]
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        for energy_grid in grid_keys:
            subset = workload_df[workload_df["energy_grid"] == energy_grid].sort_values("alpha")
            for col_idx, metric in enumerate(metrics):
                ax = axes[row_idx][col_idx]
                bar_width = 0.8 / max(len(grid_keys), 1)
                grid_idx = grid_keys.index(energy_grid)
                offset = (grid_idx - (len(grid_keys) - 1) / 2) * bar_width

                metric_by_alpha = (
                    subset.set_index("alpha")[metric]
                    .reindex(alpha_ticks)
                    .to_list()
                )
                ax.bar(
                    [x + offset for x in alpha_pos],
                    metric_by_alpha,
                    width=bar_width,
                    label=energy_grid,
                    color=color_for_grid(energy_grid, grid_idx),
                    alpha=0.9,
                )

        for col_idx, metric in enumerate(metrics):
            ax = axes[row_idx][col_idx]
            ax.axhline(0.0, color="gray", linestyle="--", linewidth=1.0)
            if row_idx == 0:
                ax.set_title(f"{metric} (% vs easy_bf)")
            if col_idx == 0:
                ax.set_ylabel(f"{workload}\n% change")
            else:
                ax.set_ylabel("% change")
            ax.set_xlabel("alpha")
            ax.set_xticks(alpha_pos)
            ax.set_xticklabels([f"{a:g}" for a in alpha_ticks])
            ax.grid(True, alpha=0.25)

    handles, labels = axes[0][0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=3, frameon=False)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    plt.show()


def print_best_alpha_by_ratio(df: pd.DataFrame) -> None:
    print(f"\n{'═'*70}")
    print("  Best alpha by trade-off ratio: |makespan_diff| / (|water_diff| + |carbon_diff|)")
    print(f"{'═'*70}")
    print(f"  {'Scenario':<40}  {'Best α':>10}  {'Ratio':>12}")
    print(f"  {'-'*40}  {'-'*10}  {'-'*12}")

    found_any = False

    for (workload, energy_grid), grp in df.groupby(["workload", "energy_grid"]):
        baseline_rows = grp[grp["algorithm"] == "easy_bf"]
        gf_rows = grp[grp["algorithm"] == "greenfilling"].sort_values("alpha")
        if baseline_rows.empty or gf_rows.empty:
            continue

        baseline = baseline_rows.iloc[0]
        ratios = []
        for _, gf in gf_rows.iterrows():
            makespan_diff = pct_change(gf["makespan"], baseline["makespan"])
            water_diff = pct_change(gf["total_water_footprint"], baseline["total_water_footprint"])
            carbon_diff = pct_change(gf["carbon_kg"], baseline["carbon_kg"])
            denominator = abs(water_diff) + abs(carbon_diff)
            if denominator == 0:
                continue
            ratio = abs(makespan_diff) / denominator
            ratios.append((gf["alpha"], ratio))

        scenario = f"{workload} | {energy_grid}"
        if not ratios:
            print(f"  {scenario:<40}  {'n/a':>10}  {'n/a':>12}")
            continue

        found_any = True
        best_alpha, best_ratio = min(ratios, key=lambda x: x[1])
        print(f"  {scenario:<40}  {best_alpha:>10.3f}  {best_ratio:>12.4f}")

    if not found_any:
        print("  No complete scenarios with a valid ratio.")


def main():
    df = build_summary()

    # Derive time-normalized metrics
    for col, (src, _) in NORMALIZED_METRICS.items():
        df[col] = df[src] / df["makespan"]

    # Parse alpha from variant_options (NaN for easy_bf rows)
    df["alpha"] = (
        df["variant_options"]
        .str.extract(r'"alpha":\s*([\d.]+)')
        .astype(float)
    )

    # ── Pretty print ──────────────────────────────────────────────────────────
    print_cols = (
        list(RAW_METRICS.items()) +
        [(col, label) for col, (_, label) in NORMALIZED_METRICS.items()]
    )

    for workload, workload_grp in df.groupby("workload"):
        grid_entries = []
        for energy_grid, grid_grp in workload_grp.groupby("energy_grid"):
            baseline_rows = grid_grp[grid_grp["algorithm"] == "easy_bf"]
            gf_rows = grid_grp[grid_grp["algorithm"] == "greenfilling"].sort_values("alpha")

            if baseline_rows.empty or gf_rows.empty:
                continue

            grid_entries.append((energy_grid, baseline_rows.iloc[0], gf_rows))

        if not grid_entries:
            print(f"\n{'═'*70}")
            print(f"  Workload: {workload}")
            print(f"{'═'*70}")
            print("  Skipping: no complete (easy_bf + greenfilling) grid data.")
            continue

        col_width = 22
        print(f"\n{'═'*70}")
        print(f"  Workload: {workload}")
        print(f"{'═'*70}")

        # Header: metric + per-grid columns
        print(f"  {'Metric':<30}", end="")
        for energy_grid, _, gf_rows in grid_entries:
            print(f"  {f'{energy_grid} easy_bf':>{col_width}}", end="")
            for _, gf in gf_rows.iterrows():
                label = f"{energy_grid} α={gf['alpha']} (Δ%)"
                print(f"  {label:>{col_width}}", end="")
        print()

        print(f"  {'-'*30}", end="")
        for _, _, gf_rows in grid_entries:
            print(f"  {'─'*col_width}", end="")
            for _ in gf_rows.iterrows():
                print(f"  {'─'*col_width}", end="")
        print()

        for col, label in print_cols:
            print(f"  {label:<30}", end="")
            for _, baseline, gf_rows in grid_entries:
                base_val = baseline[col]
                print(f"  {fmt(base_val):>{col_width}}", end="")
                for _, gf in gf_rows.iterrows():
                    delta = pct_change(gf[col], base_val)
                    cell = f"{fmt(gf[col])} ({delta:+.1f}%)"
                    print(f"  {cell:>{col_width}}", end="")
            print()

    print()
    print_best_alpha_by_ratio(df)
    print()
    pct_df = build_percent_change_df(df)
    plot_percent_changes(pct_df)


if __name__ == "__main__":
    main()

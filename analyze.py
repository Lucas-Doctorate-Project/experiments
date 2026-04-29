"""
Comparison analysis: greenfilling vs easy_bf.

Builds an in-memory summary from experiment outputs, computes time-normalized
environmental metrics (divided by makespan), then prints % change relative
to the easy_bf baseline for each workload x energy_grid x alpha combination.
"""

import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).parent.resolve()
RESULTS_DIR = BASE_DIR / "outputs" / "normalized_typical_intensities"

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
    (("mixed", "medium"), "#1f77b4"),                       # blue
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


def fmt_alpha(val):
    """Format alpha for display, gracefully handling missing values."""
    return "n/a" if pd.isna(val) else f"{val:g}"


def extract_typical_intensities_file(variant_options):
    """Extract typical intensities filename from variant options JSON."""
    if pd.isna(variant_options) or not variant_options:
        return None
    try:
        options = json.loads(variant_options)
    except json.JSONDecodeError:
        return None
    path = options.get("typical_intensities_file")
    if not path:
        return None
    try:
        return Path(path).name
    except TypeError:
        return str(path)


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

    df["typical_intensities_file"] = df["variant_options"].apply(
        extract_typical_intensities_file
    )

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

    if "alpha" not in pct_df or pct_df["alpha"].dropna().empty:
        print("No alpha values found; skipping plot.")
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


def build_pairwise_diff_df(df: pd.DataFrame) -> pd.DataFrame:
    baseline_df = df[df["algorithm"] == "easy_bf"]
    gf_df = df[df["algorithm"] == "greenfilling"].copy()

    if baseline_df.empty or gf_df.empty:
        return pd.DataFrame()

    gf_df["typical_intensities_file"] = gf_df["typical_intensities_file"].fillna("n/a")
    baseline_map = baseline_df.groupby(
        ["workload", "energy_grid", "queue_order"]
    ).first()

    rows = []
    for _, gf in gf_df.iterrows():
        key = (gf["workload"], gf["energy_grid"], gf["queue_order"])
        if key not in baseline_map.index:
            continue

        baseline = baseline_map.loc[key]
        rows.append(
            {
                "easy_bf_id": int(baseline["id"]),
                "greenfilling_id": int(gf["id"]),
                "workload": gf["workload"],
                "energy_grid": gf["energy_grid"],
                "queue_order": gf["queue_order"],
                "typical_intensities_file": gf["typical_intensities_file"],
                "alpha": gf.get("alpha"),
                "makespan_diff_pct": pct_change(gf["makespan"], baseline["makespan"]),
                "carbon_diff_pct": pct_change(gf["carbon_kg"], baseline["carbon_kg"]),
                "water_diff_pct": pct_change(
                    gf["total_water_footprint"], baseline["total_water_footprint"]
                ),
            }
        )

    return pd.DataFrame(rows)


def plot_tradeoff_scatter(
    diff_df: pd.DataFrame,
    makespan_tolerance: float = 5.0,
) -> None:
    if diff_df.empty:
        print("No pairwise comparisons available for plotting.")
        return

    diff_df = diff_df.copy().reset_index(drop=True)

    energy_grids = sorted(diff_df["energy_grid"].dropna().unique().tolist())
    palette = list(plt.get_cmap("tab10").colors)

    fig, axes = plt.subplots(1, 3, figsize=(15.6, 4.8))
    axis_defs = [
        (axes[0], "makespan_diff_pct", "carbon_diff_pct"),
        (axes[1], "makespan_diff_pct", "water_diff_pct"),
        (axes[2], "carbon_diff_pct", "water_diff_pct"),
    ]
    scatter_map = {ax: [] for ax in axes}

    for idx, grid in enumerate(energy_grids):
        subset = diff_df[diff_df["energy_grid"] == grid]
        if subset.empty:
            continue
        color = palette[idx % len(palette)]
        indices = subset.index.to_numpy()
        for ax, x_col, y_col in axis_defs:
            sc = ax.scatter(
                subset[x_col],
                subset[y_col],
                label=grid,
                color=color,
                alpha=0.75,
                s=36,
                picker=True,
                pickradius=5,
            )
            scatter_map[ax].append((sc, indices, x_col, y_col))

    for ax, x_col, y_col in axis_defs:
        ax.axhline(0.0, color="gray", linestyle="--", linewidth=1.0)
        ax.axvline(0.0, color="gray", linestyle="--", linewidth=1.0)
        if "makespan" in x_col:
            ax.axvline(
                makespan_tolerance,
                color="gray",
                linestyle=":",
                linewidth=1.0,
            )
            ax.axvline(
                -makespan_tolerance,
                color="gray",
                linestyle=":",
                linewidth=1.0,
            )

    highlight_points = []
    for ax, x_col, y_col in axis_defs:
        highlight = ax.scatter(
            [],
            [],
            s=160,
            facecolors="none",
            edgecolors="black",
            linewidth=1.2,
            zorder=4,
        )
        highlight_points.append((highlight, x_col, y_col))

    info_text = fig.text(
        0.01,
        0.98,
        "Hover over a point to see details.",
        ha="left",
        va="top",
    )

    def update_highlight(row_idx: int) -> None:
        row = diff_df.loc[row_idx]
        info_text.set_text(
            "easy_bf={easy_bf_id} | gf={greenfilling_id} | workload={workload} "
            "| grid={energy_grid} | queue={queue_order} | typical={typical_intensities_file}".format(
                **row
            )
        )
        for highlight, x_col, y_col in highlight_points:
            highlight.set_offsets([[row[x_col], row[y_col]]])
        fig.canvas.draw_idle()

    def clear_highlight() -> None:
        info_text.set_text("Hover over a point to see details.")
        for highlight, _, _ in highlight_points:
            highlight.set_offsets([])
        fig.canvas.draw_idle()

    def on_move(event) -> None:
        if event.inaxes is None:
            clear_highlight()
            return

        ax = event.inaxes
        for sc, indices, _, _ in scatter_map.get(ax, []):
            contains, info = sc.contains(event)
            if contains and info.get("ind"):
                global_index = indices[info["ind"][0]]
                update_highlight(int(global_index))
                return
        clear_highlight()

    axes[0].set_title("Carbon vs makespan (diff %)")
    axes[0].set_xlabel("Makespan diff (%)")
    axes[0].set_ylabel("Carbon diff (%)")

    axes[1].set_title("Water vs makespan (diff %)")
    axes[1].set_xlabel("Makespan diff (%)")
    axes[1].set_ylabel("Water diff (%)")

    axes[2].set_title("Water vs carbon (diff %)")
    axes[2].set_xlabel("Carbon diff (%)")
    axes[2].set_ylabel("Water diff (%)")

    fig.canvas.mpl_connect("motion_notify_event", on_move)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=4, frameon=False)

    fig.tight_layout(rect=(0, 0, 1, 0.92))
    plt.show()


def print_comparisons_by_typical_intensity(df: pd.DataFrame) -> None:
    print_cols = (
        list(RAW_METRICS.items()) +
        [(col, label) for col, (_, label) in NORMALIZED_METRICS.items()]
    )

    baseline_df = df[df["algorithm"] == "easy_bf"]
    gf_df = df[df["algorithm"] == "greenfilling"].copy()

    if baseline_df.empty:
        print("No easy_bf rows found for comparison.")
        return
    if gf_df.empty:
        print("No greenfilling rows found for comparison.")
        return

    gf_df["typical_intensities_file"] = gf_df["typical_intensities_file"].fillna("n/a")
    baseline_groups = baseline_df.groupby(["workload", "energy_grid", "queue_order"])
    scenario_groups = gf_df.groupby(
        ["workload", "energy_grid", "queue_order", "typical_intensities_file"]
    )

    found_any = False
    for (workload, energy_grid, queue_order, tif), grp in scenario_groups:
        key = (workload, energy_grid, queue_order)
        if key not in baseline_groups.indices:
            continue

        baseline = baseline_groups.get_group(key).iloc[0]
        for _, gf in grp.iterrows():
            found_any = True
            header_parts = [
                f"Workload: {workload}",
                f"Grid: {energy_grid}",
                f"Queue: {queue_order}",
                f"Typical: {tif}",
            ]
            if not pd.isna(gf.get("alpha")):
                header_parts.append(f"alpha: {fmt_alpha(gf['alpha'])}")

            print("\n" + "=" * 88)
            print(" | ".join(header_parts))
            print("=" * 88)
            print(
                f"IDs: easy_bf={int(baseline['id'])} | greenfilling={int(gf['id'])}"
            )

            for col, label in print_cols:
                base_val = baseline[col]
                gf_val = gf[col]
                delta = pct_change(gf_val, base_val)
                print(
                    f"  {label:<28} "
                    f"easy_bf={fmt(base_val):>12}  "
                    f"greenfilling={fmt(gf_val):>12}  "
                    f"delta%={delta:+.1f}%"
                )

    if not found_any:
        print("No matching easy_bf baselines found for greenfilling runs.")


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

    print_comparisons_by_typical_intensity(df)

    diff_df = build_pairwise_diff_df(df)
    plot_tradeoff_scatter(diff_df)


if __name__ == "__main__":
    main()

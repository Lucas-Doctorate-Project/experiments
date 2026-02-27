#!/usr/bin/env python3
"""Plot power over time for one or more experiments from a chosen experiment set."""

import json
import math
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import questionary
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

OUTPUTS_DIR = Path(__file__).parent / "outputs"
ENERGY_FILE = "batsim_output_consumed_energy.csv"
CARBON_FILE = "batsim_output_carbon_footprint.csv"
EXPERIMENTS_CSV = "experiments.csv"

DEFAULT_EMA_ALPHA = 0.5

console = Console()


# ── data helpers ──────────────────────────────────────────────────────────────

def list_experiment_sets() -> list[Path]:
    return sorted(
        p for p in OUTPUTS_DIR.iterdir()
        if p.is_dir() and (p / EXPERIMENTS_CSV).exists()
    )


def list_experiments(exp_set: Path) -> pd.DataFrame:
    meta = pd.read_csv(exp_set / EXPERIMENTS_CSV)
    return meta[meta["status"] == "success"].reset_index(drop=True)


def make_label(row: pd.Series) -> str:
    label = (
        f"{row['output_dir']}  "
        f"{row['workload']}, {row['energy_grid']}, "
        f"{row['algorithm']}, {row['queue_order']}"
    )
    opts = row.get("variant_options", "")
    if pd.notna(opts) and opts:
        try:
            d = json.loads(opts)
            parts = ", ".join(f"{k}={v}" for k, v in d.items())
            label += f", {parts}"
        except (json.JSONDecodeError, TypeError):
            pass
    return label


def make_plot_label(row: pd.Series) -> str:
    """Short label for the plot legend (keeps the legend compact)."""
    out_dir = str(row.get("output_dir", "")).strip()
    exp_id = out_dir.replace("experiment_", "exp")

    algo = str(row.get("algorithm", "")).strip()
    algo_short = {
        "easy_bf": "easy",
        "greenfilling": "gf",
    }.get(algo, algo or "algo")

    parts = [exp_id, algo_short]

    opts = row.get("variant_options", "")
    if pd.notna(opts) and opts:
        try:
            d = json.loads(opts)
        except (json.JSONDecodeError, TypeError):
            d = {}
        if "alpha" in d:
            try:
                parts.append(f"a={float(d['alpha']):g}")
            except (TypeError, ValueError):
                parts.append(f"a={d['alpha']}")

    return " ".join(parts)


def get_alpha(row1: pd.Series, row2: pd.Series) -> float:
    """Return the greenfilling alpha from either experiment, or the default."""
    for row in [row1, row2]:
        opts = row.get("variant_options", "")
        if pd.notna(opts) and opts:
            try:
                d = json.loads(opts)
                if "alpha" in d:
                    return float(d["alpha"])
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    return DEFAULT_EMA_ALPHA


def get_alpha_from_rows(rows: list[pd.Series]) -> float:
    """Return the first found greenfilling alpha across selected experiments."""
    for row in rows:
        opts = row.get("variant_options", "")
        if pd.notna(opts) and opts:
            try:
                d = json.loads(opts)
                if "alpha" in d:
                    return float(d["alpha"])
            except (json.JSONDecodeError, TypeError, ValueError):
                pass
    return DEFAULT_EMA_ALPHA


def load_power(exp_dir: Path) -> pd.DataFrame:
    """Load epower vs time, resolving degenerate simultaneous events.

    Batsim occasionally writes several energy rows with the *same* timestamp,
    for example when intensity queries or DVFS events happen at the exact same
    simulation time as job events. Some of these trailing rows may carry
    inconsistent or placeholder ``epower`` values (including zeros or NaNs)
    that do **not** correspond to a sustained cluster state.

    To obtain a stable power trace, keep the *first* row for each timestamp,
    which corresponds to the canonical energy update emitted before any extra
    bookkeeping events at the same time.
    """
    df = pd.read_csv(exp_dir / ENERGY_FILE, usecols=["time", "epower"])
    df = df.drop_duplicates(subset=["time"], keep="first").reset_index(drop=True)
    return df.sort_values("time").reset_index(drop=True)


def load_intensity(exp_dir: Path) -> pd.DataFrame:
    """Load carbon and water intensity vs time from the carbon footprint CSV.

    The CSV mixes three event types per timestamp:
      - 'mix': regular 900 s grid updates (carries both intensities)
      - 'ci'/'wi': initialisation rows at t=0 with partial values

    We take the 'wi' row at t=0 (first real values) and all 'mix' rows
    for t > 0, giving a clean uniform 900 s time series.
    """
    df = pd.read_csv(exp_dir / CARBON_FILE,
                     usecols=["time", "event_type",
                               "carbon_intensity(gCO2e/kWh)",
                               "water_intensity(L/kWh)"])
    df = df.rename(columns={
        "carbon_intensity(gCO2e/kWh)": "ci",
        "water_intensity(L/kWh)": "wi",
    })
    init = df[(df["time"] == 0) & (df["event_type"] == "wi")]
    regular = df[(df["event_type"] == "mix") & (df["time"] > 0)]
    out = pd.concat([init, regular], ignore_index=True)
    return out[["time", "ci", "wi"]].sort_values("time").reset_index(drop=True)


# ── plot helpers ──────────────────────────────────────────────────────────────

def _nice_step(span: float, target: int = 7) -> float:
    """Round span/target up to the nearest 1/2/5 × 10^n."""
    rough = span / target
    if rough <= 0:
        return 1.0
    mag = 10 ** math.floor(math.log10(rough))
    for s in (1, 2, 5, 10):
        if rough <= s * mag:
            return s * mag
    return 10 * mag


def extend_to_window(
    df: pd.DataFrame, col: str, t_start: float, t_end: float
) -> pd.DataFrame:
    """Extend a step trace to cover [t_start, t_end] with no edge gaps.

    Prepends the last known value at/before t_start and appends a closing
    point at t_end, so ax.step() fills the entire requested window.
    """
    if df.empty:
        return df

    before_start = df[df["time"] <= t_start]
    left_val = (
        before_start[col].iloc[-1] if not before_start.empty else df[col].iloc[0]
    )

    inside = df[(df["time"] > t_start) & (df["time"] < t_end)][["time", col]]

    before_end = df[df["time"] <= t_end]
    right_val = before_end[col].iloc[-1] if not before_end.empty else left_val

    result = pd.concat(
        [
            pd.DataFrame({"time": [t_start], col: [left_val]}),
            inside,
            pd.DataFrame({"time": [t_end], col: [right_val]}),
        ],
        ignore_index=True,
    )
    return result.sort_values("time").reset_index(drop=True)


def time_unit_for(max_time_s: float) -> tuple[float, str]:
    if max_time_s > 3600:
        return 3600.0, "h"
    elif max_time_s > 60:
        return 60.0, "min"
    return 1.0, "s"


def ask_time_window(min_t: float, max_t: float) -> tuple[float, float]:
    """Ask for a time window; limits come from the power data range."""
    divisor, unit = time_unit_for(max_t)
    lo, hi = min_t / divisor, max_t / divisor

    def validate_float(v: str, *, lo=lo, hi=hi) -> bool | str:
        try:
            f = float(v)
        except ValueError:
            return "Enter a number."
        if not (lo <= f <= hi):
            return f"Must be between {lo:.2f} and {hi:.2f}."
        return True

    console.print(
        f"[dim]Available range:[/dim] [bold]{lo:.2f} – {hi:.2f} {unit}[/bold]"
    )

    t_start = questionary.text(
        f"Start time ({unit}):",
        default=f"{lo:.2f}",
        validate=validate_float,
    ).ask()
    if t_start is None:
        sys.exit(0)

    t_end_lo = float(t_start)

    def validate_end(v: str) -> bool | str:
        try:
            f = float(v)
        except ValueError:
            return "Enter a number."
        if not (t_end_lo < f <= hi):
            return f"Must be greater than {t_end_lo:.2f} and at most {hi:.2f}."
        return True

    t_end = questionary.text(
        f"End time ({unit}):",
        default=f"{hi:.2f}",
        validate=validate_end,
    ).ask()
    if t_end is None:
        sys.exit(0)

    start_s = float(t_start) * divisor
    end_s = float(t_end) * divisor

    console.print(Panel(
        f"[bold]{float(t_start):.2f} – {float(t_end):.2f} {unit}[/bold]",
        title="Time window", expand=False,
    ))

    return start_s, end_s


# ── plot ──────────────────────────────────────────────────────────────────────

def plot_power(
    series: list[tuple[pd.DataFrame, str]],
    intensity: pd.DataFrame,
    ema_alpha: float,
    grid_label: str,
    title: str,
    t_start: float,
    t_end: float,
):
    fig, ax_power = plt.subplots(figsize=(13, 5))
    ax_int = ax_power.twinx()

    divisor, unit = time_unit_for(t_end)

    # Tick locator: multiples of the display unit, ~7 ticks across the window.
    step_s = _nice_step((t_end - t_start) / divisor) * divisor
    ax_power.xaxis.set_major_locator(ticker.MultipleLocator(step_s))
    ax_power.xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x / divisor:g}")
    )

    # ── high-intensity background ─────────────────────────────────────────────
    # EMA uses adjust=False — standard recursive formula EMA_t = α·x_t + (1−α)·EMA_{t−1}.
    # Computed on the full dataset so values inside the window carry prior history,
    # matching what the greenfilling algorithm sees at runtime.
    intensity = intensity.copy()
    ci_ema = intensity["ci"].ewm(alpha=ema_alpha, adjust=False).mean()
    wi_ema = intensity["wi"].ewm(alpha=ema_alpha, adjust=False).mean()
    intensity["allowed"] = (
        (intensity["ci"] <= ci_ema) & (intensity["wi"] <= wi_ema)
    ).astype(int)

    isub = extend_to_window(intensity[["time", "allowed"]], "allowed", t_start, t_end)

    ax_int.fill_between(
        isub["time"], isub["allowed"],
        step="post", color="#2ca02c", alpha=0.12, zorder=1,
        label=f"CI & WI ≤ EMA — backfilling allowed (α={ema_alpha}, {grid_label})",
    )
    ax_int.set_ylim(0, 1)
    ax_int.yaxis.set_visible(False)
    ax_int.set_xlim(t_start, t_end)

    # ── power foreground ──────────────────────────────────────────────────────
    colors = plt.rcParams["axes.prop_cycle"].by_key().get(
        "color", ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    )

    for i, (df, label) in enumerate(series):
        color = colors[i % len(colors)]
        windowed = extend_to_window(df, "epower", t_start, t_end)
        ax_power.step(
            windowed["time"], windowed["epower"] / 1e3,
            where="post", label=label, color=color,
            linewidth=1.4, alpha=0.9, zorder=3,
        )

    ax_power.set_xlabel(f"Simulation time ({unit})")
    ax_power.set_ylabel("Power (kW)")
    ax_power.set_title(title)
    ax_power.set_xlim(t_start, t_end)
    ax_power.set_ylim(bottom=0)
    ax_power.grid(axis="y", linestyle="--", alpha=0.35, zorder=0)
    ax_power.set_zorder(ax_int.get_zorder() + 1)
    ax_power.patch.set_visible(False)

    # Combined legend — place it below the plot to avoid covering data
    h1, l1 = ax_power.get_legend_handles_labels()
    h2, l2 = ax_int.get_legend_handles_labels()
    ncol = min(4, max(1, len(h1 + h2)))
    ax_power.legend(
        h1 + h2,
        l1 + l2,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.15),
        borderaxespad=0.0,
        fontsize=8,
        ncol=ncol,
    )

    fig.tight_layout(rect=[0, 0.08, 1, 1])
    plt.show()


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    # Step 1: pick experiment set
    exp_sets = list_experiment_sets()
    if not exp_sets:
        console.print(f"[red]No experiment sets found in {OUTPUTS_DIR}[/red]")
        sys.exit(1)

    chosen_name = questionary.select(
        "Select experiment set:",
        choices=[p.name for p in exp_sets],
        use_indicator=True,
    ).ask()
    if chosen_name is None:
        sys.exit(0)

    chosen_set = next(p for p in exp_sets if p.name == chosen_name)
    console.print(Panel(
        Text(str(chosen_set), style="bold cyan"),
        title="Experiment set", expand=False,
    ))

    # Step 2: pick experiments (multi-select)
    experiments = list_experiments(chosen_set)
    if len(experiments) < 2:
        console.print("[red]Need at least 2 successful experiments to compare.[/red]")
        sys.exit(1)

    labels = [make_label(row) for _, row in experiments.iterrows()]

    choices = questionary.checkbox(
        "Select experiments to plot (space to toggle, enter to confirm):",
        choices=labels,
        validate=lambda selected: True if len(selected) >= 2 else "Select at least 2 experiments.",
    ).ask()
    if choices is None:
        sys.exit(0)

    selected_idxs = [labels.index(c) for c in choices]
    selected_rows = [experiments.iloc[i] for i in selected_idxs]
    ref_row = selected_rows[0]

    console.print(Panel(
        Text("\n".join(choices), style="bold"),
        title=f"Selected experiments ({len(choices)})",
        expand=False,
    ))

    # Step 4: load data
    ema_alpha = get_alpha_from_rows(selected_rows)

    with console.status("Loading power and intensity data…"):
        series: list[tuple[pd.DataFrame, str]] = []
        for row, _label in zip(selected_rows, choices):
            df = load_power(chosen_set / row["output_dir"])
            series.append((df, make_plot_label(row)))
        intensity = load_intensity(chosen_set / ref_row["output_dir"])

    ref_grid = ref_row.get("energy_grid", "")
    grid_label = ref_grid if pd.notna(ref_grid) and ref_grid else "grid"

    grids = []
    for row in selected_rows:
        g = row.get("energy_grid", "")
        if pd.notna(g) and g:
            grids.append(str(g))
    unique_grids = sorted(set(grids))
    if len(unique_grids) > 1:
        console.print(
            f"[yellow]Warning:[/yellow] selected experiments use different grids "
            f"({', '.join(unique_grids)}). Showing intensity for {grid_label}."
        )

    for (df, label), row in zip(series, selected_rows):
        console.print(
            f"[dim]{row['output_dir']}:[/dim] {len(df)} points, "
            f"max [bold]{df['epower'].max() / 1e3:.1f} kW[/bold] "
            f"[dim]({label})[/dim]"
        )
    console.print(
        f"[dim]EMA α:[/dim] [bold]{ema_alpha}[/bold]"
        + ("" if ema_alpha != DEFAULT_EMA_ALPHA else " [dim](default)[/dim]")
    )

    # Step 6: time window — limits from power data
    global_min = min(df["time"].min() for df, _ in series)
    global_max = max(df["time"].max() for df, _ in series)
    t_start, t_end = ask_time_window(global_min, global_max)

    plot_power(
        series,
        intensity, ema_alpha, grid_label,
        f"Power over time — {chosen_set.name}",
        t_start, t_end,
    )


if __name__ == "__main__":
    main()

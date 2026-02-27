#!/usr/bin/env python3
"""Plot power over time for one or more experiments from a chosen experiment set."""

import json
import math
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.widgets import CheckButtons
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
    """Legend/window label: uppercase experiment directory name."""
    out_dir = str(row.get("output_dir", "")).strip()
    return out_dir.upper().replace("_", " ") if out_dir else "EXPERIMENT"


def get_alpha_from_row(row: pd.Series) -> float:
    """Return this experiment's greenfilling alpha, or default if missing."""
    opts = row.get("variant_options", "")
    if pd.notna(opts) and opts:
        try:
            d = json.loads(opts)
            if "alpha" in d:
                return float(d["alpha"])
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
    return DEFAULT_EMA_ALPHA


def is_greenfilling(row: pd.Series) -> bool:
    """True when the selected algorithm is a greenfilling variant."""
    algo = str(row.get("algorithm", "")).strip().lower()
    return "greenfilling" in algo


def is_easy_bf(row: pd.Series) -> bool:
    """True when the selected algorithm is easy backfilling."""
    algo = str(row.get("algorithm", "")).strip().lower()
    return algo == "easy_bf"


def make_window_label(row: pd.Series, alpha: float) -> str:
    """Window toggle label; mirrors legend experiment naming."""
    _ = alpha
    return make_plot_label(row)


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


def power_difference_vs_baseline(
    baseline_df: pd.DataFrame, other_df: pd.DataFrame, t_start: float, t_end: float
) -> pd.DataFrame:
    """Return step-aligned (other - baseline) power difference over the window."""
    bsub = extend_to_window(baseline_df[["time", "epower"]], "epower", t_start, t_end)
    osub = extend_to_window(other_df[["time", "epower"]], "epower", t_start, t_end)

    times = sorted(set(bsub["time"].tolist()) | set(osub["time"].tolist()))
    b = bsub.set_index("time")["epower"].reindex(times).ffill()
    o = osub.set_index("time")["epower"].reindex(times).ffill()

    return pd.DataFrame({
        "time": times,
        "diff_kw": (o - b) / 1e3,
    })


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

    # Avoid suggesting defaults that become invalid after decimal rounding.
    precision = 2
    scale = 10 ** precision
    lo_display = math.ceil(lo * scale) / scale
    hi_display = math.floor(hi * scale) / scale
    if lo_display > hi_display:
        lo_display, hi_display = lo, hi

    def validate_float(v: str, *, lo=lo_display, hi=hi_display) -> bool | str:
        try:
            f = float(v)
        except ValueError:
            return "Enter a number."
        if not (lo <= f <= hi):
            return f"Must be between {lo:.2f} and {hi:.2f}."
        return True

    console.print(
        f"[dim]Available range:[/dim] [bold]{lo_display:.2f} – {hi_display:.2f} {unit}[/bold]"
    )

    t_start = questionary.text(
        f"Start time ({unit}):",
        default=f"{lo_display:.2f}",
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
        if not (t_end_lo < f <= hi_display):
            return f"Must be greater than {t_end_lo:.2f} and at most {hi_display:.2f}."
        return True

    t_end = questionary.text(
        f"End time ({unit}):",
        default=f"{hi_display:.2f}",
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
    green_windows: list[dict],
    t_start: float,
    t_end: float,
):
    fig, (ax_power, ax_diff) = plt.subplots(
        2,
        1,
        figsize=(13, 7),
        sharex=True,
        gridspec_kw={"height_ratios": [3.0, 1.4]},
    )
    ax_int = ax_power.twinx()

    divisor, unit = time_unit_for(t_end)

    # Tick locator: multiples of the display unit, ~7 ticks across the window.
    step_s = _nice_step((t_end - t_start) / divisor) * divisor
    ax_diff.xaxis.set_major_locator(ticker.MultipleLocator(step_s))
    ax_diff.xaxis.set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"{x / divisor:g}")
    )

    # ── high-intensity windows (one per greenfilling experiment) ─────────────
    # EMA uses adjust=False — standard recursive formula EMA_t = α·x_t + (1−α)·EMA_{t−1}.
    # Computed on full intensity traces so values inside the window carry prior
    # history, matching what greenfilling sees at runtime.
    window_artists: list = []
    window_labels: list[str] = []
    window_colors_used: list = []
    baseline_color = "#111111"
    window_palette = [
        "#ff7f0e",  # orange
        "#2ca02c",  # green
        "#d62728",  # red
        "#9467bd",  # purple
        "#8c564b",  # brown
        "#e377c2",  # pink
        "#17becf",  # cyan
        "#bcbd22",  # olive
    ]

    for i, window in enumerate(green_windows):
        intensity = window["intensity"].copy()
        alpha = window["alpha"]
        ci_ema = intensity["ci"].ewm(alpha=alpha, adjust=False).mean()
        wi_ema = intensity["wi"].ewm(alpha=alpha, adjust=False).mean()
        intensity["allowed"] = (
            (intensity["ci"] <= ci_ema) & (intensity["wi"] <= wi_ema)
        ).astype(int)

        isub = extend_to_window(
            intensity[["time", "allowed"]], "allowed", t_start, t_end
        )
        color = window_palette[i % len(window_palette)]
        artist = ax_int.fill_between(
            isub["time"],
            isub["allowed"],
            step="post",
            color=color,
            alpha=0.11,
            zorder=1,
            label="_nolegend_",
        )
        window_artists.append(artist)
        window_labels.append(window["label"])
        window_colors_used.append(color)

    ax_int.set_ylim(0, 1)
    ax_int.yaxis.set_visible(False)
    ax_int.set_xlim(t_start, t_end)

    # ── power foreground ──────────────────────────────────────────────────────
    line_artists_by_label: dict[str, list] = {}
    diff_line_artist_by_label: dict[str, any] = {}
    window_color_by_label = {
        label: color for label, color in zip(window_labels, window_colors_used)
    }
    baseline_df, _baseline_label = series[0]

    for i, (df, label) in enumerate(series):
        # Baseline (first selected series) stays visually distinct.
        color = (
            window_color_by_label.get(label, baseline_color)
            if i == 0
            else window_color_by_label.get(label, "#1f77b4")
        )
        windowed = extend_to_window(df, "epower", t_start, t_end)
        line_artist = ax_power.step(
            windowed["time"], windowed["epower"] / 1e3,
            where="post", label=label, color=color,
            linewidth=1.4, alpha=0.9, zorder=3,
        )[0]
        line_artists_by_label.setdefault(label, []).append(line_artist)
        if i > 0:
            diff = power_difference_vs_baseline(baseline_df, df, t_start, t_end)
            diff_artist = ax_diff.step(
                diff["time"],
                diff["diff_kw"],
                where="post",
                color=color,
                label=label,
                linewidth=1.3,
                alpha=0.95,
            )[0]
            diff_line_artist_by_label[label] = diff_artist

    ax_power.tick_params(axis="x", which="both", labelbottom=False)
    ax_power.set_ylabel("Power (kW)")
    ax_power.set_xlim(t_start, t_end)
    ax_power.set_ylim(bottom=0)
    ax_power.grid(axis="y", linestyle="--", alpha=0.35, zorder=0)
    ax_power.set_zorder(ax_int.get_zorder() + 1)
    ax_power.patch.set_visible(False)

    ax_diff.axhline(0.0, color="#777777", linewidth=1.0, linestyle="--", zorder=1)
    ax_diff.set_ylabel("Delta kW")
    ax_diff.set_xlabel(f"Simulation time ({unit})")
    ax_diff.set_xlim(t_start, t_end)
    ax_diff.grid(axis="y", linestyle="--", alpha=0.35, zorder=0)

    # Keep legend clean: only selected experiment lines.
    h1, l1 = ax_power.get_legend_handles_labels()
    ncol = min(4, max(1, len(h1)))
    ax_power.legend(
        h1,
        l1,
        loc="lower left",
        bbox_to_anchor=(0.0, 1.02),
        borderaxespad=0.0,
        fontsize=8,
        ncol=ncol,
    )

    if window_artists:
        window_line_artists = [
            line_artists_by_label.get(label, []) for label in window_labels
        ]
        for artist, lines in zip(window_artists, window_line_artists):
            artist.set_visible(False)
            for ln in lines:
                ln.set_visible(False)
        for label in window_labels:
            diff_artist = diff_line_artist_by_label.get(label)
            if diff_artist is not None:
                diff_artist.set_visible(False)

        fig.subplots_adjust(right=0.74, bottom=0.1, top=0.9, hspace=0.1)
        top = ax_power.get_position()
        bottom = ax_diff.get_position()
        panel_y0 = bottom.y0
        panel_y1 = top.y1
        panel = fig.add_axes([0.76, panel_y0, 0.22, panel_y1 - panel_y0])
        panel.set_xticks([])
        panel.set_yticks([])
        checks = CheckButtons(panel, window_labels, [False] * len(window_labels))
        for i, txt in enumerate(checks.labels):
            txt.set_fontsize(8)
            txt.set_color(window_colors_used[i])

        def _on_toggle(label: str) -> None:
            idx = window_labels.index(label)
            visible = not window_artists[idx].get_visible()
            window_artists[idx].set_visible(visible)
            for ln in window_line_artists[idx]:
                ln.set_visible(visible)
            diff_artist = diff_line_artist_by_label.get(label)
            if diff_artist is not None:
                diff_artist.set_visible(visible)
            fig.canvas.draw_idle()

        checks.on_clicked(_on_toggle)
    else:
        fig.subplots_adjust(bottom=0.1, top=0.9, hspace=0.1)

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

    # Step 2: choose one easy_bf baseline
    experiments = list_experiments(chosen_set)
    if len(experiments) < 2:
        console.print("[red]Need at least 2 successful experiments to compare.[/red]")
        sys.exit(1)

    baseline_candidates = [
        row for _, row in experiments.iterrows() if is_easy_bf(row)
    ]
    if not baseline_candidates:
        console.print("[red]No successful easy_bf experiment found for baseline.[/red]")
        sys.exit(1)

    baseline_labels = [make_label(row) for row in baseline_candidates]
    chosen_baseline_label = questionary.select(
        "Select baseline experiment (easy_bf):",
        choices=baseline_labels,
        use_indicator=True,
    ).ask()
    if chosen_baseline_label is None:
        sys.exit(0)

    baseline_row = baseline_candidates[baseline_labels.index(chosen_baseline_label)]

    # Step 3: choose one or more greenfilling experiments for comparison
    comparison_candidates = [
        row for _, row in experiments.iterrows() if is_greenfilling(row)
    ]
    if not comparison_candidates:
        console.print("[red]No successful greenfilling experiments found.[/red]")
        sys.exit(1)

    comparison_labels = [make_label(row) for row in comparison_candidates]
    chosen_comparison_labels = questionary.checkbox(
        "Select greenfilling experiments to compare (space to toggle, enter to confirm):",
        choices=comparison_labels,
        validate=lambda selected: (
            True if len(selected) >= 1 else "Select at least 1 greenfilling experiment."
        ),
    ).ask()
    if chosen_comparison_labels is None:
        sys.exit(0)

    comparison_rows = [
        comparison_candidates[comparison_labels.index(label)]
        for label in chosen_comparison_labels
    ]
    selected_rows = [baseline_row, *comparison_rows]
    console.print(Panel(
        Text(
            "Baseline:\n"
            f"{chosen_baseline_label}\n\n"
            "Comparisons:\n"
            + "\n".join(chosen_comparison_labels),
            style="bold",
        ),
        title=f"Selected experiments ({len(selected_rows)})",
        expand=False,
    ))

    # Step 4: load data
    with console.status("Loading power and intensity data…"):
        series: list[tuple[pd.DataFrame, str]] = []
        green_windows: list[dict] = []
        for row in selected_rows:
            df = load_power(chosen_set / row["output_dir"])
            series.append((df, make_plot_label(row)))
            if is_greenfilling(row):
                gf_alpha = get_alpha_from_row(row)
                gf_intensity = load_intensity(chosen_set / row["output_dir"])
                green_windows.append({
                    "label": make_window_label(row, gf_alpha),
                    "intensity": gf_intensity,
                    "alpha": gf_alpha,
                })

    for (df, label), row in zip(series, selected_rows):
        console.print(
            f"[dim]{row['output_dir']}:[/dim] {len(df)} points, "
            f"max [bold]{df['epower'].max() / 1e3:.1f} kW[/bold] "
            f"[dim]({label})[/dim]"
        )
    if green_windows:
        for gw in green_windows:
            default_tag = " [dim](default α)[/dim]" if gw["alpha"] == DEFAULT_EMA_ALPHA else ""
            console.print(
                f"[dim]Window:[/dim] {gw['label']}{default_tag}"
            )
    else:
        console.print(
            "[yellow]No greenfilling experiments selected.[/yellow] "
            "Only power traces will be shown."
        )

    # Step 6: time window — limits from power data
    global_min = min(df["time"].min() for df, _ in series)
    global_max = max(df["time"].max() for df, _ in series)
    t_start, t_end = ask_time_window(global_min, global_max)

    plot_power(
        series,
        green_windows,
        t_start, t_end,
    )


if __name__ == "__main__":
    main()

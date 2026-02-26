import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from datetime import datetime, timedelta, timezone
from pathlib import Path

plt.rcParams["font.family"] = "Roboto"

START_DATE = datetime(year=2026, month=1, day=11, tzinfo=timezone.utc)

DATA_DIR = Path("energy-data")
PLOTS_DIR = Path("plots")

TRACE_FILES = {
    "clean_energy": "clean_energy_trace.csv",
    "fossil_heavy": "fossil_heavy_trace.csv",
    "mixed": "mixed_trace.csv",
}

GRID_LABELS = {
    "clean_energy": "Clean Energy Grid",
    "fossil_heavy": "Fossil Heavy Grid",
    "mixed": "Mixed Grid",
}


def _parse_mapping(s: str) -> dict[str, float]:
    parts = [p for p in s.split(";") if p]
    mapping: dict[str, float] = {}
    for part in parts:
        tech, value = part.split(":")
        mapping[tech.strip()] = float(value)
    return mapping


def compute_effective_intensities(df: pd.DataFrame) -> pd.DataFrame:
    carbon_row = df[df["property_name"] == "carbon_intensity"].iloc[0]
    water_row = df[df["property_name"] == "water_intensity"].iloc[0]

    carbon_intensity_map = _parse_mapping(carbon_row["new_value"])
    water_intensity_map = _parse_mapping(water_row["new_value"])

    mix_rows = df[df["property_name"] == "energy_mix"].copy()

    def effective_intensity(mix_str: str, intensity_map: dict[str, float]) -> float:
        mix = _parse_mapping(mix_str)
        total = 0.0
        for tech, share in mix.items():
            if tech in intensity_map:
                total += (share / 100.0) * intensity_map[tech]
        return total

    mix_rows["effective_carbon_intensity"] = mix_rows["new_value"].apply(
        lambda s: effective_intensity(s, carbon_intensity_map)
    )
    mix_rows["effective_water_intensity"] = mix_rows["new_value"].apply(
        lambda s: effective_intensity(s, water_intensity_map)
    )

    mix_rows = mix_rows.sort_values("timestamp").reset_index(drop=True)
    return mix_rows[["timestamp", "effective_carbon_intensity", "effective_water_intensity"]]


def plot_effective_intensities_for_trace(key: str, filename: str) -> None:
    df = pd.read_csv(DATA_DIR / filename)
    effective = compute_effective_intensities(df)

    timestamps = [START_DATE + timedelta(seconds=t) for t in effective["timestamp"]]
    x_start = timestamps[0]

    # Derive the date span from the trace duration so ticks and labels
    # cover the full time range and stay aligned with the data.
    start_midnight = START_DATE.replace(hour=0, minute=0, second=0, microsecond=0)
    last_time = float(effective["timestamp"].max())
    end_datetime = START_DATE + timedelta(seconds=last_time)
    end_midnight = end_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(20, 6))

    ax1.plot(timestamps, effective["effective_carbon_intensity"])
    ax1.set_ylabel("Effective Carbon Intensity (gCO2e/kWh)")

    ax2.plot(timestamps, effective["effective_water_intensity"])
    ax2.set_ylabel("Effective Water Intensity (L/kWh)")

    fig.suptitle(
        f"Effective Intensities from Trace — {GRID_LABELS[key]}",
        fontweight="bold",
    )

    x_plot_end = end_midnight + timedelta(days=1)
    tick_positions = []
    current = start_midnight
    while current <= x_plot_end:
        for ax in (ax1, ax2):
            ax.axvline(current, color="gray", linestyle="--", alpha=0.5, linewidth=0.7)
        if current >= x_start:
            tick_positions.append(current)
        current += timedelta(hours=12)

    for ax in (ax1, ax2):
        ax.set_xlim(x_start, x_plot_end)
        ax.set_xticks(tick_positions)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.yaxis.set_major_locator(ticker.LinearLocator(numticks=5))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.1f}"))

    num_days = (end_midnight - start_midnight).days + 1
    for day in range(num_days):
        noon = start_midnight + timedelta(days=day, hours=12)
        ax2.text(
            noon,
            -0.18,
            noon.strftime("%b %d"),
            transform=ax2.get_xaxis_transform(),
            ha="center",
            va="top",
            fontweight="bold",
            fontsize=plt.rcParams["xtick.labelsize"],
            clip_on=False,
        )

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PLOTS_DIR / f"intensities_trace_{key}.png"
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.12)
    plt.savefig(output_path)
    plt.close(fig)


def main() -> None:
    for key, filename in TRACE_FILES.items():
        plot_effective_intensities_for_trace(key, filename)


if __name__ == "__main__":
    main()


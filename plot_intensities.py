import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker
from datetime import datetime, timedelta, timezone

plt.rcParams["font.family"] = "Roboto"

START_DATE = datetime(year=2026, month=1, day=11, tzinfo=timezone.utc)
end_date = START_DATE + timedelta(days=7)

experiments = pd.read_csv("results/experiments.csv")
# Pick the first experiment for each grid type
first_by_grid = (
    experiments[experiments["output_dir"].notna()]
    .groupby("energy_grid")
    .first()
    .reset_index()[["energy_grid", "output_dir"]]
)

GRID_LABELS = {
    "clean_energy": "Clean Energy Grid",
    "fossil_heavy": "Fossil Heavy Grid",
    "mixed": "Mixed Grid",
}

for _, row in first_by_grid.iterrows():
    grid = row["energy_grid"]
    output_dir = row["output_dir"]

    df = pd.read_csv(f"results/{output_dir}/batsim_output_carbon_footprint.csv")
    df = df.drop_duplicates(subset="time", keep="last")
    timestamps = [START_DATE + timedelta(seconds=t) for t in df["time"]]

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=(14, 6))

    ax1.plot(timestamps, df["carbon_intensity(gCO2e/kWh)"])
    ax1.set_ylabel("Effective Carbon intensity (gCO2e/kWh)")

    ax2.plot(timestamps, df["water_intensity(L/kWh)"])
    ax2.set_ylabel("Effective Water intensity (L/kWh)")

    fig.suptitle(f"Effective Intensities — {GRID_LABELS[grid]}", fontweight="bold")

    current = START_DATE.replace(hour=0)
    while current <= end_date:
        for ax in (ax1, ax2):
            ax.axvline(current, color="gray", linestyle="--", alpha=0.5, linewidth=0.7)
        current += timedelta(hours=12)

    for ax in (ax1, ax2):
        ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 12]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.yaxis.set_major_locator(ticker.LinearLocator(numticks=5))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.1f}"))

    for day in range(7):
        noon = START_DATE + timedelta(days=day, hours=12)
        ax2.text(noon, -0.18, noon.strftime("%b %d"),
                 transform=ax2.get_xaxis_transform(),
                 ha="center", va="top", fontweight="bold",
                 fontsize=plt.rcParams["xtick.labelsize"], clip_on=False)

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.12)
    plt.savefig(f"plots/intensities_{grid}.png")
    plt.close(fig)

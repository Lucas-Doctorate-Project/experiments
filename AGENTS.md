# Experiments

This folder contains the experimental setup for evaluating environmental-aware scheduling heuristics in the context of HPC job scheduling. The goal is to understand trade-offs between:

- Performance: bounded slowdown, makespan
- Environmental impact: carbon footprint, water footprint

Simulations are run with Batsim (simulator) and Batsched (scheduler). Dependencies are managed via a Nix flake shell (see `flake.nix`).

## Policy to be evaluated

Greenfilling is a variant of the backfilling algorithm implemented by me. It was implemented using Batsched. The oracle-based version of greenfilling (implemented 2026-03-19) replaces the EMA binary gate with a continuous modulated capacity function:

$$N_g = \lfloor N_a \times \max(\tau,\ 1 - \max(I'_c, I'_w)) \rfloor$$

where intensities are normalized as:

$$I' = \text{clamp}!\left(\frac{I - I_{\min}}{I_{\max} - I_{\min}},\ 0,\ 1\right)$$

Instead of an on/off gate, it continuously scales the number of backfill-eligible machines inversely with environmental intensity. At peak intensity, capacity is reduced but never fully cut off thanks to `tau$, default 0.1). This avoids the dam effect of the binary gate, where jobs accumulated during dirty windows and flooded the scheduler when intensity dropped.

Features needed to run it:

- `carbon_min` / `carbon_max`: full-trace min/max for carbon intensity (oracle knowledge)
- `water_min` / `water_max`: full-trace min/max for water intensity (oracle knowledge)
- `tau` ($\tau$): minimum fraction of machines always available for backfilling

These options are passed as `variant_options` in the config file and forwarded to Batsched via `--variant_options`. The min/max bounds must come from the actual trace, which is what makes this version "oracle-based": it requires knowing the full intensity range in advance rather than computing it online.

## About this repo

- `runner.py`: Reads a config file and runs the resulting experiments in parallel (up to 6 at a time), writing results to `outputs/<run-name>/experiments.csv` after each run. Each experiment launches Batsim and Batsched as subprocesses over ZMQ, with a 60-minute timeout. A `--config` file is required; the runner exits if none is provided.
  - If an output directory already exists, the runner prompts to resume, delete, or cancel. On resume, the last 6 experiments are re-run for safety (they may have been interrupted mid-execution).
  - For `greenfilling` experiments, `carbon_min`/`carbon_max` and `water_min`/`water_max` are computed automatically from each energy trace and injected into `variant_options`. These do not need to be specified manually in the config.
  - `variant_options` values in `experiments.csv` are JSON-serialized strings.
- `configs/`: Experiments configuration folder. Usually configured as `greenfilling` vs. `easy_bf` across different workloads and energy scenarios. See **Config format** below.
- `outputs/`: Not tracked by git (while important for us). Each invocation of `runner.py` creates a timestamped subdirectory here (e.g. `outputs/20260224_103000_experiments/`).
  - `outputs/<run-name>/experiments.csv`: Manifest with `id`, `workload`, `energy_grid`, `algorithm`, `queue_order`, `variant_options`, `status`, and timing info for every experiment.
  - `outputs/<run-name>/experiment_XXX/`: Per-experiment output: `batsim.log`, `batsched.log`, and Batsim result files (prefixed `batsim_output_`).
- `workloads/`: Three 7-day Batsim-compatible workloads derived from LANL Mustang traces (2011–2016), with 12 job profiles per workload. See **Workloads** below.
- `energy-data/`: Collect and process energy mix data from ENTSO-E platform.
  - `energy-data/traces/`: Three one-week energy traces (CSV) consumed by Batsim's `--environmental-footprint-dynamic` option. See **Energy traces** below.
- `platform/mustang_platform.xml`: SimGrid platform model of the Mustang supercomputer (1600 compute nodes), used for all experiments.
- `plot_*.py`, `analyze.py`: Scripts to create plots and analyze results. Currently work-in-progress and unstable.

## Key output files

Each `experiment_XXX/` directory contains these Batsim result files relevant to our metrics:

| File | Key content |
|---|---|
| `batsim_output_jobs.csv` | Per-job metrics: `waiting_time`, `turnaround_time`, `bounded_slowdown` |
| `batsim_output_carbon_footprint.csv` | Per-interval carbon and water footprint |
| `batsim_output_consumed_energy.csv` | Per-interval energy consumption (J) |
| `batsim_output_schedule.trace` | Full event log (equivalent to jobs CSV, different format) |
| `batsim_output_machine_states.csv` | Per-host power state transitions |
| `batsim_output_pstate_changes.csv` | Platform power state change events |

> Note: the `batsim_output_` prefix reflects Batsim's default output naming, which is not yet explicitly configured in the runner.

Primary metrics of interest: **bounded slowdown**, **waiting time**, and **makespan** (from jobs CSV); **carbon footprint** and **water footprint** (from the footprint CSV).

## Config format

The TOML config is a flat list of `[[experiments]]` entries. Each entry defines one experiment explicitly — there is no implicit Cartesian product.

```toml
[[experiments]]
platform = "platform/mustang_platform.xml"
workload = "workloads/small.json"
energy_trace = "energy-data/traces/clean_energy_trace.csv"
algorithm = "easy_bf"
queue_order = "fcfs"

[[experiments]]
platform = "platform/mustang_platform.xml"
workload = "workloads/small.json"
energy_trace = "energy-data/traces/clean_energy_trace.csv"
algorithm = "greenfilling"
queue_order = "fcfs"
variant_options = {tau = 0.1}
```

- `easy_bf` entries have no `variant_options`.
- For `greenfilling`, the oracle bounds (`carbon_min/max`, `water_min/max`) are computed automatically from the trace and merged into `variant_options` at runtime — they do not appear in the config file.
- Manifest labels (`workload`, `energy_grid`) are derived from the filename stems of the paths (e.g., `small.json` → `small`, `clean_energy_trace.csv` → `clean_energy_trace`).

## Workloads

Three 7-day windows from LANL Mustang traces (2011–2016), each with 12 job profiles (4 CPU performance levels × 3 communication levels, assigned by weighted percentile):

| Name | Date | Characteristics |
|---|---|---|
| `small` | 2015-08-06 | ~97% jobs ≤ 10 nodes; 22,113 submissions |
| `large` | 2012-02-07 | ~55% jobs ≥ 120 nodes; regular arrival patterns |
| `mixed` | 2012-12-13 | Balanced mix of job sizes |

## Energy traces

Three one-week traces at 15-minute intervals, fixed to **ISO week 2, 2026** for reproducibility:

| Name | Country | Profile |
|---|---|---|
| `clean_energy` | France (FR) | Low-carbon (nuclear-heavy) |
| `fossil_heavy` | Poland (PL) | High-carbon (coal-heavy) |
| `mixed` | Germany (DE) | Mixed renewable/fossil |

Traces are generated by `energy-data/build_energy_traces.py` from ENTSO-E API data. Intensity factors (gCO2e/kWh for carbon, L/kWh for water) are stored in `energy-data/intensities.json`, sourced from Macknick et al. 2012, IPCC 2014, and UNECE 2020.

## How to run

Run the experiments and scripts using the Nix Shell. You can forward commands using `nix develop --impure --command`.

## Git conventions

- Follow the conventional commits guidelines.
- Keep commit messages concise and imperative. Add details only when necessary.
- Do not add `Co-Authored-By` trailers in commit messages. Don't mention yourself.
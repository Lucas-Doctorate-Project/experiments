# Experiments

This folder contains the experimental setup for evaluating the **greenfilling** scheduling heuristic — our novel contribution — in the context of HPC job scheduling. The goal is to understand trade-offs between:

- **Performance**: bounded slowdown, makespan
- **Environmental impact**: carbon footprint, water footprint

Simulations are run with **Batsim** (simulator) and **Batsched** (scheduler), both locally compiled and installed under `../build/`. Dependencies are managed via a **Nix flake shell** (see `../flake.nix`).

## Experiment design

18 experiments = Cartesian product of:

| Dimension       | Values |
|-----------------|--------|
| Workload        | `small`, `large`, `mixed` |
| Energy scenario | `clean_energy`, `fossil_heavy`, `mixed` |
| Algorithm       | `easy_bf` (baseline, FCFS), `greenfilling` (ours, FCFS) |

Greenfilling is configured with `carbon_min`, `carbon_max`, `water_min`, `water_max`, and `green_floor`. These are passed as `variant_options` in the config file and forwarded to Batsched via `--variant_options`. The min/max bounds should be derived from the intensity range of the energy traces being used.

## Key files

- **`runner.py`** — Reads a JSON config file and runs the resulting experiments in parallel (up to 6 at a time), writing results to `outputs/<run-name>/experiments.csv` after each run. Each experiment launches Batsim and Batsched as subprocesses over ZMQ, with a 60-minute timeout. A `--config` file is required; the runner exits if none is provided.
- **`configs/experiments.json`** — Default experiment design: greenfilling vs. easy_bf across all workloads and energy scenarios.
- **`outputs/`** — Not tracked by git. Each invocation of `runner.py` creates a timestamped subdirectory here (e.g. `outputs/20260224_103000_experiments/`).
- **`outputs/<run-name>/experiments.csv`** — Manifest with `id`, `workload`, `energy_grid`, `algorithm`, `queue_order`, `variant_options`, `status`, and timing info for every experiment.
- **`outputs/<run-name>/experiment_XXX/`** — Per-experiment output: `batsim_output.*` (Batsim result files), `batsim.log`, `batsched.log`.
- **`workloads/`** — Three 7-day Batsim-compatible workloads derived from LANL Mustang traces (2011–2016), with 12 job profiles per workload.
- **`energy-data/`** — Three one-week energy traces (CSV) consumed by Batsim's `--environmental-footprint-dynamic` option, plus `intensities.json` with carbon/water intensity factors per energy technology.
- **`platform/mustang_platform.xml`** — SimGrid platform model of the Mustang supercomputer, used for all experiments.
- **`plot_intensities.py`** — Plots the three energy intensity traces; outputs go to `plots/`.

## How to run

```bash
# Enter the Nix environment first
nix develop ../

# Run the default experiment design
python runner.py --config configs/experiments.json

# Give the run an explicit name
python runner.py --config configs/experiments.json --run-name my_run

# Resume a previous run
python runner.py --config configs/experiments.json --run-name 20260224_103000_experiments
```

Each run creates a new subdirectory under `outputs/` (gitignored). If the target directory already has content, the runner will ask whether to resume, delete and restart, or cancel.

## Git conventions

- Keep commit messages concise and imperative.
- Do **not** mention Claude or add `Co-Authored-By` trailers in commit messages.

## Claude's role here

Focus areas for assistance:
1. **Analyzing results** — parsing `outputs/<run-name>/experiments.csv` and per-experiment Batsim output files to compute metrics (bounded slowdown, makespan, carbon/water footprint).
2. **Generating plots** — creating or improving visualization scripts.
3. **Understanding the codebase** — explaining how experiments are structured, how Batsim/Batsched interact, etc.

Do **not** modify `runner.py` or experiment configs unless explicitly asked.

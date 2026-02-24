# Experiments

This folder contains the experimental setup for evaluating the **greenfilling** scheduling heuristic — our novel contribution — in the context of HPC job scheduling. The goal is to understand trade-offs between:

- **Performance**: bounded slowdown, makespan
- **Environmental impact**: carbon footprint, water footprint

Simulations are run with **Batsim** (simulator) and **Batsched** (scheduler), both locally compiled and installed under `../build/`. Dependencies are managed via a **Nix flake shell** (see `../flake.nix`).

## Experiment design

72 experiments = Cartesian product of:

| Dimension       | Values |
|-----------------|--------|
| Workload        | `small`, `large`, `mixed` |
| Energy scenario | `clean_energy`, `fossil_heavy`, `mixed` |
| Algorithm       | `easy_bf` (baseline), `greenfilling` (ours) |
| Queue order     | `fcfs`, `asc_estimated_area`, `asc_f1`, `frontier` |
| Alpha (greenfilling only) | Any value between [0.0, 1.0] |

## Key files

- **`runner.py`** — Generates all 72 `ExperimentConfig` objects and runs them in parallel (up to 6 at a time), writing results to `outputs/<run-name>/experiments.csv` after each run. Each experiment launches Batsim and Batsched as subprocesses over ZMQ, with a 30-minute timeout.
- **`outputs/`** — Not tracked by git. Each invocation of `runner.py` creates a timestamped subdirectory here (e.g. `outputs/20260224_103000_full/`).
- **`outputs/<run-name>/experiments.csv`** — Manifest with `id`, `workload`, `energy_grid`, `algorithm`, `queue_order`, `status`, and timing info for every experiment.
- **`outputs/<run-name>/experiment_XXX/`** — Per-experiment output: `batsim_output.*` (Batsim result files), `batsim.log`, `batsched.log`.
- **`workloads/`** — Three 7-day Batsim-compatible workloads derived from LANL Mustang traces (2011–2016), with 12 job profiles per workload.
- **`energy-mix/`** — Three one-week carbon intensity traces (CSV) consumed by Batsim's `--environmental-footprint-dynamic` option.
- **`platform/mustang_platform.xml`** — SimGrid platform model of the Mustang supercomputer, used for all experiments.
- **`plot_intensities.py`** — Plots the three energy intensity traces; outputs go to `plots/`.

## How to run

```bash
# Enter the Nix environment first
nix develop ../

# Run all 72 experiments (output goes to outputs/YYYYMMDD_HHMMSS_full/)
python runner.py

# Run a specific config (output goes to outputs/YYYYMMDD_HHMMSS_alphas/)
python runner.py --config configs/alphas.json

# Give the run an explicit name
python runner.py --run-name my_run

# Resume a previous run
python runner.py --run-name 20260224_103000_full
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

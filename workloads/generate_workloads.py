"""
generate_workloads.py
=====================
Mustang trace  →  three Batsim workload JSON files.

Each job gets its own parallel_homogeneous profile with:
  cpu = actual_runtime_seconds × FLOPS_PER_NODE
  com = 0  (network not modelled in the platform)

Usage (from the experiments/ directory, inside the Nix shell):
    python workloads/generate_workloads.py

Outputs:
    workloads/small.json   (week 2015-08-06, mostly small jobs)
    workloads/large.json   (week 2012-02-07, mostly large jobs)
    workloads/mixed.json   (week 2012-12-13, mixed job sizes)

"""

from __future__ import annotations

import gzip
import json
import shutil
import urllib.request
from pathlib import Path
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FILENAME     = Path(__file__).parent / "mustang_release_v1.0beta.csv"
DOWNLOAD_URL = "https://ftp.pdl.cmu.edu/pub/datasets/ATLAS/mustang/mustang_release_v1.0beta.csv.gz"
OUT_DIR  = Path(__file__).parent          # workloads/
CLUSTER_CAPACITY = 1600                   # Mustang node count
CORES_PER_NODE   = 24
FLOPS_PER_NODE   = 4.6e9                  # SimGrid platform speed per node (speed="4.6Gf")

# (week_start_date, output_name)
WEEKS = [
    ("2012-02-07", "large.json"),   # heavy large-job week
    ("2015-08-06", "small.json"),   # heavy small-job week
    ("2012-12-13", "mixed.json"),   # mixed week
]

# ---------------------------------------------------------------------------
# Dataset download
# ---------------------------------------------------------------------------

def ensure_dataset(path: Path = FILENAME, url: str = DOWNLOAD_URL) -> None:
    """Download and decompress the Mustang CSV if it is not already present."""
    if path.exists():
        return
    gz_path = path.with_suffix(".csv.gz")
    print(f"Dataset not found. Downloading from {url} …")
    urllib.request.urlretrieve(url, gz_path)
    print(f"Decompressing {gz_path.name} …")
    with gzip.open(gz_path, "rb") as f_in, open(path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    gz_path.unlink()
    print(f"Saved to {path}")


# ---------------------------------------------------------------------------
# Duration helper
# ---------------------------------------------------------------------------

def compute_duration_sec(
    df: pd.DataFrame,
    start_col: str = "start_time",
    end_col: str = "end_time",
    wallclock_col: str = "wallclock_limit",
) -> pd.Series:
    """Runtime (seconds); falls back to wallclock_limit when runtime is absent."""
    start = pd.to_datetime(df[start_col], errors="coerce", utc=True)
    end   = pd.to_datetime(df[end_col],   errors="coerce", utc=True)
    runtime_sec = (end - start).dt.total_seconds()
    wall_sec    = pd.to_timedelta(df[wallclock_col], errors="coerce").dt.total_seconds()
    return runtime_sec.where(runtime_sec.notna() & (runtime_sec > 0), wall_sec).astype(float)


# ---------------------------------------------------------------------------
# Walltime parsing
# ---------------------------------------------------------------------------

def _parse_walltime_seconds(s: pd.Series) -> pd.Series:
    """
    Seconds from wallclock_limit.  Handles timedelta dtype, timedelta strings
    like '0 days 08:00:00', and numeric minutes as a fallback.
    """
    if pd.api.types.is_timedelta64_dtype(s):
        return s.dt.total_seconds()
    td          = pd.to_timedelta(s, errors="coerce").dt.total_seconds()
    num_minutes = pd.to_numeric(s, errors="coerce") * 60.0
    return td.where(td.notna(), num_minutes)


# ---------------------------------------------------------------------------
# Warm-up context extraction
# ---------------------------------------------------------------------------

def extract_running_context(
    df: pd.DataFrame,
    week_start: pd.Timestamp,
    *,
    submit_col: str = "submit_time",
    start_col: str = "start_time",
    end_col: str = "end_time",
    nodes_col: str = "node_count",
) -> tuple[list[dict], dict]:
    """
    Jobs running at week_start (start < T0 <= end) become Batsim "ctx" jobs:
      subtime = 0, walltime = remaining seconds until actual end.
    Each gets its own profile: cpu = remaining_sec × FLOPS_PER_NODE, com = 0.

    Returns (ctx_jobs_list, ctx_profiles_dict).
    """
    d = df.copy()
    for c in [start_col, end_col, submit_col]:
        d[c] = pd.to_datetime(d[c], errors="coerce", utc=True)

    if d[start_col].dt.tz is not None and week_start.tzinfo is None:
        week_start = week_start.tz_localize("UTC")

    mask    = (d[start_col] < week_start) & (d[end_col] > week_start)
    running = d[mask].copy()

    if running.empty:
        print(f"  [context] no running jobs at {week_start.date()}")
        return [], {}

    running["_remaining_sec"] = (
        d.loc[mask, end_col] - week_start
    ).dt.total_seconds().clip(lower=1.0)

    running[nodes_col] = (
        pd.to_numeric(running[nodes_col], errors="coerce")
          .fillna(1).clip(lower=1).astype(int)
    )

    ctx_jobs: list[dict] = []
    ctx_profiles: dict = {}
    for i, (idx, row) in enumerate(running.iterrows(), start=1):
        prof_name = f"ctx{i}"
        remaining = float(row["_remaining_sec"])
        ctx_profiles[prof_name] = {
            "type": "parallel_homogeneous",
            "cpu":  remaining * FLOPS_PER_NODE,
            "com":  0.0,
        }
        ctx_jobs.append({
            "id":      prof_name,
            "subtime": 0.0,
            "res":     int(row[nodes_col]),
            "profile": prof_name,
            "walltime": int(round(remaining)),
        })

    print(f"  [context] {len(ctx_jobs)} jobs at {week_start.date()} "
          f"({sum(j['res'] for j in ctx_jobs)} nodes occupied)")
    return ctx_jobs, ctx_profiles


# ---------------------------------------------------------------------------
# Batsim workload JSON builder
# ---------------------------------------------------------------------------

def make_batsim_workload_json(
    week_rows: pd.DataFrame,
    week_start: pd.Timestamp,
    week_end: pd.Timestamp,
    cluster_capacity: Optional[float],
    *,
    submit_col: str = "submit_time",
    nodes_col: str = "node_count",
    wallclock_col: str = "wallclock_limit",
    tasks_col: str = "tasks_requested",
    start_col: str = "start_time",
    end_col: str = "end_time",
    cores_per_node: int = CORES_PER_NODE,
    context_jobs: list | None = None,
    context_profiles: dict | None = None,
) -> dict:
    """
    Build the Batsim workload dict for one week.
    Each regular job gets its own profile: cpu = actual_runtime × FLOPS_PER_NODE, com = 0.
    context_jobs (warm-up) are prepended before regular jobs with subtime=0.
    """
    rows = week_rows.copy()
    rows[nodes_col] = pd.to_numeric(rows[nodes_col], errors="coerce")
    rows[tasks_col] = pd.to_numeric(rows[tasks_col], errors="coerce")
    rows["walltime_sec"] = _parse_walltime_seconds(rows[wallclock_col])
    rows = rows.dropna(subset=[submit_col]).sort_values(submit_col)

    node_fallback = rows[nodes_col][(rows[nodes_col].notna()) & (rows[nodes_col] > 0)].median()
    if pd.isna(node_fallback): node_fallback = 1.0

    def infer_nodes(n, t) -> int:
        if n is not None and not (isinstance(n, float) and np.isnan(n)) and int(n) > 0:
            return int(n)
        if t is None or (isinstance(t, float) and np.isnan(t)) or int(t) <= 0:
            return int(node_fallback)
        return (int(t) + cores_per_node - 1) // cores_per_node

    rows["_nodes_eff"] = [
        infer_nodes(n, t)
        for n, t in rows[[nodes_col, tasks_col]].itertuples(index=False, name=None)
    ]
    rows["_nodes_eff"] = (
        pd.to_numeric(rows["_nodes_eff"], errors="coerce")
          .fillna(node_fallback).clip(lower=1).astype(int)
    )

    rows["_runtime_sec"] = compute_duration_sec(
        rows, start_col=start_col, end_col=end_col, wallclock_col=wallclock_col,
    ).clip(lower=1.0)

    nb_res = int(cluster_capacity) if cluster_capacity is not None \
             else (int(max(1, rows["_nodes_eff"].max())) if len(rows) else 1)

    jobs: list[dict] = []
    profiles: dict = dict(context_profiles or {})

    for i, row in enumerate(
        rows[[submit_col, "_nodes_eff", "walltime_sec", "_runtime_sec"]]
            .itertuples(index=False, name=None),
        start=1,
    ):
        submit_time, nodes_eff, walltime_sec, runtime_sec = row
        subtime = (pd.Timestamp(submit_time) - week_start).total_seconds()
        if subtime < 0 or pd.Timestamp(submit_time) >= week_end:
            continue

        prof_name = f"job{i}"
        profiles[prof_name] = {
            "type": "parallel_homogeneous",
            "cpu":  float(runtime_sec) * FLOPS_PER_NODE,
            "com":  0.0,
        }
        job: dict = {
            "id":      prof_name,
            "subtime": float(subtime),
            "res":     int(nodes_eff),
            "profile": prof_name,
        }
        if walltime_sec is not None and not (isinstance(walltime_sec, float) and np.isnan(walltime_sec)):
            wt = float(walltime_sec)
            if wt > 0:
                job["walltime"] = int(round(wt))
        jobs.append(job)

    if cluster_capacity is None:
        nb_res = int(max(1, max((j["res"] for j in jobs), default=1)))

    return {"nb_res": nb_res, "jobs": (context_jobs or []) + jobs, "profiles": profiles}


# ---------------------------------------------------------------------------
# Top-level export
# ---------------------------------------------------------------------------

def export_weeks(
    df: pd.DataFrame,
    weeks: Sequence[tuple[str, str]],           # [(date_str, output_name), ...]
    out_dir: Union[str, Path],
    cluster_capacity: Optional[float] = None,
    week_days: int = 7,
    submit_col: str = "submit_time",
    start_col: str = "start_time",
    end_col: str = "end_time",
    nodes_col: str = "node_count",
    wallclock_col: str = "wallclock_limit",
    tasks_col: str = "tasks_requested",
) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for c in [submit_col, start_col, end_col]:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    submit_tz = df[submit_col].dt.tz

    for date_str, output_name in weeks:
        week_start = pd.to_datetime(date_str)
        if submit_tz is not None and week_start.tzinfo is None:
            week_start = week_start.tz_localize(submit_tz)
        week_end  = week_start + pd.Timedelta(days=week_days)
        date_name = week_start.strftime("%Y-%m-%d")

        print(f"\n=== {date_name} ({output_name}) ===")

        # Warm-up context: jobs already running at T0
        ctx_jobs, ctx_profiles = extract_running_context(
            df, week_start,
            submit_col=submit_col, start_col=start_col, end_col=end_col,
            nodes_col=nodes_col,
        )

        # Jobs submitted during the week
        week_rows = df[
            (df[submit_col] >= week_start) & (df[submit_col] < week_end)
        ].copy().sort_values(submit_col)

        batsim_obj = make_batsim_workload_json(
            week_rows, week_start, week_end, cluster_capacity,
            submit_col=submit_col, nodes_col=nodes_col, wallclock_col=wallclock_col,
            tasks_col=tasks_col, start_col=start_col, end_col=end_col,
            context_jobs=ctx_jobs, context_profiles=ctx_profiles,
        )

        named_json = out_dir / output_name
        with open(named_json, "w", encoding="utf-8") as f:
            json.dump(batsim_obj, f, indent=4)

        n_ctx  = sum(1 for j in batsim_obj["jobs"] if j["id"].startswith("ctx"))
        n_jobs = sum(1 for j in batsim_obj["jobs"] if j["id"].startswith("job"))
        ctx_nodes = sum(j["res"] for j in batsim_obj["jobs"] if j["id"].startswith("ctx"))
        print(f"  wrote {named_json.name}: "
              f"{n_ctx} ctx jobs ({ctx_nodes} nodes at T0) + {n_jobs} regular jobs")
        print(f"  total profiles: {len(batsim_obj['profiles'])}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ensure_dataset()
    print(f"Loading {FILENAME} …")
    df = pd.read_csv(FILENAME)
    for col in ["submit_time", "start_time", "end_time"]:
        df[col] = pd.to_datetime(df[col], utc=True)
    df = df.sort_values("submit_time").reset_index(drop=True)
    print(f"Loaded {len(df):,} rows")

    export_weeks(df, WEEKS, out_dir=OUT_DIR, cluster_capacity=CLUSTER_CAPACITY)
    print("\nDone.")

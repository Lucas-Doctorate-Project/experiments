"""
generate_workloads.py
=====================
Mustang trace  →  three Batsim workload JSON files.

Usage (from the experiments/ directory, inside the Nix shell):
    python workloads/generate_workloads.py

Outputs:
    workloads/small.json   (week 2015-08-06, mostly small jobs)
    workloads/large.json   (week 2012-02-07, mostly large jobs)
    workloads/mixed.json   (week 2012-12-13, mixed job sizes)

Each JSON also lands in workloads/<YYYY-MM-DD>/workload_batsim.json alongside
a week_fraction.csv with the raw rows.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Optional, Sequence, Union

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FILENAME = Path(__file__).parent / "mustang_release_v1.0beta.csv"
OUT_DIR  = Path(__file__).parent          # workloads/
CLUSTER_CAPACITY = 1600                   # Mustang node count
CORES_PER_NODE   = 24

# (week_start_date, output_name)
WEEKS = [
    ("2012-02-07", "large.json"),   # heavy large-job week
    ("2015-08-06", "small.json"),   # heavy small-job week
    ("2012-12-13", "mixed.json"),   # mixed week
]

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
# Global rank arrays (used for percentile-based profile assignment)
# ---------------------------------------------------------------------------

def compute_global_rank_arrays(
    df: pd.DataFrame,
    *,
    start_col: str = "start_time",
    end_col: str = "end_time",
    wallclock_col: str = "wallclock_limit",
    nodes_col: str = "node_count",
    tasks_col: str = "tasks_requested",
    cores_per_node: int = CORES_PER_NODE,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Return (log_dur_all, log_nodes_all): sorted log-arrays over the full trace,
    used by assign_profiles_4x3_global_alpha() to compute per-job percentiles.

    effective_nodes = node_count if > 0
                    else ceil(tasks_requested / cores_per_node)
    """
    dur   = compute_duration_sec(df, start_col, end_col, wallclock_col)
    dur   = dur[(dur.notna()) & (dur > 0)]

    nodes = pd.to_numeric(df[nodes_col], errors="coerce")
    tasks = (pd.to_numeric(df[tasks_col], errors="coerce")
             if tasks_col in df.columns
             else pd.Series(np.nan, index=df.index))

    inferred  = np.ceil(tasks / float(cores_per_node))
    inferred  = inferred.where(inferred.notna() & (inferred > 0), np.nan)
    eff_nodes = nodes.where(nodes.notna() & (nodes > 0), inferred)

    valid = dur.index.intersection(eff_nodes.index)
    dur2, eff2 = dur.loc[valid], eff_nodes.loc[valid]
    mask = dur2.notna() & (dur2 > 0) & eff2.notna() & (eff2 > 0)
    dur2, eff2 = dur2[mask], eff2[mask]

    if len(dur2) == 0:
        raise ValueError("No valid duration rows in trace.")
    if len(eff2) == 0:
        raise ValueError("No valid node-count rows in trace.")

    return np.sort(np.log(dur2.to_numpy())), np.sort(np.log(eff2.to_numpy()))


# ---------------------------------------------------------------------------
# Profile catalogue
# ---------------------------------------------------------------------------

def build_profiles_12(
    cpu_levels: list | None = None,
    com_levels: list | None = None,
) -> dict:
    """12 parallel_homogeneous profiles: 4 CPU tiers × 3 COM tiers → p0..p11."""
    if cpu_levels is None: cpu_levels = [1e7, 3e7, 1e8, 3e8]
    if com_levels is None: com_levels = [1e6, 1e7, 1e8]
    profiles, k = {}, 0
    for c in cpu_levels:
        for m in com_levels:
            profiles[f"p{k}"] = {
                "type": "parallel_homogeneous",
                "cpu":  float(c),
                "com":  float(m),
            }
            k += 1
    return profiles


# ---------------------------------------------------------------------------
# Profile assignment
# ---------------------------------------------------------------------------

def assign_profiles_4x3_global_alpha(
    jobs: pd.DataFrame,
    *,
    log_dur_all: np.ndarray,
    log_nodes_all: np.ndarray,
    rng: np.random.Generator | None = None,
    start_col: str = "start_time",
    end_col: str = "end_time",
    wallclock_col: str = "wallclock_limit",
    nodes_col: str = "node_count",
    alpha_cpu: float = 0.75,
    alpha_com: float = 0.85,
    jitter_prob: float = 0.0,
) -> tuple[pd.Series, np.ndarray, np.ndarray]:
    """
    Assign a profile p0..p11 to each job via global percentile mixing:
      cpu_score = alpha_cpu * dur_pct  + (1 - alpha_cpu) * node_pct  → CPU tier 0..3
      com_score = alpha_com * node_pct + (1 - alpha_com) * dur_pct   → COM tier 0..2

    Returns (profile_series, cpu_tier_array, com_tier_array).
    """
    x = jobs.copy()

    duration_sec = compute_duration_sec(x, start_col, end_col, wallclock_col)
    dur_fallback = duration_sec[(duration_sec.notna()) & (duration_sec > 0)].median()
    if pd.isna(dur_fallback): dur_fallback = 3600.0
    duration_sec = duration_sec.fillna(dur_fallback).clip(lower=1.0)

    nodes = pd.to_numeric(x[nodes_col], errors="coerce")
    node_fallback = nodes[(nodes.notna()) & (nodes > 0)].median()
    if pd.isna(node_fallback): node_fallback = 1.0
    nodes = nodes.fillna(node_fallback).clip(lower=1.0)

    log_dur  = np.log(duration_sec.to_numpy())
    log_node = np.log(nodes.to_numpy())

    dur_pct  = np.searchsorted(log_dur_all,   log_dur,  side="right") / len(log_dur_all)
    lo = np.searchsorted(log_nodes_all, log_node, side="left")
    hi = np.searchsorted(log_nodes_all, log_node, side="right")
    node_pct = (lo + hi) / 2 / len(log_nodes_all)

    cpu_score = alpha_cpu * dur_pct  + (1.0 - alpha_cpu) * node_pct
    com_score = alpha_com * node_pct + (1.0 - alpha_com) * dur_pct

    cpu_tier = np.minimum(3, (cpu_score * 4).astype(int))
    com_tier = np.minimum(2, (com_score * 3).astype(int))

    if rng is not None and len(x) > 0 and jitter_prob > 0:
        jmask    = rng.random(len(x)) < jitter_prob
        cpu_tier = np.clip(cpu_tier + jmask * rng.integers(-1, 2, size=len(x)), 0, 3)
        com_tier = np.clip(com_tier + jmask * rng.integers(-1, 2, size=len(x)), 0, 2)

    idx     = (cpu_tier * 3 + com_tier).astype(int)
    profile = pd.Series([f"p{i}" for i in idx], index=x.index, name="profile")
    return profile, cpu_tier, com_tier


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
# Event-delta helper (node occupancy)
# ---------------------------------------------------------------------------

def _build_events_delta(
    df: pd.DataFrame,
    start_col: str = "start_time",
    end_col: str = "end_time",
    size_col: str = "node_count",
) -> pd.Series:
    """
    Step-function of cluster occupancy as a delta series:
      +node_count at start_time, -node_count at end_time.
    cumsum() gives instantaneous occupancy.
    """
    tmp = df[[start_col, end_col, size_col]].dropna().copy()
    tmp[size_col] = pd.to_numeric(tmp[size_col], errors="coerce")
    tmp = tmp.dropna(subset=[size_col])
    tmp = tmp[(tmp[size_col] > 0) & (tmp[end_col] >= tmp[start_col])]
    starts = tmp.groupby(start_col)[size_col].sum()
    ends   = -tmp.groupby(end_col)[size_col].sum()
    return pd.concat([starts, ends]).groupby(level=0).sum().sort_index()


# ---------------------------------------------------------------------------
# Warm-up context extraction
# ---------------------------------------------------------------------------

def extract_running_context(
    df: pd.DataFrame,
    week_start: pd.Timestamp,
    *,
    log_dur_all: np.ndarray,
    log_nodes_all: np.ndarray,
    rng: np.random.Generator,
    submit_col: str = "submit_time",
    start_col: str = "start_time",
    end_col: str = "end_time",
    nodes_col: str = "node_count",
    wallclock_col: str = "wallclock_limit",
    alpha_cpu: float = 0.70,
    alpha_com: float = 0.85,
) -> list[dict]:
    """
    Jobs running at week_start (start < T0 <= end) become Batsim "ctx" jobs:
      subtime = 0, walltime = remaining seconds until actual end.
    Profiles are assigned with the same alpha-mixing as regular jobs.
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
        return []

    running["_remaining_sec"] = (
        d.loc[mask, end_col] - week_start
    ).dt.total_seconds().clip(lower=1.0)

    running[nodes_col] = (
        pd.to_numeric(running[nodes_col], errors="coerce")
          .fillna(1).clip(lower=1).astype(int)
    )

    # Build a synthetic frame so assign_profiles can compute duration from timestamps
    tmp = running.copy()
    tmp[wallclock_col] = pd.to_timedelta(running["_remaining_sec"], unit="s")
    tmp[start_col]     = week_start - pd.to_timedelta(running["_remaining_sec"], unit="s")
    tmp[end_col]       = week_start + pd.to_timedelta(running["_remaining_sec"], unit="s")

    profiles_assigned, _, _ = assign_profiles_4x3_global_alpha(
        tmp,
        log_dur_all=log_dur_all,
        log_nodes_all=log_nodes_all,
        rng=rng,
        start_col=start_col,
        end_col=end_col,
        wallclock_col=wallclock_col,
        nodes_col=nodes_col,
        alpha_cpu=alpha_cpu,
        alpha_com=alpha_com,
        jitter_prob=0.0,    # no jitter for context jobs
    )

    ctx_jobs = [
        {
            "id":      f"ctx{i}",
            "subtime": 0.0,
            "res":     int(row[nodes_col]),
            "profile": profiles_assigned.loc[idx],
            "walltime": int(round(row["_remaining_sec"])),
        }
        for i, (idx, row) in enumerate(running.iterrows(), start=1)
    ]

    print(f"  [context] {len(ctx_jobs)} jobs at {week_start.date()} "
          f"({sum(j['res'] for j in ctx_jobs)} nodes occupied)")
    return ctx_jobs


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
    rng: np.random.Generator,
    log_dur_all: np.ndarray,
    log_nodes_all: np.ndarray,
    alpha_cpu: float = 0.70,
    alpha_com: float = 0.85,
    jitter_prob: float = 0.25,
    cores_per_node: int = CORES_PER_NODE,
    print_grid: bool = True,
    context_jobs: list | None = None,
) -> dict:
    """
    Build the Batsim workload dict for one week.
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

    profiles = build_profiles_12()

    tmp = rows.copy()
    tmp[nodes_col] = rows["_nodes_eff"]
    rows["profile"], cpu_tier, com_tier = assign_profiles_4x3_global_alpha(
        tmp,
        log_dur_all=log_dur_all,
        log_nodes_all=log_nodes_all,
        rng=rng,
        start_col=start_col,
        end_col=end_col,
        wallclock_col=wallclock_col,
        nodes_col=nodes_col,
        alpha_cpu=alpha_cpu,
        alpha_com=alpha_com,
        jitter_prob=jitter_prob,
    )

    if print_grid:
        counts = np.zeros((4, 3), dtype=int)
        for ct, mt in zip(cpu_tier, com_tier):
            counts[int(ct), int(mt)] += 1
        total = counts.sum()
        pct   = counts / total * 100 if total > 0 else np.zeros_like(counts, dtype=float)
        print("  [profile grid %]  rows=CPU 0..3  cols=COM 0..2")
        for r in range(4):
            print("  CPU%d: %s" % (r, "  ".join(f"{pct[r,c]:6.2f}%" for c in range(3))))

    nb_res = int(cluster_capacity) if cluster_capacity is not None \
             else (int(max(1, rows["_nodes_eff"].max())) if len(rows) else 1)

    jobs: list[dict] = []
    for i, (submit_time, nodes_eff, profile, walltime_sec) in enumerate(
        rows[[submit_col, "_nodes_eff", "profile", "walltime_sec"]]
            .itertuples(index=False, name=None),
        start=1,
    ):
        subtime = (pd.Timestamp(submit_time) - week_start).total_seconds()
        if subtime < 0 or pd.Timestamp(submit_time) >= week_end:
            continue
        job: dict = {
            "id":      f"job{i}",
            "subtime": float(subtime),
            "res":     int(nodes_eff),
            "profile": str(profile),
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
    random_seed: int = 42,
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

    rng = np.random.default_rng(random_seed)
    log_dur_all, log_nodes_all = compute_global_rank_arrays(
        df,
        start_col=start_col,
        end_col=end_col,
        wallclock_col=wallclock_col,
        nodes_col=nodes_col,
        tasks_col=tasks_col,
    )

    for date_str, output_name in weeks:
        week_start = pd.to_datetime(date_str)
        if submit_tz is not None and week_start.tzinfo is None:
            week_start = week_start.tz_localize(submit_tz)
        week_end  = week_start + pd.Timedelta(days=week_days)
        date_name = week_start.strftime("%Y-%m-%d")

        print(f"\n=== {date_name} ({output_name}) ===")

        week_dir = out_dir / date_name
        week_dir.mkdir(parents=True, exist_ok=True)

        # Warm-up context: jobs already running at T0
        ctx_jobs = extract_running_context(
            df, week_start,
            log_dur_all=log_dur_all, log_nodes_all=log_nodes_all, rng=rng,
            submit_col=submit_col, start_col=start_col, end_col=end_col,
            nodes_col=nodes_col, wallclock_col=wallclock_col,
        )

        # Jobs submitted during the week
        week_rows = df[
            (df[submit_col] >= week_start) & (df[submit_col] < week_end)
        ].copy().sort_values(submit_col)

        week_rows.to_csv(week_dir / "week_fraction.csv", index=False)

        batsim_obj = make_batsim_workload_json(
            week_rows, week_start, week_end, cluster_capacity,
            submit_col=submit_col, nodes_col=nodes_col, wallclock_col=wallclock_col,
            tasks_col=tasks_col, start_col=start_col, end_col=end_col,
            rng=rng, log_dur_all=log_dur_all, log_nodes_all=log_nodes_all,
            alpha_cpu=0.70, alpha_com=0.85, jitter_prob=0.25, print_grid=True,
            context_jobs=ctx_jobs,
        )

        dated_json = week_dir / "workload_batsim.json"
        with open(dated_json, "w", encoding="utf-8") as f:
            json.dump(batsim_obj, f, indent=4)

        named_json = out_dir / output_name
        shutil.copy(dated_json, named_json)

        n_ctx  = sum(1 for j in batsim_obj["jobs"] if j["id"].startswith("ctx"))
        n_jobs = sum(1 for j in batsim_obj["jobs"] if j["id"].startswith("job"))
        ctx_nodes = sum(j["res"] for j in batsim_obj["jobs"] if j["id"].startswith("ctx"))
        print(f"  wrote {named_json.name}: "
              f"{n_ctx} ctx jobs ({ctx_nodes} nodes at T0) + {n_jobs} regular jobs")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Loading {FILENAME} …")
    df = pd.read_csv(FILENAME)
    for col in ["submit_time", "start_time", "end_time"]:
        df[col] = pd.to_datetime(df[col], utc=True)
    df = df.sort_values("submit_time").reset_index(drop=True)
    print(f"Loaded {len(df):,} rows")

    export_weeks(df, WEEKS, out_dir=OUT_DIR, cluster_capacity=CLUSTER_CAPACITY)
    print("\nDone.")

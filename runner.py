"""
Experiment runner for Batsim/Batsched simulations.

This script automates running experiments that test different combinations of:
- Workloads (small, large, mixed)
- Energy scenarios (clean, fossil, mixed)
- Scheduling algorithms (easy_bf baseline, greenfilling)
"""

import subprocess
import time
import csv
import shutil
import sys
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple


@dataclass
class ExperimentConfig:
    """Configuration for a single experiment."""
    exp_id: int
    workload_name: str
    workload_path: str
    platform_path: str
    energy_trace_name: str
    energy_trace_path: str
    algorithm: str
    queue_order: str
    variant_options: Optional[str]
    output_dir: str


@dataclass
class ExperimentResult:
    """Result of running a single experiment."""
    config: ExperimentConfig
    status: str  # "success", "timeout", "failed"
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    error_message: Optional[str] = None


def generate_experiment_configs(base_dir: Path) -> List[ExperimentConfig]:
    """
    Generate all experiment configurations.

    Creates 72 experiments from Cartesian product of:
    - 3 workloads × 3 energy scenarios = 9 combinations
    - 2 algorithms per combination
    - 4 queue orderings per algorithm

    Args:
        base_dir: Base directory containing workloads, platforms, and energy traces

    Returns:
        List of 72 ExperimentConfig objects
    """
    workloads = [
        ("small", "workloads/small.json"),
        ("large", "workloads/large.json"),
        ("mixed", "workloads/mixed.json"),
    ]

    # Single platform for all experiments (Mustang supercomputer model)
    platform_path = "platform/mustang_platform.xml"

    # Energy scenarios vary independently from the platform
    energy_scenarios = [
        ("clean_energy", "energy-mix/clean_energy_trace.csv"),
        ("fossil_heavy", "energy-mix/fossil_heavy_trace.csv"),
        ("mixed", "energy-mix/mixed_trace.csv"),
    ]

    algorithms = [
        ("easy_bf", None),
        ("greenfilling", '{"alpha": 0.3}'),
    ]

    # Queue ordering strategies
    queue_orders = [
        "fcfs",                 # First Come First Served
        "asc_estimated_area",   # Shortest Area First (SAF)
        "asc_f1",               # F1 scoring
        "frontier",             # Frontier scheduling
    ]

    configs = []
    exp_id = 1

    for workload_name, workload_path in workloads:
        for energy_trace_name, energy_trace_path in energy_scenarios:
            for algorithm, variant_options in algorithms:
                for queue_order in queue_orders:
                    config = ExperimentConfig(
                        exp_id=exp_id,
                        workload_name=workload_name,
                        workload_path=str(base_dir / workload_path),
                        platform_path=str(base_dir / platform_path),
                        energy_trace_name=energy_trace_name,
                        energy_trace_path=str(base_dir / energy_trace_path),
                        algorithm=algorithm,
                        queue_order=queue_order,
                        variant_options=variant_options,
                        output_dir=str(base_dir / "results" / f"experiment_{exp_id:03d}"),
                    )
                    configs.append(config)
                    exp_id += 1

    return configs


def cleanup_processes(processes: List[subprocess.Popen], grace_period: int = 5):
    """
    Gracefully terminate running processes.

    Args:
        processes: List of Popen objects to terminate
        grace_period: Seconds to wait after SIGTERM before sending SIGKILL
    """
    # Send SIGTERM to all processes
    for proc in processes:
        if proc.poll() is None:  # Process still running
            try:
                proc.terminate()
            except ProcessLookupError:
                pass

    # Wait for graceful shutdown
    start = time.time()
    while time.time() - start < grace_period:
        if all(proc.poll() is not None for proc in processes):
            return
        time.sleep(0.1)

    # Force kill any remaining processes
    for proc in processes:
        if proc.poll() is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    # Final wait
    for proc in processes:
        try:
            proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


def wait_with_timeout(processes: List[subprocess.Popen], timeout: int) -> Tuple[bool, Optional[str]]:
    """
    Wait for processes to complete with timeout enforcement.

    Args:
        processes: List of Popen objects to monitor
        timeout: Maximum wait time in seconds

    Returns:
        (success: bool, error_msg: Optional[str])
        - (True, None) if all processes completed successfully
        - (False, "timeout") if timeout exceeded
        - (False, error_msg) if process failed
    """
    start_time = time.time()

    while True:
        # Check if all processes have terminated
        all_done = all(proc.poll() is not None for proc in processes)

        if all_done:
            # Check return codes
            for proc in processes:
                if proc.returncode != 0:
                    return (False, f"Process exited with code {proc.returncode}")
            return (True, None)

        # Check timeout
        if time.time() - start_time > timeout:
            return (False, "timeout")

        # Sleep briefly before checking again
        time.sleep(0.5)


def run_experiment(config: ExperimentConfig, timeout: int = 1800) -> ExperimentResult:
    """
    Run a single experiment with Batsim and Batsched.

    Args:
        config: Experiment configuration
        timeout: Maximum execution time in seconds

    Returns:
        ExperimentResult with status, timing, and error info
    """
    start_time = datetime.now()

    # Create output directory
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Prepare log files
    batsim_log = output_dir / "batsim.log"
    batsched_log = output_dir / "batsched.log"

    batsim_proc = None
    batsched_proc = None

    try:
        # Construct Batsim command
        batsim_cmd = [
            "batsim",
            "-p", config.platform_path,
            "-w", config.workload_path,
            "--energy",
            "--environmental-footprint-dynamic", config.energy_trace_path,
            "-e", str(output_dir / "batsim_output"),
            "-s", "tcp://localhost:28000"
        ]

        # Start Batsim (ZMQ server)
        with open(batsim_log, "w") as log:
            batsim_proc = subprocess.Popen(
                batsim_cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True
            )

        # Wait for Batsim to initialize
        time.sleep(3)

        # Check if Batsim is still running
        if batsim_proc.poll() is not None:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            return ExperimentResult(
                config=config,
                status="failed",
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message="Batsim failed to start"
            )

        # Construct Batsched command
        batsched_cmd = [
            "batsched",
            "-v", config.algorithm,
            "-o", config.queue_order,
            "-s", "tcp://localhost:28000"
        ]

        # Add variant options for greenfilling
        if config.variant_options:
            batsched_cmd.extend(["--variant_options", config.variant_options])

        # Start Batsched (ZMQ client)
        with open(batsched_log, "w") as log:
            batsched_proc = subprocess.Popen(
                batsched_cmd,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True
            )

        # Check if Batsched started
        time.sleep(1)
        if batsched_proc.poll() is not None:
            cleanup_processes([batsim_proc])
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            return ExperimentResult(
                config=config,
                status="failed",
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration,
                error_message="Batsched failed to start"
            )

        # Wait for completion with timeout
        success, error_msg = wait_with_timeout([batsim_proc, batsched_proc], timeout)

        # Cleanup processes
        cleanup_processes([batsim_proc, batsched_proc])

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        if success:
            status = "success"
        elif error_msg == "timeout":
            status = "timeout"
        else:
            status = "failed"

        return ExperimentResult(
            config=config,
            status=status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            error_message=error_msg if not success else None
        )

    except Exception as e:
        # Handle unexpected errors
        if batsim_proc:
            cleanup_processes([batsim_proc])
        if batsched_proc:
            cleanup_processes([batsched_proc])

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        return ExperimentResult(
            config=config,
            status="failed",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            error_message=str(e)
        )


def write_manifest(results: List[ExperimentResult], output_file: Path):
    """
    Write experiment manifest CSV file.

    Args:
        results: List of ExperimentResult objects
        output_file: Path to experiments.csv
    """
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)

        # Write header
        writer.writerow([
            "id", "workload", "energy_grid", "algorithm",
            "queue_order", "variant_options", "output_dir", "status",
            "start_time", "end_time", "duration_seconds"
        ])

        # Write data rows
        for result in results:
            config = result.config
            writer.writerow([
                config.exp_id,
                config.workload_name,
                config.energy_trace_name,
                config.algorithm,
                config.queue_order,
                config.variant_options or "",
                Path(config.output_dir).name,  # Just the directory name (experiment_XXX)
                result.status,
                result.start_time.isoformat(),
                result.end_time.isoformat(),
                f"{result.duration_seconds:.2f}"
            ])


def print_progress(current: int, total: int, config: ExperimentConfig):
    """
    Print experiment progress information.

    Args:
        current: Current experiment number (1-indexed)
        total: Total number of experiments
        config: Current experiment configuration
    """
    print(f"\n[{current}/{total}] Running experiment_{config.exp_id:03d}: "
          f"{config.workload_name} + {config.energy_trace_name} + {config.algorithm} + {config.queue_order}")
    print(f"        Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def check_results_directory(results_dir: Path) -> str:
    """
    Check if results directory exists and prompt user for action.

    Args:
        results_dir: Path to results directory

    Returns:
        Action to take: "create", "delete", or "cancel"
    """
    if not results_dir.exists():
        results_dir.mkdir(parents=True)
        return "create"

    # Check if directory has contents
    if not any(results_dir.iterdir()):
        return "create"

    # Directory has contents, prompt user
    print(f"\nResults directory already exists: {results_dir}")
    print("Options:")
    print("  [d] Delete existing results and start fresh")
    print("  [c] Cancel execution")

    while True:
        choice = input("Choose an option [d/c]: ").strip().lower()
        if choice == "d":
            return "delete"
        elif choice == "c":
            return "cancel"
        else:
            print("Invalid choice. Please enter 'd' or 'c'.")


def print_summary(results: List[ExperimentResult]):
    """
    Print summary of all experiments.

    Args:
        results: List of all experiment results
    """
    print("\n" + "=" * 80)
    print("EXPERIMENT SUMMARY")
    print("=" * 80)

    total = len(results)
    successes = sum(1 for r in results if r.status == "success")
    failures = sum(1 for r in results if r.status == "failed")
    timeouts = sum(1 for r in results if r.status == "timeout")
    total_duration = sum(r.duration_seconds for r in results)

    print(f"Total experiments: {total}")
    print(f"Successes: {successes}")
    print(f"Failures: {failures}")
    print(f"Timeouts: {timeouts}")
    print(f"Total duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")

    if failures > 0 or timeouts > 0:
        print("\nFailed/Timed out experiments:")
        for result in results:
            if result.status != "success":
                print(f"  - experiment_{result.config.exp_id:03d}: {result.status}")
                if result.error_message:
                    print(f"    Error: {result.error_message}")

    print("=" * 80)


def main():
    """Main experiment runner execution."""

    # Setup
    base_dir = Path(__file__).parent.resolve()
    results_dir = base_dir / "results"

    print("=" * 80)
    print("BATSIM/BATSCHED EXPERIMENT RUNNER")
    print("=" * 80)
    print(f"Base directory: {base_dir}")
    print(f"Results directory: {results_dir}")

    # Check and prepare results directory
    action = check_results_directory(results_dir)
    if action == "cancel":
        print("\nExecution cancelled.")
        return
    elif action == "delete":
        shutil.rmtree(results_dir)
        results_dir.mkdir()
        print(f"\nDeleted existing results and created fresh directory.")

    # Generate experiment configurations
    print("\nGenerating experiment configurations...")
    configs = generate_experiment_configs(base_dir)
    print(f"Generated {len(configs)} experiment configurations.")

    # Validate input files exist
    print("\nValidating input files...")
    missing_files = []
    for config in configs:
        for path in [config.workload_path, config.platform_path, config.energy_trace_path]:
            if not Path(path).exists():
                missing_files.append(path)

    if missing_files:
        print("\nERROR: Missing input files:")
        for path in missing_files:
            print(f"  - {path}")
        return

    print("All input files validated.")

    # Run experiments sequentially
    print("\n" + "=" * 80)
    print("STARTING EXPERIMENTS")
    print("=" * 80)

    results = []

    for i, config in enumerate(configs, 1):
        print_progress(i, len(configs), config)

        result = run_experiment(config, timeout=1800)
        results.append(result)

        # Print result
        if result.status == "success":
            print(f"        ✓ Completed successfully in {result.duration_seconds:.1f}s")
        elif result.status == "timeout":
            print(f"        ✗ TIMEOUT after {result.duration_seconds:.1f}s")
        else:
            print(f"        ✗ FAILED: {result.error_message}")

        # Write updated manifest after each experiment
        write_manifest(results, results_dir / "experiments.csv")

    # Print summary
    print_summary(results)

    print(f"\nManifest written to: {results_dir / 'experiments.csv'}")
    print("\nAll experiments completed!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nExecution interrupted by user.")
        sys.exit(1)

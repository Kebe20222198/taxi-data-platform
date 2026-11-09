#!/usr/bin/env python3
"""
benchmarks/run_benchmarks.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Automated Benchmarking Harness for Apache Spark NYC Taxi Pipeline (Phase 5).

Key Capabilities:
1. Executes each optimization level (0 through 8) 3 times (N_RUNS = 3) to ensure statistical rigor.
2. Interrogates the Spark UI / Spark History Server REST API (http://localhost:4040 / http://spark-master:8080)
   to extract low-level physical execution metrics:
   - Total Job Duration (s)
   - Shuffle Volume (Shuffle Read & Shuffle Write bytes/MB)
   - Total Tasks count across all stages
   - Max Task Duration (longest running straggler task)
   - Memory Spill & Disk Spill (bytes/MB)
3. Computes the MEDIAN of the 3 runs for each metric.
4. Generates structured output files:
   - benchmarks/results/benchmark_summary.json
   - benchmarks/results/benchmark_summary.csv
   - benchmarks/results/benchmark_table.md (Markdown table for docs/performance.md)

Usage:
------
  python benchmarks/run_benchmarks.py --levels 0,1,2,3,4,5,6,7,8 --runs 3
  python benchmarks/run_benchmarks.py --levels 0,8 --runs 3 --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("benchmark_runner")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "benchmarks" / "results"

DEFAULT_SPARK_REST_URLS = [
    os.getenv("SPARK_UI_URL", "http://localhost:4040/api/v1"),
    os.getenv("SPARK_MASTER_WEBUI_URL", "http://localhost:8085/api/v1"),
    "http://spark-master:8080/api/v1",
]


# ==============================================================================
# Spark REST API Metrics Extractor
# ==============================================================================

class SparkMetricsCollector:
    """Collects low-level execution metrics from Spark REST API."""

    def __init__(self, base_urls: list[str] | None = None):
        self.base_urls = base_urls or DEFAULT_SPARK_REST_URLS
        self.active_base_url = None

    def _get(self, endpoint: str) -> Any | None:
        """Attempt GET request across configured Spark UI endpoints."""
        for base in self.base_urls:
            url = f"{base.rstrip('/')}/{endpoint.lstrip('/')}"
            try:
                resp = requests.get(url, timeout=3)
                if resp.status_code == 200:
                    self.active_base_url = base
                    return resp.json()
            except Exception:
                continue
        return None

    def fetch_latest_app_metrics(self) -> dict[str, Any]:
        """Fetch stages, tasks, shuffle, spill, and straggler duration for the last app."""
        apps = self._get("applications")
        if not apps or not isinstance(apps, list):
            return {}

        latest_app = apps[0]
        app_id = latest_app.get("id")

        stages = self._get(f"applications/{app_id}/stages") or []

        total_tasks = 0
        shuffle_read_bytes = 0
        shuffle_write_bytes = 0
        memory_spill_bytes = 0
        disk_spill_bytes = 0
        max_task_duration_ms = 0.0

        for stage in stages:
            total_tasks += stage.get("numTasks", 0)
            shuffle_read_bytes += stage.get("shuffleReadBytes", 0)
            shuffle_write_bytes += stage.get("shuffleWriteBytes", 0)
            memory_spill_bytes += stage.get("memoryBytesSpilled", 0)
            disk_spill_bytes += stage.get("diskBytesSpilled", 0)

            # Check tasks for stragglers
            stage_id = stage.get("stageId")
            attempt_id = stage.get("attemptId", 0)
            tasks = self._get(f"applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList?length=500") or []
            for t in tasks:
                dur = t.get("duration", 0)
                if dur > max_task_duration_ms:
                    max_task_duration_ms = dur

        return {
            "app_id": app_id,
            "total_tasks": total_tasks,
            "shuffle_read_mb": round(shuffle_read_bytes / (1024 * 1024), 2),
            "shuffle_write_mb": round(shuffle_write_bytes / (1024 * 1024), 2),
            "total_shuffle_mb": round((shuffle_read_bytes + shuffle_write_bytes) / (1024 * 1024), 2),
            "memory_spill_mb": round(memory_spill_bytes / (1024 * 1024), 2),
            "disk_spill_mb": round(disk_spill_bytes / (1024 * 1024), 2),
            "max_task_duration_s": round(max_task_duration_ms / 1000.0, 2),
        }


# ==============================================================================
# Benchmarking Execution Engine
# ==============================================================================

def execute_single_run(
    opt_level: int,
    run_idx: int,
    year: int = 2024,
    dry_run: bool = False,
    collector: SparkMetricsCollector | None = None,
) -> dict[str, Any]:
    """Execute a single run of benchmark_pipeline.py and capture telemetry."""
    log.info(f"▶ Executing Level {opt_level} [Run {run_idx}/3]...")
    output_json = RESULTS_DIR / f"run_lvl{opt_level}_run{run_idx}.json"
    output_json.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "spark_jobs" / "benchmark_pipeline.py"),
        "--opt-level", str(opt_level),
        "--year", str(year),
        "--output-json", str(output_json),
    ]
    if dry_run:
        cmd.append("--dry-run")

    t0 = time.monotonic()
    proc = subprocess.run(cmd, capture_output=True, text=True)
    duration_s = round(time.monotonic() - t0, 2)

    if proc.returncode != 0:
        log.error(f"❌ Run failed (exit {proc.returncode}):\n{proc.stderr}")
        return {
            "opt_level": opt_level,
            "run_idx": run_idx,
            "duration_s": duration_s,
            "status": "FAILED",
            "error": proc.stderr[-500:],
        }

    # Load telemetry file
    telemetry = {}
    if output_json.exists():
        try:
            with open(output_json, "r", encoding="utf-8") as f:
                telemetry = json.load(f)
        except Exception:
            pass

    # Fetch Spark REST UI stats
    spark_stats = {}
    if collector:
        spark_stats = collector.fetch_latest_app_metrics()

    result = {
        "opt_level": opt_level,
        "run_idx": run_idx,
        "duration_s": telemetry.get("total_duration_seconds", duration_s),
        "total_tasks": spark_stats.get("total_tasks", 0),
        "total_shuffle_mb": spark_stats.get("total_shuffle_mb", 0.0),
        "shuffle_read_mb": spark_stats.get("shuffle_read_mb", 0.0),
        "shuffle_write_mb": spark_stats.get("shuffle_write_mb", 0.0),
        "memory_spill_mb": spark_stats.get("memory_spill_mb", 0.0),
        "disk_spill_mb": spark_stats.get("disk_spill_mb", 0.0),
        "max_task_duration_s": spark_stats.get("max_task_duration_s", 0.0),
        "status": "SUCCESS",
    }
    log.info(
        f"  Completed Run {run_idx}: duration={result['duration_s']}s, "
        f"shuffle={result['total_shuffle_mb']}MB, tasks={result['total_tasks']}, "
        f"max_task={result['max_task_duration_s']}s"
    )
    return result


def evaluate_level(
    opt_level: int,
    runs: int = 3,
    year: int = 2024,
    dry_run: bool = False,
    collector: SparkMetricsCollector | None = None,
) -> dict[str, Any]:
    """Execute 'runs' iterations for one level and compute median metrics."""
    log.info("=" * 70)
    log.info(f"📊 Evaluating Optimization Level {opt_level} ({runs} iterations)...")
    log.info("=" * 70)

    run_results = []
    for r in range(1, runs + 1):
        res = execute_single_run(opt_level, r, year=year, dry_run=dry_run, collector=collector)
        if res.get("status") == "SUCCESS":
            run_results.append(res)
        # 2s cooldown between runs
        time.sleep(2)

    if not run_results:
        log.error(f"All runs failed for Level {opt_level}")
        return {"opt_level": opt_level, "status": "FAILED"}

    durations = [r["duration_s"] for r in run_results]
    shuffles = [r["total_shuffle_mb"] for r in run_results]
    tasks = [r["total_tasks"] for r in run_results]
    max_tasks = [r["max_task_duration_s"] for r in run_results]
    mem_spills = [r["memory_spill_mb"] for r in run_results]
    disk_spills = [r["disk_spill_mb"] for r in run_results]

    level_names = {
        0: "Level 0 : Naive Baseline",
        1: "Level 1 : + Explicit Schema (StructType)",
        2: "Level 2 : + Partition Pruning (year/month)",
        3: "Level 3 : + BroadcastHashJoin (dim_zone)",
        4: "Level 4 : + Tuned Shuffle (8 partitions)",
        5: "Level 5 : + Skew Handling (Key Salting)",
        6: "Level 6 : + Cache DataFrame (MEMORY_AND_DISK)",
        7: "Level 7 : + Compaction (coalesce/repartition)",
        8: "Level 8 : + Full AQE (Adaptive Execution)",
    }

    median_summary = {
        "opt_level": opt_level,
        "name": level_names.get(opt_level, f"Level {opt_level}"),
        "successful_runs": len(run_results),
        "median_duration_s": round(statistics.median(durations), 2),
        "min_duration_s": round(min(durations), 2),
        "max_duration_s": round(max(durations), 2),
        "median_shuffle_mb": round(statistics.median(shuffles), 2),
        "median_tasks": int(statistics.median(tasks)),
        "median_max_task_duration_s": round(statistics.median(max_tasks), 2),
        "median_memory_spill_mb": round(statistics.median(mem_spills), 2),
        "median_disk_spill_mb": round(statistics.median(disk_spills), 2),
        "status": "SUCCESS",
    }
    return median_summary


# ==============================================================================
# Reporting & Artifact Exporters
# ==============================================================================

def export_results(summaries: list[dict[str, Any]]) -> None:
    """Save results to JSON, CSV, and Markdown table formats."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    baseline = next((s for s in summaries if s["opt_level"] == 0 and s["status"] == "SUCCESS"), None)
    baseline_duration = baseline["median_duration_s"] if baseline else 1.0

    for s in summaries:
        if s["status"] == "SUCCESS":
            speedup = baseline_duration / s["median_duration_s"] if s["median_duration_s"] > 0 else 1.0
            reduction_pct = ((baseline_duration - s["median_duration_s"]) / baseline_duration) * 100.0 if baseline_duration > 0 else 0.0
            s["speedup_factor"] = round(speedup, 2)
            s["reduction_pct"] = round(reduction_pct, 1)

    # 1. JSON Export
    json_path = RESULTS_DIR / "benchmark_summary.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(summaries, f, indent=2)
    log.info(f"✅ JSON summary exported to: {json_path}")

    # 2. CSV Export
    csv_path = RESULTS_DIR / "benchmark_summary.csv"
    keys = [
        "opt_level", "name", "median_duration_s", "speedup_factor",
        "reduction_pct", "median_shuffle_mb", "median_tasks",
        "median_max_task_duration_s", "median_memory_spill_mb", "median_disk_spill_mb"
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summaries)
    log.info(f"✅ CSV summary exported to: {csv_path}")

    # 3. Markdown Table Export
    md_path = RESULTS_DIR / "benchmark_table.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("| Configuration | Durée Médiane (s) | Gain (%) | Speedup | Shuffle (Mo) | Tâches | Tâche Max (s) | Spill Disque (Mo) |\n")
        f.write("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |\n")
        for s in summaries:
            if s["status"] == "SUCCESS":
                f.write(
                    f"| **{s['name']}** | {s['median_duration_s']}s | "
                    f"-{s.get('reduction_pct', 0.0)}% | {s.get('speedup_factor', 1.0)}x | "
                    f"{s['median_shuffle_mb']} Mo | {s['median_tasks']} | "
                    f"{s['median_max_task_duration_s']}s | {s['median_disk_spill_mb']} Mo |\n"
                )
    log.info(f"✅ Markdown table exported to: {md_path}")


# ==============================================================================
# CLI Entrypoint
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Automated Spark Benchmarking Suite (Phase 5 Performance Study)",
    )
    parser.add_argument(
        "--levels",
        type=str,
        default="0,1,2,3,4,5,6,7,8",
        help="Comma-separated list of levels to benchmark (e.g. 0,1,2,3 or 0,8)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of iterations per optimization level to compute median (default: 3)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Target year for 12 months benchmark (default: 2024)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry-run mode executing count actions without writing to storage",
    )

    args = parser.parse_args()
    levels = [int(lvl.strip()) for lvl in args.levels.split(",")]

    log.info("=" * 78)
    log.info("🧪 STARTING APACHE SPARK BENCHMARK AUTOMATION HARNESS")
    log.info(f"   Levels to test: {levels}")
    log.info(f"   Repetitions:    {args.runs} runs per level (Median selection)")
    log.info(f"   Dataset:        12 months of Yellow Taxi ({args.year})")
    log.info("=" * 78)

    collector = SparkMetricsCollector()
    summaries = []

    for lvl in levels:
        summary = evaluate_level(lvl, runs=args.runs, year=args.year, dry_run=args.dry_run, collector=collector)
        summaries.append(summary)

    export_results(summaries)
    log.info("🎉 All benchmark iterations completed successfully!")


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
import math
import platform
import sqlite3
import sys

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import mongoeco

from mongoeco.core.json_compat import get_json_backend_name


REPORT_SCHEMA = "mongoeco-benchmark-report/v2"
_SOURCE_PATHS = (
    "benchmarks/data/generator.py",
    "benchmarks/engines/base.py",
    "benchmarks/report.py",
    "benchmarks/run.py",
    "benchmarks/runners/metrics.py",
    "benchmarks/runners/workloads.py",
)
_REQUIRED_METRICS = (
    "repetitions",
    "wall_time_mean_sec",
    "wall_time_median_sec",
    "wall_time_min_sec",
    "wall_time_max_sec",
    "cpu_user_mean_sec",
    "cpu_sys_mean_sec",
    "rss_delta_mean_mb",
    "rss_peak_max_mb",
)


class BenchmarkContractError(ValueError):
    """Raised when a benchmark artifact cannot support the requested claim."""


def _installed_version(distribution: str) -> str | None:
    try:
        return version(distribution)
    except PackageNotFoundError:
        return None


def _source_hash(project_root: Path, paths: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    for relative_path in paths:
        path = project_root / relative_path
        digest.update(relative_path.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


def build_report_document(
    *,
    results: dict[str, Any],
    size: int,
    warmup: int,
    repetitions: int,
    workload_names: tuple[str, ...],
    project_root: Path,
    git_revision: str | None,
    git_dirty: bool | None,
    rss_sampling_interval_ms: float,
) -> dict[str, Any]:
    return {
        "schema": REPORT_SCHEMA,
        "environment": {
            "python": platform.python_version(),
            "pythonImplementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "sqlite": sqlite3.sqlite_version,
            "jsonBackend": get_json_backend_name(),
            "mongoeco": mongoeco.__version__,
            "dependencies": {
                name: _installed_version(name)
                for name in ("mongomock", "orjson", "psutil", "pymongo", "usearch")
            },
            "executable": sys.executable,
        },
        "source": {
            "gitRevision": git_revision,
            "gitDirty": git_dirty,
            "mongoecoModule": str(Path(mongoeco.__file__).resolve()),
            "harnessSha256": _source_hash(project_root, _SOURCE_PATHS),
            "datasetSha256": _source_hash(
                project_root,
                ("benchmarks/data/generator.py",),
            ),
        },
        "config": {
            "size": size,
            "warmup": warmup,
            "repetitions": repetitions,
            "workloads": list(workload_names),
            "rssPeakSamplingIntervalMs": rss_sampling_interval_ms,
        },
        "results": results,
    }


def report_results(document: dict[str, Any]) -> dict[str, Any]:
    results = document.get("results")
    if document.get("schema") == REPORT_SCHEMA and isinstance(results, dict):
        return results
    return document


def validate_report(document: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if document.get("schema") != REPORT_SCHEMA:
        return [f"report schema must be {REPORT_SCHEMA}"]
    for section in ("environment", "source", "config", "results"):
        if not isinstance(document.get(section), dict):
            issues.append(f"report section {section!r} is missing or invalid")
    if issues:
        return issues

    config = document["config"]
    workloads = config.get("workloads")
    repetitions = config.get("repetitions")
    if not isinstance(config.get("size"), int) or config["size"] < 1:
        issues.append("config.size must be a positive integer")
    if not isinstance(config.get("warmup"), int) or config["warmup"] < 0:
        issues.append("config.warmup must be a non-negative integer")
    if (
        not isinstance(workloads, list)
        or not workloads
        or not all(isinstance(item, str) and item for item in workloads)
    ):
        issues.append("config.workloads must be a non-empty list of names")
    if not isinstance(repetitions, int) or repetitions < 1:
        issues.append("config.repetitions must be a positive integer")
    source = document["source"]
    if (
        not isinstance(source.get("mongoecoModule"), str)
        or not source["mongoecoModule"]
    ):
        issues.append("source.mongoecoModule must identify the imported package")
    for field in ("harnessSha256", "datasetSha256"):
        if not isinstance(source.get(field), str) or not source[field].startswith(
            "sha256:"
        ):
            issues.append(f"source.{field} must be a sha256 digest")
    results = document["results"]
    if not results:
        issues.append("results must contain at least one engine")
    for engine_name, engine_result in results.items():
        if not isinstance(engine_result, dict):
            issues.append(f"engine {engine_name!r} result is invalid")
            continue
        if "error" in engine_result:
            issues.append(f"engine {engine_name!r} failed: {engine_result['error']}")
            continue
        skipped = engine_result.get("_skipped_workloads", {})
        for workload_name in workloads if isinstance(workloads, list) else ():
            workload_result = engine_result.get(workload_name)
            if workload_result is None:
                if isinstance(skipped, dict) and workload_name in skipped:
                    continue
                issues.append(
                    f"engine {engine_name!r} is missing workload {workload_name!r}"
                )
                continue
            if not isinstance(workload_result, dict) or not workload_result:
                issues.append(
                    f"engine {engine_name!r} workload {workload_name!r} has no tasks"
                )
                continue
            for task_name, metrics in workload_result.items():
                location = f"{engine_name}/{workload_name}/{task_name}"
                issues.extend(
                    _validate_task_metrics(
                        metrics,
                        location=location,
                        repetitions=repetitions,
                    )
                )
    issues.extend(_validate_sync_async_outcomes(results, workloads))
    return issues


def _validate_sync_async_outcomes(
    results: dict[str, Any],
    workloads: object,
) -> list[str]:
    if not isinstance(workloads, list):
        return []
    issues = []
    for sync_name, async_name in (
        ("memory-sync", "memory-async"),
        ("sqlite-sync", "sqlite-async"),
    ):
        sync_result = results.get(sync_name)
        async_result = results.get(async_name)
        if not isinstance(sync_result, dict) or not isinstance(async_result, dict):
            continue
        for workload_name in workloads:
            sync_tasks = sync_result.get(workload_name)
            async_tasks = async_result.get(workload_name)
            if not isinstance(sync_tasks, dict) or not isinstance(async_tasks, dict):
                continue
            for task_name in sync_tasks.keys() & async_tasks.keys():
                sync_metadata = sync_tasks[task_name].get("metadata", {})
                async_metadata = async_tasks[task_name].get("metadata", {})
                if (
                    sync_metadata.get("outcome_oracle"),
                    sync_metadata.get("outcome_sha256"),
                ) != (
                    async_metadata.get("outcome_oracle"),
                    async_metadata.get("outcome_sha256"),
                ):
                    issues.append(
                        f"{sync_name}/{async_name}/{workload_name}/{task_name}: "
                        "sync and async outcomes differ"
                    )
    return issues


def _validate_task_metrics(
    metrics: object,
    *,
    location: str,
    repetitions: object,
) -> list[str]:
    if not isinstance(metrics, dict):
        return [f"{location}: metrics are invalid"]
    issues = []
    for field in _REQUIRED_METRICS:
        value = metrics.get(field)
        if not isinstance(value, int | float) or isinstance(value, bool):
            issues.append(f"{location}: metric {field!r} is missing or non-numeric")
            continue
        if not math.isfinite(float(value)):
            issues.append(f"{location}: metric {field!r} is not finite")
    if isinstance(repetitions, int) and metrics.get("repetitions") != repetitions:
        issues.append(f"{location}: repetition count does not match report config")
    wall_mean = metrics.get("wall_time_mean_sec")
    if isinstance(wall_mean, int | float) and wall_mean < 0:
        issues.append(f"{location}: wall time cannot be negative")
    metadata = metrics.get("metadata")
    if not isinstance(metadata, dict):
        issues.append(f"{location}: correctness metadata is missing")
    else:
        if metadata.get("outcome_oracle") not in {
            "exact-output",
            "ann-result-shape",
        }:
            issues.append(f"{location}: outcome oracle is missing or invalid")
        if not isinstance(metadata.get("outcome_sha256"), str) or not metadata[
            "outcome_sha256"
        ].startswith("sha256:"):
            issues.append(f"{location}: outcome SHA-256 is missing")
    return issues


def compare_reports(
    current: dict[str, Any],
    baseline: dict[str, Any],
    *,
    max_wall_regression_percent: float | None = None,
) -> list[str]:
    if max_wall_regression_percent is not None and max_wall_regression_percent < 0:
        return ["max wall regression percent must be non-negative"]
    issues = [
        *(f"current: {issue}" for issue in validate_report(current)),
        *(f"baseline: {issue}" for issue in validate_report(baseline)),
    ]
    if issues:
        return issues
    for path in (
        ("config", "size"),
        ("config", "warmup"),
        ("config", "repetitions"),
        ("config", "workloads"),
        ("config", "rssPeakSamplingIntervalMs"),
        ("source", "harnessSha256"),
        ("source", "datasetSha256"),
        ("environment", "pythonImplementation"),
        ("environment", "python"),
        ("environment", "platform"),
        ("environment", "machine"),
        ("environment", "jsonBackend"),
        ("environment", "sqlite"),
        ("environment", "dependencies"),
    ):
        current_value = current[path[0]].get(path[1])
        baseline_value = baseline[path[0]].get(path[1])
        if current_value != baseline_value:
            issues.append(
                f"incomparable {path[0]}.{path[1]}: "
                f"current={current_value!r}, baseline={baseline_value!r}"
            )
    current_results = current["results"]
    baseline_results = baseline["results"]
    if set(current_results) != set(baseline_results):
        issues.append("incomparable engine sets")
        return issues
    for engine_name, engine_result in current_results.items():
        baseline_engine = baseline_results[engine_name]
        for workload_name in current["config"]["workloads"]:
            current_skipped = workload_name in engine_result.get(
                "_skipped_workloads", {}
            )
            baseline_skipped = workload_name in baseline_engine.get(
                "_skipped_workloads", {}
            )
            if current_skipped != baseline_skipped:
                issues.append(
                    f"incomparable skip state for {engine_name}/{workload_name}"
                )
                continue
            if current_skipped:
                continue
            current_tasks = engine_result[workload_name]
            baseline_tasks = baseline_engine[workload_name]
            if set(current_tasks) != set(baseline_tasks):
                issues.append(
                    f"incomparable task sets for {engine_name}/{workload_name}"
                )
                continue
            for task_name, metrics in current_tasks.items():
                current_oracle = metrics["metadata"]["outcome_oracle"]
                baseline_oracle = baseline_tasks[task_name]["metadata"][
                    "outcome_oracle"
                ]
                if current_oracle != baseline_oracle:
                    issues.append(
                        f"{engine_name}/{workload_name}/{task_name}: "
                        "outcome oracle differs from baseline"
                    )
                current_outcome = metrics["metadata"]["outcome_sha256"]
                baseline_outcome = baseline_tasks[task_name]["metadata"][
                    "outcome_sha256"
                ]
                if current_outcome != baseline_outcome:
                    issues.append(
                        f"{engine_name}/{workload_name}/{task_name}: "
                        "outcome differs from baseline"
                    )
                if max_wall_regression_percent is None:
                    continue
                baseline_mean = float(baseline_tasks[task_name]["wall_time_mean_sec"])
                current_mean = float(metrics["wall_time_mean_sec"])
                if baseline_mean == 0:
                    issues.append(
                        f"{engine_name}/{workload_name}/{task_name}: "
                        "baseline wall time is zero"
                    )
                    continue
                regression = ((current_mean - baseline_mean) / baseline_mean) * 100
                if regression > max_wall_regression_percent:
                    issues.append(
                        f"{engine_name}/{workload_name}/{task_name}: wall regression "
                        f"{regression:.1f}% exceeds {max_wall_regression_percent:.1f}%"
                    )
    return issues


def require_valid_comparison(
    current: dict[str, Any],
    baseline: dict[str, Any],
    *,
    max_wall_regression_percent: float | None = None,
) -> None:
    issues = compare_reports(
        current,
        baseline,
        max_wall_regression_percent=max_wall_regression_percent,
    )
    if issues:
        raise BenchmarkContractError("; ".join(issues))

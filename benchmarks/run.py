import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

if __package__ in {None, ""}:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))

from benchmarks.engines.mongoeco_mem import MongoecoMemoryEngine
from benchmarks.engines.mongoeco_async import (
    MongoecoMemoryAsyncEngine,
    MongoecoSQLiteAsyncEngine,
)
from benchmarks.engines.mongoeco_sql import MongoecoSQLEngine
from benchmarks.runners.workloads import (
    cursor_consumption,
    filter_selectivity,
    materializing_aggregation,
    predicate_diagnostics,
    search_diagnostics,
    secondary_lookup_diagnostics,
    secondary_lookup_indexed,
    secondary_lookup_unindexed,
    simple_aggregation,
    sort_shape_diagnostics,
    sort_limit,
    vector_search_diagnostics,
)
from benchmarks.runners.metrics import summarize


def load_engine(name: str):
    if name == "memory":
        return MongoecoMemoryEngine()
    elif name == "memory-sync":
        return MongoecoMemoryEngine()
    elif name == "memory-async":
        return MongoecoMemoryAsyncEngine()
    elif name == "sqlite":
        return MongoecoSQLEngine()
    elif name == "sqlite-sync":
        return MongoecoSQLEngine()
    elif name == "sqlite-async":
        return MongoecoSQLiteAsyncEngine()
    elif name == "mongomock":
        from benchmarks.engines.mongomock_adapter import MongomockEngine
        return MongomockEngine()
    else:
        raise ValueError(f"Unknown engine: {name}")


WORKLOADS = {
    "secondary_lookup_indexed": secondary_lookup_indexed,
    "secondary_lookup_unindexed": secondary_lookup_unindexed,
    "secondary_lookup_diagnostics": secondary_lookup_diagnostics,
    "simple_aggregation": simple_aggregation,
    "materializing_aggregation": materializing_aggregation,
    "sort_limit": sort_limit,
    "cursor_consumption": cursor_consumption,
    "filter_selectivity": filter_selectivity,
    "predicate_diagnostics": predicate_diagnostics,
    "sort_shape_diagnostics": sort_shape_diagnostics,
    "search_diagnostics": search_diagnostics,
    "vector_search_diagnostics": vector_search_diagnostics,
}

WORKLOAD_ORDER = (
    "secondary_lookup_indexed",
    "secondary_lookup_unindexed",
    "secondary_lookup_diagnostics",
    "simple_aggregation",
    "materializing_aggregation",
    "sort_limit",
    "cursor_consumption",
    "filter_selectivity",
    "predicate_diagnostics",
    "sort_shape_diagnostics",
)


def resolve_workload_names(selected: list[str] | None) -> tuple[str, ...]:
    if not selected:
        return WORKLOAD_ORDER
    seen: set[str] = set()
    resolved: list[str] = []
    for name in selected:
        if name in seen:
            continue
        if name not in WORKLOADS:
            raise ValueError(f"Unknown workload: {name}")
        seen.add(name)
        resolved.append(name)
    return tuple(resolved)


def run_benchmarks(
    *,
    engine: str,
    size: int,
    warmup: int,
    repetitions: int,
    workload_names: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    selected_workloads = workload_names or WORKLOAD_ORDER
    engines_to_run = (
        ["memory-sync", "sqlite-sync", "memory-async", "sqlite-async", "mongomock"]
        if engine == "all"
        else [engine]
    )
    results: dict[str, Any] = {}
    for engine_name in engines_to_run:
        print(f"Running {engine_name}...", file=sys.stderr)
        try:
            results[engine_name] = _run_engine_workloads(
                engine_name=engine_name,
                size=size,
                warmup=warmup,
                repetitions=repetitions,
                workload_names=selected_workloads,
            )
        except Exception as exc:
            print(f"Engine {engine_name} failed: {exc}", file=sys.stderr)
            results[engine_name] = {"error": str(exc)}
    return results


def _run_engine_workloads(
    engine_name: str,
    size: int,
    warmup: int,
    repetitions: int,
    *,
    workload_names: tuple[str, ...],
) -> dict[str, Any]:
    engine = load_engine(engine_name)
    results: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for workload_name in workload_names:
        workload_fn = WORKLOADS[workload_name]
        task_samples: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for _ in range(warmup):
            workload_fn(engine, size)
        for _ in range(repetitions):
            measured = workload_fn(engine, size)
            for task_name, metrics in measured.items():
                task_samples[task_name].append(metrics)
        results[workload_name] = {
            task_name: _summarize_task_samples(samples)
            for task_name, samples in task_samples.items()
        }
    return results


def _summarize_task_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    from benchmarks.runners.metrics import Metrics

    metadata = samples[0].get("metadata") if samples else None
    metrics = [
        Metrics(
            wall_time_sec=float(sample["wall_time_sec"]),
            cpu_user_sec=float(sample["cpu_user_sec"]),
            cpu_sys_sec=float(sample["cpu_sys_sec"]),
            rss_delta_bytes=int(round(float(sample["rss_delta_mb"]) * 1024 * 1024)),
            rss_peak_bytes=int(round(float(sample["rss_peak_mb"]) * 1024 * 1024)),
        )
        for sample in samples
    ]
    result = summarize(metrics).to_dict()
    if metadata is not None:
        result["metadata"] = metadata
    return result


def _render_table(results: dict[str, Any], size: int, workload_names: tuple[str, ...]) -> None:
    for workload in workload_names:
        print(f"\n--- Workload: {workload.upper()} ({size} docs) ---")
        subtasks: list[str] = []
        for engine_result in results.values():
            if "error" in engine_result:
                continue
            for task in engine_result[workload].keys():
                if task not in subtasks:
                    subtasks.append(task)
        for task in subtasks:
            print(f"\nTask: {task}")
            headers = [
                "Engine",
                "Runs",
                "Wall Mean (s)",
                "Wall Median (s)",
                "Wall Min (s)",
                "Wall Max (s)",
                "CPU User Mean (s)",
                "Plan",
                "RSS Delta Mean (MB)",
                "RSS Peak Max (MB)",
            ]
            table = []
            for engine_name, engine_result in results.items():
                if "error" in engine_result:
                    table.append([engine_name, "ERROR", "-", "-", "-", "-", "-", "-", "-", "-"])
                    continue
                metrics = engine_result[workload].get(task)
                if metrics:
                    metadata = metrics.get("metadata")
                    plan_summary = "-"
                    if isinstance(metadata, dict):
                        plan_summary = str(metadata.get("summary", "-"))
                    table.append([
                        engine_name,
                        metrics["repetitions"],
                        metrics["wall_time_mean_sec"],
                        metrics["wall_time_median_sec"],
                        metrics["wall_time_min_sec"],
                        metrics["wall_time_max_sec"],
                        metrics["cpu_user_mean_sec"],
                        plan_summary,
                        metrics["rss_delta_mean_mb"],
                        metrics["rss_peak_max_mb"],
                    ])
            print(tabulate(table, headers=headers, floatfmt=".4f"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Mongoeco2 Benchmark Suite")
    parser.add_argument(
        "--engine",
        choices=[
            "memory",
            "memory-sync",
            "memory-async",
            "sqlite",
            "sqlite-sync",
            "sqlite-async",
            "mongomock",
            "all",
        ],
        default="all",
    )
    parser.add_argument("--size", type=int, default=10000, help="Number of documents to generate")
    parser.add_argument("--format", choices=["json", "table"], default="table")
    parser.add_argument("--warmup", type=int, default=1, help="Number of warmup runs per workload.")
    parser.add_argument("--repetitions", type=int, default=5, help="Number of measured runs per workload.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument(
        "--workload",
        action="append",
        choices=sorted(WORKLOADS.keys()),
        help="Run only the selected workload(s). Can be passed multiple times.",
    )

    args = parser.parse_args()
    workload_names = resolve_workload_names(args.workload)
    results = run_benchmarks(
        engine=args.engine,
        size=args.size,
        warmup=args.warmup,
        repetitions=args.repetitions,
        workload_names=workload_names,
    )

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")

    if args.format == "json":
        print(json.dumps(results, indent=2))
        return 0

    if not HAS_TABULATE:
        print("tabulate is not installed. Falling back to JSON.")
        print(json.dumps(results, indent=2))
        return 0

    _render_table(results, args.size, workload_names)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

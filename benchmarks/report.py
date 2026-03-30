import argparse
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from mongoeco.core.json_compat import get_json_backend_name

from benchmarks.run import WORKLOAD_ORDER, run_benchmarks, resolve_workload_names


def _git_revision(project_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _git_is_dirty(project_root: Path) -> bool | None:
    try:
        result = subprocess.run(
            ["git", "diff", "--quiet"],
            cwd=project_root,
            capture_output=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return result.returncode == 1


def _optional_dependency_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _pct_change(current: float, baseline: float) -> str:
    if baseline == 0:
        return "n/a"
    delta = ((current - baseline) / baseline) * 100
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.1f}%"


def _baseline_lookup(
    baseline: dict[str, Any] | None,
    engine_name: str,
    workload_name: str,
    task_name: str,
) -> dict[str, Any] | None:
    if baseline is None:
        return None
    engine_data = baseline.get(engine_name)
    if not isinstance(engine_data, dict):
        return None
    workload_data = engine_data.get(workload_name)
    if not isinstance(workload_data, dict):
        return None
    task_data = workload_data.get(task_name)
    if not isinstance(task_data, dict):
        return None
    return task_data


def render_markdown_report(
    *,
    results: dict[str, Any],
    size: int,
    warmup: int,
    repetitions: int,
    baseline: dict[str, Any] | None = None,
    project_root: Path | None = None,
    workload_names: tuple[str, ...] = WORKLOAD_ORDER,
) -> str:
    project_root = project_root or Path.cwd()
    revision = _git_revision(project_root)
    dirty = _git_is_dirty(project_root)
    psutil_version = _optional_dependency_version("psutil")
    mongomock_version = _optional_dependency_version("mongomock")
    orjson_version = _optional_dependency_version("orjson")
    lines = [
        "# Benchmark Report",
        "",
        "## Environment",
        "",
        f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
        f"- Python: {sys.version.split()[0]}",
        f"- Platform: {platform.platform()}",
        f"- Dataset size: {size}",
        f"- Warmup runs: {warmup}",
        f"- Measured repetitions: {repetitions}",
        f"- Workloads: {', '.join(workload_names)}",
        f"- JSON backend: {get_json_backend_name()}",
        f"- MONGOECO_JSON_BACKEND: {os.getenv('MONGOECO_JSON_BACKEND', 'unset')}",
    ]
    if revision:
        lines.append(f"- Git revision: `{revision}`")
    if dirty is not None:
        lines.append(f"- Git worktree dirty: `{dirty}`")
    lines.append(f"- psutil: `{psutil_version or 'not installed'}`")
    lines.append(f"- mongomock: `{mongomock_version or 'not installed'}`")
    lines.append(f"- orjson: `{orjson_version or 'not installed'}`")
    lines.append("")
    for workload_name in workload_names:
        lines.append(f"## {workload_name}")
        lines.append("")
        task_names: list[str] = []
        for engine_result in results.values():
            if isinstance(engine_result, dict) and "error" not in engine_result:
                workload_data = engine_result.get(workload_name, {})
                for task_name in workload_data:
                    if task_name not in task_names:
                        task_names.append(task_name)
        for task_name in task_names:
            lines.append(f"### {task_name}")
            lines.append("")
            metadata_notes: list[str] = []
            if baseline is None:
                lines.append(
                    "| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |"
                )
                lines.append(
                    "| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |"
                )
            else:
                lines.append(
                    "| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |"
                )
                lines.append(
                    "| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |"
                )
            for engine_name, engine_result in results.items():
                if "error" in engine_result:
                    if baseline is None:
                        lines.append(f"| {engine_name} | ERROR | - | - | - | - | - | - |")
                    else:
                        lines.append(f"| {engine_name} | ERROR | - | - | - | - | - | - |")
                    continue
                metrics = engine_result[workload_name][task_name]
                metadata = metrics.get("metadata")
                plan_summary = "-"
                if isinstance(metadata, dict):
                    plan_summary = str(metadata.get("summary", "-"))
                if baseline is None:
                    lines.append(
                        f"| {engine_name} | {metrics['repetitions']} | "
                        f"{metrics['wall_time_mean_sec']:.4f} | "
                        f"{metrics['wall_time_median_sec']:.4f} | "
                        f"{metrics['cpu_user_mean_sec']:.4f} | "
                        f"{plan_summary} | "
                        f"{metrics['rss_delta_mean_mb']:.2f} | "
                        f"{metrics['rss_peak_max_mb']:.2f} |"
                    )
                    if isinstance(metadata, dict):
                        for key in ("query_shape", "pipeline_shape", "consumption_mode", "selectivity", "streaming_batch_execution", "remaining_stage_count", "planning_mode"):
                            if key in metadata:
                                metadata_notes.append(f"- `{engine_name}` `{key}`: `{metadata[key]}`")
                    continue
                baseline_metrics = _baseline_lookup(
                    baseline,
                    engine_name,
                    workload_name,
                    task_name,
                )
                delta = "-"
                if baseline_metrics is not None:
                    delta = _pct_change(
                        float(metrics["wall_time_mean_sec"]),
                        float(baseline_metrics["wall_time_mean_sec"]),
                    )
                lines.append(
                    f"| {engine_name} | {metrics['repetitions']} | "
                    f"{metrics['wall_time_mean_sec']:.4f} | "
                    f"{delta} | "
                    f"{metrics['cpu_user_mean_sec']:.4f} | "
                    f"{plan_summary} | "
                    f"{metrics['rss_delta_mean_mb']:.2f} | "
                    f"{metrics['rss_peak_max_mb']:.2f} |"
                )
                if isinstance(metadata, dict):
                    for key in ("query_shape", "pipeline_shape", "consumption_mode", "selectivity", "streaming_batch_execution", "remaining_stage_count", "planning_mode"):
                        if key in metadata:
                            metadata_notes.append(f"- `{engine_name}` `{key}`: `{metadata[key]}`")
            if metadata_notes:
                lines.append("")
                lines.extend(metadata_notes)
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run benchmarks and generate presentable reports."
    )
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
    parser.add_argument("--size", type=int, default=10000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    parser.add_argument(
        "--baseline-json",
        type=Path,
        help="Optional baseline JSON generated by a previous benchmark run.",
    )
    parser.add_argument(
        "--workload",
        action="append",
        choices=list(WORKLOAD_ORDER),
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
    baseline = _load_json(args.baseline_json)

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(results, indent=2) + "\n",
            encoding="utf-8",
        )
    else:
        print(json.dumps(results, indent=2))

    markdown = render_markdown_report(
        results=results,
        size=args.size,
        warmup=args.warmup,
        repetitions=args.repetitions,
        baseline=baseline,
        project_root=Path(__file__).resolve().parents[1],
        workload_names=workload_names,
    )
    if args.output_markdown is not None:
        args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.output_markdown.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

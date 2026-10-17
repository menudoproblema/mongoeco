"""Compare installed artifacts with fresh processes; allocation tracing is separate."""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import time
import tracemalloc

from functools import partial
from pathlib import Path


def measured(call):
    start = time.perf_counter_ns()
    call()
    elapsed = time.perf_counter_ns() - start
    tracemalloc.start()
    call()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"elapsed_ns": elapsed, "python_peak_bytes": peak}


async def measured_async(call):
    start = time.perf_counter_ns()
    await call()
    elapsed = time.perf_counter_ns() - start
    tracemalloc.start()
    await call()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {"elapsed_ns": elapsed, "python_peak_bytes": peak}


def worker():
    import mongoeco  # noqa: PLC0415

    from mongoeco import AsyncMongoClient, MongoClient  # noqa: PLC0415
    from mongoeco.engines import MemoryEngine, SQLiteEngine  # noqa: PLC0415

    if "site-packages" not in Path(mongoeco.__file__).parts:
        message = "benchmark requires an installed artifact"
        raise RuntimeError(message)
    rows = {}
    for engine_name, factory in (("memory", MemoryEngine), ("sqlite", SQLiteEngine)):
        for size in (1, 100, 1000):
            documents = [{"_id": i, "group": "a", "counter": 0} for i in range(size)]
            with MongoClient(factory()) as client:
                collection = client.benchmark.items
                collection.insert_many(documents)
                collection.update_many({}, {"$inc": {"counter": 1}})
                operations = {
                    "find": partial(collection.find_one, {"_id": size - 1}),
                    "count": partial(collection.count_documents, {"group": "a"}),
                    "update": partial(
                        collection.update_many, {"group": "a"}, {"$inc": {"counter": 1}}
                    ),
                }
                for name, call in operations.items():
                    call()
                    rows[f"{engine_name}/sync/{size}/{name}"] = measured(call)
                if collection.count_documents({"counter": 4}) != size:
                    message = "benchmark mutation effects diverged"
                    raise AssertionError(message)

            async def run_async(
                factory=factory, size=size, engine_name=engine_name, documents=documents
            ):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.benchmark.items
                    await collection.insert_many(documents)
                    await collection.update_many({}, {"$inc": {"counter": 1}})
                    operations = {
                        "find": partial(collection.find_one, {"_id": size - 1}),
                        "count": partial(collection.count_documents, {"group": "a"}),
                        "update": partial(
                            collection.update_many,
                            {"group": "a"},
                            {"$inc": {"counter": 1}},
                        ),
                    }
                    for name, call in operations.items():
                        await call()
                        rows[
                            f"{engine_name}/async/{size}/{name}"
                        ] = await measured_async(call)
                    if await collection.count_documents({"counter": 4}) != size:
                        message = "benchmark mutation effects diverged"
                        raise AssertionError(message)

            asyncio.run(run_async())
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--baseline-python", type=Path)
    parser.add_argument("--candidate-python", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.worker:
        sys.stdout.write(json.dumps(worker()) + "\n")
        return
    samples = {"baseline": [], "candidate": []}
    for sample in range(7):
        order = (
            ("baseline", "candidate") if sample % 2 == 0 else ("candidate", "baseline")
        )
        for label in order:
            python = (
                args.baseline_python if label == "baseline" else args.candidate_python
            )
            result = subprocess.run(  # noqa: S603 - explicit interpreter arguments
                [str(python), str(Path(__file__).resolve()), "--worker"],
                capture_output=True,
                text=True,
                check=True,
            )
            samples[label].append(json.loads(result.stdout))
    summary = {}
    for key in samples["baseline"][0]:
        item = {}
        for label, artifact_samples in samples.items():
            item[label] = {}
            for metric in ("elapsed_ns", "python_peak_bytes"):
                values = [sample[key][metric] for sample in artifact_samples]
                quartiles = statistics.quantiles(values, n=4)
                item[label][metric] = {
                    "median": statistics.median(values),
                    "iqr": quartiles[2] - quartiles[0],
                    "values": values,
                }
        item["time_ratio"] = (
            item["candidate"]["elapsed_ns"]["median"]
            / item["baseline"]["elapsed_ns"]["median"]
        )
        summary[key] = item
    args.output.write_text(
        json.dumps(
            {
                "fresh_processes_per_artifact": 7,
                "timing": "perf_counter_ns; tracing disabled during timing",
                "memory": "separate tracemalloc peak Python allocations; not RSS",
                "samples": samples,
                "summary": summary,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()

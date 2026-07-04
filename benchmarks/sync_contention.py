from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Callable

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


@dataclass
class BenchClient:
    client: MongoClient
    cleanup: Callable[[], None]


def _documents(size: int) -> list[dict[str, Any]]:
    return [
        {'_id': index, 'value': index, 'kind': f'k{index % 7}'}
        for index in range(size)
    ]


def _memory_client() -> BenchClient:
    return BenchClient(MongoClient(engine=MemoryEngine()), lambda: None)


def _sqlite_client() -> BenchClient:
    descriptor, raw_path = tempfile.mkstemp(suffix=".sqlite")
    os.close(descriptor)
    path = Path(raw_path)

    def _cleanup() -> None:
        path.unlink(missing_ok=True)

    return BenchClient(
        MongoClient(engine=SQLiteEngine(path=str(path))),
        _cleanup,
    )


def _build_client(engine: str) -> BenchClient:
    if engine == 'memory':
        return _memory_client()
    if engine == 'sqlite':
        return _sqlite_client()
    msg = f"Unsupported engine: {engine}"
    raise ValueError(msg)


def _seed(
    client: MongoClient,
    *,
    worker: int,
    documents: list[dict[str, Any]],
) -> None:
    collection = client['bench'][f'items_{worker}']
    collection.insert_many(documents)


def _run_worker(
    client: MongoClient,
    *,
    worker: int,
    ops: int,
    size: int,
) -> list[float]:
    collection = client['bench'][f'items_{worker}']
    latencies: list[float] = []
    for index in range(ops):
        started_at = time.perf_counter()
        document = collection.find_one({'_id': index % size})
        if document is None:
            msg = 'seeded document was not found'
            raise AssertionError(msg)
        latencies.append(time.perf_counter() - started_at)
    return latencies


def _flatten(latency_groups: list[list[float]]) -> list[float]:
    return [latency for group in latency_groups for latency in group]


def _run_shared(
    engine: str,
    *,
    workers: int,
    ops: int,
    size: int,
) -> dict[str, float]:
    bench_client = _build_client(engine)
    try:
        documents = _documents(size)
        for worker in range(workers):
            _seed(bench_client.client, worker=worker, documents=documents)
        started_at = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            latency_groups = list(
                executor.map(
                    lambda worker: _run_worker(
                        bench_client.client,
                        worker=worker,
                        ops=ops,
                        size=size,
                    ),
                    range(workers),
                ),
            )
        elapsed = time.perf_counter() - started_at
    finally:
        bench_client.client.close()
        bench_client.cleanup()
    return _summarize(
        elapsed,
        _flatten(latency_groups),
        workers=workers,
        ops=ops,
    )


def _run_per_worker(
    engine: str,
    *,
    workers: int,
    ops: int,
    size: int,
) -> dict[str, float]:
    bench_clients = [_build_client(engine) for _ in range(workers)]
    try:
        documents = _documents(size)
        for worker, bench_client in enumerate(bench_clients):
            _seed(bench_client.client, worker=worker, documents=documents)
        started_at = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as executor:
            latency_groups = list(
                executor.map(
                    lambda item: _run_worker(
                        item[1].client,
                        worker=item[0],
                        ops=ops,
                        size=size,
                    ),
                    enumerate(bench_clients),
                ),
            )
        elapsed = time.perf_counter() - started_at
    finally:
        for bench_client in bench_clients:
            bench_client.client.close()
            bench_client.cleanup()
    return _summarize(
        elapsed,
        _flatten(latency_groups),
        workers=workers,
        ops=ops,
    )


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * percentile))
    return ordered[index]


def _summarize(
    elapsed: float,
    latencies: list[float],
    *,
    workers: int,
    ops: int,
) -> dict[str, float]:
    total_ops = workers * ops
    mean_latency = (
        statistics.fmean(latencies) * 1_000_000
        if latencies
        else 0.0
    )
    return {
        'seconds': elapsed,
        'ops_per_second': total_ops / elapsed if elapsed else 0.0,
        'latency_p50_micros': _percentile(latencies, 0.50) * 1_000_000,
        'latency_p95_micros': _percentile(latencies, 0.95) * 1_000_000,
        'latency_mean_micros': mean_latency,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Measure sync client contention.',
    )
    parser.add_argument(
        '--engine',
        choices=['memory', 'sqlite', 'all'],
        default='all',
    )
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--ops-per-worker', type=int, default=500)
    parser.add_argument('--documents', type=int, default=1000)
    args = parser.parse_args()

    engines = ['memory', 'sqlite'] if args.engine == 'all' else [args.engine]
    report: dict[str, object] = {
        'workers': args.workers,
        'ops_per_worker': args.ops_per_worker,
        'documents': args.documents,
        'engines': {},
    }
    for engine in engines:
        report['engines'][engine] = {
            'shared_client': _run_shared(
                engine,
                workers=args.workers,
                ops=args.ops_per_worker,
                size=args.documents,
            ),
            'client_per_worker': _run_per_worker(
                engine,
                workers=args.workers,
                ops=args.ops_per_worker,
                size=args.documents,
            ),
        }
    sys.stdout.write(json.dumps(report, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()

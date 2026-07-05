#!/usr/bin/env python3
from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import statistics
import sys
import time
from pathlib import Path
from typing import Callable


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'src'
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mongoeco import MongoClient
from mongoeco.api.operations import compile_update_operation
from mongoeco.engines.memory import MemoryEngine


COLLECTION_SIZE = 2_000
FIND_ITERATIONS = 5_000
UPDATE_ITERATIONS = 2_000
COMPILE_ITERATIONS = 20_000
REPEATS = 3

UPDATE_SHAPE = {
    '$set': {
        'meta.last_seen': 0,
        'meta.flags.hot': True,
    },
}

SUSPENDING_PATHS = (
    'change streams: AsyncChangeStreamCursor uses asyncio.to_thread',
    'admin failpoints: failCommand blockTimeMS uses asyncio.sleep',
    'search/index latency simulation: MemoryEngine can delay readiness',
    'SQLite: blocking work is routed through run_in_executor',
    'cursor/aggregate iteration: materialization can cross async boundaries',
)


def _document(index: int) -> dict[str, object]:
    return {
        '_id': index,
        'kind': f'kind-{index % 7}',
        'tags': [f'tag-{index % 5}', 'sync', 'hot'],
        'meta': {
            'owner': f'user-{index % 17}',
            'score': index * 3,
            'last_seen': None,
            'flags': {
                'hot': False,
                'archived': index % 11 == 0,
            },
        },
        'history': [
            {
                'at': step,
                'state': 'new' if step == 0 else 'seen',
                'score': index + step,
            }
            for step in range(4)
        ],
    }


def _seed_collection() -> tuple[MongoClient, object]:
    client = MongoClient(MemoryEngine())
    collection = client.bench.hot
    collection.insert_many(
        [_document(index) for index in range(COLLECTION_SIZE)]
    )
    return client, collection


def _time_per_op(callback: Callable[[], None], iterations: int) -> float:
    start = time.perf_counter()
    for _ in range(iterations):
        callback()
    elapsed = time.perf_counter() - start
    return elapsed / iterations


def bench_find_one() -> float:
    client, collection = _seed_collection()
    try:
        cursor = 0

        def _run() -> None:
            nonlocal cursor
            document = collection.find_one({'_id': cursor % COLLECTION_SIZE})
            if document is None:
                raise AssertionError('document not found')
            cursor += 1

        return _time_per_op(_run, FIND_ITERATIONS)
    finally:
        client.close()


def bench_update_one() -> float:
    client, collection = _seed_collection()
    try:
        cursor = 0

        def _run() -> None:
            nonlocal cursor
            target = cursor % COLLECTION_SIZE
            result = collection.update_one(
                {'_id': target},
                {
                    '$set': {
                        'meta.last_seen': cursor,
                        'meta.flags.hot': True,
                    },
                },
            )
            if result.matched_count != 1:
                raise AssertionError(
                    'update did not match exactly one document'
                )
            cursor += 1

        return _time_per_op(_run, UPDATE_ITERATIONS)
    finally:
        client.close()


def bench_compile_update_operation() -> float:
    cursor = 0

    def _run() -> None:
        nonlocal cursor
        compile_update_operation(
            {'_id': cursor % COLLECTION_SIZE},
            update_spec={
                '$set': {
                    'meta.last_seen': cursor,
                    'meta.flags.hot': True,
                },
            },
        )
        cursor += 1

    return _time_per_op(_run, COMPILE_ITERATIONS)


def _format_us(seconds: float) -> str:
    return f'{seconds * 1_000_000:.1f}us/op'


def _run_repeated(name: str, callback: Callable[[], float]) -> None:
    samples = [callback() for _ in range(REPEATS)]
    best = min(samples)
    rendered = ', '.join(_format_us(sample) for sample in samples)
    mean = statistics.mean(samples)
    print(
        f'{name}: best={_format_us(best)} mean={_format_us(mean)} [{rendered}]'
    )


def _profile_update_one() -> None:
    profiler = cProfile.Profile()
    profiler.enable()
    sample = bench_update_one()
    profiler.disable()

    stream = io.StringIO()
    stats = (
        pstats.Stats(profiler, stream=stream)
        .strip_dirs()
        .sort_stats('cumtime')
    )
    stats.print_stats(30)
    print(stream.getvalue())
    print(f'profiled update_one: {_format_us(sample)}')

    for func, stat in stats.stats.items():
        filename, line_number, function_name = func
        if function_name != '_decode_codec_payload':
            continue
        primitive_calls, total_calls, total_time, cumulative_time, _callers = (
            stat
        )
        per_op = cumulative_time / UPDATE_ITERATIONS
        print(
            '_decode_codec_payload: '
            f'calls={total_calls} primitive={primitive_calls} '
            f'cum={cumulative_time:.6f}s total={total_time:.6f}s '
            f'per_op={_format_us(per_op)} at {filename}:{line_number}'
        )


def _print_inventory() -> None:
    print('Suspending paths kept out of sync inline fast-path:')
    for path in SUSPENDING_PATHS:
        print(f'- {path}')


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', action='store_true')
    parser.add_argument('--inventory', action='store_true')
    args = parser.parse_args()

    if args.inventory:
        _print_inventory()
    if args.profile:
        _profile_update_one()
        return 0

    _run_repeated('find_one({_id})', bench_find_one)
    _run_repeated('update_one({_id}, $set meta.*)', bench_update_one)
    _run_repeated(
        'compile_update_operation($set meta.*)', bench_compile_update_operation
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

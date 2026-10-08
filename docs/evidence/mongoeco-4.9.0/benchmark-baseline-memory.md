# Benchmark Report

## Environment

- Generated at: 2026-10-08T13:57:24.810734+00:00
- Python: 3.14.5
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 20000
- Warmup runs: 1
- Measured repetitions: 5
- Workloads: simple_aggregation, materializing_aggregation, aggregation_spill_diagnostics, secondary_lookup_indexed, cursor_consumption
- JSON backend: stdlib
- MONGOECO_JSON_BACKEND: unset
- Git revision: `aa626c51fea8a055510f89a2acf2f8ed1ce33761`
- Git worktree dirty: `True`
- Report schema: `mongoeco-benchmark-report/v2`
- Harness SHA-256: `sha256:a46f2e9c5ecbf5811289439caba8210f62a24928d1a5ad295da6a7337e8b3de3`
- Dataset SHA-256: `sha256:c2b95c2ef946b1eee07d3311fedac30f6712de6ff7668d0f095f4ba705f4df63`
- RSS peak sampling interval: `5.0 ms`
- psutil: `7.2.2`
- mongomock: `4.3.0`
- orjson: `3.11.9`

## simple_aggregation

### simple_aggregation_topk

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.0740 | 0.0692 | 0.0720 | memory/python scan>filter>sort>project>slice | 5.47 | 167.89 |

- `memory-sync` `pipeline_shape`: `match-sort-limit-project`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`

## materializing_aggregation

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.4981 | 0.4916 | 0.4960 | memory/python scan>filter | 0.20 | 183.34 |

- `memory-sync` `pipeline_shape`: `match-group-sort-limit`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `3`
- `memory-sync` `planning_mode`: `strict`

## aggregation_spill_diagnostics

### group_low_cardinality_first

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.2816 | 0.2710 | 0.2800 | memory/python scan>filter | 0.12 | 189.33 |

- `memory-sync` `pipeline_shape`: `group-low-cardinality-first`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`

### group_high_cardinality_first

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 1.9394 | 1.9386 | 1.8260 | memory/python scan>filter | 1.03 | 189.47 |

- `memory-sync` `pipeline_shape`: `group-high-cardinality-first`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`

## secondary_lookup_indexed

### secondary_lookup_indexed_1k

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.2169 | 0.2019 | 0.1800 | memory/python scan>filter>slice | 0.00 | 214.91 |

- `memory-sync` `query_shape`: `username equality`
- `memory-sync` `planning_mode`: `strict`

## cursor_consumption

### cursor_consumption_first_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.0384 | 0.0375 | 0.0320 | memory/python scan>filter | -1.60 | 410.92 |

- `memory-sync` `query_shape`: `city equality`
- `memory-sync` `consumption_mode`: `first`
- `memory-sync` `planning_mode`: `strict`

### cursor_consumption_materialized_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 5.9968 | 5.9322 | 5.7520 | memory/python scan>filter | 29.82 | 423.91 |

- `memory-sync` `query_shape`: `city equality`
- `memory-sync` `consumption_mode`: `materialized`
- `memory-sync` `planning_mode`: `strict`

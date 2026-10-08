# Benchmark Report

## Environment

- Generated at: 2026-10-08T14:53:43.107950+00:00
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
| sqlite-sync | 5 | 0.0331 | 0.0330 | 0.0300 | sqlite/sql scan>filter>sort>slice>project | 0.07 | 175.73 |

- `sqlite-sync` `pipeline_shape`: `match-sort-limit-project`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`

## materializing_aggregation

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.4021 | 0.4015 | 0.3800 | sqlite/sql scan>filter | 0.31 | 193.41 |

- `sqlite-sync` `pipeline_shape`: `match-group-sort-limit`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `3`
- `sqlite-sync` `planning_mode`: `strict`

## aggregation_spill_diagnostics

### group_low_cardinality_first

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.1939 | 0.1931 | 0.1700 | sqlite/sql scan>filter | 0.00 | 196.52 |

- `sqlite-sync` `pipeline_shape`: `group-low-cardinality-first`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`

### group_high_cardinality_first

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 1.5668 | 1.5563 | 1.4240 | sqlite/sql scan>filter | 0.72 | 196.89 |

- `sqlite-sync` `pipeline_shape`: `group-high-cardinality-first`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`

## secondary_lookup_indexed

### secondary_lookup_indexed_1k

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.3694 | 0.3672 | 0.2320 | sqlite/sql opaque | -1.39 | 217.28 |

- `sqlite-sync` `query_shape`: `username equality`
- `sqlite-sync` `planning_mode`: `strict`

## cursor_consumption

### cursor_consumption_first_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.0755 | 0.0695 | 0.0460 | sqlite/sql opaque | -10.40 | 634.55 |

- `sqlite-sync` `query_shape`: `city equality`
- `sqlite-sync` `consumption_mode`: `first`
- `sqlite-sync` `planning_mode`: `strict`

### cursor_consumption_materialized_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 8.4395 | 8.0873 | 6.8080 | sqlite/sql opaque | 380.79 | 893.14 |

- `sqlite-sync` `query_shape`: `city equality`
- `sqlite-sync` `consumption_mode`: `materialized`
- `sqlite-sync` `planning_mode`: `strict`

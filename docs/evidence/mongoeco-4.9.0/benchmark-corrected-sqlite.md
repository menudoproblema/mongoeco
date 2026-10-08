# Benchmark Report

## Environment

- Generated at: 2026-10-08T15:37:32.560246+00:00
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

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.0326 | -1.5% | 0.0320 | sqlite/sql scan>filter>sort>slice>project | 1.37 | 165.52 |

- `sqlite-sync` `pipeline_shape`: `match-sort-limit-project`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`

## materializing_aggregation

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.3983 | -0.9% | 0.3740 | sqlite/sql scan>filter | 0.21 | 188.28 |

- `sqlite-sync` `pipeline_shape`: `match-group-sort-limit`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `3`
- `sqlite-sync` `planning_mode`: `strict`

## aggregation_spill_diagnostics

### group_low_cardinality_first

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.1849 | -4.6% | 0.1640 | sqlite/sql scan>filter | 0.00 | 195.23 |

- `sqlite-sync` `pipeline_shape`: `group-low-cardinality-first`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`

### group_high_cardinality_first

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 1.4291 | -8.8% | 1.3680 | sqlite/sql scan>filter | 0.95 | 197.41 |

- `sqlite-sync` `pipeline_shape`: `group-high-cardinality-first`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`

## secondary_lookup_indexed

### secondary_lookup_indexed_1k

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.3641 | -1.4% | 0.2300 | sqlite/sql opaque | -0.60 | 215.95 |

- `sqlite-sync` `query_shape`: `username equality`
- `sqlite-sync` `planning_mode`: `strict`

## cursor_consumption

### cursor_consumption_first_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 0.0709 | -6.1% | 0.0460 | sqlite/sql opaque | -11.80 | 604.69 |

- `sqlite-sync` `query_shape`: `city equality`
- `sqlite-sync` `consumption_mode`: `first`
- `sqlite-sync` `planning_mode`: `strict`

### cursor_consumption_materialized_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 5 | 8.1538 | -3.4% | 6.6320 | sqlite/sql opaque | 391.92 | 891.86 |

- `sqlite-sync` `query_shape`: `city equality`
- `sqlite-sync` `consumption_mode`: `materialized`
- `sqlite-sync` `planning_mode`: `strict`

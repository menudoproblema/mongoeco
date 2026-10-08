# Benchmark Report

## Environment

- Generated at: 2026-10-08T15:27:12.419202+00:00
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
| memory-sync | 5 | 0.0624 | -15.7% | 0.0640 | memory/python scan>filter>sort>project>slice | 6.02 | 131.75 |

- `memory-sync` `pipeline_shape`: `match-sort-limit-project`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`

## materializing_aggregation

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.4227 | -15.1% | 0.4180 | memory/python scan>filter | 0.00 | 147.27 |

- `memory-sync` `pipeline_shape`: `match-group-sort-limit`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `3`
- `memory-sync` `planning_mode`: `strict`

## aggregation_spill_diagnostics

### group_low_cardinality_first

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.2149 | -23.7% | 0.2140 | memory/python scan>filter | 0.07 | 160.59 |

- `memory-sync` `pipeline_shape`: `group-low-cardinality-first`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`

### group_high_cardinality_first

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 1.3930 | -28.2% | 1.3760 | memory/python scan>filter | 1.42 | 160.86 |

- `memory-sync` `pipeline_shape`: `group-high-cardinality-first`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`

## secondary_lookup_indexed

### secondary_lookup_indexed_1k

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.1530 | -29.5% | 0.1300 | memory/python scan>filter>slice | 0.00 | 186.44 |

- `memory-sync` `query_shape`: `username equality`
- `memory-sync` `planning_mode`: `strict`

## cursor_consumption

### cursor_consumption_first_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 0.0298 | -22.4% | 0.0220 | memory/python scan>filter | -1.00 | 395.38 |

- `memory-sync` `query_shape`: `city equality`
- `memory-sync` `consumption_mode`: `first`
- `memory-sync` `planning_mode`: `strict`

### cursor_consumption_materialized_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 5 | 4.8761 | -18.7% | 4.6760 | memory/python scan>filter | 23.48 | 406.20 |

- `memory-sync` `query_shape`: `city equality`
- `memory-sync` `consumption_mode`: `materialized`
- `memory-sync` `planning_mode`: `strict`

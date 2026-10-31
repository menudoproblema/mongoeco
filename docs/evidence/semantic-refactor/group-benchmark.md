# Benchmark Report

## Environment

- Generated at: 2026-10-30T20:21:38.143393+00:00
- Python: 3.14.5
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 1000
- Warmup runs: 1
- Measured repetitions: 3
- Workloads: materializing_aggregation, simple_aggregation
- JSON backend: stdlib
- MONGOECO_JSON_BACKEND: unset
- Git revision: `b38ef96764ce4a976883005ade83f82f7b3ef547`
- Git worktree dirty: `True`
- Report schema: `mongoeco-benchmark-report/v2`
- Harness SHA-256: `sha256:a5182bee9d40c4a0c92908263f2825a6c038be00495d771f6b6d29095841be72`
- Dataset SHA-256: `sha256:c2b95c2ef946b1eee07d3311fedac30f6712de6ff7668d0f095f4ba705f4df63`
- RSS peak sampling interval: `5.0 ms`
- psutil: `7.2.2`
- mongomock: `4.3.0`
- orjson: `3.11.9`

## materializing_aggregation

### materializing_aggregation_nested_group

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.0263 | +24.6% | 0.0300 | - | 0.01 | 81.38 |

- `memory-sync` `pipeline_shape`: `facet-match-group-sort-limit`

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.0190 | +0.5% | 0.0200 | memory/python scan>filter | 0.00 | 81.38 |

- `memory-sync` `pipeline_shape`: `match-group-sort-limit`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `3`
- `memory-sync` `planning_mode`: `strict`

## simple_aggregation

### simple_aggregation_topk

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.0035 | -5.4% | 0.0033 | memory/python scan>filter>sort>project>slice | 0.00 | 82.19 |

- `memory-sync` `pipeline_shape`: `match-sort-limit-project`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`

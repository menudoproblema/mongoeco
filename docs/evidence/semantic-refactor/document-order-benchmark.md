# Benchmark Report

## Environment

- Generated at: 2026-10-30T21:05:05.688707+00:00
- Python: 3.14.5
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 1000
- Warmup runs: 1
- Measured repetitions: 3
- Workloads: sort_shape_diagnostics
- JSON backend: stdlib
- MONGOECO_JSON_BACKEND: unset
- Git revision: `b38ef96764ce4a976883005ade83f82f7b3ef547`
- Git worktree dirty: `True`
- Report schema: `mongoeco-benchmark-report/v2`
- Harness SHA-256: `sha256:4a6949d179df72d2c7de595975c08afc7819619cb1bbaa37e3c286ab29df827f`
- Dataset SHA-256: `sha256:c2b95c2ef946b1eee07d3311fedac30f6712de6ff7668d0f095f4ba705f4df63`
- RSS peak sampling interval: `5.0 ms`
- psutil: `7.2.2`
- mongomock: `4.3.0`
- orjson: `3.11.9`

## sort_shape_diagnostics

### sort_shape_document_full_20

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 1.3650 | +5.0% | 1.3533 | memory/python scan>filter>sort | 0.01 | 115.78 |

- `memory-sync` `query_shape`: `sort document profile asc with ID tie-breaker`
- `memory-sync` `planning_mode`: `strict`

### sort_shape_top_level_limit_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.2558 | +12.9% | 0.2233 | memory/python scan>filter>sort>slice | 0.00 | 115.78 |

- `memory-sync` `query_shape`: `sort top-level score desc + limit 10`
- `memory-sync` `planning_mode`: `strict`

### sort_shape_nested_limit_200

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.4561 | +5.5% | 0.4333 | memory/python scan>filter>sort>slice | -0.33 | 115.78 |

- `memory-sync` `query_shape`: `sort nested profile.score desc + limit 10`
- `memory-sync` `planning_mode`: `strict`

### sort_shape_top_level_full_50

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.7167 | +4.6% | 0.6933 | memory/python scan>filter>sort | 0.39 | 115.78 |

- `memory-sync` `query_shape`: `sort top-level score desc`
- `memory-sync` `planning_mode`: `strict`

### sort_shape_nested_full_50

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 3 | 0.7374 | +1.2% | 0.7167 | memory/python scan>filter>sort | 0.34 | 115.78 |

- `memory-sync` `query_shape`: `sort nested profile.score desc`
- `memory-sync` `planning_mode`: `strict`

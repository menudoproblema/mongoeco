# Benchmark Report

## Environment

- Generated at: 2026-10-30T20:56:12.798479+00:00
- Python: 3.14.5
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 1000
- Warmup runs: 1
- Measured repetitions: 3
- Workloads: predicate_diagnostics
- JSON backend: stdlib
- MONGOECO_JSON_BACKEND: unset
- Git revision: `b38ef96764ce4a976883005ade83f82f7b3ef547`
- Git worktree dirty: `True`
- Report schema: `mongoeco-benchmark-report/v2`
- Harness SHA-256: `sha256:b7dd32520feeb6fc5f87385ac77de5f4a7edb691c076defd99ab6939c1a6a13e`
- Dataset SHA-256: `sha256:c2b95c2ef946b1eee07d3311fedac30f6712de6ff7668d0f095f4ba705f4df63`
- RSS peak sampling interval: `5.0 ms`
- psutil: `7.2.2`
- mongomock: `4.3.0`
- orjson: `3.11.9`

## predicate_diagnostics

### predicate_indexed_numeric_equality_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.1438 | -2.1% | 0.1100 | sqlite/sql scan>filter | -3.13 | 184.92 |

- `sqlite-sync` `query_shape`: `indexed scalar integer equality`
- `sqlite-sync` `planning_mode`: `strict`

### predicate_eq_bool_high_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.6774 | +2.6% | 0.5400 | sqlite/sql scan>filter | 10.51 | 186.64 |

- `sqlite-sync` `query_shape`: `top-level bool equality`
- `sqlite-sync` `selectivity`: `high`
- `sqlite-sync` `planning_mode`: `strict`

### predicate_eq_string_medium_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.2564 | -4.2% | 0.2067 | sqlite/sql opaque | 0.58 | 187.97 |

- `sqlite-sync` `query_shape`: `top-level string equality`
- `sqlite-sync` `selectivity`: `medium`
- `sqlite-sync` `planning_mode`: `strict`

### predicate_cmp_int_medium_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.5591 | -0.9% | 0.4367 | sqlite/sql scan>filter | -0.30 | 187.98 |

- `sqlite-sync` `query_shape`: `top-level int comparison`
- `sqlite-sync` `selectivity`: `medium`
- `sqlite-sync` `planning_mode`: `strict`

### predicate_indexed_conjunct_residual_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.8998 | -2.6% | 0.8333 | sqlite/python scan>filter | -1.67 | 187.98 |

- `sqlite-sync` `query_shape`: `indexed scalar conjunct + expression residual`
- `sqlite-sync` `selectivity`: `medium prefilter, lower residual`
- `sqlite-sync` `planning_mode`: `strict`

### predicate_unindexed_conjunct_residual_100

| Engine | Runs | Wall mean (s) | Wall delta vs baseline | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| sqlite-sync | 3 | 0.8930 | -4.0% | 0.8267 | sqlite/python scan>filter | -0.67 | 186.98 |

- `sqlite-sync` `query_shape`: `unindexed scalar conjunct + expression residual`
- `sqlite-sync` `selectivity`: `full Python reference`
- `sqlite-sync` `planning_mode`: `strict`

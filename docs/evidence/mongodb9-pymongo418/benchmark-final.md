# Benchmark Report

## Environment

- Generated at: 2026-10-08T02:21:53.209784+00:00
- Python: 3.14.5
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 250
- Warmup runs: 0
- Measured repetitions: 1
- Workloads: filter_selectivity, simple_aggregation, search_diagnostics, vector_search_diagnostics
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
- orjson: `3.13.0`

## filter_selectivity

### filter_selectivity_low_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0342 | 0.0342 | 0.0200 | memory/python scan>filter | 0.02 | 77.77 |
| sqlite-sync | 1 | 0.0735 | 0.0735 | 0.0500 | sqlite/sql scan>filter | 0.03 | 99.44 |
| memory-async | 1 | 0.0221 | 0.0221 | 0.0200 | memory/python scan>filter | 0.00 | 115.84 |
| sqlite-async | 1 | 0.0644 | 0.0644 | 0.0500 | sqlite/sql scan>filter | 0.42 | 124.73 |
| mongomock | 1 | 0.0178 | 0.0178 | 0.0200 | mongomock/python opaque | 0.00 | 133.25 |

- `memory-sync` `query_shape`: `username equality`
- `memory-sync` `selectivity`: `low`
- `memory-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_shape`: `username equality`
- `sqlite-sync` `selectivity`: `low`
- `sqlite-sync` `planning_mode`: `strict`
- `memory-async` `query_shape`: `username equality`
- `memory-async` `selectivity`: `low`
- `memory-async` `planning_mode`: `strict`
- `sqlite-async` `query_shape`: `username equality`
- `sqlite-async` `selectivity`: `low`
- `sqlite-async` `planning_mode`: `strict`
- `mongomock` `query_shape`: `username equality`
- `mongomock` `selectivity`: `low`

### filter_selectivity_medium_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0583 | 0.0583 | 0.0500 | memory/python scan>filter | 0.80 | 78.58 |
| sqlite-sync | 1 | 0.0905 | 0.0905 | 0.0700 | sqlite/sql scan>filter | 0.00 | 99.44 |
| memory-async | 1 | 0.0566 | 0.0566 | 0.0500 | memory/python scan>filter | 0.00 | 115.84 |
| sqlite-async | 1 | 0.0811 | 0.0811 | 0.0600 | sqlite/sql scan>filter | 0.00 | 124.73 |
| mongomock | 1 | 0.0254 | 0.0254 | 0.0300 | mongomock/python opaque | 0.00 | 133.25 |

- `memory-sync` `query_shape`: `city equality`
- `memory-sync` `selectivity`: `medium`
- `memory-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_shape`: `city equality`
- `sqlite-sync` `selectivity`: `medium`
- `sqlite-sync` `planning_mode`: `strict`
- `memory-async` `query_shape`: `city equality`
- `memory-async` `selectivity`: `medium`
- `memory-async` `planning_mode`: `strict`
- `sqlite-async` `query_shape`: `city equality`
- `sqlite-async` `selectivity`: `medium`
- `sqlite-async` `planning_mode`: `strict`
- `mongomock` `query_shape`: `city equality`
- `mongomock` `selectivity`: `medium`

### filter_selectivity_high_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1449 | 0.1449 | 0.1300 | memory/python scan>filter | 5.09 | 83.70 |
| sqlite-sync | 1 | 0.2067 | 0.2067 | 0.1600 | sqlite/sql scan>filter | 14.53 | 113.97 |
| memory-async | 1 | 0.1273 | 0.1273 | 0.1300 | memory/python scan>filter | 0.34 | 116.19 |
| sqlite-async | 1 | 0.1852 | 0.1852 | 0.1500 | sqlite/sql scan>filter | 7.59 | 132.33 |
| mongomock | 1 | 0.0528 | 0.0528 | 0.0600 | mongomock/python opaque | 0.02 | 133.27 |

- `memory-sync` `query_shape`: `active equality`
- `memory-sync` `selectivity`: `high`
- `memory-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_shape`: `active equality`
- `sqlite-sync` `selectivity`: `high`
- `sqlite-sync` `planning_mode`: `strict`
- `memory-async` `query_shape`: `active equality`
- `memory-async` `selectivity`: `high`
- `memory-async` `planning_mode`: `strict`
- `sqlite-async` `query_shape`: `active equality`
- `sqlite-async` `selectivity`: `high`
- `sqlite-async` `planning_mode`: `strict`
- `mongomock` `query_shape`: `active equality`
- `mongomock` `selectivity`: `high`

## simple_aggregation

### simple_aggregation_topk

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0013 | 0.0013 | 0.0000 | memory/python scan>filter>sort>project>slice | 0.00 | 84.09 |
| sqlite-sync | 1 | 0.0018 | 0.0018 | 0.0000 | sqlite/sql scan>filter>sort>slice>project | 0.00 | 114.23 |
| memory-async | 1 | 0.0012 | 0.0012 | 0.0000 | memory/python scan>filter>sort>project>slice | 0.00 | 116.23 |
| sqlite-async | 1 | 0.0017 | 0.0017 | 0.0000 | sqlite/sql scan>filter>sort>slice>project | 0.00 | 129.52 |
| mongomock | 1 | 0.0026 | 0.0026 | 0.0100 | mongomock/python opaque | 0.00 | 133.27 |

- `memory-sync` `pipeline_shape`: `match-sort-limit-project`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `sqlite-sync` `pipeline_shape`: `match-sort-limit-project`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `memory-async` `pipeline_shape`: `match-sort-limit-project`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `sqlite-async` `pipeline_shape`: `match-sort-limit-project`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `mongomock` `pipeline_shape`: `match-sort-limit-project`
- `mongomock` `streaming_batch_execution`: `False`
- `mongomock` `remaining_stage_count`: `4`

## search_diagnostics

### search_text_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1440 | 0.1440 | 0.1400 | memory/search opaque | 0.00 | 84.77 |
| sqlite-sync | 1 | 0.2937 | 0.2937 | 0.2800 | sqlite/search opaque | 0.00 | 114.36 |
| memory-async | 1 | 0.1416 | 0.1416 | 0.1400 | memory/search opaque | 0.00 | 116.25 |
| sqlite-async | 1 | 0.2936 | 0.2936 | 0.2900 | sqlite/search opaque | 0.02 | 129.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.text title/body ada`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `text`
- `sqlite-sync` `query_shape`: `$search.text title/body ada`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `text`
- `memory-async` `query_shape`: `$search.text title/body ada`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `text`
- `sqlite-async` `query_shape`: `$search.text title/body ada`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `text`

### search_autocomplete_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1446 | 0.1446 | 0.1500 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.3020 | 0.3020 | 0.3000 | sqlite/search opaque | 0.00 | 114.44 |
| memory-async | 1 | 0.1324 | 0.1324 | 0.1300 | memory/search opaque | 0.00 | 116.25 |
| sqlite-async | 1 | 0.2947 | 0.2947 | 0.2900 | sqlite/search opaque | 0.00 | 129.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.autocomplete title/body alg`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `autocomplete`
- `sqlite-sync` `query_shape`: `$search.autocomplete title/body alg`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `autocomplete`
- `memory-async` `query_shape`: `$search.autocomplete title/body alg`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `autocomplete`
- `sqlite-async` `query_shape`: `$search.autocomplete title/body alg`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `autocomplete`

### search_wildcard_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1011 | 0.1011 | 0.1000 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.2868 | 0.2868 | 0.2800 | sqlite/search opaque | 0.00 | 114.84 |
| memory-async | 1 | 0.0983 | 0.0983 | 0.1000 | memory/search opaque | 0.00 | 116.25 |
| sqlite-async | 1 | 0.2856 | 0.2856 | 0.2800 | sqlite/search opaque | 0.00 | 129.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.wildcard body *vector*`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `wildcard`
- `sqlite-sync` `query_shape`: `$search.wildcard body *vector*`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `wildcard`
- `memory-async` `query_shape`: `$search.wildcard body *vector*`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `wildcard`
- `sqlite-async` `query_shape`: `$search.wildcard body *vector*`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `wildcard`

### search_regex_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0965 | 0.0965 | 0.1000 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.8892 | 0.8892 | 0.8800 | sqlite/search opaque | 0.00 | 114.84 |
| memory-async | 1 | 0.0936 | 0.0936 | 0.0900 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.8960 | 0.8960 | 0.8800 | sqlite/search opaque | 0.00 | 129.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.regex title Ada.*algorithms`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `regex`
- `sqlite-sync` `query_shape`: `$search.regex title Ada.*algorithms`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `regex`
- `memory-async` `query_shape`: `$search.regex title Ada.*algorithms`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `regex`
- `sqlite-async` `query_shape`: `$search.regex title Ada.*algorithms`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `regex`

### search_compound_hybrid_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1668 | 0.1668 | 0.1700 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.3645 | 0.3645 | 0.3600 | sqlite/search opaque | 0.00 | 114.84 |
| memory-async | 1 | 0.1650 | 0.1650 | 0.1600 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.3624 | 0.3624 | 0.3600 | sqlite/search opaque | 0.00 | 129.48 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+filter(exists,wildcard)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+filter(exists,wildcard)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+filter(exists,wildcard)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+filter(exists,wildcard)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_should_near_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1657 | 0.1657 | 0.1600 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.3850 | 0.3850 | 0.3800 | sqlite/search opaque | 0.02 | 114.86 |
| memory-async | 1 | 0.1631 | 0.1631 | 0.1600 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.3840 | 0.3840 | 0.3700 | sqlite/search opaque | 0.00 | 129.55 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+filter(wildcard)+should(exists,near)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+filter(wildcard)+should(exists,near)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+filter(wildcard)+should(exists,near)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+filter(wildcard)+should(exists,near)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_candidateable_should_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.3776 | 0.3776 | 0.3800 | memory/search opaque | 0.00 | 85.00 |
| sqlite-sync | 1 | 0.1950 | 0.1950 | 0.1900 | sqlite/search opaque | 0.00 | 114.86 |
| memory-async | 1 | 0.3748 | 0.3748 | 0.3700 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.1961 | 0.1961 | 0.1900 | sqlite/search opaque | 0.03 | 129.59 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_candidateable_should_matched_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.3263 | 0.3263 | 0.3300 | memory/search opaque | 0.00 | 85.03 |
| sqlite-sync | 1 | 0.2391 | 0.2391 | 0.2300 | sqlite/search opaque | 0.00 | 114.86 |
| memory-async | 1 | 0.3243 | 0.3243 | 0.3200 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.2420 | 0.2420 | 0.2300 | sqlite/search opaque | 0.02 | 129.61 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(kind=note)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `2`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(kind=note)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `2`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(kind=note)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `2`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(kind=note)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `2`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_candidateable_should_title_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.2006 | 0.2006 | 0.2000 | memory/search opaque | 0.00 | 85.03 |
| sqlite-sync | 1 | 0.2388 | 0.2388 | 0.2300 | sqlite/search opaque | 0.00 | 114.88 |
| memory-async | 1 | 0.1980 | 0.1980 | 0.1900 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.2379 | 0.2379 | 0.2300 | sqlite/search opaque | 0.00 | 129.61 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(title=Ada algorithms)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `2`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(title=Ada algorithms)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `2`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(title=Ada algorithms)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `2`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(title=Ada algorithms)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `2`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_candidateable_should_msm2_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.3072 | 0.3072 | 0.3000 | memory/search opaque | 0.00 | 85.06 |
| sqlite-sync | 1 | 0.1980 | 0.1980 | 0.1900 | sqlite/search opaque | 0.00 | 114.91 |
| memory-async | 1 | 0.3046 | 0.3046 | 0.3000 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.1947 | 0.1947 | 0.1900 | sqlite/search opaque | 0.00 | 129.66 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+minimumShouldMatch(2)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+minimumShouldMatch(2)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+minimumShouldMatch(2)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+minimumShouldMatch(2)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

### search_compound_candidateable_should_tie_heavy_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.1503 | 0.1503 | 0.1500 | memory/search opaque | 0.00 | 85.06 |
| sqlite-sync | 1 | 0.1837 | 0.1837 | 0.1800 | sqlite/search opaque | 0.00 | 114.91 |
| memory-async | 1 | 0.1470 | 0.1470 | 0.1500 | memory/search opaque | 0.00 | 116.27 |
| sqlite-async | 1 | 0.1808 | 0.1808 | 0.1700 | sqlite/search opaque | 0.00 | 129.66 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$search.compound must(text=vector)+should(exists(title),exists(body),wildcard(body=*vector*))`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `1`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `compound`
- `sqlite-sync` `query_shape`: `$search.compound must(text=vector)+should(exists(title),exists(body),wildcard(body=*vector*))`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `1`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `compound`
- `memory-async` `query_shape`: `$search.compound must(text=vector)+should(exists(title),exists(body),wildcard(body=*vector*))`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `1`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `compound`
- `sqlite-async` `query_shape`: `$search.compound must(text=vector)+should(exists(title),exists(body),wildcard(body=*vector*))`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `1`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `compound`

## vector_search_diagnostics

### vector_search_ann_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0666 | 0.0666 | 0.0700 | memory/search opaque | 0.08 | 88.45 |
| sqlite-sync | 1 | 0.1144 | 0.1144 | 0.1100 | sqlite/search opaque | 0.00 | 115.53 |
| memory-async | 1 | 0.0644 | 0.0644 | 0.0600 | memory/search opaque | 0.00 | 118.91 |
| sqlite-async | 1 | 0.1103 | 0.1103 | 0.1000 | sqlite/search opaque | 0.00 | 130.17 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `250`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `candidates_requested`: `24`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `250`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `candidates_requested`: `24`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_cosine_low_candidates_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0669 | 0.0669 | 0.0600 | memory/search opaque | 0.00 | 88.45 |
| sqlite-sync | 1 | 0.0944 | 0.0944 | 0.0900 | sqlite/search opaque | 0.00 | 115.53 |
| memory-async | 1 | 0.0645 | 0.0645 | 0.0600 | memory/search opaque | 0.00 | 118.91 |
| sqlite-async | 1 | 0.0919 | 0.0919 | 0.0800 | sqlite/search opaque | 0.00 | 130.17 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + numCandidates(10)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `candidates_requested`: `10`
- `memory-sync` `candidates_evaluated`: `250`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + numCandidates(10)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `candidates_requested`: `10`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + numCandidates(10)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `candidates_requested`: `10`
- `memory-async` `candidates_evaluated`: `250`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + numCandidates(10)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `candidates_requested`: `10`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_cosine_high_candidates_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0661 | 0.0661 | 0.0700 | memory/search opaque | 0.00 | 88.45 |
| sqlite-sync | 1 | 0.1454 | 0.1454 | 0.1400 | sqlite/search opaque | 0.00 | 115.53 |
| memory-async | 1 | 0.0647 | 0.0647 | 0.0600 | memory/search opaque | 0.00 | 118.91 |
| sqlite-async | 1 | 0.1404 | 0.1404 | 0.1300 | sqlite/search opaque | 0.00 | 130.17 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + numCandidates(48)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `candidates_requested`: `48`
- `memory-sync` `candidates_evaluated`: `250`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + numCandidates(48)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `candidates_requested`: `48`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + numCandidates(48)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `candidates_requested`: `48`
- `memory-async` `candidates_evaluated`: `250`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + numCandidates(48)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `candidates_requested`: `48`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_dot_product_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0665 | 0.0665 | 0.0700 | memory/search opaque | 0.00 | 91.48 |
| sqlite-sync | 1 | 0.1133 | 0.1133 | 0.1000 | sqlite/search opaque | 0.00 | 115.84 |
| memory-async | 1 | 0.0651 | 0.0651 | 0.0600 | memory/search opaque | 0.00 | 121.84 |
| sqlite-async | 1 | 0.1097 | 0.1097 | 0.1100 | sqlite/search opaque | 0.00 | 130.25 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch dotProduct topk`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `dotProduct`
- `memory-sync` `mode`: `exact`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `250`
- `sqlite-sync` `query_shape`: `$vectorSearch dotProduct topk`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `dotProduct`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `candidates_requested`: `24`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch dotProduct topk`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `dotProduct`
- `memory-async` `mode`: `exact`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `250`
- `sqlite-async` `query_shape`: `$vectorSearch dotProduct topk`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `dotProduct`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `candidates_requested`: `24`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_euclidean_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0674 | 0.0674 | 0.0700 | memory/search opaque | 0.11 | 95.67 |
| sqlite-sync | 1 | 0.1120 | 0.1120 | 0.1000 | sqlite/search opaque | 0.00 | 115.97 |
| memory-async | 1 | 0.0632 | 0.0632 | 0.0600 | memory/search opaque | 0.00 | 123.86 |
| sqlite-async | 1 | 0.1099 | 0.1099 | 0.1000 | sqlite/search opaque | 0.00 | 130.45 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch euclidean topk`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `euclidean`
- `memory-sync` `mode`: `exact`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `250`
- `sqlite-sync` `query_shape`: `$vectorSearch euclidean topk`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `euclidean`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `candidates_requested`: `24`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch euclidean topk`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `euclidean`
- `memory-async` `mode`: `exact`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `250`
- `sqlite-async` `query_shape`: `$vectorSearch euclidean topk`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `euclidean`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `candidates_requested`: `24`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_filtered_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0691 | 0.0691 | 0.0700 | memory/search opaque | 0.05 | 95.73 |
| sqlite-sync | 1 | 0.1032 | 0.1032 | 0.0900 | sqlite/search opaque | 0.00 | 115.97 |
| memory-async | 1 | 0.0670 | 0.0670 | 0.0600 | memory/search opaque | 0.00 | 123.86 |
| sqlite-async | 1 | 0.1155 | 0.1155 | 0.1100 | sqlite/search opaque | 0.00 | 130.45 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + post-filter`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `filter_mode`: `candidate-prefilter`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `84`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + post-filter`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `filter_mode`: `candidate-prefilter`
- `sqlite-sync` `candidates_requested`: `32`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + post-filter`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `filter_mode`: `candidate-prefilter`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `84`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + post-filter`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `filter_mode`: `candidate-prefilter`
- `sqlite-async` `candidates_requested`: `32`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_filtered_boolean_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0715 | 0.0715 | 0.0700 | memory/search opaque | 0.06 | 95.80 |
| sqlite-sync | 1 | 0.1073 | 0.1073 | 0.1000 | sqlite/search opaque | 0.00 | 115.97 |
| memory-async | 1 | 0.0825 | 0.0825 | 0.0800 | memory/search opaque | 0.02 | 123.89 |
| sqlite-async | 1 | 0.1056 | 0.1056 | 0.1000 | sqlite/search opaque | 0.00 | 130.45 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + boolean candidate filter`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `filter_mode`: `candidate-prefilter`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `113`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + boolean candidate filter`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `filter_mode`: `candidate-prefilter`
- `sqlite-sync` `candidates_requested`: `24`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + boolean candidate filter`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `filter_mode`: `candidate-prefilter`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `113`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + boolean candidate filter`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `filter_mode`: `candidate-prefilter`
- `sqlite-async` `candidates_requested`: `24`
- `sqlite-async` `candidates_evaluated`: `10`

### vector_search_filtered_underflow_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0716 | 0.0716 | 0.0700 | memory/search opaque | 0.02 | 95.83 |
| sqlite-sync | 1 | 0.1110 | 0.1110 | 0.1000 | sqlite/search opaque | 0.00 | 115.97 |
| memory-async | 1 | 0.0692 | 0.0692 | 0.0700 | memory/search opaque | 0.00 | 123.89 |
| sqlite-async | 1 | 0.1086 | 0.1086 | 0.1000 | sqlite/search opaque | 0.00 | 130.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + rare boolean candidate filter`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `filter_mode`: `candidate-prefilter`
- `memory-sync` `candidates_requested`: `10`
- `memory-sync` `candidates_evaluated`: `15`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + rare boolean candidate filter`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `filter_mode`: `candidate-prefilter`
- `sqlite-sync` `exact_fallback_reason`: `candidate-prefilter-underflow`
- `sqlite-sync` `candidates_requested`: `10`
- `sqlite-sync` `candidates_evaluated`: `0`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + rare boolean candidate filter`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `filter_mode`: `candidate-prefilter`
- `memory-async` `candidates_requested`: `10`
- `memory-async` `candidates_evaluated`: `15`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + rare boolean candidate filter`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `filter_mode`: `candidate-prefilter`
- `sqlite-async` `exact_fallback_reason`: `candidate-prefilter-underflow`
- `sqlite-async` `candidates_requested`: `10`
- `sqlite-async` `candidates_evaluated`: `0`

### vector_search_filtered_min_score_topk_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | Plan | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| memory-sync | 1 | 0.0704 | 0.0704 | 0.0700 | memory/search opaque | 0.05 | 95.88 |
| sqlite-sync | 1 | 0.1086 | 0.1086 | 0.1000 | sqlite/search opaque | 0.00 | 115.97 |
| memory-async | 1 | 0.0678 | 0.0678 | 0.0700 | memory/search opaque | 0.00 | 123.89 |
| sqlite-async | 1 | 0.1060 | 0.1060 | 0.1000 | sqlite/search opaque | 0.00 | 130.47 |
| mongomock | SKIPPED | - | - | - | - | - | - |

- `memory-sync` `query_shape`: `$vectorSearch cosine topk + filter(kind=note) + minScore(0.999)`
- `memory-sync` `streaming_batch_execution`: `False`
- `memory-sync` `remaining_stage_count`: `0`
- `memory-sync` `planning_mode`: `strict`
- `memory-sync` `query_operator`: `vectorSearch`
- `memory-sync` `similarity`: `cosine`
- `memory-sync` `mode`: `exact`
- `memory-sync` `filter_mode`: `candidate-prefilter`
- `memory-sync` `candidates_requested`: `24`
- `memory-sync` `candidates_evaluated`: `166`
- `sqlite-sync` `query_shape`: `$vectorSearch cosine topk + filter(kind=note) + minScore(0.999)`
- `sqlite-sync` `streaming_batch_execution`: `False`
- `sqlite-sync` `remaining_stage_count`: `0`
- `sqlite-sync` `planning_mode`: `strict`
- `sqlite-sync` `query_operator`: `vectorSearch`
- `sqlite-sync` `similarity`: `cosine`
- `sqlite-sync` `mode`: `ann`
- `sqlite-sync` `filter_mode`: `candidate-prefilter`
- `sqlite-sync` `candidates_requested`: `24`
- `sqlite-sync` `candidates_evaluated`: `10`
- `memory-async` `query_shape`: `$vectorSearch cosine topk + filter(kind=note) + minScore(0.999)`
- `memory-async` `streaming_batch_execution`: `False`
- `memory-async` `remaining_stage_count`: `0`
- `memory-async` `planning_mode`: `strict`
- `memory-async` `query_operator`: `vectorSearch`
- `memory-async` `similarity`: `cosine`
- `memory-async` `mode`: `exact`
- `memory-async` `filter_mode`: `candidate-prefilter`
- `memory-async` `candidates_requested`: `24`
- `memory-async` `candidates_evaluated`: `166`
- `sqlite-async` `query_shape`: `$vectorSearch cosine topk + filter(kind=note) + minScore(0.999)`
- `sqlite-async` `streaming_batch_execution`: `False`
- `sqlite-async` `remaining_stage_count`: `0`
- `sqlite-async` `planning_mode`: `strict`
- `sqlite-async` `query_operator`: `vectorSearch`
- `sqlite-async` `similarity`: `cosine`
- `sqlite-async` `mode`: `ann`
- `sqlite-async` `filter_mode`: `candidate-prefilter`
- `sqlite-async` `candidates_requested`: `24`
- `sqlite-async` `candidates_evaluated`: `10`

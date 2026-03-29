# Benchmark Report

## Environment

- Generated at: 2026-03-29T13:59:11+00:00
- Python: 3.14.0
- Platform: macOS-15.6-arm64-arm-64bit-Mach-O
- Dataset size: 500
- Warmup runs: 0
- Measured repetitions: 1
- Git revision: `fdff5c01c862ac184ef2622007fe813d52b082b6`

## secondary_lookup_indexed

### secondary_lookup_indexed_1k

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.1013 | 0.1013 | 0.0600 | 0.61 | 44.17 |
| sqlite-sync | 1 | 0.2722 | 0.2722 | 0.1900 | 1.33 | 63.48 |
| memory-async | 1 | 0.0998 | 0.0998 | 0.0800 | 0.00 | 73.91 |
| sqlite-async | 1 | 0.3095 | 0.3095 | 0.2400 | 0.06 | 71.58 |
| mongomock | 1 | 0.2347 | 0.2347 | 0.2300 | 0.00 | 84.81 |

## secondary_lookup_unindexed

### secondary_lookup_unindexed_1k

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.3722 | 0.3722 | 0.3200 | 0.02 | 44.19 |
| sqlite-sync | 1 | 0.2620 | 0.2620 | 0.1800 | 0.56 | 64.33 |
| memory-async | 1 | 0.3878 | 0.3878 | 0.3700 | 0.00 | 70.91 |
| sqlite-async | 1 | 0.2950 | 0.2950 | 0.2200 | 0.05 | 71.62 |
| mongomock | 1 | 0.2362 | 0.2362 | 0.2300 | 0.00 | 84.81 |

## simple_aggregation

### simple_aggregation_topk

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.0415 | 0.0415 | 0.0400 | 0.03 | 44.22 |
| sqlite-sync | 1 | 0.0160 | 0.0160 | 0.0100 | 0.59 | 65.56 |
| memory-async | 1 | 0.0446 | 0.0446 | 0.0500 | 0.00 | 70.91 |
| sqlite-async | 1 | 0.0156 | 0.0156 | 0.0100 | 0.00 | 71.66 |
| mongomock | 1 | 0.0072 | 0.0072 | 0.0000 | 0.00 | 84.81 |

## materializing_aggregation

### materializing_aggregation_group_sort

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.5172 | 0.5172 | 0.5200 | 0.03 | 44.25 |
| sqlite-sync | 1 | 0.2649 | 0.2649 | 0.2300 | 0.28 | 66.50 |
| memory-async | 1 | 0.5556 | 0.5556 | 0.5500 | 0.00 | 70.91 |
| sqlite-async | 1 | 0.2858 | 0.2858 | 0.2400 | 0.06 | 71.72 |
| mongomock | 1 | 0.0077 | 0.0077 | 0.0100 | 0.00 | 84.81 |

## sort_limit

### sort_limit_indexed_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 6.1214 | 6.1214 | 5.9900 | 2.33 | 46.58 |
| sqlite-sync | 1 | 1.6268 | 1.6268 | 1.3900 | 0.00 | 66.80 |
| memory-async | 1 | 6.0720 | 6.0720 | 6.0100 | 0.06 | 70.97 |
| sqlite-async | 1 | 1.7111 | 1.7111 | 1.5100 | 0.20 | 71.98 |
| mongomock | 1 | 0.8689 | 0.8689 | 0.8700 | 0.00 | 84.81 |

### sort_limit_unindexed_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 6.0885 | 6.0885 | 6.0000 | 0.31 | 46.89 |
| sqlite-sync | 1 | 1.6410 | 1.6410 | 1.4000 | 0.17 | 65.22 |
| memory-async | 1 | 6.0716 | 6.0716 | 6.0100 | 0.06 | 71.03 |
| sqlite-async | 1 | 1.8793 | 1.8793 | 1.6700 | 0.11 | 71.12 |
| mongomock | 1 | 0.8564 | 0.8564 | 0.8500 | 0.00 | 84.81 |

## cursor_consumption

### cursor_consumption_first_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.0339 | 0.0339 | 0.0300 | 0.00 | 46.92 |
| sqlite-sync | 1 | 0.1120 | 0.1120 | 0.0900 | 0.08 | 65.80 |
| memory-async | 1 | 0.0322 | 0.0322 | 0.0200 | 0.00 | 71.03 |
| sqlite-async | 1 | 0.1130 | 0.1130 | 0.0800 | 0.00 | 71.12 |
| mongomock | 1 | 0.1338 | 0.1338 | 0.1300 | 0.00 | 84.81 |

### cursor_consumption_materialized_200

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 4.6609 | 4.6609 | 4.1300 | 3.03 | 49.95 |
| sqlite-sync | 1 | 9.0448 | 9.0448 | 6.7000 | 5.98 | 71.78 |
| memory-async | 1 | 3.8710 | 3.8710 | 3.8200 | 0.45 | 71.48 |
| sqlite-async | 1 | 8.5145 | 8.5145 | 6.7500 | 3.67 | 74.80 |
| mongomock | 1 | 0.1390 | 0.1390 | 0.1400 | 0.09 | 84.91 |

## filter_selectivity

### filter_selectivity_low_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 0.1453 | 0.1453 | 0.1400 | 0.00 | 49.95 |
| sqlite-sync | 1 | 0.1004 | 0.1004 | 0.0800 | 0.05 | 68.83 |
| memory-async | 1 | 0.1435 | 0.1435 | 0.1400 | 0.00 | 71.48 |
| sqlite-async | 1 | 0.1017 | 0.1017 | 0.0800 | 0.00 | 74.80 |
| mongomock | 1 | 0.0467 | 0.0467 | 0.0400 | 0.00 | 84.94 |

### filter_selectivity_medium_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 7.2320 | 7.2320 | 6.9500 | 0.16 | 50.11 |
| sqlite-sync | 1 | 4.4582 | 4.4582 | 3.2900 | 0.81 | 69.64 |
| memory-async | 1 | 6.9353 | 6.9353 | 6.8900 | 0.00 | 71.48 |
| sqlite-async | 1 | 3.9731 | 3.9731 | 3.1400 | 0.33 | 75.12 |
| mongomock | 1 | 0.0737 | 0.0737 | 0.0800 | 0.03 | 84.97 |

### filter_selectivity_high_100

| Engine | Runs | Wall mean (s) | Wall median (s) | CPU user mean (s) | RSS delta mean (MB) | RSS peak max (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| memory-sync | 1 | 28.4623 | 28.4623 | 27.1900 | 9.14 | 59.25 |
| sqlite-sync | 1 | 18.9779 | 18.9779 | 14.1300 | 6.30 | 75.94 |
| memory-async | 1 | 27.3847 | 27.3847 | 27.2800 | 0.00 | 71.48 |
| sqlite-async | 1 | 16.2966 | 16.2966 | 12.7400 | 4.55 | 79.67 |
| mongomock | 1 | 0.1479 | 0.1479 | 0.1400 | 1.97 | 86.94 |

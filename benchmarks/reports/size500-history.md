# Benchmark History (`size=500`)

Orden cronológico, de la evaluación más antigua a la más reciente.

## Evaluation 1

- Generated at: `2026-03-29T13:59:11+0000`
- Git revision: `fdff5c01c862ac184ef2622007fe813d52b082b6`
- Source: `benchmarks/reports/latest-500.md`

| Task                                   | memory-sync | sqlite-sync | memory-async | sqlite-async | mongomock |
| -------------------------------------- | ----------: | ----------: | -----------: | -----------: | --------: |
| `secondary_lookup_indexed_1k`          |     0.1013s |     0.2722s |      0.0998s |      0.3095s |   0.2347s |
| `secondary_lookup_unindexed_1k`        |     0.3722s |     0.2620s |      0.3878s |      0.2950s |   0.2362s |
| `simple_aggregation_topk`              |     0.0415s |     0.0160s |      0.0446s |      0.0156s |   0.0072s |
| `materializing_aggregation_group_sort` |     0.5172s |     0.2649s |      0.5556s |      0.2858s |   0.0077s |
| `sort_limit_indexed_200`               |     6.1214s |     1.6268s |      6.0720s |      1.7111s |   0.8689s |
| `sort_limit_unindexed_200`             |     6.0885s |     1.6410s |      6.0716s |      1.8793s |   0.8564s |
| `cursor_consumption_first_200`         |     0.0339s |     0.1120s |      0.0322s |      0.1130s |   0.1338s |
| `cursor_consumption_materialized_200`  |     4.6609s |     9.0448s |      3.8710s |      8.5145s |   0.1390s |
| `filter_selectivity_low_100`           |     0.1453s |     0.1004s |      0.1435s |      0.1017s |   0.0467s |
| `filter_selectivity_medium_100`        |     7.2320s |     4.4582s |      6.9353s |      3.9731s |   0.0737s |
| `filter_selectivity_high_100`          |    28.4623s |    18.9779s |     27.3847s |     16.2966s |   0.1479s |

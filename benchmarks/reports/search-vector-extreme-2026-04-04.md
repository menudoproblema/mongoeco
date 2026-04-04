# Search / Vector Performance Report

Date: 2026-04-04

Command baseline:

```bash
python -m benchmarks.run --engine memory-sync --size 1000 --warmup 0 --repetitions 3 --workload search_diagnostics --workload vector_search_diagnostics --format table
python -m benchmarks.run --engine sqlite-sync --size 1000 --warmup 0 --repetitions 3 --workload search_diagnostics --workload vector_search_diagnostics --format table
```

## Before / After

The "before" values below are the last stable measurements taken before the broad compound and vector prefilter/materialization work in this optimization round.

| Engine | Workload | Before (s) | After (s) | Notes |
| --- | --- | ---: | ---: | --- |
| sqlite-sync | `search_compound_candidateable_should_topk_100` | ~0.86 | 0.3801 | Strong improvement from partial ranking, cached materialized search entries and pre-cut by `matchedShould` tiers. |
| sqlite-sync | `search_compound_candidateable_should_matched_topk_100` | ~0.81 | 0.4470 | Downstream `$match` candidateability, clause refinement and pre-cut by `matchedShould` reduce ranking work materially. |
| sqlite-sync | `search_compound_candidateable_should_title_topk_100` | ~0.65 | 0.4073 | Title/path-specific downstream refinement now prunes earlier, with less exact-score work at the cutoff tier. |
| sqlite-sync | `search_compound_candidateable_should_msm2_topk_100` | n/a | 0.3987 | New diagnostic workload for a harder `minimumShouldMatch` case. |
| memory-sync | `search_compound_candidateable_should_matched_topk_100` | ~0.41 | 0.3080 | Candidateable downstream filters and stronger pruning improved the baseline engine. |
| memory-sync | `search_compound_candidateable_should_title_topk_100` | ~0.19 | 0.1980 | Roughly stable; no meaningful regression. |
| memory-sync | `vector_search_ann_topk_100` | ~0.26 | 0.2738 | Essentially flat; still the next visible hotspot. |
| memory-sync | `vector_search_filtered_topk_100` | ~0.32 | 0.2745 | Better due to materialized vector entries and candidateable scalar prefilter. |
| memory-sync | `vector_search_filtered_boolean_topk_100` | n/a | 0.3080 | New boolean-filter workload. |
| sqlite-sync | `vector_search_ann_topk_100` | ~0.09 | 0.0933 | Stable. |
| sqlite-sync | `vector_search_filtered_topk_100` | ~0.08 | median 0.0868 | One large outlier remains when ANN prefilter underflows and exact fallback is triggered. |
| sqlite-sync | `vector_search_filtered_boolean_topk_100` | n/a | 0.0835 | New boolean-filter workload; stable and candidateable. |

## Reading

- The biggest win in this round is `SQLite compound` with broad `should` clauses.
- The latest sub-round specifically improved the broad-`should` path by discarding lower `matchedShould` tiers before computing exact `shouldScore`.
- `MemoryEngine` improved meaningfully on `compound + downstream match`.
- `Memory vectorSearch` improved on filtered runs, but pure ANN top-k remains comparatively expensive.
- `SQLite vectorSearch` remains fast on normal filtered cases, but still shows a visible long-tail outlier when candidate prefiltering underflows and exact fallback is forced.

## Next Hotspots

1. `memory-sync` `vector_search_ann_topk_100`
2. `memory-sync` `vector_search_filtered_boolean_topk_100`
3. `sqlite-sync` long-tail fallback behavior in `vector_search_filtered_topk_100`

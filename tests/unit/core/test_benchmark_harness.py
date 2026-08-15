import unittest

from unittest.mock import Mock, patch

from benchmarks.report import main as report_main, render_markdown_report
from benchmarks.run import (
    SKIPPED_WORKLOADS_KEY,
    WORKLOAD_ORDER,
    _run_engine_workloads,
    main as benchmark_main,
    resolve_workload_names,
)
from benchmarks.runners.workloads import _augment_search_documents


class BenchmarkHarnessTests(unittest.TestCase):
    def test_search_benchmark_embeddings_are_deterministic_and_unique(self):
        first = [{"city": "Madrid"} for _ in range(8)]
        second = [{"city": "Madrid"} for _ in range(8)]

        _augment_search_documents(first)
        _augment_search_documents(second)

        first_embeddings = [tuple(document["embedding"]) for document in first]
        self.assertEqual(first, second)
        self.assertEqual(len(set(first_embeddings)), len(first_embeddings))

    def test_engine_workloads_record_capability_based_skips(self):
        engine = Mock(
            benchmark_capabilities=frozenset({"crud", "aggregation"}),
        )

        with patch("benchmarks.run.load_engine", return_value=engine):
            results = _run_engine_workloads(
                "limited",
                10,
                0,
                1,
                workload_names=("search_diagnostics",),
            )

        self.assertEqual(
            results[SKIPPED_WORKLOADS_KEY],
            {
                "search_diagnostics": ("adapter lacks benchmark capabilities: search"),
            },
        )

    def test_resolve_workload_names_returns_default_order_when_not_filtered(self):
        self.assertEqual(resolve_workload_names(None), WORKLOAD_ORDER)

    def test_resolve_workload_names_preserves_requested_order_and_deduplicates(self):
        self.assertEqual(
            resolve_workload_names(
                [
                    "sort_shape_diagnostics",
                    "predicate_diagnostics",
                    "sort_shape_diagnostics",
                ]
            ),
            ("sort_shape_diagnostics", "predicate_diagnostics"),
        )

    def test_resolve_workload_names_supports_search_and_vector_diagnostics(self):
        self.assertEqual(
            resolve_workload_names(["search_diagnostics", "vector_search_diagnostics"]),
            ("search_diagnostics", "vector_search_diagnostics"),
        )

    def test_render_markdown_report_can_limit_output_to_selected_workloads(self):
        results = {
            "memory-sync": {
                "predicate_diagnostics": {
                    "predicate_eq_bool_high_100": {
                        "repetitions": 1,
                        "wall_time_mean_sec": 0.1,
                        "wall_time_median_sec": 0.1,
                        "wall_time_min_sec": 0.1,
                        "wall_time_max_sec": 0.1,
                        "cpu_user_mean_sec": 0.05,
                        "cpu_sys_mean_sec": 0.0,
                        "rss_delta_mean_mb": 0.0,
                        "rss_peak_max_mb": 1.0,
                        "metadata": {"summary": "memory/python scan>filter"},
                    }
                }
            }
        }

        markdown = render_markdown_report(
            results=results,
            size=100,
            warmup=0,
            repetitions=1,
            workload_names=("predicate_diagnostics",),
        )

        self.assertIn("## predicate_diagnostics", markdown)
        self.assertNotIn("## sort_limit", markdown)

    def test_render_markdown_report_includes_selected_workloads_and_json_backend(self):
        results = {"memory-sync": {"predicate_diagnostics": {}}}

        with patch("benchmarks.report.get_json_backend_name", return_value="stdlib"):
            markdown = render_markdown_report(
                results=results,
                size=100,
                warmup=0,
                repetitions=1,
                workload_names=("predicate_diagnostics",),
            )

        self.assertIn("- Workloads: predicate_diagnostics", markdown)
        self.assertIn("- JSON backend: stdlib", markdown)

    def test_render_markdown_report_includes_vector_metadata_notes(self):
        results = {
            "sqlite-sync": {
                "vector_search_diagnostics": {
                    "vector_search_ann_topk_100": {
                        "repetitions": 1,
                        "wall_time_mean_sec": 0.1,
                        "wall_time_median_sec": 0.1,
                        "wall_time_min_sec": 0.1,
                        "wall_time_max_sec": 0.1,
                        "cpu_user_mean_sec": 0.05,
                        "cpu_sys_mean_sec": 0.0,
                        "rss_delta_mean_mb": 0.0,
                        "rss_peak_max_mb": 1.0,
                        "metadata": {
                            "summary": "sqlite/search opaque",
                            "query_shape": "$vectorSearch cosine topk",
                            "similarity": "cosine",
                            "candidates_requested": 24,
                            "candidates_evaluated": 10,
                            "exact_fallback_reason": None,
                        },
                    }
                }
            }
        }

        markdown = render_markdown_report(
            results=results,
            size=1000,
            warmup=0,
            repetitions=1,
            workload_names=("vector_search_diagnostics",),
        )

        self.assertIn("- `sqlite-sync` `similarity`: `cosine`", markdown)
        self.assertIn("- `sqlite-sync` `candidates_requested`: `24`", markdown)

    def test_render_markdown_report_marks_unsupported_workloads_as_skipped(self):
        results = {
            "memory-sync": {
                "search_diagnostics": {
                    "text": {
                        "repetitions": 1,
                        "wall_time_mean_sec": 0.1,
                        "wall_time_median_sec": 0.1,
                        "wall_time_min_sec": 0.1,
                        "wall_time_max_sec": 0.1,
                        "cpu_user_mean_sec": 0.05,
                        "cpu_sys_mean_sec": 0.0,
                        "rss_delta_mean_mb": 0.0,
                        "rss_peak_max_mb": 1.0,
                    },
                },
            },
            "mongomock": {
                SKIPPED_WORKLOADS_KEY: {
                    "search_diagnostics": (
                        "adapter lacks benchmark capabilities: search"
                    ),
                },
            },
        }

        markdown = render_markdown_report(
            results=results,
            size=100,
            warmup=0,
            repetitions=1,
            workload_names=("search_diagnostics",),
        )

        self.assertIn("| mongomock | SKIPPED |", markdown)

    def test_benchmark_entrypoints_return_nonzero_when_an_engine_fails(self):
        failed_results = {"memory-sync": {"error": "boom"}}
        argv = [
            "benchmark",
            "--engine",
            "memory-sync",
            "--size",
            "1",
            "--warmup",
            "0",
            "--repetitions",
            "1",
        ]

        with (
            patch("benchmarks.run.run_benchmarks", return_value=failed_results),
            patch("benchmarks.run.sys.argv", [*argv, "--format", "json"]),
        ):
            self.assertEqual(benchmark_main(), 1)

        with (
            patch("benchmarks.report.run_benchmarks", return_value=failed_results),
            patch("benchmarks.report.sys.argv", argv),
        ):
            self.assertEqual(report_main(), 1)

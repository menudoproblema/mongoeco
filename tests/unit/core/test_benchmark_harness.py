import unittest
from unittest.mock import patch

from benchmarks.report import render_markdown_report
from benchmarks.run import WORKLOAD_ORDER, resolve_workload_names


class BenchmarkHarnessTests(unittest.TestCase):
    def test_resolve_workload_names_returns_default_order_when_not_filtered(self):
        self.assertEqual(resolve_workload_names(None), WORKLOAD_ORDER)

    def test_resolve_workload_names_preserves_requested_order_and_deduplicates(self):
        self.assertEqual(
            resolve_workload_names(
                ["sort_shape_diagnostics", "predicate_diagnostics", "sort_shape_diagnostics"]
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

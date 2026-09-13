import unittest

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

from benchmarks._subject import require_imported_subject, resolve_subject_root
from benchmarks.contracts import REPORT_SCHEMA, compare_reports, validate_report
from benchmarks.engines.mongoeco_async import MongoecoSQLiteAsyncEngine
from benchmarks.engines.mongoeco_mem import MongoecoMemoryEngine
from benchmarks.engines.mongoeco_sql import MongoecoSQLEngine
from benchmarks.report import main as report_main, render_markdown_report
from benchmarks.run import (
    SKIPPED_WORKLOADS_KEY,
    WORKLOAD_ORDER,
    _run_engine_workloads,
    _summarize_task_samples,
    main as benchmark_main,
    resolve_workload_names,
)
from benchmarks.runners.workloads import (
    _ann_outcome_contract,
    _augment_search_documents,
    _outcome_sha256,
    _summarize_aggregate_explain,
    aggregation_spill_diagnostics,
)


class BenchmarkHarnessTests(unittest.TestCase):
    @staticmethod
    def _report(*, wall_time: float = 1.0) -> dict[str, object]:
        return {
            "schema": REPORT_SCHEMA,
            "environment": {
                "python": "3.13.0",
                "pythonImplementation": "CPython",
                "platform": "test-platform",
                "machine": "test-machine",
                "sqlite": "3.50.0",
                "jsonBackend": "stdlib",
            },
            "source": {
                "gitRevision": "abc123",
                "gitDirty": False,
                "mongoecoModule": str(
                    Path("subject/src/mongoeco/__init__.py").resolve()
                ),
                "harnessSha256": "sha256:harness",
                "datasetSha256": "sha256:dataset",
            },
            "config": {
                "size": 100,
                "warmup": 1,
                "repetitions": 5,
                "workloads": ["lookup"],
                "rssPeakSamplingIntervalMs": 5.0,
            },
            "results": {
                "memory-sync": {
                    "lookup": {
                        "point": {
                            "repetitions": 5,
                            "wall_time_mean_sec": wall_time,
                            "wall_time_median_sec": wall_time,
                            "wall_time_min_sec": wall_time,
                            "wall_time_max_sec": wall_time,
                            "cpu_user_mean_sec": 0.5,
                            "cpu_sys_mean_sec": 0.0,
                            "rss_delta_mean_mb": 0.0,
                            "rss_peak_max_mb": 1.0,
                            "metadata": {
                                "outcome_oracle": "exact-output",
                                "outcome_sha256": "sha256:stable-outcome",
                            },
                        }
                    }
                }
            },
        }

    def test_benchmark_contract_rejects_missing_mandatory_scenario(self):
        report = self._report()
        report["results"]["memory-sync"].pop("lookup")

        issues = validate_report(report)

        self.assertTrue(any("missing workload 'lookup'" in issue for issue in issues))

    def test_subject_root_requires_a_real_source_package(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(SystemExit, "must contain"):
                resolve_subject_root(
                    ["benchmark", "--subject-root", str(root)],
                )

            package = root / "src" / "mongoeco"
            package.mkdir(parents=True)
            package.joinpath("__init__.py").touch()
            self.assertEqual(
                resolve_subject_root(
                    ["benchmark", f"--subject-root={root}"],
                ),
                root.resolve(),
            )

    def test_subject_root_rejects_a_different_import_origin(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            package = root / "src" / "mongoeco"
            package.mkdir(parents=True)
            module = package / "__init__.py"
            module.touch()

            self.assertEqual(
                require_imported_subject(root, str(module)),
                module.resolve(),
            )
            with self.assertRaisesRegex(SystemExit, "did not provide"):
                require_imported_subject(
                    root,
                    str(root.parent / "other" / "mongoeco" / "__init__.py"),
                )

    def test_benchmark_contract_rejects_sync_async_result_drift(self):
        report = self._report()
        report["results"]["memory-async"] = deepcopy(report["results"]["memory-sync"])
        report["results"]["memory-async"]["lookup"]["point"]["metadata"][
            "outcome_sha256"
        ] = "sha256:async-drift"

        issues = validate_report(report)

        self.assertTrue(
            any("sync and async outcomes differ" in issue for issue in issues)
        )

    def test_benchmark_comparison_rejects_legacy_or_incompatible_artifacts(self):
        legacy = {"memory-sync": {}}
        current = self._report()

        self.assertTrue(compare_reports(current, legacy))
        incompatible = self._report()
        incompatible["config"]["size"] = 1000
        issues = compare_reports(current, incompatible)
        self.assertTrue(any("incomparable config.size" in issue for issue in issues))

    def test_benchmark_comparison_detects_seeded_wall_regression(self):
        baseline = self._report(wall_time=1.0)
        current = self._report(wall_time=1.25)

        issues = compare_reports(
            current,
            baseline,
            max_wall_regression_percent=10.0,
        )

        self.assertTrue(any("25.0% exceeds 10.0%" in issue for issue in issues))

    def test_benchmark_comparison_detects_changed_outcome(self):
        baseline = self._report()
        current = self._report()
        current["results"]["memory-sync"]["lookup"]["point"]["metadata"][
            "outcome_sha256"
        ] = "sha256:changed-outcome"

        issues = compare_reports(current, baseline)

        self.assertTrue(
            any("outcome differs from baseline" in issue for issue in issues)
        )

    def test_search_benchmark_embeddings_are_deterministic_and_unique(self):
        first = [{"city": "Madrid"} for _ in range(8)]
        second = [{"city": "Madrid"} for _ in range(8)]

        _augment_search_documents(first)
        _augment_search_documents(second)

        first_embeddings = [tuple(document["embedding"]) for document in first]
        self.assertEqual(first, second)
        self.assertEqual(len(set(first_embeddings)), len(first_embeddings))

    def test_outcome_digest_is_order_independent_for_mapping_keys(self):
        first = {"a": [1, 2], "b": {"value": True}}
        second = {"b": {"value": True}, "a": [1, 2]}

        self.assertEqual(_outcome_sha256(first), _outcome_sha256(second))
        self.assertNotEqual(_outcome_sha256(first), _outcome_sha256({"a": [2, 1]}))

    def test_aggregate_summary_retains_bounded_execution_evidence(self):
        summary = _summarize_aggregate_explain(
            {
                "engine_plan": {"engine": "memory", "strategy": "scan"},
                "remaining_pipeline": [{"$group": {"_id": "$kind"}}],
                "pushdown": {
                    "incrementalGroupInput": True,
                    "partitionedGroupStateCandidate": True,
                    "streamingGroupOutputCandidate": True,
                    "incrementalSortInput": False,
                    "streamingSortOutput": True,
                },
            }
        )

        self.assertTrue(summary["incremental_group_input"])
        self.assertTrue(summary["partitioned_group_state_candidate"])
        self.assertTrue(summary["streaming_group_output_candidate"])
        self.assertFalse(summary["incremental_sort_input"])
        self.assertTrue(summary["streaming_sort_output"])

    def test_sqlite_benchmark_adapters_propagate_spill_threshold(self):
        sync_adapter = MongoecoSQLEngine(spill_threshold=17)
        with (
            patch("mongoeco.engines.sqlite.SQLiteEngine") as sync_engine,
            patch("benchmarks.engines.mongoeco_sql.MongoClient"),
        ):
            sync_adapter.setup()
            sync_engine.assert_called_once_with(
                path=sync_adapter.db_path,
                aggregation_spill_threshold=17,
            )
        sync_adapter.teardown()

        async_adapter = MongoecoSQLiteAsyncEngine(spill_threshold=23)
        with patch("mongoeco.engines.sqlite.SQLiteEngine") as async_engine:
            async_adapter._build_engine()
            async_engine.assert_called_once_with(
                path=async_adapter.db_path,
                aggregation_spill_threshold=23,
            )
        async_adapter.teardown()

    def test_aggregation_spill_diagnostics_crosses_only_high_cardinality_limit(self):
        results = aggregation_spill_diagnostics(
            MongoecoMemoryEngine(spill_threshold=10),
            12,
        )

        low_metadata = results["group_low_cardinality_first"]["metadata"]
        high_metadata = results["group_high_cardinality_first"]["metadata"]
        self.assertFalse(low_metadata["spill_expected"])
        self.assertTrue(high_metadata["spill_expected"])
        self.assertLessEqual(low_metadata["expected_group_cardinality"], 10)
        self.assertEqual(high_metadata["expected_group_cardinality"], 12)
        self.assertEqual(
            low_metadata["outcome_oracle"],
            high_metadata["outcome_oracle"],
        )

    def test_ann_oracle_allows_candidate_variation_but_rejects_duplicates(self):
        first = [[{"_id": 1, "score": 1.0}, {"_id": 2, "score": 0.9}]]
        second = [[{"_id": 3, "score": 0.8}, {"_id": 4, "score": 0.7}]]

        self.assertEqual(
            _outcome_sha256(_ann_outcome_contract(first)),
            _outcome_sha256(_ann_outcome_contract(second)),
        )
        with self.assertRaisesRegex(ValueError, "duplicate identities"):
            _ann_outcome_contract([[{"_id": 1}, {"_id": 1}]])

    def test_task_summary_rejects_unstable_outcomes(self):
        sample = {
            "wall_time_sec": 1.0,
            "cpu_user_sec": 0.5,
            "cpu_sys_sec": 0.0,
            "rss_delta_mb": 0.0,
            "rss_peak_mb": 1.0,
        }

        with self.assertRaisesRegex(RuntimeError, "outcome changed"):
            _summarize_task_samples(
                [
                    {
                        **sample,
                        "metadata": {
                            "outcome_oracle": "exact-output",
                            "outcome_sha256": "sha256:first",
                        },
                    },
                    {
                        **sample,
                        "metadata": {
                            "outcome_oracle": "exact-output",
                            "outcome_sha256": "sha256:second",
                        },
                    },
                ]
            )

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

    def test_spill_diagnostics_require_an_owned_adapter_capability(self):
        engine = Mock(benchmark_capabilities=frozenset({"aggregation"}))

        with patch("benchmarks.run.load_engine", return_value=engine):
            results = _run_engine_workloads(
                "external",
                100_000,
                1,
                5,
                workload_names=("aggregation_spill_diagnostics",),
            )

        self.assertEqual(
            results[SKIPPED_WORKLOADS_KEY],
            {
                "aggregation_spill_diagnostics": (
                    "adapter lacks benchmark capabilities: "
                    "aggregation-spill-diagnostics"
                )
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

    def test_resolve_workload_names_supports_aggregation_spill_diagnostics(self):
        self.assertEqual(
            resolve_workload_names(["aggregation_spill_diagnostics"]),
            ("aggregation_spill_diagnostics",),
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

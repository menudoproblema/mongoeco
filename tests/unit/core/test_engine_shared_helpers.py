import datetime
import unittest

from mongoeco.engines._shared_namespace_admin import (
    merge_profile_collection_names,
    merge_profile_database_names,
    namespace_collection_names,
    namespace_collection_options,
    namespace_database_names,
    namespace_exists,
)
from mongoeco.engines._shared_search_admin import (
    ensure_search_index_query_supported,
    normalize_search_index_definition,
    search_index_not_found,
)
from mongoeco.engines._shared_ttl import (
    coerce_ttl_datetime,
    document_expired_by_ttl,
    ttl_expiration_datetime,
)
from mongoeco.engines._runtime_metrics import LocalRuntimeMetrics
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.errors import CollectionInvalid, OperationFailure
from mongoeco.types import EngineIndexRecord, SearchIndexDefinition
from mongoeco.core.search import SearchTextQuery, SearchVectorQuery


class SharedTtlHelperTests(unittest.TestCase):
    def test_ttl_helpers_handle_naive_aware_and_non_date_values(self):
        naive = datetime.datetime(2026, 4, 1, 12, 0)
        aware = datetime.datetime(2026, 4, 1, 12, 0, tzinfo=datetime.timezone.utc)

        self.assertEqual(coerce_ttl_datetime(naive), aware)
        self.assertEqual(coerce_ttl_datetime(aware), aware)
        self.assertIsNone(coerce_ttl_datetime("nope"))

    def test_ttl_expiration_datetime_uses_oldest_valid_candidate(self):
        expires_at = ttl_expiration_datetime(
            [
                "skip",
                datetime.datetime(2026, 4, 2, 12, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 4, 1, 12, 0),
            ],
            expire_after_seconds=60,
        )

        self.assertEqual(
            expires_at,
            datetime.datetime(2026, 4, 1, 12, 1, tzinfo=datetime.timezone.utc),
        )
        self.assertIsNone(ttl_expiration_datetime(["skip"], expire_after_seconds=60))
        self.assertIsNone(ttl_expiration_datetime([datetime.datetime(2026, 4, 1, 12, 0)], expire_after_seconds=None))

    def test_document_expired_by_ttl_uses_extract_values_and_ignores_non_dates(self):
        index = EngineIndexRecord(
            name="expires_at_1",
            fields=["expires_at"],
            key=[("expires_at", 1)],
            unique=False,
            expire_after_seconds=60,
        )
        document = {"expires_at": ["skip", datetime.datetime(2026, 4, 1, 12, 0)]}

        self.assertTrue(
            document_expired_by_ttl(
                document,
                index,
                now=datetime.datetime(2026, 4, 1, 12, 2, tzinfo=datetime.timezone.utc),
                extract_values=lambda payload, field: payload[field],
            )
        )


class SharedNamespaceAdminTests(unittest.TestCase):
    def test_namespace_helpers_collect_names_and_options(self):
        profiler = EngineProfiler("memory")
        profiler.set_level("db", 1)
        storage = {"db": {"docs": {}}}
        indexes = {"db": {"docs": []}}
        search_indexes = {"db": {"search_docs": []}}
        collections = {"db": {"docs", "meta"}}
        options = {"db": {"meta": {"capped": True}}}

        self.assertEqual(
            namespace_database_names(
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
            ),
            ["db"],
        )
        self.assertEqual(
            namespace_collection_names(
                "db",
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
            ),
            ["docs", "meta", "search_docs"],
        )
        self.assertTrue(
            namespace_exists(
                "db",
                "docs",
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
            )
        )
        self.assertEqual(
            namespace_collection_options(
                "db",
                "meta",
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
                profiler=profiler,
                profile_collection_name="system.profile",
            ),
            {"capped": True},
        )
        self.assertEqual(
            namespace_collection_options(
                "db",
                "system.profile",
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
                profiler=profiler,
                profile_collection_name="system.profile",
            ),
            {},
        )
        with self.assertRaises(CollectionInvalid):
            namespace_collection_options(
                "db",
                "missing",
                storage=storage,
                indexes=indexes,
                search_indexes=search_indexes,
                collections=collections,
                options=options,
                profiler=profiler,
                profile_collection_name="system.profile",
            )

        self.assertEqual(merge_profile_database_names(["db"], profiler), ["db"])
        self.assertEqual(
            merge_profile_collection_names(
                ["docs"],
                db_name="db",
                profiler=profiler,
                profile_collection_name="system.profile",
            ),
            ["docs", "system.profile"],
        )


class RuntimeMetricsTests(unittest.TestCase):
    def test_runtime_metrics_ignore_non_positive_and_unknown_operations(self):
        metrics = LocalRuntimeMetrics()

        metrics.record("query", amount=0)
        metrics.record("unknown", amount=2)
        metrics.record("query", amount=2)

        self.assertEqual(metrics.snapshot()["query"], 2)


class SharedSearchAdminTests(unittest.TestCase):
    def test_search_admin_helpers_normalize_and_validate_support(self):
        definition = normalize_search_index_definition(
            SearchIndexDefinition(
                {"mappings": {"dynamic": True}},
                name="search_idx",
                index_type="search",
            )
        )
        self.assertEqual(definition.name, "search_idx")
        self.assertEqual(str(search_index_not_found("missing")), "search index not found with name [missing]")

        ensure_search_index_query_supported(
            definition,
            SearchTextQuery(index_name="search_idx", raw_query="ada", terms=("ada",), paths=None),
            ready=True,
        )
        with self.assertRaisesRegex(OperationFailure, "is not ready yet"):
            ensure_search_index_query_supported(
                definition,
                SearchTextQuery(index_name="search_idx", raw_query="ada", terms=("ada",), paths=None),
                ready=False,
            )
        with self.assertRaisesRegex(OperationFailure, "does not support \\$vectorSearch"):
            ensure_search_index_query_supported(
                definition,
                SearchVectorQuery(index_name="search_idx", path="embedding", query_vector=[0.1], limit=5, num_candidates=None),
                ready=True,
            )

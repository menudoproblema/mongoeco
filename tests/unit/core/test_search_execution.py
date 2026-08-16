import unittest
import warnings

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import patch

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search import compile_search_stage
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_planning import (
    SearchPipelinePlan,
    SearchPipelineStrategy,
    SearchWindow,
)
from mongoeco.core.search_models import (
    SearchCollectorPlan,
    SearchCountResult,
    SearchDegradation,
    SearchExecutionMetric,
    SearchExecutionMode,
    SearchExecutionOutcome,
    SearchExecutionPhase,
    SearchExecutionState,
    SearchExecutionTrace,
    SearchExplainVerbosity,
    SearchFacetBucket,
    SearchFacetDefinition,
    SearchFacetResult,
    SearchHighlightPassage,
    SearchHighlightSegment,
    SearchHighlightSpan,
    SearchHit,
    SearchMetadata,
    SearchMetricAvailability,
    SearchMetricDomain,
    SearchMetricExactness,
    SearchMetricName,
    SearchMetricOrigin,
)
from mongoeco.engines._sqlite_search_runtime import _eligible_collector_options
from mongoeco.engines.adapter import adapt_engine
from mongoeco.engines.capabilities import (
    EngineCapabilities,
    SearchEngineCapabilities,
    validate_engine_contract,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import QueryPlanExplanation, SearchIndexDefinition

from tests.support import ENGINE_FACTORIES, open_engine


def _request() -> SearchRequest:
    specification = {
        "index": "by_text",
        "text": {"query": "ada", "path": "title"},
    }
    return SearchRequest(
        operator="$search",
        specification=specification,
        query=compile_search_stage("$search", specification),
        mode=SearchExecutionMode.HITS,
        operation_context=OperationContext.create(
            dialect=MONGODB_DIALECT_70,
        ),
        downstream_filter_spec={"kind": "note"},
    )


class _NativeSearchEngine:
    capabilities = EngineCapabilities(
        batch_inserts=False,
        explicit_read_snapshots=False,
        search=SearchEngineCapabilities(),
    )

    def __init__(self) -> None:
        self.request = None

    async def execute_search(self, _db_name, _coll_name, request):
        self.request = request
        return SearchExecutionOutcome.from_documents(
            [{"_id": 1}],
            backend="native",
        )

    async def insert_document(self, *_args, **_kwargs): ...
    async def get_document(self, *_args, **_kwargs): ...
    async def count_find_semantics(self, *_args, **_kwargs): ...
    async def update_with_operation(self, *_args, **_kwargs): ...
    async def delete_with_operation(self, *_args, **_kwargs): ...
    async def merge_document(self, *_args, **_kwargs): ...
    def scan_find_semantics(self, *_args, **_kwargs): ...


class _LegacySearchEngine:
    def __init__(self) -> None:
        self.call = None

    async def search_documents(self, *args, **kwargs):
        self.call = (args, kwargs)
        return [{"_id": "legacy"}]


def _explanation() -> QueryPlanExplanation:
    return QueryPlanExplanation(
        engine="test",
        strategy="search",
        plan="test-plan",
        sort=None,
        skip=0,
        limit=None,
        hint=None,
        hinted_index="by_text",
        comment=None,
        max_time_ms=None,
    )


class SearchExecutionContractTests(unittest.IsolatedAsyncioTestCase):
    def test_sqlite_collector_eligibility_declines_non_collector_queries(
        self,
    ) -> None:
        vector_query = compile_search_stage(
            "$vectorSearch",
            {
                "index": "by_vector",
                "path": "embedding",
                "queryVector": [1.0],
                "numCandidates": 1,
                "limit": 1,
            },
        )

        self.assertIsNone(_eligible_collector_options(vector_query))
        self.assertIsNone(_eligible_collector_options(_request().query))

    def test_request_owns_nested_input(self) -> None:
        specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": ["title"]},
        }
        downstream_filter = {"kind": {"$in": ["note"]}}
        request = SearchRequest(
            operator="$search",
            specification=specification,
            query=compile_search_stage("$search", specification),
            mode=SearchExecutionMode.HITS,
            operation_context=OperationContext.create(
                dialect=MONGODB_DIALECT_70,
            ),
            downstream_filter_spec=downstream_filter,
        )

        specification["text"]["path"].append("body")
        downstream_filter["kind"]["$in"].append("draft")

        self.assertEqual(request.specification["text"]["path"], ["title"])
        self.assertEqual(
            request.downstream_filter_spec,
            {"kind": {"$in": ["note"]}},
        )
        with self.assertRaises(TypeError):
            request.specification["index"] = "other"

    def test_request_validates_every_public_boundary_argument(self) -> None:
        request = _request()

        for field_name, invalid_value, error in (
            ("operator", "$invalid", ValueError),
            ("runtime_operator", "$searchMeta", ValueError),
            ("mode", "hits", TypeError),
            ("operation_context", object(), TypeError),
            ("max_time_ms", True, ValueError),
            ("max_time_ms", 0, ValueError),
            ("result_limit_hint", False, ValueError),
            ("result_limit_hint", -1, ValueError),
        ):
            with (
                self.subTest(field=field_name, value=invalid_value),
                self.assertRaises(error),
            ):
                replace(request, **{field_name: invalid_value})

    def test_request_rejects_cross_field_execution_shape_drift(self) -> None:
        hit_request = _request()
        metadata_specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "count": {"type": "total"},
        }
        metadata_query = compile_search_stage("$searchMeta", metadata_specification)
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        invalid_factories = {
            "compiled-query": lambda: replace(
                hit_request,
                specification={
                    "index": "by_text",
                    "text": {"query": "grace", "path": "title"},
                },
            ),
            "metadata-mode": lambda: SearchRequest(
                operator="$searchMeta",
                specification=metadata_specification,
                query=metadata_query,
                mode=SearchExecutionMode.HITS,
                operation_context=context,
                runtime_operator="$search",
                runtime_specification=metadata_specification,
            ),
            "metadata-runtime": lambda: SearchRequest(
                operator="$searchMeta",
                specification=metadata_specification,
                query=metadata_query,
                mode=SearchExecutionMode.METADATA,
                operation_context=context,
            ),
            "hits-mode": lambda: replace(
                hit_request,
                mode=SearchExecutionMode.METADATA,
            ),
        }

        for name, factory in invalid_factories.items():
            with self.subTest(case=name), self.assertRaises(ValueError):
                factory()

    def test_request_owns_runtime_specification_and_resolves_effective_values(
        self,
    ) -> None:
        request = _request()
        self.assertEqual(request.effective_operator, "$search")
        self.assertIs(request.effective_specification, request.specification)

        runtime_specification = {"text": {"query": ["ada"], "path": "title"}}
        with self.assertRaisesRegex(ValueError, r"reserved for \$searchMeta"):
            replace(
                request,
                runtime_operator="$vectorSearch",
                runtime_specification=runtime_specification,
            )

    def test_search_value_objects_reject_invalid_contracts(self) -> None:
        for kwargs, error in (
            ({"name": "", "path": "kind"}, ValueError),
            ({"name": None, "path": ""}, ValueError),
            ({"name": None, "path": "kind", "facet_type": ""}, ValueError),
            ({"name": None, "path": "kind", "num_buckets": True}, ValueError),
            ({"name": None, "path": "kind", "include_meta": 1}, TypeError),
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(error):
                SearchFacetDefinition(**kwargs)

        for start, end in ((-1, 1), (1, 1), (2, 1)):
            with (
                self.subTest(start=start, end=end),
                self.assertRaises(ValueError),
            ):
                SearchHighlightSpan(start=start, end=end)

        with self.assertRaisesRegex(TypeError, "offsets"):
            SearchHighlightSpan(start=False, end=1)

    def test_search_result_models_reject_impossible_states(self) -> None:
        definition = SearchFacetDefinition(name=None, path="kind")
        bucket = SearchFacetBucket(value="note", count=2)
        segment = SearchHighlightSegment(
            segment_type="hit",
            value="Ada",
            start=0,
            end=3,
        )

        invalid_factories = (
            lambda: SearchCountResult(mode="total", value=1, exact=True, threshold=1),
            lambda: SearchFacetBucket(value="note", count=0),
            lambda: SearchFacetResult(
                definition=definition,
                buckets=(bucket,),
                distinct_value_count=0,
            ),
            lambda: SearchFacetResult(
                definition=definition,
                buckets=(bucket,),
                counted_value_count=1,
            ),
            lambda: SearchHighlightPassage(
                text="Eve",
                start=0,
                end=3,
                segments=(segment,),
            ),
            lambda: SearchCollectorPlan(
                backend="sqlite",
                pushed_down=True,
                candidate_exact=False,
            ),
            lambda: SearchExecutionTrace(
                backend="sqlite",
                collector_pushdown=True,
            ),
            lambda: SearchExecutionTrace(
                backend="sqlite",
                degradations=("fallback", "fallback"),
            ),
            lambda: SearchExecutionOutcome(
                hits=SearchExecutionOutcome.from_documents(
                    [{"_id": 1}],
                    backend="test",
                ).hits,
                metadata=SearchMetadata(
                    count=SearchCountResult(mode="total", value=1, exact=True),
                ),
            ),
        )
        for factory in invalid_factories:
            with (
                self.subTest(factory=factory),
                self.assertRaises(
                    (TypeError, ValueError),
                ),
            ):
                factory()

    def test_search_result_models_own_documents_and_nested_outputs(self) -> None:
        source = {"_id": 1, "nested": {"values": [1]}}
        outcome = SearchExecutionOutcome.from_documents([source], backend="test")
        facet_value = {"range": [1, 2]}
        bucket = SearchFacetBucket(value=facet_value, count=1)

        source["nested"]["values"].append(2)
        facet_value["range"].append(3)
        first_read = outcome.documents
        first_read[0]["nested"]["values"].append(3)

        self.assertEqual(outcome.documents, [{"_id": 1, "nested": {"values": [1]}}])
        self.assertEqual(bucket.value, {"range": [1, 2]})

    def test_search_result_models_validate_all_public_state_boundaries(self) -> None:
        unnamed = SearchFacetDefinition(name=None, path="kind")
        first_named = SearchFacetDefinition(name="first", path="kind")
        second_named = SearchFacetDefinition(name="second", path="kind")
        bucket = SearchFacetBucket(value="note", count=2)
        unnamed_result = SearchFacetResult(unnamed, (bucket,))
        first_result = SearchFacetResult(first_named, (bucket,))
        second_result = SearchFacetResult(second_named, (bucket,))
        segment = SearchHighlightSegment("hit", "Ada", 0, 3)
        valid_plan = SearchCollectorPlan(
            backend="sqlite",
            pushed_down=True,
            candidate_exact=True,
        )

        invalid_factories = {
            "count-mode": lambda: SearchCountResult(
                mode="unknown",
                value=1,
                exact=True,
            ),
            "count-value": lambda: SearchCountResult(
                mode="total",
                value=False,
                exact=True,
            ),
            "count-flags": lambda: SearchCountResult(
                mode="total",
                value=1,
                exact=1,
            ),
            "count-threshold": lambda: SearchCountResult(
                mode="lowerBound",
                value=1,
                exact=False,
                threshold=0,
            ),
            "count-capped": lambda: SearchCountResult(
                mode="lowerBound",
                value=1,
                exact=True,
                threshold=1,
                capped_by_threshold=True,
            ),
            "facet-definition": lambda: SearchFacetResult(object(), (bucket,)),
            "facet-buckets": lambda: SearchFacetResult(unnamed, [bucket]),
            "facet-distinct-type": lambda: SearchFacetResult(
                unnamed,
                (bucket,),
                distinct_value_count=False,
            ),
            "facet-counted-type": lambda: SearchFacetResult(
                unnamed,
                (bucket,),
                counted_value_count=-1,
            ),
            "segment-type-type": lambda: SearchHighlightSegment(1, "Ada", 0, 3),
            "segment-type-value": lambda: SearchHighlightSegment(
                "invalid",
                "Ada",
                0,
                3,
            ),
            "segment-value": lambda: SearchHighlightSegment("hit", 1, 0, 3),
            "segment-offset-type": lambda: SearchHighlightSegment(
                segment_type="hit",
                value="Ada",
                start=False,
                end=3,
            ),
            "segment-offset-order": lambda: SearchHighlightSegment(
                "hit",
                "Ada",
                3,
                2,
            ),
            "segment-offset-length": lambda: SearchHighlightSegment(
                "hit",
                "Ada",
                0,
                2,
            ),
            "passage-text": lambda: SearchHighlightPassage(1, 0, 1, (segment,)),
            "passage-offset-type": lambda: SearchHighlightPassage(
                text="Ada",
                start=False,
                end=3,
                segments=(segment,),
            ),
            "passage-offset-order": lambda: SearchHighlightPassage(
                "Ada",
                3,
                2,
                (segment,),
            ),
            "passage-offset-length": lambda: SearchHighlightPassage(
                "Ada",
                0,
                2,
                (segment,),
            ),
            "passage-segment-type": lambda: SearchHighlightPassage(
                "Ada",
                0,
                3,
                [segment],
            ),
            "passage-empty": lambda: SearchHighlightPassage("Ada", 0, 3, ()),
            "passage-gap": lambda: SearchHighlightPassage(
                "Ada",
                0,
                3,
                (SearchHighlightSegment("hit", "da", 1, 3),),
            ),
            "passage-incomplete": lambda: SearchHighlightPassage(
                "Ada",
                0,
                3,
                (SearchHighlightSegment("hit", "Ad", 0, 2),),
            ),
            "passage-values": lambda: SearchHighlightPassage(
                "Ada",
                0,
                3,
                (SearchHighlightSegment("hit", "Eve", 0, 3),),
            ),
            "metadata-count": lambda: SearchMetadata(count=object()),
            "metadata-facets": lambda: SearchMetadata(facets=[unnamed_result]),
            "metadata-mixed-names": lambda: SearchMetadata(
                facets=(unnamed_result, first_result),
            ),
            "metadata-duplicate-names": lambda: SearchMetadata(
                facets=(first_result, first_result),
            ),
            "metadata-multiple-unnamed": lambda: SearchMetadata(
                facets=(unnamed_result, unnamed_result),
            ),
            "plan-backend": lambda: SearchCollectorPlan(
                backend="",
                pushed_down=False,
                candidate_exact=None,
            ),
            "plan-pushdown-type": lambda: SearchCollectorPlan(
                backend="test",
                pushed_down=1,
                candidate_exact=True,
            ),
            "plan-exact-type": lambda: SearchCollectorPlan(
                backend="test",
                pushed_down=False,
                candidate_exact=1,
            ),
            "plan-strategy": lambda: SearchCollectorPlan(
                backend="test",
                pushed_down=False,
                candidate_exact=None,
                count_strategy="",
            ),
            "trace-backend": lambda: SearchExecutionTrace(""),
            "trace-count": lambda: SearchExecutionTrace("test", matched_count=False),
            "trace-executed-type": lambda: SearchExecutionTrace(
                "test",
                executed=1,
            ),
            "trace-collector-count": lambda: SearchExecutionTrace(
                "test",
                collector_count=-1,
            ),
            "trace-pushdown-type": lambda: SearchExecutionTrace(
                "test",
                collector_pushdown=1,
            ),
            "trace-collector-backend": lambda: SearchExecutionTrace(
                "test",
                collector_backend="",
            ),
            "trace-plan": lambda: SearchExecutionTrace("test", collector_plan=object()),
            "trace-degradations": lambda: SearchExecutionTrace(
                "test",
                degradations=["fallback"],
            ),
            "trace-pushdown-plan": lambda: SearchExecutionTrace(
                "test",
                collector_pushdown=True,
                collector_plan=SearchCollectorPlan(
                    backend="test",
                    pushed_down=False,
                    candidate_exact=True,
                ),
            ),
            "hit-document": lambda: SearchHit(document=[]),
            "outcome-hits": lambda: SearchExecutionOutcome(
                hits=[SearchHit({"_id": 1})]
            ),
            "outcome-metadata": lambda: SearchExecutionOutcome(metadata=object()),
            "outcome-trace": lambda: SearchExecutionOutcome(trace=object()),
        }

        for name, factory in invalid_factories.items():
            with self.subTest(case=name), self.assertRaises((TypeError, ValueError)):
                factory()

        metadata = SearchMetadata(facets=(first_result, second_result))
        self.assertEqual(len(metadata.facets), 2)
        trace = SearchExecutionTrace.from_explain_details(
            {
                "backend": "",
                "matchedCount": 2,
                "candidateCountBeforeTopK": 4,
                "documentsScanned": 5,
                "exactFallbackReason": "residual",
                "fallbackReason": "residual",
            },
            default_backend="memory",
        )
        self.assertEqual(trace.backend, "memory")
        self.assertEqual(trace.matched_count, 2)
        self.assertEqual(trace.query_matched_count, 2)
        self.assertEqual(trace.candidate_count, 4)
        self.assertEqual(trace.documents_scanned, 5)
        self.assertEqual(trace.degradations, ("memory.exact-fallback",))
        self.assertEqual(
            trace.degradation_details[0].message,
            "residual",
        )
        self.assertEqual(valid_plan.to_document()["backend"], "sqlite")

        planned = SearchExecutionTrace("test", executed=False)
        self.assertIs(planned.execution_state, SearchExecutionState.PLANNED)
        self.assertFalse(planned.executed)

    def test_search_trace_uses_typed_metrics_and_rejects_impossible_states(
        self,
    ) -> None:
        metric = SearchExecutionMetric(
            name=SearchMetricName.QUERY_MATCHED_COUNT,
            domain=SearchMetricDomain.QUERY,
            value=3,
        )
        trace = SearchExecutionTrace("memory", metrics=(metric,))

        self.assertEqual(trace.query_matched_count, 3)
        self.assertEqual(trace.matched_count, 3)
        self.assertEqual(trace.phases, (SearchExecutionPhase.QUERY,))
        serialized_metrics = trace.to_document()["metrics"]
        self.assertEqual(serialized_metrics[0], metric.to_document())
        self.assertEqual(
            {item["name"] for item in serialized_metrics},
            {name.value for name in SearchMetricName},
        )
        self.assertTrue(
            all(
                item["availability"] == "unavailable" for item in serialized_metrics[1:]
            ),
        )

        with self.assertRaisesRegex(ValueError, "domain"):
            SearchExecutionMetric(
                name=SearchMetricName.QUERY_MATCHED_COUNT,
                domain=SearchMetricDomain.RESULT,
                value=1,
            )
        with self.assertRaisesRegex(ValueError, "unknown exactness"):
            SearchExecutionMetric(
                name=SearchMetricName.CANDIDATE_COUNT,
                domain=SearchMetricDomain.CANDIDATE,
                value=None,
                exactness=SearchMetricExactness.EXACT,
                availability=SearchMetricAvailability.UNAVAILABLE,
            )
        with self.assertRaisesRegex(ValueError, "non-executed"):
            SearchExecutionTrace(
                "memory",
                execution_state=SearchExecutionState.PLANNED,
                metrics=(metric,),
            )
        with self.assertRaisesRegex(ValueError, "residual"):
            SearchExecutionTrace(
                "memory",
                downstream_filtered_count=1,
                phases=(SearchExecutionPhase.QUERY,),
            )

        with self.assertRaisesRegex(ValueError, "bound context"):
            SearchExecutionTrace(
                "memory",
                execution_state=SearchExecutionState.EXECUTED,
                context_bound=False,
                snapshot_captured=True,
            )
        with self.assertRaisesRegex(ValueError, "captured snapshot"):
            SearchExecutionTrace(
                "memory",
                operation_id="operation",
                execution_state=SearchExecutionState.EXECUTED,
                snapshot_captured=False,
            )
        with self.assertRaisesRegex(TypeError, "context_bound"):
            SearchExecutionTrace("memory", context_bound="yes")
        with self.assertRaisesRegex(ValueError, "requires operation_id"):
            SearchExecutionTrace(
                "memory",
                context_bound=True,
                snapshot_captured=True,
            )

    def test_search_trace_separates_collector_work_from_pipeline_output(self) -> None:
        trace = SearchExecutionTrace(
            "memory",
            operation_id="operation",
            snapshot_captured=True,
            collector_count=2,
            collector_document_count=5,
            pipeline_output_count=1,
        )

        self.assertEqual(trace.collector_count, 2)
        self.assertEqual(trace.collector_document_count, 5)
        self.assertEqual(trace.pipeline_output_count, 1)
        metrics = {metric.name: metric for metric in trace.metrics}
        self.assertEqual(metrics[SearchMetricName.COLLECTOR_COUNT].value, 2)
        self.assertEqual(
            metrics[SearchMetricName.COLLECTOR_DOCUMENT_COUNT].value,
            5,
        )
        self.assertEqual(metrics[SearchMetricName.PIPELINE_OUTPUT_COUNT].value, 1)
        self.assertEqual(
            trace.to_document()["executionContext"],
            {"bound": True, "snapshotCaptured": True},
        )

    def test_search_observability_rejects_invalid_typed_contracts(self) -> None:
        metric_cases = (
            lambda: SearchExecutionMetric(
                "queryMatchedCount", SearchMetricDomain.QUERY, 1
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT, "query", 1
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT,
                SearchMetricDomain.QUERY,
                1,
                exactness="exact",
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT,
                SearchMetricDomain.QUERY,
                1,
                origin="engine",
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT,
                SearchMetricDomain.QUERY,
                1,
                availability="available",
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT,
                SearchMetricDomain.QUERY,
                -1,
            ),
            lambda: SearchExecutionMetric(
                SearchMetricName.QUERY_MATCHED_COUNT,
                SearchMetricDomain.QUERY,
                1,
                exactness=SearchMetricExactness.UNKNOWN,
                availability=SearchMetricAvailability.UNAVAILABLE,
            ),
        )
        for factory in metric_cases:
            with (
                self.subTest(factory=factory),
                self.assertRaises((TypeError, ValueError)),
            ):
                factory()

        degradation_cases = (
            lambda: SearchDegradation("", "fallback"),
            lambda: SearchDegradation("search.fallback", ""),
            lambda: SearchDegradation(
                "search.fallback",
                "fallback",
                origin="engine",
            ),
        )
        for factory in degradation_cases:
            with (
                self.subTest(factory=factory),
                self.assertRaises((TypeError, ValueError)),
            ):
                factory()

        trace_cases = (
            lambda: SearchExecutionTrace("memory", operation_id=""),
            lambda: SearchExecutionTrace("memory", execution_state="executed"),
            lambda: SearchExecutionTrace("memory", executed="yes"),
            lambda: SearchExecutionTrace(
                "memory",
                executed=False,
                execution_state=SearchExecutionState.EXECUTED,
            ),
            lambda: SearchExecutionTrace("memory", collector_backend=""),
            lambda: SearchExecutionTrace("memory", metrics=("bad",)),
            lambda: SearchExecutionTrace("memory", phases=("query",)),
            lambda: SearchExecutionTrace(
                "memory",
                phases=(SearchExecutionPhase.QUERY, SearchExecutionPhase.QUERY),
            ),
            lambda: SearchExecutionTrace(
                "memory",
                metrics=(
                    SearchExecutionMetric(
                        SearchMetricName.QUERY_MATCHED_COUNT,
                        SearchMetricDomain.QUERY,
                        1,
                    ),
                )
                * 2,
            ),
            lambda: SearchExecutionTrace("memory", degradation_details=("bad",)),
            lambda: SearchExecutionTrace("memory", engine_details=[]),
            lambda: SearchExecutionTrace(
                "memory",
                execution_state=SearchExecutionState.PLANNED,
                phases=(SearchExecutionPhase.QUERY,),
            ),
            lambda: SearchExecutionTrace(
                "memory",
                collector_count=1,
                phases=(SearchExecutionPhase.QUERY,),
            ),
            lambda: SearchExecutionTrace(
                "memory",
                query_matched_count=1,
                returned_hit_count=2,
            ),
            lambda: SearchExecutionTrace(
                "memory",
                query_matched_count=2,
                metrics=(
                    SearchExecutionMetric(
                        SearchMetricName.QUERY_MATCHED_COUNT,
                        SearchMetricDomain.QUERY,
                        1,
                    ),
                ),
            ),
            lambda: SearchExecutionTrace(
                "memory",
                degradation_details=(
                    SearchDegradation("duplicate", "first"),
                    SearchDegradation("duplicate", "second"),
                ),
            ),
            lambda: SearchHit({"_id": 1}, runtime_metadata={}),
        )
        for factory in trace_cases:
            with (
                self.subTest(factory=factory),
                self.assertRaises((TypeError, ValueError)),
            ):
                factory()

        degradation = SearchDegradation(
            "search.fallback",
            "fallback",
            SearchMetricOrigin.SEMANTIC_CORE,
        )
        trace = SearchExecutionTrace(
            "memory",
            degradations=("legacy-code",),
            degradation_details=(degradation,),
        )
        self.assertEqual(
            trace.degradations,
            ("search.fallback", "legacy-code"),
        )
        residual = SearchExecutionTrace("memory", downstream_filtered_count=1)
        self.assertIn(SearchExecutionPhase.RESIDUAL_FILTER, residual.phases)

    def test_search_outcome_and_collector_plan_have_stable_public_shapes(
        self,
    ) -> None:
        outcome = SearchExecutionOutcome.from_documents(
            [{"_id": 1}, {"_id": 2}],
            backend="test",
        )
        plan = SearchCollectorPlan(
            backend="sqlite",
            pushed_down=True,
            candidate_exact=True,
            count_strategy="sql-limit",
        )

        self.assertEqual(outcome.documents, [{"_id": 1}, {"_id": 2}])
        self.assertEqual(outcome.trace.matched_count, 2)
        self.assertEqual(outcome.trace.returned_hit_count, 2)
        self.assertEqual(
            plan.to_document(),
            {
                "backend": "sqlite",
                "pushedDown": True,
                "candidateExact": True,
                "countStrategy": "sql-limit",
                "facetStrategy": None,
                "fallbackReason": None,
            },
        )

    def test_search_request_rejects_plans_that_diverge_from_execution(self) -> None:
        request = _request()
        plans = (
            (object(), {}),
            (SearchPipelinePlan(), {}),
            (
                SearchPipelinePlan(
                    strategy=SearchPipelineStrategy.DIRECT_WINDOW,
                    window=SearchWindow(limit=1),
                    result_limit_hint=1,
                    downstream_filter_spec={"kind": "note"},
                ),
                {},
            ),
            (
                SearchPipelinePlan(
                    strategy=SearchPipelineStrategy.PREFIX_ITERATIVE,
                    prefix_output_limit=2,
                    downstream_filter_spec={"kind": "note"},
                ),
                {},
            ),
            (
                SearchPipelinePlan(downstream_filter_spec={"kind": "note"}),
                {"result_limit_hint": 1},
            ),
        )

        for plan, overrides in plans:
            with self.subTest(plan=plan), self.assertRaises((TypeError, ValueError)):
                replace(request, pipeline_plan=plan, **overrides)

    def test_search_capabilities_validate_contract_version_and_types(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "contract version"):
            SearchEngineCapabilities(contract_version="search-v2")
        with self.assertRaisesRegex(TypeError, "metadata_collectors"):
            SearchEngineCapabilities(metadata_collectors=1)
        with self.assertRaisesRegex(ValueError, "similarities"):
            SearchEngineCapabilities(
                operators=frozenset({"$search", "$vectorSearch"}),
            )
        with self.assertRaisesRegex(ValueError, "operators"):
            SearchEngineCapabilities(operators={"$search"})  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "operators"):
            SearchEngineCapabilities(operators=frozenset({"$searchMeta"}))
        with self.assertRaisesRegex(ValueError, "similarities"):
            SearchEngineCapabilities(vector_similarities={"cosine"})  # type: ignore[arg-type]
        with self.assertRaisesRegex(ValueError, "similarities"):
            SearchEngineCapabilities(
                vector_similarities=frozenset({"manhattan"}),
            )
        with self.assertRaisesRegex(ValueError, "require"):
            SearchEngineCapabilities(
                vector_similarities=frozenset({"cosine"}),
            )
        with self.assertRaisesRegex(TypeError, "search must"):
            EngineCapabilities(search=object())

    def test_search_request_owns_zero_window_and_rejects_unsafe_prefilters(
        self,
    ) -> None:
        specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
        }
        zero_window = SearchRequest(
            operator="$search",
            specification=specification,
            query=compile_search_stage("$search", specification),
            mode=SearchExecutionMode.HITS,
            operation_context=OperationContext.create(dialect=MONGODB_DIALECT_70),
            result_limit_hint=0,
        )
        self.assertEqual(zero_window.result_limit_hint, 0)

        vector_specification = {
            "index": "by_vector",
            "path": "embedding",
            "queryVector": [1.0],
            "numCandidates": 1,
            "limit": 1,
        }
        with self.assertRaisesRegex(ValueError, "downstream"):
            SearchRequest(
                operator="$vectorSearch",
                specification=vector_specification,
                query=compile_search_stage("$vectorSearch", vector_specification),
                mode=SearchExecutionMode.HITS,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                downstream_filter_spec={"kind": "note"},
            )

        highlight_specification = {
            **specification,
            "highlight": {"path": "title"},
        }
        with self.assertRaisesRegex(ValueError, "metadata"):
            SearchRequest(
                operator="$search",
                specification=highlight_specification,
                query=compile_search_stage("$search", highlight_specification),
                mode=SearchExecutionMode.HITS,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                downstream_filter_spec={"kind": "note"},
            )

    def test_declared_search_capability_requires_primitive(self) -> None:
        engine = _NativeSearchEngine()
        engine.execute_search = None

        with self.assertRaisesRegex(TypeError, "execute_search"):
            validate_engine_contract(engine, engine.capabilities)

    def test_declared_search_explain_capability_requires_primitive(
        self,
    ) -> None:
        engine = _NativeSearchEngine()
        engine.capabilities = EngineCapabilities(
            batch_inserts=False,
            explicit_read_snapshots=False,
            search=SearchEngineCapabilities(explain_verbosity=True),
        )

        with self.assertRaisesRegex(TypeError, "explain_search"):
            validate_engine_contract(engine, engine.capabilities)

    async def test_native_search_returns_stable_outcome(self) -> None:
        engine = _NativeSearchEngine()
        request = _request()

        outcome = await adapt_engine(engine).execute_search(
            "db",
            "items",
            request,
        )

        self.assertIs(engine.request, request)
        self.assertEqual(outcome.documents, [{"_id": 1}])
        self.assertEqual(outcome.trace.backend, "native")

    async def test_adapter_enforces_declared_optional_search_capabilities(self) -> None:
        engine = _NativeSearchEngine()
        metadata_specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "count": {"type": "total"},
        }
        metadata_request = SearchRequest(
            operator="$searchMeta",
            specification=metadata_specification,
            query=compile_search_stage("$searchMeta", metadata_specification),
            mode=SearchExecutionMode.METADATA,
            operation_context=OperationContext.create(dialect=MONGODB_DIALECT_70),
            runtime_operator="$search",
            runtime_specification=metadata_specification,
        )
        highlight_specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "highlight": {"path": "title"},
        }
        highlight_request = SearchRequest(
            operator="$search",
            specification=highlight_specification,
            query=compile_search_stage("$search", highlight_specification),
            mode=SearchExecutionMode.HITS,
            operation_context=OperationContext.create(dialect=MONGODB_DIALECT_70),
        )

        with self.assertRaisesRegex(OperationFailure, "metadata collector"):
            await adapt_engine(engine).execute_search(
                "db",
                "items",
                metadata_request,
            )
        with self.assertRaisesRegex(OperationFailure, "highlight"):
            await adapt_engine(engine).execute_search(
                "db",
                "items",
                highlight_request,
            )

        vector_specification = {
            "index": "by_vector",
            "path": "embedding",
            "queryVector": [1.0],
            "numCandidates": 1,
            "limit": 1,
        }
        vector_request = SearchRequest(
            operator="$vectorSearch",
            specification=vector_specification,
            query=compile_search_stage("$vectorSearch", vector_specification),
            mode=SearchExecutionMode.HITS,
            operation_context=OperationContext.create(dialect=MONGODB_DIALECT_70),
        )
        with self.assertRaisesRegex(OperationFailure, r"\$vectorSearch"):
            await adapt_engine(engine).execute_search("db", "items", vector_request)

        engine.capabilities = replace(
            engine.capabilities,
            search=SearchEngineCapabilities(
                operators=frozenset({"$search", "$vectorSearch"}),
                vector_similarities=frozenset({"dotProduct"}),
            ),
        )
        with self.assertRaisesRegex(OperationFailure, "similarity"):
            await adapt_engine(engine).execute_search("db", "items", vector_request)
        self.assertIsNone(engine.request)

    async def test_adapter_rejects_invalid_search_inputs_and_returns(
        self,
    ) -> None:
        request = _request()
        engine = _NativeSearchEngine()

        with self.assertRaisesRegex(TypeError, "SearchRequest"):
            await adapt_engine(engine).execute_search("db", "items", object())

        async def invalid_outcome(*_args, **_kwargs):
            return [{"_id": 1}]

        engine.execute_search = invalid_outcome
        with self.assertRaisesRegex(TypeError, "SearchExecutionOutcome"):
            await adapt_engine(engine).execute_search("db", "items", request)

        engine.capabilities = replace(
            engine.capabilities,
            search=SearchEngineCapabilities(metadata_collectors=True),
        )
        metadata_specification = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "count": {"type": "total"},
        }
        metadata_request = SearchRequest(
            operator="$searchMeta",
            specification=metadata_specification,
            query=compile_search_stage("$searchMeta", metadata_specification),
            mode=SearchExecutionMode.METADATA,
            operation_context=OperationContext.create(dialect=MONGODB_DIALECT_70),
            runtime_operator="$search",
            runtime_specification=metadata_specification,
        )

        async def hits_in_metadata_mode(*_args, **_kwargs):
            return SearchExecutionOutcome.from_documents(
                [{"_id": "unexpected"}],
                backend="invalid",
            )

        engine.execute_search = hits_in_metadata_mode
        with self.assertRaisesRegex(RuntimeError, "cannot contain hits"):
            await adapt_engine(engine).execute_search(
                "db",
                "items",
                metadata_request,
            )

        async def collectors_in_hits_mode(*_args, **_kwargs):
            return SearchExecutionOutcome(
                metadata=SearchMetadata(
                    count=SearchCountResult(
                        value=1,
                        mode="total",
                        exact=True,
                    ),
                ),
            )

        engine.execute_search = collectors_in_hits_mode
        with self.assertRaisesRegex(RuntimeError, "collector metadata"):
            await adapt_engine(engine).execute_search("db", "items", request)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with self.assertRaises(OperationFailure):
                await adapt_engine(object()).execute_search(
                    "db",
                    "items",
                    request,
                )

        legacy = _LegacySearchEngine()

        async def invalid_documents(*_args, **_kwargs):
            return [1]

        legacy.search_documents = invalid_documents
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with self.assertRaisesRegex(TypeError, "list of documents"):
                await adapt_engine(legacy).execute_search(
                    "db",
                    "items",
                    request,
                )

    async def test_adapter_isolates_native_and_legacy_explain_contracts(
        self,
    ) -> None:
        request = _request()
        native = _NativeSearchEngine()
        native.capabilities = replace(
            native.capabilities,
            search=SearchEngineCapabilities(explain_verbosity=True),
        )

        async def invalid_explanation(*_args, **_kwargs):
            return {}

        native.explain_search = invalid_explanation
        adapter = adapt_engine(native)
        with self.assertRaisesRegex(TypeError, "SearchRequest"):
            await adapter.explain_search(
                "db",
                "items",
                object(),
                SearchExplainVerbosity.QUERY_PLANNER,
            )
        with self.assertRaisesRegex(TypeError, "SearchExplainVerbosity"):
            await adapter.explain_search("db", "items", request, "queryPlanner")
        with self.assertRaisesRegex(TypeError, "QueryPlanExplanation"):
            await adapter.explain_search(
                "db",
                "items",
                request,
                SearchExplainVerbosity.EXECUTION_STATS,
            )

        legacy_without_explain = _LegacySearchEngine()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            planner = await adapt_engine(legacy_without_explain).explain_search(
                "db",
                "items",
                request,
                SearchExplainVerbosity.QUERY_PLANNER,
            )
        self.assertEqual(planner.plan, "unsupported-search-engine")

        legacy = _LegacySearchEngine()

        async def explain(*_args, **_kwargs):
            return _explanation()

        legacy.explain_search_documents = explain
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            adapter = adapt_engine(legacy)
            planner = await adapter.explain_search(
                "db",
                "items",
                request,
                SearchExplainVerbosity.QUERY_PLANNER,
            )
            execution = await adapter.explain_search(
                "db",
                "items",
                request,
                SearchExplainVerbosity.EXECUTION_STATS,
            )
        self.assertEqual(planner.plan, "legacy-search-contract")
        self.assertEqual(execution, _explanation())

        legacy.explain_search_documents = invalid_explanation
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with self.assertRaisesRegex(TypeError, "legacy Search explain"):
                await adapt_engine(legacy).explain_search(
                    "db",
                    "items",
                    request,
                    SearchExplainVerbosity.EXECUTION_STATS,
                )

    async def test_legacy_list_return_is_isolated_in_adapter(self) -> None:
        engine = _LegacySearchEngine()
        request = _request()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            outcome = await adapt_engine(engine).execute_search(
                "db",
                "items",
                request,
            )

        self.assertEqual(outcome.documents, [{"_id": "legacy"}])
        _args, kwargs = engine.call
        self.assertIs(
            kwargs["context"],
            request.operation_context.session,
        )

    async def test_metadata_mode_returns_collectors_without_public_hits(
        self,
    ) -> None:
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    for document in (
                        {"_id": 1, "title": "Ada", "kind": ["note", "note"]},
                        {"_id": 2, "title": "Ada", "kind": "reference"},
                    ):
                        await engine.insert_document(
                            "db",
                            "items",
                            document,
                            operation_context=OperationContext.create(
                                dialect=MONGODB_DIALECT_70,
                            ),
                        )
                    await engine.create_search_index(
                        "db",
                        "items",
                        SearchIndexDefinition(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {
                                        "title": {"type": "string"},
                                        "kind": {"type": "token"},
                                    },
                                },
                            },
                            name="by_text",
                        ),
                    )
                    specification = {
                        "index": "by_text",
                        "text": {"query": "ada", "path": "title"},
                        "count": {"type": "total"},
                        "facet": {
                            "path": "kind",
                            "type": "token",
                            "numBuckets": 5,
                        },
                    }
                    request = SearchRequest(
                        operator="$searchMeta",
                        specification=specification,
                        query=compile_search_stage(
                            "$searchMeta",
                            specification,
                        ),
                        mode=SearchExecutionMode.METADATA,
                        operation_context=OperationContext.create(
                            dialect=MONGODB_DIALECT_70,
                        ),
                        runtime_operator="$search",
                        runtime_specification=specification,
                    )

                    outcome = await adapt_engine(engine).execute_search(
                        "db",
                        "items",
                        request,
                    )

                    self.assertEqual(outcome.hits, ())
                    self.assertEqual(outcome.metadata.count.value, 2)
                    self.assertEqual(
                        [
                            (bucket.value, bucket.count)
                            for bucket in outcome.metadata.facets[0].buckets
                        ],
                        [("note", 1), ("reference", 1)],
                    )
                    if engine_name == "sqlite":
                        self.assertTrue(outcome.trace.collector_pushdown)
                        self.assertEqual(
                            outcome.trace.collector_backend,
                            "sqlite",
                        )
                        self.assertEqual(outcome.trace.documents_scanned, 0)
                        self.assertTrue(
                            outcome.trace.collector_plan.pushed_down,
                        )
                        self.assertFalse(engine._connection.in_transaction)

                    with patch.object(
                        engine,
                        "execute_search",
                        wraps=engine.execute_search,
                    ) as execute_search:
                        explanation = await adapt_engine(engine).explain_search(
                            "db",
                            "items",
                            request,
                            SearchExplainVerbosity.EXECUTION_STATS,
                        )

                    self.assertEqual(execute_search.await_count, 1)
                    execution_stats = explanation.details["executionStats"]
                    self.assertEqual(execution_stats["matchedCount"], 2)
                    self.assertEqual(execution_stats["collectorCount"], 2)
                    self.assertEqual(
                        explanation.details["collectorPlan"],
                        execution_stats["collectorPlan"],
                    )
                    self.assertEqual(
                        execution_stats["collectorPushdown"],
                        engine_name == "sqlite",
                    )
                    if engine_name == "sqlite":
                        self.assertFalse(engine._connection.in_transaction)

    async def test_sqlite_exact_collectors_do_not_decode_documents(
        self,
    ) -> None:
        async with open_engine("sqlite") as engine:
            await engine.insert_document(
                "db",
                "items",
                {"_id": 1, "title": "Ada", "kind": "note"},
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
            )
            await engine.create_search_index(
                "db",
                "items",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "kind": {"type": "token"},
                            },
                        },
                    },
                    name="by_text",
                ),
            )
            specification = {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "count": {"type": "total"},
                "facet": {"path": "kind", "type": "token"},
            }
            request = SearchRequest(
                operator="$searchMeta",
                specification=specification,
                query=compile_search_stage("$searchMeta", specification),
                mode=SearchExecutionMode.METADATA,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                runtime_operator="$search",
                runtime_specification=specification,
            )
            with patch.object(
                engine,
                "_load_documents",
                side_effect=AssertionError("pushdown decoded documents"),
            ):
                outcome = await adapt_engine(engine).execute_search(
                    "db",
                    "items",
                    request,
                )

            self.assertEqual(outcome.metadata.count.value, 1)
            self.assertTrue(outcome.trace.collector_pushdown)

            with self.assertRaisesRegex(ValueError, "hit-domain optimizations"):
                SearchRequest(
                    operator="$searchMeta",
                    specification=specification,
                    query=compile_search_stage("$searchMeta", specification),
                    mode=SearchExecutionMode.METADATA,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                    ),
                    runtime_operator="$search",
                    runtime_specification=specification,
                    downstream_filter_spec={"kind": "NOTE"},
                )

    async def test_sqlite_metadata_pushdown_preserves_missing_index_error(
        self,
    ) -> None:
        specification = {
            "index": "missing",
            "text": {"query": "ada", "path": "title"},
            "count": {"type": "total"},
        }
        request = SearchRequest(
            operator="$searchMeta",
            specification=specification,
            query=compile_search_stage("$searchMeta", specification),
            mode=SearchExecutionMode.METADATA,
            operation_context=OperationContext.create(
                dialect=MONGODB_DIALECT_70,
            ),
            runtime_operator="$search",
            runtime_specification=specification,
        )

        async with open_engine("sqlite") as engine:
            with self.assertRaisesRegex(OperationFailure, "index not found"):
                await adapt_engine(engine).execute_search(
                    "db",
                    "items",
                    request,
                )

    async def test_sqlite_unsupported_facet_type_uses_semantic_core(
        self,
    ) -> None:
        async with open_engine("sqlite") as engine:
            for identifier, published in (
                (1, datetime(2026, 1, 1, tzinfo=UTC)),
                (2, datetime(2026, 1, 2, tzinfo=UTC)),
            ):
                await engine.insert_document(
                    "db",
                    "items",
                    {
                        "_id": identifier,
                        "title": "Ada",
                        "published": published,
                    },
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                    ),
                )
            await engine.create_search_index(
                "db",
                "items",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "published": {"type": "date"},
                            },
                        },
                    },
                    name="by_text",
                ),
            )
            specification = {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "count": {"type": "total"},
                "facet": {"path": "published", "type": "date"},
            }
            request = SearchRequest(
                operator="$searchMeta",
                specification=specification,
                query=compile_search_stage("$searchMeta", specification),
                mode=SearchExecutionMode.METADATA,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                runtime_operator="$search",
                runtime_specification=specification,
            )

            outcome = await adapt_engine(engine).execute_search(
                "db",
                "items",
                request,
            )

            self.assertEqual(outcome.metadata.count.value, 2)
            self.assertEqual(len(outcome.metadata.facets[0].buckets), 2)
            self.assertFalse(outcome.trace.collector_pushdown)
            self.assertEqual(
                outcome.trace.collector_backend,
                "semantic-core",
            )

    async def test_sqlite_lower_bound_stops_after_threshold_plus_one(
        self,
    ) -> None:
        async with open_engine("sqlite") as engine:
            for identifier in range(10):
                await engine.insert_document(
                    "db",
                    "items",
                    {"_id": identifier, "title": "Ada"},
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                    ),
                )
            await engine.create_search_index(
                "db",
                "items",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {"title": {"type": "string"}},
                        },
                    },
                    name="by_text",
                ),
            )
            specification = {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "count": {"type": "lowerBound", "threshold": 2},
            }
            request = SearchRequest(
                operator="$searchMeta",
                specification=specification,
                query=compile_search_stage("$searchMeta", specification),
                mode=SearchExecutionMode.METADATA,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                runtime_operator="$search",
                runtime_specification=specification,
            )

            outcome = await adapt_engine(engine).execute_search(
                "db",
                "items",
                request,
            )

            self.assertEqual(outcome.metadata.count.value, 2)
            self.assertFalse(outcome.metadata.count.exact)
            self.assertTrue(outcome.metadata.count.capped_by_threshold)
            self.assertEqual(outcome.trace.candidate_count, 3)
            self.assertTrue(outcome.trace.collector_pushdown)

    async def test_sqlite_dynamic_mapping_declines_collector_pushdown(
        self,
    ) -> None:
        async with open_engine("sqlite") as engine:
            await engine.insert_document(
                "db",
                "items",
                {"_id": 1, "title": "Ada"},
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
            )
            await engine.create_search_index(
                "db",
                "items",
                SearchIndexDefinition(
                    {"mappings": {"dynamic": True}},
                    name="by_text",
                ),
            )
            specification = {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "count": {"type": "total"},
            }
            request = SearchRequest(
                operator="$searchMeta",
                specification=specification,
                query=compile_search_stage("$searchMeta", specification),
                mode=SearchExecutionMode.METADATA,
                operation_context=OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                ),
                runtime_operator="$search",
                runtime_specification=specification,
            )

            outcome = await adapt_engine(engine).execute_search(
                "db",
                "items",
                request,
            )

            self.assertEqual(outcome.metadata.count.value, 1)
            self.assertFalse(outcome.trace.collector_pushdown)
            self.assertEqual(
                outcome.trace.collector_backend,
                "semantic-core",
            )

    async def test_query_planner_never_executes_or_materializes_search(
        self,
    ) -> None:
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.create_search_index(
                        "db",
                        "items",
                        SearchIndexDefinition(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {
                                        "title": {"type": "string"},
                                    },
                                },
                            },
                            name="by_text",
                        ),
                    )
                    request = _request()

                    async def _unexpected_execute(*_args, **_kwargs):
                        self.fail("queryPlanner executed Search")

                    engine.execute_search = _unexpected_execute
                    patch_target = (
                        "mongoeco.engines._memory_search_runtime."
                        "execute_search_documents"
                        if engine_name == "memory"
                        else "mongoeco.engines._sqlite_search_runtime."
                        "ensure_search_backend_sync"
                    )
                    with patch(
                        patch_target,
                        side_effect=AssertionError(
                            "queryPlanner materialized Search",
                        ),
                    ):
                        explanation = await adapt_engine(
                            engine,
                        ).explain_search(
                            "db",
                            "items",
                            request,
                            SearchExplainVerbosity.QUERY_PLANNER,
                        )

                    details = explanation.details
                    self.assertEqual(details["contractVersion"], "search-v1")
                    self.assertEqual(details["verbosity"], "queryPlanner")
                    self.assertIsNone(details["executionStats"])
                    self.assertEqual(
                        details["collectorPlan"],
                        {
                            "count": False,
                            "facetCount": 0,
                            "execution": "semantic-core",
                        },
                    )
                    self.assertEqual(
                        details["highlightPlan"]["storage"],
                        "runtime-envelope",
                    )


if __name__ == "__main__":
    unittest.main()

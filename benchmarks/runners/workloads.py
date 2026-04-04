import datetime
import random
from collections.abc import Callable
from typing import Any

from benchmarks.data.generator import generate_users
from benchmarks.engines.base import BenchmarkEngine
from benchmarks.runners.metrics import Metrics, measure


type MetricDocument = dict[str, Any]


def _load_users(
    engine: BenchmarkEngine,
    count: int,
    *,
    mutate_docs: Callable[[list[dict[str, Any]]], None] | None = None,
) -> tuple[str, str, list[dict[str, Any]]]:
    db_name = "test_db"
    coll_name = "users"
    docs = generate_users(count)
    if mutate_docs is not None:
        mutate_docs(docs)
    engine.setup()
    engine.drop_collection(db_name, coll_name)
    engine.insert_many(db_name, coll_name, docs)
    return db_name, coll_name, docs


def _operation_summary(explain: dict[str, Any]) -> str:
    plan = explain.get("engine_plan", explain)
    engine = plan.get("engine", "?")
    strategy = plan.get("strategy", "?")
    physical_plan = plan.get("physical_plan")
    if isinstance(physical_plan, list):
        operations = [
            str(step.get("operation"))
            for step in physical_plan
            if isinstance(step, dict) and step.get("operation") is not None
        ]
    else:
        operations = []
    suffix = ">".join(operations) if operations else "opaque"
    return f"{engine}/{strategy} {suffix}"


def _summarize_find_explain(explain: dict[str, Any]) -> dict[str, Any]:
    return {
        "summary": _operation_summary(explain),
        "engine": explain.get("engine"),
        "strategy": explain.get("strategy"),
        "planning_mode": explain.get("planning_mode"),
        "limit": explain.get("limit"),
        "physical_plan": explain.get("physical_plan"),
    }


def _summarize_aggregate_explain(explain: dict[str, Any]) -> dict[str, Any]:
    engine_plan = explain.get("engine_plan")
    plan = engine_plan if isinstance(engine_plan, dict) else explain
    remaining_pipeline = explain.get("remaining_pipeline")
    return {
        "summary": _operation_summary(plan),
        "engine": plan.get("engine"),
        "strategy": plan.get("strategy"),
        "planning_mode": explain.get("planning_mode", plan.get("planning_mode")),
        "streaming_batch_execution": explain.get("streaming_batch_execution"),
        "remaining_stage_count": len(remaining_pipeline) if isinstance(remaining_pipeline, list) else None,
        "physical_plan": plan.get("physical_plan"),
    }


def _summarize_search_explain(explain: dict[str, Any]) -> dict[str, Any]:
    summary = _summarize_aggregate_explain(explain)
    engine_plan = explain.get("engine_plan")
    details = engine_plan.get("details") if isinstance(engine_plan, dict) else None
    pushdown = explain.get("pushdown")
    if not isinstance(details, dict):
        return summary
    summary["query_operator"] = details.get("queryOperator")
    summary["backend"] = details.get("backend")
    summary["mode"] = details.get("mode")
    summary["similarity"] = details.get("similarity")
    summary["min_score"] = details.get("minScore")
    summary["filter_mode"] = details.get("filterMode")
    summary["exact_fallback_reason"] = details.get("exactFallbackReason")
    summary["candidates_evaluated"] = details.get("candidatesEvaluated")
    summary["candidates_requested"] = details.get("candidatesRequested")
    summary["documents_filtered"] = details.get("documentsFiltered")
    summary["documents_filtered_by_min_score"] = details.get("documentsFilteredByMinScore")
    summary["candidate_count"] = details.get("candidateCount")
    summary["candidate_count_before_topk"] = details.get("candidateCountBeforeTopK")
    summary["topk_limit_hint"] = details.get("topKLimitHint")
    summary["topk_prefilter"] = details.get("topKPrefilter")
    summary["ranking_source"] = details.get("rankingSource")
    summary["compound_prefilter"] = details.get("compoundPrefilter")
    summary["downstream_filter_prefilter"] = details.get("downstreamFilterPrefilter")
    summary["vector_filter_prefilter"] = details.get("vectorFilterPrefilter")
    summary["vector_filter_residual"] = details.get("vectorFilterResidual")
    summary["candidate_expansion_strategy"] = details.get("candidateExpansionStrategy")
    summary["documents_scanned"] = details.get("documentsScanned")
    summary["documents_scanned_after_prefilter"] = details.get("documentsScannedAfterPrefilter")
    if isinstance(pushdown, dict):
        summary["search_topk_strategy"] = pushdown.get("searchTopKStrategy")
        summary["search_topk_growth_strategy"] = pushdown.get("searchTopKGrowthStrategy")
    return summary


def _metrics_with_metadata(
    metrics: Metrics,
    *,
    metadata: dict[str, Any] | None = None,
) -> MetricDocument:
    result = metrics.to_dict()
    if metadata is not None:
        result["metadata"] = metadata
    return result


def _measure_single_task(
    metric_store: list[Metrics],
    callback: Callable[[], Any],
    *,
    metadata: dict[str, Any] | None = None,
) -> MetricDocument:
    with measure(metric_store):
        callback()
    return _metrics_with_metadata(metric_store[0], metadata=metadata)


def _augment_search_documents(docs: list[dict[str, Any]]) -> None:
    people = ("Ada", "Grace", "Barbara", "Donald")
    topics = ("algorithms", "compilers", "indexes", "vectors")
    summaries = (
        "analytical engine design notes",
        "compiler construction handbook",
        "sqlite indexing patterns",
        "vector retrieval and ranking",
    )
    embeddings = (
        (1.0, 0.0, 0.0),
        (0.0, 1.0, 0.0),
        (0.0, 0.0, 1.0),
        (0.8, 0.2, 0.0),
    )
    for index, document in enumerate(docs):
        person = people[index % len(people)]
        topic = topics[index % len(topics)]
        summary = summaries[index % len(summaries)]
        cluster = embeddings[index % len(embeddings)]
        document["title"] = f"{person} {topic}"
        document["body"] = (
            f"{person} wrote about {topic}. "
            f"{summary}. "
            f"{document['city']} field report {index % 7}."
        )
        document["kind"] = "reference" if index % 3 == 0 else "note"
        document["embedding"] = [float(value) for value in cluster]
        document["score"] = float((index % 11) + 1)
        document["publishedAt"] = datetime.datetime(2024, 1, 1) + datetime.timedelta(days=index % 30)


def secondary_lookup_indexed(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Lookup por campo secundario con indice explicito."""
    lookup_metrics: list[Metrics] = []
    db_name, coll_name, docs = _load_users(engine, count)
    try:
        engine.create_index(db_name, coll_name, [("username", 1)])
        rng = random.Random(42)
        target_ids = [rng.randint(0, count - 1) for _ in range(min(1000, count))]
        target_usernames = [docs[i]["username"] for i in target_ids]
        explain = engine.explain_find(
            db_name,
            coll_name,
            {"username": target_usernames[0]},
            limit=1,
        )
        metadata = {
            **_summarize_find_explain(explain),
            "query_shape": "username equality",
            "index_expected": True,
        }
        return {
            "secondary_lookup_indexed_1k": _measure_single_task(
                lookup_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {"username": username}, limit=1)
                    for username in target_usernames
                ],
                metadata=metadata,
            ),
        }
    finally:
        engine.teardown()


def secondary_lookup_unindexed(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Lookup por campo secundario sin indice."""
    lookup_metrics: list[Metrics] = []
    db_name, coll_name, docs = _load_users(engine, count)
    try:
        rng = random.Random(42)
        target_ids = [rng.randint(0, count - 1) for _ in range(min(1000, count))]
        target_usernames = [docs[i]["username"] for i in target_ids]
        explain = engine.explain_find(
            db_name,
            coll_name,
            {"username": target_usernames[0]},
            limit=1,
        )
        metadata = {
            **_summarize_find_explain(explain),
            "query_shape": "username equality",
            "index_expected": False,
        }
        return {
            "secondary_lookup_unindexed_1k": _measure_single_task(
                lookup_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {"username": username}, limit=1)
                    for username in target_usernames
                ],
                metadata=metadata,
            ),
        }
    finally:
        engine.teardown()


def secondary_lookup_diagnostics(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Aisla lookup primario y lookups secundarios repetidos sobre el mismo valor."""
    id_metrics: list[Metrics] = []
    indexed_hot_metrics: list[Metrics] = []
    unindexed_hot_metrics: list[Metrics] = []

    db_name, coll_name, docs = _load_users(engine, count)
    try:
        target_doc = docs[min(3, len(docs) - 1)]
        target_id = target_doc["_id"]
        target_username = target_doc["username"]

        id_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, {"_id": target_id}, limit=1)),
            "query_shape": "_id equality",
            "index_expected": True,
            "lookup_pattern": "random-like stable",
        }
        engine.create_index(db_name, coll_name, [("username", 1)])
        indexed_hot_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, {"username": target_username}, limit=1)),
            "query_shape": "username equality",
            "index_expected": True,
            "lookup_pattern": "same value repeated",
        }
        indexed_hot_result = _measure_single_task(
            indexed_hot_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, {"username": target_username}, limit=1)
                for _ in range(min(1000, count))
            ],
            metadata=indexed_hot_metadata,
        )
        id_result = _measure_single_task(
            id_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, {"_id": target_id}, limit=1)
                for _ in range(min(1000, count))
            ],
            metadata=id_metadata,
        )
    finally:
        engine.teardown()

    db_name, coll_name, docs = _load_users(engine, count)
    try:
        target_doc = docs[min(3, len(docs) - 1)]
        target_username = target_doc["username"]
        unindexed_hot_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, {"username": target_username}, limit=1)),
            "query_shape": "username equality",
            "index_expected": False,
            "lookup_pattern": "same value repeated",
        }
        unindexed_hot_result = _measure_single_task(
            unindexed_hot_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, {"username": target_username}, limit=1)
                for _ in range(min(1000, count))
            ],
            metadata=unindexed_hot_metadata,
        )
    finally:
        engine.teardown()

    return {
        "primary_lookup_id_1k": id_result,
        "secondary_lookup_indexed_hot_1k": indexed_hot_result,
        "secondary_lookup_unindexed_hot_1k": unindexed_hot_result,
    }


def simple_aggregation(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Agregacion mayoritariamente streamable."""
    agg_metrics: list[Metrics] = []
    db_name, coll_name, _docs = _load_users(engine, count)
    try:
        pipeline = [
            {"$match": {"active": True, "age": {"$gte": 20}}},
            {"$sort": {"score": -1}},
            {"$limit": 10},
            {"$project": {"_id": 1, "username": 1, "score": 1, "city": 1}},
        ]
        metadata = _summarize_aggregate_explain(
            engine.explain_aggregate(db_name, coll_name, pipeline, allow_disk_use=True)
        )
        metadata["pipeline_shape"] = "match-sort-limit-project"
        return {
            "simple_aggregation_topk": _measure_single_task(
                agg_metrics,
                callback=lambda: engine.aggregate(
                    db_name,
                    coll_name,
                    pipeline,
                    allow_disk_use=True,
                ),
                metadata=metadata,
            ),
        }
    finally:
        engine.teardown()


def materializing_aggregation(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Agregacion con group y sort posteriores."""
    agg_metrics: list[Metrics] = []
    db_name, coll_name, _docs = _load_users(engine, count)
    try:
        pipeline = [
            {"$match": {"active": True, "age": {"$gte": 20}}},
            {
                "$group": {
                    "_id": "$city",
                    "avg_score": {"$avg": "$score"},
                    "total_users": {"$sum": 1},
                    "roles": {"$addToSet": "$role"},
                }
            },
            {"$sort": {"avg_score": -1}},
            {"$limit": 10},
        ]
        metadata = _summarize_aggregate_explain(
            engine.explain_aggregate(db_name, coll_name, pipeline, allow_disk_use=True)
        )
        metadata["pipeline_shape"] = "match-group-sort-limit"
        return {
            "materializing_aggregation_group_sort": _measure_single_task(
                agg_metrics,
                callback=lambda: engine.aggregate(
                    db_name,
                    coll_name,
                    pipeline,
                    allow_disk_use=True,
                ),
                metadata=metadata,
            ),
        }
    finally:
        engine.teardown()


def sort_limit(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Compara sort+limit con y sin indice sobre el campo de ordenacion."""
    indexed_metrics: list[Metrics] = []
    unindexed_metrics: list[Metrics] = []
    db_name, coll_name, _docs = _load_users(engine, count)
    try:
        sort_spec = [("score", -1)]
        indexed_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=sort_spec, limit=10)
            ),
            "query_shape": "sort score desc + limit 10",
            "index_expected": True,
        }
        unindexed_metadata = {
            **indexed_metadata,
            "index_expected": False,
        }
        engine.create_index(db_name, coll_name, [("score", -1)])
        indexed = _measure_single_task(
            indexed_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, {}, sort=sort_spec, limit=10)
                for _ in range(200)
            ],
            metadata=indexed_metadata,
        )
    finally:
        engine.teardown()

    db_name, coll_name, _docs = _load_users(engine, count)
    try:
        sort_spec = [("score", -1)]
        unindexed_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=sort_spec, limit=10)
            ),
            "query_shape": "sort score desc + limit 10",
            "index_expected": False,
        }
        unindexed = _measure_single_task(
            unindexed_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, {}, sort=sort_spec, limit=10)
                for _ in range(200)
            ],
            metadata=unindexed_metadata,
        )
    finally:
        engine.teardown()

    return {
        "sort_limit_indexed_200": indexed,
        "sort_limit_unindexed_200": unindexed,
    }


def cursor_consumption(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Separa coste de consumir solo el primero frente a materializar."""
    first_metrics: list[Metrics] = []
    materialized_metrics: list[Metrics] = []
    db_name, coll_name, _docs = _load_users(engine, count)
    try:
        engine.create_index(db_name, coll_name, [("city", 1)])
        filter_spec = {"city": "Madrid"}
        metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, filter_spec)),
            "query_shape": "city equality",
            "index_expected": True,
        }
        first_result = _measure_single_task(
            first_metrics,
            callback=lambda: [
                engine.find_first(db_name, coll_name, filter_spec)
                for _ in range(200)
            ],
            metadata={**metadata, "consumption_mode": "first"},
        )
        materialized_result = _measure_single_task(
            materialized_metrics,
            callback=lambda: [
                engine.find(db_name, coll_name, filter_spec)
                for _ in range(200)
            ],
            metadata={**metadata, "consumption_mode": "materialized"},
        )
        return {
            "cursor_consumption_first_200": first_result,
            "cursor_consumption_materialized_200": materialized_result,
        }
    finally:
        engine.teardown()


def filter_selectivity(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Mide el coste de materializar filtros con distinta selectividad."""
    low_metrics: list[Metrics] = []
    medium_metrics: list[Metrics] = []
    high_metrics: list[Metrics] = []
    db_name, coll_name, docs = _load_users(engine, count)
    try:
        low_filter = {"username": docs[min(3, len(docs) - 1)]["username"]}
        medium_filter = {"city": "Madrid"}
        high_filter = {"active": True}
        low_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, low_filter)),
            "query_shape": "username equality",
            "selectivity": "low",
        }
        medium_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, medium_filter)),
            "query_shape": "city equality",
            "selectivity": "medium",
        }
        high_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, high_filter)),
            "query_shape": "active equality",
            "selectivity": "high",
        }
        return {
            "filter_selectivity_low_100": _measure_single_task(
                low_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, low_filter)
                    for _ in range(100)
                ],
                metadata=low_metadata,
            ),
            "filter_selectivity_medium_100": _measure_single_task(
                medium_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, medium_filter)
                    for _ in range(100)
                ],
                metadata=medium_metadata,
            ),
            "filter_selectivity_high_100": _measure_single_task(
                high_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, high_filter)
                    for _ in range(100)
                ],
                metadata=high_metadata,
            ),
        }
    finally:
        engine.teardown()


def predicate_diagnostics(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Aisla predicados frecuentes para detectar mejoras en igualdad y comparación."""
    eq_bool_metrics: list[Metrics] = []
    eq_string_metrics: list[Metrics] = []
    cmp_int_metrics: list[Metrics] = []
    db_name, coll_name, docs = _load_users(engine, count)
    try:
        eq_bool_filter = {"active": True}
        eq_string_filter = {"city": "Madrid"}
        cmp_int_filter = {"age": {"$gte": 40}}

        eq_bool_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, eq_bool_filter)),
            "query_shape": "top-level bool equality",
            "operator_shape": "$eq",
            "field_shape": "top-level scalar",
            "selectivity": "high",
        }
        eq_string_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, eq_string_filter)),
            "query_shape": "top-level string equality",
            "operator_shape": "$eq",
            "field_shape": "top-level scalar",
            "selectivity": "medium",
        }
        cmp_int_metadata = {
            **_summarize_find_explain(engine.explain_find(db_name, coll_name, cmp_int_filter)),
            "query_shape": "top-level int comparison",
            "operator_shape": "$gte",
            "field_shape": "top-level scalar",
            "selectivity": "medium",
        }

        return {
            "predicate_eq_bool_high_100": _measure_single_task(
                eq_bool_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, eq_bool_filter)
                    for _ in range(100)
                ],
                metadata=eq_bool_metadata,
            ),
            "predicate_eq_string_medium_100": _measure_single_task(
                eq_string_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, eq_string_filter)
                    for _ in range(100)
                ],
                metadata=eq_string_metadata,
            ),
            "predicate_cmp_int_medium_100": _measure_single_task(
                cmp_int_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, cmp_int_filter)
                    for _ in range(100)
                ],
                metadata=cmp_int_metadata,
            ),
        }
    finally:
        engine.teardown()


def sort_shape_diagnostics(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Compara sort por campo top-level frente a path anidado equivalente."""
    top_level_metrics: list[Metrics] = []
    nested_metrics: list[Metrics] = []
    top_level_full_metrics: list[Metrics] = []
    nested_full_metrics: list[Metrics] = []

    def _add_nested_sort_fields(docs: list[dict[str, Any]]) -> None:
        for doc in docs:
            doc["profile"] = {
                "score": doc["score"],
                "city": doc["city"],
            }

    db_name, coll_name, _docs = _load_users(engine, count, mutate_docs=_add_nested_sort_fields)
    try:
        top_level_sort = [("score", -1)]
        nested_sort = [("profile.score", -1)]

        top_level_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=top_level_sort, limit=10)
            ),
            "query_shape": "sort top-level score desc + limit 10",
            "field_shape": "top-level scalar",
        }
        nested_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=nested_sort, limit=10)
            ),
            "query_shape": "sort nested profile.score desc + limit 10",
            "field_shape": "nested scalar path",
        }
        top_level_full_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=top_level_sort)
            ),
            "query_shape": "sort top-level score desc",
            "field_shape": "top-level scalar",
        }
        nested_full_metadata = {
            **_summarize_find_explain(
                engine.explain_find(db_name, coll_name, {}, sort=nested_sort)
            ),
            "query_shape": "sort nested profile.score desc",
            "field_shape": "nested scalar path",
        }

        return {
            "sort_shape_top_level_limit_200": _measure_single_task(
                top_level_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {}, sort=top_level_sort, limit=10)
                    for _ in range(200)
                ],
                metadata=top_level_metadata,
            ),
            "sort_shape_nested_limit_200": _measure_single_task(
                nested_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {}, sort=nested_sort, limit=10)
                    for _ in range(200)
                ],
                metadata=nested_metadata,
            ),
            "sort_shape_top_level_full_50": _measure_single_task(
                top_level_full_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {}, sort=top_level_sort)
                    for _ in range(50)
                ],
                metadata=top_level_full_metadata,
            ),
            "sort_shape_nested_full_50": _measure_single_task(
                nested_full_metrics,
                callback=lambda: [
                    engine.find(db_name, coll_name, {}, sort=nested_sort)
                    for _ in range(50)
                ],
                metadata=nested_full_metadata,
            ),
        }
    finally:
        engine.teardown()


def search_diagnostics(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Mide operadores de $search sobre el subset local actual."""
    text_metrics: list[Metrics] = []
    autocomplete_metrics: list[Metrics] = []
    wildcard_metrics: list[Metrics] = []
    regex_metrics: list[Metrics] = []
    compound_metrics: list[Metrics] = []
    compound_near_metrics: list[Metrics] = []
    compound_candidateable_should_metrics: list[Metrics] = []
    compound_candidateable_should_matched_metrics: list[Metrics] = []
    compound_candidateable_should_title_metrics: list[Metrics] = []
    compound_candidateable_should_msm2_metrics: list[Metrics] = []
    compound_candidateable_should_tie_heavy_metrics: list[Metrics] = []

    db_name, coll_name, _docs = _load_users(engine, count, mutate_docs=_augment_search_documents)
    try:
        engine.create_search_index(
            db_name,
            coll_name,
            {
                "mappings": {
                    "dynamic": False,
                    "fields": {
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                        "kind": {"type": "token"},
                        "score": {"type": "number"},
                        "publishedAt": {"type": "date"},
                    },
                }
            },
            name="by_text",
        )

        text_pipeline = [
            {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
            {"$limit": 10},
        ]
        autocomplete_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "autocomplete": {"query": "alg", "path": ["title", "body"]},
                }
            },
            {"$limit": 10},
        ]
        wildcard_pipeline = [
            {"$search": {"index": "by_text", "wildcard": {"query": "*vector*", "path": "body"}}},
            {"$limit": 10},
        ]
        regex_pipeline = [
            {"$search": {"index": "by_text", "regex": {"query": "Ada.*algorithms", "path": "title"}}},
            {"$limit": 10},
        ]
        compound_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "filter": [
                            {"exists": {"path": "title"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                        ],
                    },
                }
            },
            {"$limit": 10},
        ]
        compound_near_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "filter": [{"wildcard": {"query": "*vector*", "path": "body"}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"near": {"path": "score", "origin": 7, "pivot": 2}},
                        ],
                        "minimumShouldMatch": 2,
                    },
                }
            },
            {"$limit": 10},
        ]
        compound_candidateable_should_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                        ],
                        "minimumShouldMatch": 1,
                    },
                }
            },
            {"$limit": 10},
        ]
        compound_candidateable_should_matched_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                        ],
                        "minimumShouldMatch": 1,
                    },
                }
            },
            {"$match": {"kind": "note"}},
            {"$limit": 10},
        ]
        compound_candidateable_should_title_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                        ],
                        "minimumShouldMatch": 1,
                    },
                }
            },
            {"$match": {"title": "Ada algorithms"}},
            {"$limit": 10},
        ]
        compound_candidateable_should_msm2_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "report 0", "path": ["title", "body"]}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                        ],
                        "minimumShouldMatch": 2,
                    },
                }
            },
            {"$limit": 10},
        ]
        compound_candidateable_should_tie_heavy_pipeline = [
            {
                "$search": {
                    "index": "by_text",
                    "compound": {
                        "must": [{"text": {"query": "vector", "path": ["title", "body"]}}],
                        "should": [
                            {"exists": {"path": "title"}},
                            {"exists": {"path": "body"}},
                            {"wildcard": {"query": "*vector*", "path": "body"}},
                        ],
                        "minimumShouldMatch": 1,
                    },
                }
            },
            {"$limit": 10},
        ]

        return {
            "search_text_topk_100": _measure_single_task(
                text_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, text_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, text_pipeline)
                    ),
                    "query_shape": "$search.text title/body ada",
                },
            ),
            "search_autocomplete_topk_100": _measure_single_task(
                autocomplete_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, autocomplete_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, autocomplete_pipeline)
                    ),
                    "query_shape": "$search.autocomplete title/body alg",
                },
            ),
            "search_wildcard_topk_100": _measure_single_task(
                wildcard_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, wildcard_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, wildcard_pipeline)
                    ),
                    "query_shape": "$search.wildcard body *vector*",
                },
            ),
            "search_regex_topk_100": _measure_single_task(
                regex_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, regex_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, regex_pipeline)
                    ),
                    "query_shape": "$search.regex title Ada.*algorithms",
                },
            ),
            "search_compound_hybrid_topk_100": _measure_single_task(
                compound_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, compound_pipeline)
                    ),
                    "query_shape": "$search.compound must(text=report 0)+filter(exists,wildcard)",
                },
            ),
            "search_compound_should_near_topk_100": _measure_single_task(
                compound_near_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_near_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, compound_near_pipeline)
                    ),
                    "query_shape": "$search.compound must(text=report 0)+filter(wildcard)+should(exists,near)",
                },
            ),
            "search_compound_candidateable_should_topk_100": _measure_single_task(
                compound_candidateable_should_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_candidateable_should_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(
                            db_name,
                            coll_name,
                            compound_candidateable_should_pipeline,
                        )
                    ),
                    "query_shape": "$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)",
                },
            ),
            "search_compound_candidateable_should_matched_topk_100": _measure_single_task(
                compound_candidateable_should_matched_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_candidateable_should_matched_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(
                            db_name,
                            coll_name,
                            compound_candidateable_should_matched_pipeline,
                        )
                    ),
                    "query_shape": "$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(kind=note)",
                },
            ),
            "search_compound_candidateable_should_title_topk_100": _measure_single_task(
                compound_candidateable_should_title_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_candidateable_should_title_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(
                            db_name,
                            coll_name,
                            compound_candidateable_should_title_pipeline,
                        )
                    ),
                    "query_shape": "$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+match(title=Ada algorithms)",
                },
            ),
            "search_compound_candidateable_should_msm2_topk_100": _measure_single_task(
                compound_candidateable_should_msm2_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_candidateable_should_msm2_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(
                            db_name,
                            coll_name,
                            compound_candidateable_should_msm2_pipeline,
                        )
                    ),
                    "query_shape": "$search.compound must(text=report 0)+should(exists,wildcard,autocomplete)+minimumShouldMatch(2)",
                },
            ),
            "search_compound_candidateable_should_tie_heavy_topk_100": _measure_single_task(
                compound_candidateable_should_tie_heavy_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, compound_candidateable_should_tie_heavy_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(
                            db_name,
                            coll_name,
                            compound_candidateable_should_tie_heavy_pipeline,
                        )
                    ),
                    "query_shape": "$search.compound must(text=vector)+should(exists(title),exists(body),wildcard(body=*vector*))",
                },
            ),
        }
    finally:
        engine.teardown()


def vector_search_diagnostics(engine: BenchmarkEngine, count: int) -> dict[str, Any]:
    """Mide ANN local y el coste adicional del filtro post-candidato."""
    ann_metrics: list[Metrics] = []
    ann_low_candidates_metrics: list[Metrics] = []
    ann_high_candidates_metrics: list[Metrics] = []
    dot_metrics: list[Metrics] = []
    euclidean_metrics: list[Metrics] = []
    filtered_metrics: list[Metrics] = []
    filtered_boolean_metrics: list[Metrics] = []
    filtered_underflow_metrics: list[Metrics] = []
    filtered_min_score_metrics: list[Metrics] = []

    db_name, coll_name, _docs = _load_users(engine, count, mutate_docs=_augment_search_documents)
    try:
        engine.create_search_index(
            db_name,
            coll_name,
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "cosine",
                        "connectivity": 8,
                        "expansionAdd": 16,
                        "expansionSearch": 24,
                    }
                ]
            },
            name="by_vector_cosine",
            index_type="vectorSearch",
        )
        engine.create_search_index(
            db_name,
            coll_name,
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "dotProduct",
                        "connectivity": 8,
                        "expansionAdd": 16,
                        "expansionSearch": 24,
                    }
                ]
            },
            name="by_vector_dot",
            index_type="vectorSearch",
        )
        engine.create_search_index(
            db_name,
            coll_name,
            {
                "fields": [
                    {
                        "type": "vector",
                        "path": "embedding",
                        "numDimensions": 3,
                        "similarity": "euclidean",
                        "connectivity": 8,
                        "expansionAdd": 16,
                        "expansionSearch": 24,
                    }
                ]
            },
            name="by_vector_euclidean",
            index_type="vectorSearch",
        )

        ann_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                }
            }
        ]
        ann_low_candidates_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 10,
                    "limit": 10,
                }
            }
        ]
        ann_high_candidates_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 48,
                    "limit": 10,
                }
            }
        ]
        dot_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_dot",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                }
            }
        ]
        euclidean_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_euclidean",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                }
            }
        ]
        filtered_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                    "filter": {"kind": "reference"},
                }
            }
        ]
        filtered_boolean_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                    "filter": {
                        "$or": [
                            {"kind": "reference"},
                            {"score": {"$gte": 10}},
                        ]
                    },
                }
            }
        ]
        filtered_underflow_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 10,
                    "limit": 10,
                    "filter": {
                        "$and": [
                            {"kind": "reference"},
                            {"score": {"$gte": 10}},
                        ]
                    },
                }
            }
        ]
        filtered_min_score_pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector_cosine",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "numCandidates": 24,
                    "limit": 10,
                    "filter": {"kind": "note"},
                    "minScore": 0.999,
                }
            }
        ]

        return {
            "vector_search_ann_topk_100": _measure_single_task(
                ann_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, ann_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, ann_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk",
                },
            ),
            "vector_search_cosine_low_candidates_topk_100": _measure_single_task(
                ann_low_candidates_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, ann_low_candidates_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, ann_low_candidates_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + numCandidates(10)",
                },
            ),
            "vector_search_cosine_high_candidates_topk_100": _measure_single_task(
                ann_high_candidates_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, ann_high_candidates_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, ann_high_candidates_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + numCandidates(48)",
                },
            ),
            "vector_search_dot_product_topk_100": _measure_single_task(
                dot_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, dot_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, dot_pipeline)
                    ),
                    "query_shape": "$vectorSearch dotProduct topk",
                },
            ),
            "vector_search_euclidean_topk_100": _measure_single_task(
                euclidean_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, euclidean_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, euclidean_pipeline)
                    ),
                    "query_shape": "$vectorSearch euclidean topk",
                },
            ),
            "vector_search_filtered_topk_100": _measure_single_task(
                filtered_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, filtered_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, filtered_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + post-filter",
                },
            ),
            "vector_search_filtered_boolean_topk_100": _measure_single_task(
                filtered_boolean_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, filtered_boolean_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, filtered_boolean_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + boolean candidate filter",
                },
            ),
            "vector_search_filtered_underflow_topk_100": _measure_single_task(
                filtered_underflow_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, filtered_underflow_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, filtered_underflow_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + rare boolean candidate filter",
                },
            ),
            "vector_search_filtered_min_score_topk_100": _measure_single_task(
                filtered_min_score_metrics,
                callback=lambda: [
                    engine.aggregate(db_name, coll_name, filtered_min_score_pipeline)
                    for _ in range(100)
                ],
                metadata={
                    **_summarize_search_explain(
                        engine.explain_aggregate(db_name, coll_name, filtered_min_score_pipeline)
                    ),
                    "query_shape": "$vectorSearch cosine topk + filter(kind=note) + minScore(0.999)",
                },
            ),
        }
    finally:
        engine.teardown()

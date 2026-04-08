from collections.abc import AsyncIterator
from copy import deepcopy
import datetime
import math
import time

from mongoeco.api._async.cursor import (
    HintSpec,
    _ensure_operation_executable,
    _operation_issue_message,
    _resolve_planning_mode,
)
from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.cxp import build_mongodb_explain_projection
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core._search_contract import TEXT_SEARCH_OPERATOR_NAMES
from mongoeco.core.aggregation import (
    AggregationCostPolicy,
    Pipeline,
    _CURRENT_COLLECTION_RESOLVER_KEY,
    AggregationSpillPolicy,
    apply_pipeline,
    has_materializing_aggregation_stage,
    is_streamable_aggregation_stage,
    split_pushdown_pipeline,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import normalize_collation
from mongoeco.core.search import (
    build_search_meta_document,
    compile_search_stage,
    strip_search_result_metadata,
)
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import AggregateExplanation, Document, QueryPlanExplanation


class AsyncAggregationCursor:
    """Cursor async mínimo para resultados de aggregate()."""

    _SEARCH_TOPK_SAFE_STAGE_OPERATORS = frozenset(
        {
            "$project",
            "$unset",
            "$addFields",
            "$set",
            "$replaceRoot",
            "$replaceWith",
        }
    )
    _SEARCH_PREFIX_MONOTONIC_STAGE_OPERATORS = frozenset(
        _SEARCH_TOPK_SAFE_STAGE_OPERATORS
        | {
            "$match",
            "$skip",
            "$limit",
        }
    )

    def __init__(
        self,
        collection,
        operation: AggregateOperation | Pipeline,
        *,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: bool | None = None,
        collation: dict[str, object] | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ):
        self._collection = collection
        if not isinstance(operation, AggregateOperation):
            operation = compile_aggregate_operation(
                operation,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                allow_disk_use=allow_disk_use,
                collation=collation,
                let=let,
                dialect=getattr(collection, "mongodb_dialect", MONGODB_DIALECT_70),
                planning_mode=_resolve_planning_mode(collection),
            )
        self._operation = operation
        self._pipeline = operation.pipeline
        self._hint = operation.hint
        self._comment = operation.comment
        self._max_time_ms = operation.max_time_ms
        self._batch_size = operation.batch_size
        self._allow_disk_use = operation.allow_disk_use
        self._collation = normalize_collation(operation.collation)
        self._let = operation.let
        self._session = session

    def _leading_search_stage(self) -> tuple[str, object] | None:
        if not self._pipeline:
            return None
        stage = self._pipeline[0]
        if not isinstance(stage, dict) or len(stage) != 1:
            return None
        operator, spec = next(iter(stage.items()))
        if operator not in {"$search", "$searchMeta", "$vectorSearch"}:
            return None
        return operator, spec

    def _effective_pipeline(self) -> Pipeline:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            return self._pipeline
        return self._pipeline[1:]

    def _cxp_explain_projection(self) -> dict[str, object]:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            return build_mongodb_explain_projection(capability='aggregation')
        operator, _spec = leading_search
        if operator == '$vectorSearch':
            return build_mongodb_explain_projection(
                capability='aggregation',
                additional_capabilities=('vector_search',),
            )
        return build_mongodb_explain_projection(
            capability='aggregation',
            additional_capabilities=('search',),
        )

    @classmethod
    def _search_result_limit_hint(cls, pipeline: Pipeline) -> int | None:
        trailing_skip = 0
        trailing_limit: int | None = None
        seen_window = False
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                return None
            operator, spec = next(iter(stage.items()))
            if operator == "$skip":
                seen_window = True
                trailing_skip += int(spec)
                continue
            if operator == "$limit":
                seen_window = True
                value = int(spec)
                trailing_limit = value if trailing_limit is None else min(trailing_limit, value)
                continue
            if seen_window or operator not in cls._SEARCH_TOPK_SAFE_STAGE_OPERATORS:
                return None
        if trailing_limit is None:
            return None
        return trailing_skip + trailing_limit

    @classmethod
    def _search_prefix_output_limit(cls, pipeline: Pipeline) -> int | None:
        output_cap: int | None = None
        seen_limit = False
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                return None
            operator, spec = next(iter(stage.items()))
            if operator not in cls._SEARCH_PREFIX_MONOTONIC_STAGE_OPERATORS:
                return None
            if operator == "$limit":
                seen_limit = True
                value = int(spec)
                output_cap = value if output_cap is None else min(output_cap, value)
                continue
            if operator == "$skip" and output_cap is not None:
                output_cap = max(output_cap - int(spec), 0)
        if not seen_limit:
            return None
        return output_cap

    @staticmethod
    def _leading_search_downstream_filter_spec(pipeline: Pipeline) -> dict[str, object] | None:
        clauses: list[dict[str, object]] = []
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                break
            operator, spec = next(iter(stage.items()))
            if operator != "$match":
                break
            if not isinstance(spec, dict):
                return None
            clauses.append(deepcopy(spec))
        if not clauses:
            return None
        if len(clauses) == 1:
            return clauses[0]
        return {"$and": clauses}

    @staticmethod
    def _next_search_prefix_fetch_limit(
        current_fetch_limit: int,
        fetched_count: int,
        transformed_count: int,
        output_limit: int,
    ) -> int:
        if transformed_count <= 0:
            return max(current_fetch_limit + 1, current_fetch_limit * 4)
        estimated = math.ceil((fetched_count * output_limit) / transformed_count)
        safety_margin = max(1, output_limit - transformed_count)
        return max(current_fetch_limit + 1, estimated + safety_margin)

    @staticmethod
    def _split_terminal_writeback_stage(pipeline: Pipeline) -> tuple[Pipeline, tuple[str, object] | None]:
        if not pipeline:
            return pipeline, None
        terminal = pipeline[-1]
        if not isinstance(terminal, dict) or len(terminal) != 1:
            return pipeline, None
        operator, spec = next(iter(terminal.items()))
        if operator != "$merge":
            return pipeline, None
        for stage in pipeline[:-1]:
            if isinstance(stage, dict) and "$merge" in stage:
                raise OperationFailure("$merge is only supported as the final aggregation stage")
        return pipeline[:-1], (operator, spec)

    def _target_database(self, db_name: str):
        database = self._collection.database
        if db_name == database.name:
            return database
        return type(database)(
            self._collection._engine,
            db_name,
            mongodb_dialect=self._collection._mongodb_dialect,
            mongodb_dialect_resolution=self._collection._mongodb_dialect_resolution,
            pymongo_profile=self._collection._pymongo_profile,
            pymongo_profile_resolution=self._collection._pymongo_profile_resolution,
            write_concern=self._collection._write_concern,
            read_concern=self._collection._read_concern,
            read_preference=self._collection._read_preference,
            codec_options=self._collection._codec_options,
            change_hub=self._collection._change_hub,
            change_stream_history_size=self._collection._change_stream_history_size,
            change_stream_journal_path=self._collection._change_stream_journal_path,
            change_stream_journal_fsync=self._collection._change_stream_journal_fsync,
            change_stream_journal_max_bytes=self._collection._change_stream_journal_max_bytes,
        )

    async def _apply_merge_stage(self, documents: list[Document], spec: object) -> None:
        if not isinstance(spec, dict):
            raise OperationFailure("$merge requires a document specification")
        into = spec.get("into")
        if isinstance(into, str):
            target_db_name = self._collection._db_name
            target_coll_name = into
        elif isinstance(into, dict):
            target_db_name = spec_db = into.get("db", self._collection._db_name)
            target_coll_name = into.get("coll")
            if not isinstance(spec_db, str) or not spec_db:
                raise OperationFailure("$merge.into.db must be a non-empty string")
        else:
            raise OperationFailure("$merge.into must be a collection name or {db, coll}")
        if not isinstance(target_coll_name, str) or not target_coll_name:
            raise OperationFailure("$merge.into.coll must be a non-empty string")
        on = spec.get("on", "_id")
        if on not in (None, "_id"):
            raise OperationFailure("$merge currently supports only omitted on or on: '_id'")
        when_matched = spec.get("whenMatched", "merge")
        if isinstance(when_matched, list):
            raise OperationFailure("$merge whenMatched pipelines are not supported in the local runtime")
        if when_matched not in {"replace", "merge", "keepExisting", "fail"}:
            raise OperationFailure("$merge whenMatched currently supports replace, merge, keepExisting or fail")
        when_not_matched = spec.get("whenNotMatched", "insert")
        if when_not_matched not in {"insert", "discard", "fail"}:
            raise OperationFailure("$merge whenNotMatched currently supports insert, discard or fail")

        target_collection = self._target_database(target_db_name).get_collection(target_coll_name)
        for source_document in documents:
            candidate = strip_search_result_metadata(deepcopy(source_document))
            if "_id" not in candidate:
                raise OperationFailure("$merge currently requires documents with _id")
            existing = await target_collection.find_one({"_id": candidate["_id"]}, session=self._session)
            if existing is None:
                if when_not_matched == "insert":
                    await target_collection.insert_one(candidate, session=self._session)
                    continue
                if when_not_matched == "discard":
                    continue
                raise OperationFailure(f"$merge whenNotMatched=fail found no target document for _id={candidate['_id']!r}")
            if when_matched == "keepExisting":
                continue
            if when_matched == "fail":
                raise OperationFailure(f"$merge whenMatched=fail found an existing target document for _id={candidate['_id']!r}")
            if when_matched == "replace":
                await target_collection.replace_one({"_id": candidate["_id"]}, candidate, session=self._session)
                continue
            merged = deepcopy(existing)
            merged.update(candidate)
            await target_collection.replace_one({"_id": candidate["_id"]}, merged, session=self._session)

    async def _search_documents(self) -> list[Document]:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            raise OperationFailure("search stage was not present")
        operator, spec = leading_search
        effective_pipeline, writeback_stage = self._split_terminal_writeback_stage(self._effective_pipeline())
        result_limit_hint = None if writeback_stage is not None else self._search_result_limit_hint(effective_pipeline)
        downstream_filter_spec = self._leading_search_downstream_filter_spec(effective_pipeline)
        search_documents = getattr(self._collection._engine, "search_documents", None)
        if not callable(search_documents):
            raise OperationFailure(f"{operator} is not supported by this engine")
        resolved_operator = "$search" if operator == "$searchMeta" else operator
        resolved_spec = self._search_runtime_spec_for_stage(operator, spec)
        query = compile_search_stage(operator, spec) if operator == "$searchMeta" else None
        documents = await search_documents(
            self._collection._db_name,
            self._collection._collection_name,
            resolved_operator,
            resolved_spec,
            max_time_ms=self._max_time_ms,
            context=self._session,
            result_limit_hint=result_limit_hint,
            downstream_filter_spec=downstream_filter_spec,
        )
        if operator != "$searchMeta":
            return documents
        assert query is not None
        return [build_search_meta_document(documents, query=query)]

    @staticmethod
    def _search_runtime_spec_for_stage(operator: str, spec: object) -> object:
        if operator != "$searchMeta" or not isinstance(spec, dict):
            return spec
        facet_spec = spec.get("facet")
        if not isinstance(facet_spec, dict):
            return spec
        operator_spec = facet_spec.get("operator")
        if not isinstance(operator_spec, dict):
            return spec
        clause_names = [name for name in TEXT_SEARCH_OPERATOR_NAMES if name in operator_spec]
        if len(clause_names) != 1:
            return spec
        clause_name = clause_names[0]
        normalized = dict(spec)
        normalized_facet = dict(facet_spec)
        normalized_facet.pop("operator", None)
        normalized["facet"] = normalized_facet
        normalized[clause_name] = operator_spec[clause_name]
        return normalized

    async def _materialize_leading_search_pipeline(
        self,
        pipeline: Pipeline,
        *,
        dialect,
    ) -> tuple[list[Document], Pipeline]:
        result_limit_hint = self._search_result_limit_hint(pipeline)
        if result_limit_hint is not None:
            return await self._search_documents(), pipeline

        output_limit = self._search_prefix_output_limit(pipeline)
        if output_limit is None:
            return await self._search_documents(), pipeline
        downstream_filter_spec = self._leading_search_downstream_filter_spec(pipeline)

        leading_search = self._leading_search_stage()
        if leading_search is None:
            raise OperationFailure("search stage was not present")
        operator, spec = leading_search
        search_documents = getattr(self._collection._engine, "search_documents", None)
        if not callable(search_documents):
            raise OperationFailure(f"{operator} is not supported by this engine")

        if output_limit == 0:
            return [], []

        fetch_limit = max(output_limit, 1)
        previous_count = -1
        while True:
            documents = await search_documents(
                self._collection._db_name,
                self._collection._collection_name,
                operator,
                spec,
                max_time_ms=self._max_time_ms,
                context=self._session,
                result_limit_hint=fetch_limit,
                downstream_filter_spec=downstream_filter_spec,
            )
            transformed = apply_pipeline(
                documents,
                pipeline,
                variables=self._let,
                dialect=dialect,
                collation=self._collation,
                spill_policy=self._spill_policy(),
            )
            if len(transformed) >= output_limit:
                return transformed, []
            if len(documents) == previous_count or len(documents) < fetch_limit:
                return transformed, []
            previous_count = len(documents)
            fetch_limit = self._next_search_prefix_fetch_limit(
                fetch_limit,
                len(documents),
                len(transformed),
                output_limit,
            )

    @staticmethod
    def _split_streamable_pipeline(
        pipeline: Pipeline,
        *,
        dialect=MONGODB_DIALECT_70,
    ) -> tuple[Pipeline, int, int | None] | None:
        streamable_pipeline: Pipeline = []
        trailing_skip = 0
        trailing_limit: int | None = None
        seen_trailing_window = False

        for stage in pipeline:
            operator, spec = next(iter(stage.items()))
            if operator in {"$skip", "$limit"}:
                seen_trailing_window = True
                if operator == "$skip":
                    trailing_skip += int(spec)
                else:
                    value = int(spec)
                    trailing_limit = value if trailing_limit is None else min(trailing_limit, value)
                continue
            if seen_trailing_window:
                return None
            if not is_streamable_aggregation_stage(operator, dialect=dialect):
                return None
            streamable_pipeline.append(stage)

        return streamable_pipeline, trailing_skip, trailing_limit

    def _collect_collection_names(self, pipeline: Pipeline) -> set[str]:
        names: set[str] = set()
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1:
                continue
            if "$facet" in stage:
                spec = stage["$facet"]
                if isinstance(spec, dict):
                    for subpipeline in spec.values():
                        if isinstance(subpipeline, list):
                            names.update(self._collect_collection_names(subpipeline))
                continue
            if "$lookup" in stage:
                spec = stage["$lookup"]
                if not isinstance(spec, dict):
                    continue
                from_collection = spec.get("from")
                if isinstance(from_collection, str):
                    names.add(from_collection)
                pipeline_spec = spec.get("pipeline")
                if isinstance(pipeline_spec, list):
                    names.update(self._collect_collection_names(pipeline_spec))
                continue
            if "$unionWith" in stage:
                spec = stage["$unionWith"]
                if isinstance(spec, str):
                    if spec:
                        names.add(spec)
                    continue
                if not isinstance(spec, dict):
                    continue
                coll = spec.get("coll")
                if isinstance(coll, str):
                    names.add(coll)
                elif "pipeline" in spec:
                    names.add(_CURRENT_COLLECTION_RESOLVER_KEY)
                pipeline_spec = spec.get("pipeline")
                if isinstance(pipeline_spec, list):
                    names.update(self._collect_collection_names(pipeline_spec))
        return names

    def _collect_lookup_names(self, pipeline: Pipeline) -> set[str]:
        # Compatibilidad interna con tests y callers existentes; ahora recoge
        # también colecciones referenciadas por $unionWith.
        return {
            name
            for name in self._collect_collection_names(pipeline)
            if name != _CURRENT_COLLECTION_RESOLVER_KEY
        }

    async def _load_referenced_collections(self) -> dict[str, list[Document]]:
        names = self._collect_collection_names(self._effective_pipeline())
        loaded: dict[str, list[Document]] = {}
        if _CURRENT_COLLECTION_RESOLVER_KEY in names:
            collection_name = getattr(self._collection, "_collection_name", None)
            if isinstance(collection_name, str):
                loaded[_CURRENT_COLLECTION_RESOLVER_KEY] = await self._load_collection_documents(
                    collection_name,
                )
        for name in names:
            if name == _CURRENT_COLLECTION_RESOLVER_KEY:
                continue
            loaded[name] = await self._load_collection_documents(name)
        return loaded

    def _collect_collstats_scales(self, pipeline: Pipeline) -> set[int]:
        scales: set[int] = set()
        for stage in pipeline:
            if not isinstance(stage, dict) or len(stage) != 1 or "$collStats" not in stage:
                continue
            spec = stage["$collStats"]
            if not isinstance(spec, dict):
                continue
            storage_spec = spec.get("storageStats")
            if isinstance(storage_spec, dict):
                scale = storage_spec.get("scale", 1)
                if isinstance(scale, int) and not isinstance(scale, bool) and scale > 0:
                    scales.add(scale)
            else:
                scales.add(1)
        return scales

    def _collect_current_op_requested(self, pipeline: Pipeline) -> bool:
        return any(
            isinstance(stage, dict) and len(stage) == 1 and "$currentOp" in stage
            for stage in pipeline
        )

    def _collect_plan_cache_stats_requested(self, pipeline: Pipeline) -> bool:
        return any(
            isinstance(stage, dict) and len(stage) == 1 and "$planCacheStats" in stage
            for stage in pipeline
        )

    def _collect_list_sessions_requested(self, pipeline: Pipeline) -> bool:
        return any(
            isinstance(stage, dict) and len(stage) == 1 and "$listSessions" in stage
            for stage in pipeline
        )

    async def _load_collstats_snapshots(self, pipeline: Pipeline) -> dict[int, Document]:
        snapshots: dict[int, Document] = {}
        scales = self._collect_collstats_scales(pipeline)
        if not scales:
            return snapshots
        database = self._collection.database
        for scale in sorted(scales):
            snapshot = await database._admin._collection_stats(
                self._collection._collection_name,
                scale=scale,
                session=self._session,
            )
            snapshots[scale] = snapshot.to_document()
        return snapshots

    def _collect_index_stats_requested(self, pipeline: Pipeline) -> bool:
        return any(
            isinstance(stage, dict) and len(stage) == 1 and "$indexStats" in stage
            for stage in pipeline
        )

    async def _load_index_stats_snapshot(self, pipeline: Pipeline) -> list[Document]:
        if not self._collect_index_stats_requested(pipeline):
            return []
        index_documents = await self._collection.list_indexes(session=self._session).to_list()
        captured_at = datetime.datetime.now(datetime.UTC)
        snapshot: list[Document] = []
        for index_document in index_documents:
            snapshot.append(
                {
                    "name": index_document.get("name"),
                    "key": deepcopy(index_document.get("key")),
                    "spec": deepcopy(index_document),
                    "accesses": {
                        "ops": 0,
                        "since": captured_at,
                    },
                }
            )
        return snapshot

    def _load_plan_cache_stats_snapshot(self, pipeline: Pipeline) -> list[Document]:
        if not self._collect_plan_cache_stats_requested(pipeline):
            return []
        runtime_diagnostics = getattr(self._collection._engine, "_runtime_diagnostics_info", None)
        diagnostics = runtime_diagnostics() if callable(runtime_diagnostics) else {}
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        captured_at = datetime.datetime.now(datetime.UTC)
        return [
            {
                "ns": f"{self._collection._db_name}.{self._collection._collection_name}",
                "isActive": True,
                "isPinned": False,
                "works": 0,
                "timeOfCreation": captured_at,
                "createdFromQuery": {
                    "stage": "$planCacheStats",
                    "runtime": "mongoeco-local",
                },
                "cachedPlan": deepcopy(diagnostics),
            }
        ]

    def _load_list_sessions_snapshot(self, pipeline: Pipeline) -> list[Document]:
        if not self._collect_list_sessions_requested(pipeline):
            return []
        captured_at = datetime.datetime.now(datetime.UTC)
        snapshot_by_id: dict[str, Document] = {}
        if self._session is not None:
            snapshot_by_id[self._session.session_id] = {
                "_id": {"id": self._session.session_id},
                "lastUse": captured_at,
                "causalConsistency": self._session.causal_consistency,
                "inTransaction": self._session.in_transaction,
                "transactionNumber": self._session.transaction_number,
                "engineState": deepcopy(self._session.engine_state),
            }
        snapshot_active_operations = getattr(self._collection._engine, "_snapshot_active_operations", None)
        operation_snapshot = snapshot_active_operations() if callable(snapshot_active_operations) else []
        if isinstance(operation_snapshot, list):
            for operation in operation_snapshot:
                if not isinstance(operation, dict):
                    continue
                session_id = operation.get("sessionId")
                if not isinstance(session_id, str) or not session_id:
                    continue
                snapshot_by_id.setdefault(
                    session_id,
                    {
                        "_id": {"id": session_id},
                        "lastUse": captured_at,
                        "fromCurrentOp": True,
                    },
                )
        return list(snapshot_by_id.values())

    def _scan_collection_with_operation(
        self,
        collection_name: str,
        operation: FindOperation,
    ):
        engine = self._collection._engine
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

        semantics = compile_find_semantics_from_operation(operation, dialect=dialect)
        return engine.scan_find_semantics(
            self._collection._db_name,
            collection_name,
            semantics,
            context=self._session,
        )

    async def _load_collection_documents(self, collection_name: str) -> list[Document]:
        operation = compile_find_operation(
            {},
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
        )
        return [
            document
            async for document in self._scan_collection_with_operation(
                collection_name,
                operation,
            )
        ]

    def _build_pushdown_cursor(self, operation: FindOperation):
        build_cursor = getattr(self._collection, "_build_cursor", None)
        if callable(build_cursor):
            return build_cursor(
                operation,
                session=self._session,
            )
        return self._collection.find(
            operation.filter_spec,
            operation.projection,
            collation=operation.collation,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            batch_size=operation.batch_size,
            session=self._session,
        )

    async def _materialize(self) -> list[Document]:
        _ensure_operation_executable(self._collection, self._operation)
        deadline = operation_deadline(self._max_time_ms)
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        pipeline = self._effective_pipeline()
        pipeline, writeback_stage = self._split_terminal_writeback_stage(pipeline)
        with track_active_operation(
            self._collection._engine,
            command_name="aggregate",
            operation_type="aggregate",
            namespace=f"{self._collection._db_name}.{self._collection._collection_name}",
            session=self._session,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
        ):
            enforce_deadline(deadline)
            if self._leading_search_stage() is not None:
                documents, remaining_pipeline = await self._materialize_leading_search_pipeline(
                    pipeline,
                    dialect=dialect,
                )
            else:
                pushdown = split_pushdown_pipeline(
                    pipeline,
                    dialect=dialect,
                )
                remaining_pipeline = pushdown.remaining_pipeline
                if (
                    remaining_pipeline
                    and isinstance(remaining_pipeline[0], dict)
                    and (
                        "$collStats" in remaining_pipeline[0]
                        or "$indexStats" in remaining_pipeline[0]
                        or "$currentOp" in remaining_pipeline[0]
                        or "$planCacheStats" in remaining_pipeline[0]
                        or "$listSessions" in remaining_pipeline[0]
                    )
                ):
                    documents = []
                else:
                    documents = await self._build_pushdown_cursor(
                        self._pushdown_find_operation()
                    ).to_list()
            referenced_collections = await self._load_referenced_collections()
            collstats_snapshots = await self._load_collstats_snapshots(remaining_pipeline)
            index_stats_snapshot = await self._load_index_stats_snapshot(remaining_pipeline)
            current_op_requested = self._collect_current_op_requested(remaining_pipeline)
            plan_cache_stats_snapshot = self._load_plan_cache_stats_snapshot(remaining_pipeline)
            list_sessions_requested = self._collect_list_sessions_requested(remaining_pipeline)
            list_sessions_snapshot = self._load_list_sessions_snapshot(remaining_pipeline)
            snapshot_active_operations = getattr(self._collection._engine, "_snapshot_active_operations", None)
            current_op_snapshot = (
                snapshot_active_operations()
                if current_op_requested and callable(snapshot_active_operations)
                else []
            )
            enforce_deadline(deadline)
            enforce_deadline(deadline)
            self._enforce_materialization_budget(
                len(documents),
                remaining_pipeline,
                dialect=dialect,
            )
            collection_stats_resolver = None
            if collstats_snapshots:
                default_collstats_snapshot = next(iter(collstats_snapshots.values()))
                collection_stats_resolver = lambda scale: deepcopy(
                    collstats_snapshots.get(scale, default_collstats_snapshot)
                )
            index_stats_resolver = None
            if index_stats_snapshot:
                index_stats_resolver = lambda: deepcopy(index_stats_snapshot)
            current_op_resolver = None
            if current_op_requested:
                current_op_resolver = lambda: deepcopy(current_op_snapshot)
            plan_cache_stats_resolver = None
            if plan_cache_stats_snapshot:
                plan_cache_stats_resolver = lambda: deepcopy(plan_cache_stats_snapshot)
            list_sessions_resolver = None
            if list_sessions_requested:
                list_sessions_resolver = lambda: deepcopy(list_sessions_snapshot)
            result = apply_pipeline(
                documents,
                remaining_pipeline,
                collection_resolver=referenced_collections.get,
                collection_stats_resolver=collection_stats_resolver,
                index_stats_resolver=index_stats_resolver,
                current_op_resolver=current_op_resolver,
                plan_cache_stats_resolver=plan_cache_stats_resolver,
                list_sessions_resolver=list_sessions_resolver,
                variables=self._let,
                dialect=dialect,
                collation=self._collation,
                spill_policy=self._spill_policy(),
            )
            enforce_deadline(deadline)
            if writeback_stage is not None:
                _operator, spec = writeback_stage
                await self._apply_merge_stage(result, spec)
                return []
            return [DocumentCodec.to_public(strip_search_result_metadata(document)) for document in result]

    def _cost_policy(self) -> AggregationCostPolicy | None:
        policy = getattr(self._collection._engine, "aggregation_cost_policy", None)
        if isinstance(policy, AggregationCostPolicy):
            return policy
        return None

    def _enforce_materialization_budget(
        self,
        document_count: int,
        pipeline: Pipeline,
        *,
        dialect,
    ) -> None:
        policy = self._cost_policy()
        if policy is None:
            return
        policy.enforce_budget(
            document_count=document_count,
            has_materializing_stage=has_materializing_aggregation_stage(
                pipeline,
                dialect=dialect,
            ),
            spill_available=self._spill_policy() is not None,
        )

    def _pushdown_find_operation(self, *, batch_size: int | None = None) -> FindOperation:
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        pushdown = split_pushdown_pipeline(
            self._effective_pipeline(),
            dialect=dialect,
        )
        return compile_find_operation(
            pushdown.filter_spec,
            projection=pushdown.projection,
            sort=pushdown.sort,
            skip=pushdown.skip,
            limit=pushdown.limit,
            collation=self._operation.collation,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=batch_size if batch_size is not None else self._batch_size,
            dialect=dialect,
        )

    async def _stream_batches(self) -> AsyncIterator[Document]:
        _ensure_operation_executable(self._collection, self._operation)
        if self._leading_search_stage() is not None:
            for document in await self._materialize():
                yield document
            return
        effective_pipeline, writeback_stage = self._split_terminal_writeback_stage(self._effective_pipeline())
        if writeback_stage is not None:
            for document in await self._materialize():
                yield document
            return
        if self._batch_size in (None, 0):
            for document in await self._materialize():
                yield document
            return

        deadline = operation_deadline(self._max_time_ms)
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        pushdown = split_pushdown_pipeline(
            effective_pipeline,
            dialect=dialect,
        )
        stream_plan = self._split_streamable_pipeline(
            pushdown.remaining_pipeline,
            dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
        )
        if stream_plan is None:
            for document in await self._materialize():
                yield document
            return

        streamable_pipeline, trailing_skip, trailing_limit = stream_plan
        enforce_deadline(deadline)
        referenced_collections = await self._load_referenced_collections()
        enforce_deadline(deadline)

        source_offset = 0
        remaining_limit = trailing_limit
        while True:
            if remaining_limit == 0:
                return
            page_limit = self._batch_size
            if pushdown.limit is not None:
                remaining_source = pushdown.limit - source_offset
                if remaining_source <= 0:
                    return
                page_limit = min(page_limit, remaining_source)

            page = await self._build_pushdown_cursor(
                self._pushdown_find_operation().with_overrides(
                    skip=pushdown.skip + source_offset,
                    limit=page_limit,
                )
            ).to_list()
            enforce_deadline(deadline)
            if not page:
                return

            source_offset += len(page)
            transformed = apply_pipeline(
                page,
                streamable_pipeline,
                collection_resolver=referenced_collections.get,
                variables=self._let,
                dialect=dialect,
                collation=self._collation,
                spill_policy=self._spill_policy(),
            )
            enforce_deadline(deadline)

            if trailing_skip:
                if len(transformed) <= trailing_skip:
                    trailing_skip -= len(transformed)
                    continue
                transformed = transformed[trailing_skip:]
                trailing_skip = 0

            if remaining_limit is not None:
                transformed = transformed[:remaining_limit]
                remaining_limit -= len(transformed)

            for document in transformed:
                yield DocumentCodec.to_public(strip_search_result_metadata(document))

    async def to_list(self) -> list[Document]:
        started_at = time.perf_counter_ns()
        try:
            documents = [document async for document in self]
        except Exception as exc:
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="command",
                    command={"aggregate": self._collection._collection_name, "pipeline": list(self._pipeline)},
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
            raise
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="command",
                command={"aggregate": self._collection._collection_name, "pipeline": list(self._pipeline)},
                duration_ns=time.perf_counter_ns() - started_at,
            )
        return documents

    async def first(self) -> Document | None:
        started_at = time.perf_counter_ns()
        async for document in self:
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="command",
                    command={"aggregate": self._collection._collection_name, "pipeline": list(self._pipeline)},
                    duration_ns=time.perf_counter_ns() - started_at,
                )
            return document
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="command",
                command={"aggregate": self._collection._collection_name, "pipeline": list(self._pipeline)},
                duration_ns=time.perf_counter_ns() - started_at,
            )
        return None

    async def explain(self) -> dict[str, object]:
        if self._operation.planning_issues:
            explanation = AggregateExplanation(
                engine_plan=QueryPlanExplanation(
                    engine="planner",
                    strategy="deferred",
                    plan="planning-issues",
                    sort=None,
                    skip=0,
                    limit=None,
                    hint=self._hint,
                    hinted_index=None,
                    comment=self._comment,
                    max_time_ms=self._max_time_ms,
                    details={"reason": _operation_issue_message(self._operation)},
                    planning_mode=self._operation.planning_mode,
                    planning_issues=self._operation.planning_issues,
                ),
                remaining_pipeline=list(self._pipeline),
                pushdown={
                    "mode": "deferred",
                    "totalStages": len(self._pipeline),
                    "pushedDownStages": 0,
                    "remainingStages": len(self._pipeline),
                    "streamingEligible": False,
                    "streamableStageCount": 0,
                },
                hint=self._hint,
                comment=self._comment,
                max_time_ms=self._max_time_ms,
                batch_size=self._batch_size,
                allow_disk_use=self._allow_disk_use,
                let=self._let,
                streaming_batch_execution=False,
                planning_mode=self._operation.planning_mode,
                planning_issues=self._operation.planning_issues,
            ).to_document()
            explanation['cxp'] = self._cxp_explain_projection()
            return explanation
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        streamable_pipeline: list[object] = []
        pushdown_summary: dict[str, object]
        if self._leading_search_stage() is not None:
            operator, spec = self._leading_search_stage()
            remaining_pipeline = self._effective_pipeline()
            result_limit_hint = self._search_result_limit_hint(remaining_pipeline)
            prefix_output_limit = self._search_prefix_output_limit(remaining_pipeline)
            effective_limit_hint = result_limit_hint if result_limit_hint is not None else prefix_output_limit
            streamable_pipeline = remaining_pipeline
            pushdown_summary = {
                "mode": "search",
                "totalStages": len(self._pipeline),
                "pushedDownStages": 1,
                "remainingStages": len(remaining_pipeline),
                "leadingSearchOperator": operator,
                "searchResultLimitHint": effective_limit_hint,
                "searchTopKStrategy": (
                    "direct-window"
                    if result_limit_hint is not None
                    else "prefix-iterative"
                    if prefix_output_limit is not None
                    else None
                ),
                "searchTopKGrowthStrategy": (
                    "adaptive-retention" if result_limit_hint is None and prefix_output_limit is not None else None
                ),
                "searchDownstreamFilterPrefilter": self._leading_search_downstream_filter_spec(remaining_pipeline) is not None,
            }
            explain_search_documents = getattr(
                self._collection._engine,
                "explain_search_documents",
                None,
            )
            if callable(explain_search_documents):
                engine_plan = await explain_search_documents(
                    self._collection._db_name,
                    self._collection._collection_name,
                    operator,
                    spec,
                    max_time_ms=self._max_time_ms,
                    context=self._session,
                    result_limit_hint=effective_limit_hint,
                    downstream_filter_spec=self._leading_search_downstream_filter_spec(remaining_pipeline),
                )
            else:
                engine_plan = QueryPlanExplanation(
                    engine="search",
                    strategy="search",
                    plan="unsupported-search-engine",
                    sort=None,
                    skip=0,
                    limit=None,
                    hint=self._hint,
                    hinted_index=None,
                    comment=self._comment,
                    max_time_ms=self._max_time_ms,
                    details={"operator": operator},
                )
        else:
            pushdown = split_pushdown_pipeline(
                self._effective_pipeline(),
                dialect=dialect,
            )
            remaining_pipeline = pushdown.remaining_pipeline
            streamable_pipeline = pushdown.remaining_pipeline
            pushdown_summary = {
                "mode": "pipeline-prefix",
                "totalStages": len(self._pipeline),
                "pushedDownStages": len(self._effective_pipeline()) - len(remaining_pipeline),
                "remainingStages": len(remaining_pipeline),
            }
            operation = self._pushdown_find_operation()
            from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

            semantics = compile_find_semantics_from_operation(operation, dialect=dialect)
            engine_plan = await self._collection._engine.explain_find_semantics(
                self._collection._db_name,
                self._collection._collection_name,
                semantics,
                context=self._session,
            )
        streaming_split = self._split_streamable_pipeline(
            streamable_pipeline,
            dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
        )
        pushdown_summary["streamingEligible"] = (
            self._batch_size not in (None, 0) and streaming_split is not None
        )
        pushdown_summary["streamableStageCount"] = (
            len(streaming_split[0]) if streaming_split is not None else 0
        )
        explanation = AggregateExplanation(
            engine_plan=engine_plan,
            remaining_pipeline=remaining_pipeline,
            pushdown=pushdown_summary,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            allow_disk_use=self._allow_disk_use,
            let=self._let,
            streaming_batch_execution=bool(pushdown_summary["streamingEligible"]),
        ).to_document()
        explanation['cxp'] = self._cxp_explain_projection()
        return explanation

    def __aiter__(self) -> AsyncIterator[Document]:
        return self._stream_batches()

    def _spill_policy(self) -> AggregationSpillPolicy | None:
        if self._allow_disk_use is False:
            return None
        policy = getattr(self._collection._engine, "aggregation_spill_policy", None)
        if isinstance(policy, AggregationSpillPolicy):
            return policy
        if callable(policy):
            resolved = policy()
            if isinstance(resolved, AggregationSpillPolicy):
                return resolved
        return None

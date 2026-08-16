import datetime
import math
import time

from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass

from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api._async.cursor import (
    HintSpec,
    _ensure_operation_executable,
    _operation_issue_message,
    _resolve_planning_mode,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core._search_contract import TEXT_SEARCH_OPERATOR_NAMES
from mongoeco.core.aggregation import (
    _CURRENT_COLLECTION_RESOLVER_KEY,
    AggregationCostPolicy,
    AggregationSpillPolicy,
    Pipeline,
    apply_pipeline,
    has_materializing_aggregation_stage,
    is_streamable_aggregation_stage,
    split_pushdown_pipeline,
)
from mongoeco.core.aggregation.runtime_state import apply_pipeline_states
from mongoeco.core.bson_scalars import utc_bson_now
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import normalize_collation
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
)
from mongoeco.core.identity import (
    assert_valid_root_document_id,
)
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
    resolve_operation_session,
)
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    prepare_persistence_document,
    prepare_public_document,
)
from mongoeco.core.search import (
    compile_search_stage,
    serialize_search_metadata,
    strip_search_result_metadata,
)
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import (
    SearchExecutionMode,
    SearchExplainVerbosity,
)
from mongoeco.core.search_planning import (
    SearchPipelinePlan,
    SearchPipelineStrategy,
    SearchPlanningMode,
    compile_search_pipeline_plan,
    leading_search_downstream_filter_spec,
    search_prefix_output_limit,
    search_result_limit_hint,
)
from mongoeco.cxp import build_mongodb_explain_projection
from mongoeco.engines.adapter import adapt_engine
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session_guards import ensure_session_can_use_engine
from mongoeco.types import (
    AggregateExplanation,
    Document,
    ObjectId,
    QueryPlanExplanation,
)


@dataclass(frozen=True, slots=True)
class _StreamWindow:
    start: int = 0
    end: int | None = None

    def skip(self, count: int) -> "_StreamWindow":
        next_start = self.start + count
        if self.end is not None:
            next_start = min(next_start, self.end)
        return _StreamWindow(next_start, self.end)

    def limit(self, count: int) -> "_StreamWindow":
        next_end = self.start + count
        if self.end is not None:
            next_end = min(next_end, self.end)
        return _StreamWindow(self.start, next_end)

    def as_skip_limit(self) -> tuple[int, int | None]:
        if self.end is None:
            return self.start, None
        return self.start, max(self.end - self.start, 0)


_SearchOptimizationStrategy = SearchPipelineStrategy
_SearchOptimizationPlan = SearchPipelinePlan


class AsyncAggregationCursor:
    """Cursor async mínimo para resultados de aggregate()."""

    def __await__(self):
        async def _resolve():
            return self

        return _resolve().__await__()

    _force_full_search_execution = False

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
                dialect=getattr(
                    collection,
                    "mongodb_dialect",
                    MONGODB_DIALECT_70,
                ),
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
        create_context = getattr(
            self._collection,
            "_new_operation_context",
            None,
        )
        if operation.context is not None:
            self._operation_context = operation.context
            self._execution_context = operation.context.expressions
        else:
            context = (
                create_context(
                    session=session,
                    collation=self._collation,
                    bindings=self._let,
                )
                if callable(create_context)
                else None
            )
            if context is None:
                context = OperationContext.create(
                    dialect=getattr(
                        collection,
                        "mongodb_dialect",
                        MONGODB_DIALECT_70,
                    ),
                    codec_options=getattr(collection, "_codec_options", None),
                    session=session,
                    collation=self._collation,
                    expressions=ExpressionExecutionContext(
                        now=utc_bson_now(),
                    ).with_bindings(self._let),
                )
                self._operation_context = context
                self._execution_context = context.expressions
                self._operation = operation.bind(context)
            else:
                self._operation_context = context
                self._execution_context = context.expressions
                self._operation = operation.bind(context)
        self._session = resolve_operation_session(
            self._operation_context,
            session,
        )
        self._active_async_iterator: AsyncIterator[Document] | None = None
        self._closed = False

    def _execution_variables(self) -> ExpressionExecutionContext:
        return self._execution_context

    def _ensure_session_can_use_engine(self) -> None:
        ensure_session_can_use_engine(self._collection._engine, self._session)

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
            return build_mongodb_explain_projection(capability="aggregation")
        operator, _spec = leading_search
        if operator == "$vectorSearch":
            return build_mongodb_explain_projection(
                capability="aggregation",
                additional_capabilities=("vector_search",),
            )
        return build_mongodb_explain_projection(
            capability="aggregation",
            additional_capabilities=("search",),
        )

    @classmethod
    def _search_result_limit_hint(cls, pipeline: Pipeline) -> int | None:
        return search_result_limit_hint(pipeline)

    @classmethod
    def _search_prefix_output_limit(cls, pipeline: Pipeline) -> int | None:
        return search_prefix_output_limit(pipeline)

    @staticmethod
    def _leading_search_downstream_filter_spec(
        pipeline: Pipeline,
    ) -> dict[str, object] | None:
        return leading_search_downstream_filter_spec(pipeline)

    def _search_optimization_plan(
        self,
        operator: str,
        spec: object,
        pipeline: Pipeline,
        *,
        writeback: bool,
    ) -> _SearchOptimizationPlan:
        return compile_search_pipeline_plan(
            operator,
            spec,
            pipeline,
            writeback=writeback,
            mode=(
                SearchPlanningMode.REFERENCE
                if self._force_full_search_execution
                else SearchPlanningMode.OPTIMIZED
            ),
        )

    @staticmethod
    def _next_search_prefix_fetch_limit(
        current_fetch_limit: int,
        fetched_count: int,
        transformed_count: int,
        output_limit: int,
    ) -> int:
        if transformed_count <= 0:
            return max(current_fetch_limit + 1, current_fetch_limit * 4)
        estimated = math.ceil(
            (fetched_count * output_limit) / transformed_count,
        )
        safety_margin = max(1, output_limit - transformed_count)
        return max(current_fetch_limit + 1, estimated + safety_margin)

    @staticmethod
    def _split_terminal_writeback_stage(
        pipeline: Pipeline,
    ) -> tuple[Pipeline, tuple[str, object] | None]:
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
                raise OperationFailure(
                    "$merge is only supported as the final aggregation stage",
                )
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
            now_factory=self._collection._now_factory,
        )

    async def _apply_merge_stage(
        self,
        documents: list[Document] | list[RuntimeDocumentState],
        spec: object,
    ) -> None:
        if not isinstance(spec, dict):
            raise OperationFailure("$merge requires a document specification")
        into = spec.get("into")
        if isinstance(into, str):
            target_db_name = self._collection._db_name
            target_coll_name = into
        elif isinstance(into, dict):
            target_db_name = spec_db = into.get(
                "db",
                self._collection._db_name,
            )
            target_coll_name = into.get("coll")
            if not isinstance(spec_db, str) or not spec_db:
                raise OperationFailure(
                    "$merge.into.db must be a non-empty string",
                )
        else:
            raise OperationFailure(
                "$merge.into must be a collection name or {db, coll}",
            )
        if not isinstance(target_coll_name, str) or not target_coll_name:
            raise OperationFailure(
                "$merge.into.coll must be a non-empty string",
            )
        on = spec.get("on", "_id")
        if on not in (None, "_id"):
            raise OperationFailure(
                "$merge currently supports only omitted on or on: '_id'",
            )
        when_matched = spec.get("whenMatched", "merge")
        if isinstance(when_matched, list):
            raise OperationFailure(
                "$merge whenMatched pipelines are not supported in the local runtime",
            )
        if when_matched not in {"replace", "merge", "keepExisting", "fail"}:
            raise OperationFailure(
                "$merge whenMatched currently supports replace, merge, keepExisting or fail",
            )
        when_not_matched = spec.get("whenNotMatched", "insert")
        if when_not_matched not in {"insert", "discard", "fail"}:
            raise OperationFailure(
                "$merge whenNotMatched currently supports insert, discard or fail",
            )

        target_collection = self._target_database(
            target_db_name,
        ).get_collection(target_coll_name)

        for source_document in documents:
            candidate = (
                prepare_persistence_document(source_document)
                if isinstance(source_document, RuntimeDocumentState)
                else strip_search_result_metadata(
                    deepcopy(source_document),
                    for_persistence=True,
                )
            )
            if "_id" not in candidate:
                candidate["_id"] = ObjectId()
            assert_valid_root_document_id(candidate["_id"])
            internal_candidate = DocumentCodec.to_internal(candidate)
            operation_context = target_collection._new_operation_context(
                session=self._session,
                expressions=self._execution_context,
                publication=(
                    ChangePublicationPolicy.EMIT
                    if target_collection._should_publish_change_events(
                        session=self._session,
                    )
                    else ChangePublicationPolicy.RECORD_GAP
                ),
            )
            outcome = await target_collection._runtime.engine_merge_document(
                internal_candidate,
                when_matched=when_matched,
                when_not_matched=when_not_matched,
                operation_context=operation_context,
            )
            if outcome.matched and when_matched == "fail":
                raise OperationFailure(
                    "$merge whenMatched=fail found an existing target document "
                    f"for _id={candidate['_id']!r}",
                )
            if not outcome.matched and when_not_matched == "fail":
                raise OperationFailure(
                    "$merge whenNotMatched=fail found no target document "
                    f"for _id={candidate['_id']!r}",
                )

    async def _search_states(
        self,
        optimization: _SearchOptimizationPlan | None = None,
    ) -> list[RuntimeDocumentState]:
        if optimization is None:
            optimization = _SearchOptimizationPlan()
        leading_search = self._leading_search_stage()
        if leading_search is None:
            message = "search stage was not present"
            raise OperationFailure(message)
        operator, spec = leading_search
        _query, outcome = await self._execute_search(
            operator,
            spec,
            result_limit_hint=optimization.result_limit_hint,
            downstream_filter_spec=optimization.downstream_filter_spec,
            pipeline_plan=optimization,
        )
        documents = outcome.runtime_states
        if operator != "$searchMeta":
            return documents
        return [RuntimeDocumentState(serialize_search_metadata(outcome.metadata))]

    async def _search_documents(
        self,
        optimization: _SearchOptimizationPlan | None = None,
    ) -> list[Document]:
        """Compatibility view for callers of the pre-4.6 private helper."""
        return [
            state.public_document() for state in await self._search_states(optimization)
        ]

    async def _execute_search(
        self,
        operator: str,
        spec: object,
        *,
        result_limit_hint: int | None,
        downstream_filter_spec: dict[str, object] | None,
        pipeline_plan: SearchPipelinePlan | None = None,
    ):
        query, request = self._build_search_request(
            operator,
            spec,
            result_limit_hint=result_limit_hint,
            downstream_filter_spec=downstream_filter_spec,
            pipeline_plan=pipeline_plan,
        )
        outcome = await adapt_engine(
            self._collection._engine,
        ).execute_search(
            self._collection._db_name,
            self._collection._collection_name,
            request,
        )
        return query, outcome

    def _build_search_request(
        self,
        operator: str,
        spec: object,
        *,
        result_limit_hint: int | None,
        downstream_filter_spec: dict[str, object] | None,
        pipeline_plan: SearchPipelinePlan | None = None,
    ):
        if self._operation_context is None:
            message = "Search execution requires OperationContext"
            raise RuntimeError(message)
        query = compile_search_stage(operator, spec)
        request = SearchRequest(
            operator=operator,
            specification=spec,
            query=query,
            mode=(
                SearchExecutionMode.METADATA
                if operator == "$searchMeta"
                else SearchExecutionMode.HITS
            ),
            operation_context=self._operation_context,
            runtime_operator=("$search" if operator == "$searchMeta" else None),
            runtime_specification=(
                self._search_runtime_spec_for_stage(operator, spec)
                if operator == "$searchMeta"
                else None
            ),
            max_time_ms=self._max_time_ms,
            result_limit_hint=result_limit_hint,
            downstream_filter_spec=downstream_filter_spec,
            pipeline_plan=pipeline_plan,
        )
        return query, request

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
        clause_names = [
            name for name in TEXT_SEARCH_OPERATOR_NAMES if name in operator_spec
        ]
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
        writeback: bool = False,
    ) -> tuple[list[RuntimeDocumentState], Pipeline]:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            raise OperationFailure("search stage was not present")
        operator, spec = leading_search
        optimization = self._search_optimization_plan(
            operator,
            spec,
            pipeline,
            writeback=writeback,
        )
        if optimization.strategy in {
            _SearchOptimizationStrategy.DIRECT_WINDOW,
            _SearchOptimizationStrategy.EMPTY,
        }:
            return await self._search_states(optimization), pipeline

        output_limit = optimization.prefix_output_limit
        if output_limit is None:
            return await self._search_states(optimization), pipeline

        fetch_limit = max(output_limit, 1)
        previous_count = -1
        while True:
            _query, outcome = await self._execute_search(
                operator,
                spec,
                result_limit_hint=fetch_limit,
                downstream_filter_spec=optimization.downstream_filter_spec,
                pipeline_plan=optimization,
            )
            documents = outcome.runtime_states
            transformed = apply_pipeline_states(
                documents,
                pipeline,
                variables=self._execution_variables(),
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
        trailing_window = _StreamWindow()
        seen_trailing_window = False

        for stage in pipeline:
            operator, spec = next(iter(stage.items()))
            if operator in {"$skip", "$limit"}:
                seen_trailing_window = True
                if operator == "$skip":
                    trailing_window = trailing_window.skip(int(spec))
                else:
                    trailing_window = trailing_window.limit(int(spec))
                continue
            if seen_trailing_window:
                return None
            if not is_streamable_aggregation_stage(operator, dialect=dialect):
                return None
            streamable_pipeline.append(stage)

        trailing_skip, trailing_limit = trailing_window.as_skip_limit()
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
                            names.update(
                                self._collect_collection_names(subpipeline),
                            )
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
            collection_name = getattr(
                self._collection,
                "_collection_name",
                None,
            )
            if isinstance(collection_name, str):
                loaded[
                    _CURRENT_COLLECTION_RESOLVER_KEY
                ] = await self._load_collection_documents(
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
            if (
                not isinstance(stage, dict)
                or len(stage) != 1
                or "$collStats" not in stage
            ):
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

    async def _load_collstats_snapshots(
        self,
        pipeline: Pipeline,
    ) -> dict[int, Document]:
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

    async def _load_index_stats_snapshot(
        self,
        pipeline: Pipeline,
    ) -> list[Document]:
        if not self._collect_index_stats_requested(pipeline):
            return []
        index_documents = await self._collection.list_indexes(
            session=self._session,
        ).to_list()
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
                },
            )
        return snapshot

    def _load_plan_cache_stats_snapshot(
        self,
        pipeline: Pipeline,
    ) -> list[Document]:
        if not self._collect_plan_cache_stats_requested(pipeline):
            return []
        runtime_diagnostics = getattr(
            self._collection._engine,
            "_runtime_diagnostics_info",
            None,
        )
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
            },
        ]

    def _load_list_sessions_snapshot(
        self,
        pipeline: Pipeline,
    ) -> list[Document]:
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
        snapshot_active_operations = getattr(
            self._collection._engine,
            "_snapshot_active_operations",
            None,
        )
        operation_snapshot = (
            snapshot_active_operations() if callable(snapshot_active_operations) else []
        )
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
        dialect = getattr(
            self._collection,
            "mongodb_dialect",
            MONGODB_DIALECT_70,
        )
        from mongoeco.engines.semantic_core import (  # noqa: PLC0415
            compile_find_semantics_from_operation,
        )

        bound_operation = (
            operation
            if self._operation_context is None
            else operation.bind(self._operation_context)
        )
        semantics = compile_find_semantics_from_operation(
            bound_operation,
            dialect=dialect,
            variables=self._execution_variables(),
        )
        return adapt_engine(engine).open_read_snapshot(
            self._collection._db_name,
            collection_name,
            semantics,
            operation_context=self._operation_context,
        )

    async def _load_collection_documents(
        self,
        collection_name: str,
    ) -> list[Document]:
        operation = compile_find_operation(
            {},
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            dialect=getattr(
                self._collection,
                "mongodb_dialect",
                MONGODB_DIALECT_70,
            ),
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
            try:
                return build_cursor(
                    operation,
                    session=self._session,
                    apply_codec_options=False,
                    execution_variables=self._execution_variables(),
                )
            except TypeError as exc:
                if "apply_codec_options" not in str(exc):
                    raise
                return build_cursor(
                    operation,
                    session=self._session,
                )
        options = {
            "collation": operation.collation,
            "sort": operation.sort,
            "skip": operation.skip,
            "limit": operation.limit,
            "hint": operation.hint,
            "comment": operation.comment,
            "max_time_ms": operation.max_time_ms,
            "batch_size": operation.batch_size,
            "let": operation.let,
            "session": self._session,
        }
        try:
            return self._collection.find(
                operation.filter_spec,
                operation.projection,
                **options,
            )
        except TypeError as exc:
            if "let" not in str(exc):
                raise
            options.pop("let")
            return self._collection.find(
                operation.filter_spec,
                operation.projection,
                **options,
            )

    async def _materialize(  # noqa: PLR0915 - operation orchestration boundary
        self,
    ) -> list[Document]:
        _ensure_operation_executable(self._collection, self._operation)
        self._ensure_session_can_use_engine()
        deadline = operation_deadline(self._max_time_ms)
        dialect = getattr(
            self._collection,
            "mongodb_dialect",
            MONGODB_DIALECT_70,
        )
        pipeline = self._effective_pipeline()
        pipeline, writeback_stage = self._split_terminal_writeback_stage(
            pipeline,
        )
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
                (
                    documents,
                    remaining_pipeline,
                ) = await self._materialize_leading_search_pipeline(
                    pipeline,
                    dialect=dialect,
                    writeback=writeback_stage is not None,
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
                        self._pushdown_find_operation(),
                    ).to_list()
            referenced_collections = await self._load_referenced_collections()
            collstats_snapshots = await self._load_collstats_snapshots(
                remaining_pipeline,
            )
            index_stats_snapshot = await self._load_index_stats_snapshot(
                remaining_pipeline,
            )
            current_op_requested = self._collect_current_op_requested(
                remaining_pipeline,
            )
            plan_cache_stats_snapshot = self._load_plan_cache_stats_snapshot(
                remaining_pipeline,
            )
            list_sessions_requested = self._collect_list_sessions_requested(
                remaining_pipeline,
            )
            list_sessions_snapshot = self._load_list_sessions_snapshot(
                remaining_pipeline,
            )
            snapshot_active_operations = getattr(
                self._collection._engine,
                "_snapshot_active_operations",
                None,
            )
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
                default_collstats_snapshot = next(
                    iter(collstats_snapshots.values()),
                )
                collection_stats_resolver = lambda scale: deepcopy(
                    collstats_snapshots.get(scale, default_collstats_snapshot),
                )
            index_stats_resolver = None
            if index_stats_snapshot:
                index_stats_resolver = lambda: deepcopy(index_stats_snapshot)
            current_op_resolver = None
            if current_op_requested:
                current_op_resolver = lambda: deepcopy(current_op_snapshot)
            plan_cache_stats_resolver = None
            if plan_cache_stats_snapshot:
                plan_cache_stats_resolver = lambda: deepcopy(
                    plan_cache_stats_snapshot,
                )
            list_sessions_resolver = None
            if list_sessions_requested:
                list_sessions_resolver = lambda: deepcopy(
                    list_sessions_snapshot,
                )
            pipeline_kwargs = {
                "collection_resolver": referenced_collections.get,
                "collection_stats_resolver": collection_stats_resolver,
                "index_stats_resolver": index_stats_resolver,
                "current_op_resolver": current_op_resolver,
                "plan_cache_stats_resolver": plan_cache_stats_resolver,
                "list_sessions_resolver": list_sessions_resolver,
                "variables": self._execution_variables(),
                "dialect": dialect,
                "collation": self._collation,
                "spill_policy": self._spill_policy(),
            }
            result = (
                apply_pipeline_states(
                    documents,
                    remaining_pipeline,
                    **pipeline_kwargs,
                )
                if self._leading_search_stage() is not None
                else apply_pipeline(
                    documents,
                    remaining_pipeline,
                    **pipeline_kwargs,
                )
            )
            enforce_deadline(deadline)
            if writeback_stage is not None:
                _operator, spec = writeback_stage
                await self._apply_merge_stage(result, spec)
                return []
            return [
                DocumentCodec.to_public(
                    prepare_public_document(document)
                    if isinstance(document, RuntimeDocumentState)
                    else strip_search_result_metadata(document)
                )
                for document in result
            ]

    def _cost_policy(self) -> AggregationCostPolicy | None:
        policy = getattr(
            self._collection._engine,
            "aggregation_cost_policy",
            None,
        )
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

    def _pushdown_find_operation(
        self,
        *,
        batch_size: int | None = None,
    ) -> FindOperation:
        dialect = getattr(
            self._collection,
            "mongodb_dialect",
            MONGODB_DIALECT_70,
        )
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
            variables=self._let,
            dialect=dialect,
        ).with_overrides(
            context=self._operation_context,
            let=self._execution_context,
        )

    def _materialize_document(self, document: Document) -> Document:
        applier = getattr(
            self._collection,
            "_apply_codec_options_to_document",
            None,
        )
        if callable(applier):
            return applier(document)
        return document

    async def _stream_batches(self) -> AsyncIterator[Document]:
        _ensure_operation_executable(self._collection, self._operation)
        self._ensure_session_can_use_engine()
        if self._leading_search_stage() is not None:
            for document in await self._materialize():
                yield self._materialize_document(document)
            return
        effective_pipeline, writeback_stage = self._split_terminal_writeback_stage(
            self._effective_pipeline(),
        )
        if writeback_stage is not None:
            for document in await self._materialize():
                yield self._materialize_document(document)
            return
        if self._batch_size in (None, 0):
            for document in await self._materialize():
                yield self._materialize_document(document)
            return

        deadline = operation_deadline(self._max_time_ms)
        dialect = getattr(
            self._collection,
            "mongodb_dialect",
            MONGODB_DIALECT_70,
        )
        pushdown = split_pushdown_pipeline(effective_pipeline, dialect=dialect)
        stream_plan = self._split_streamable_pipeline(
            pushdown.remaining_pipeline,
            dialect=dialect,
        )
        if stream_plan is None:
            for document in await self._materialize():
                yield self._materialize_document(document)
            return

        streamable_pipeline, trailing_skip, remaining_limit = stream_plan
        if remaining_limit == 0:
            return
        source_cursor = None
        source_iterator = None
        try:
            referenced_collections = await self._load_referenced_collections()
            source_cursor = self._build_pushdown_cursor(
                self._pushdown_find_operation(batch_size=self._batch_size),
            )
            source_iterator_factory = getattr(source_cursor, "__aiter__", None)
            source_iterator = (
                source_iterator_factory() if callable(source_iterator_factory) else None
            )
            pull_chunk = getattr(source_iterator, "pull_chunk", None)
            materialized_source = (
                None if callable(pull_chunk) else await source_cursor.to_list()
            )
            source_offset = 0
            while remaining_limit != 0:
                if callable(pull_chunk):
                    page = await pull_chunk(self._batch_size)
                else:
                    page = materialized_source[
                        source_offset : source_offset + self._batch_size
                    ]
                    source_offset += len(page)
                if not page:
                    return
                enforce_deadline(deadline)
                transformed = apply_pipeline(
                    page,
                    streamable_pipeline,
                    collection_resolver=referenced_collections.get,
                    variables=self._execution_variables(),
                    dialect=dialect,
                    collation=self._collation,
                    spill_policy=self._spill_policy(),
                )
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
                    yield self._materialize_document(
                        DocumentCodec.to_public(
                            strip_search_result_metadata(document),
                        ),
                    )
        finally:
            close_source = getattr(source_cursor, "close", None)
            if callable(close_source):
                await close_source()
            else:
                close_iterator = getattr(source_iterator, "aclose", None)
                if callable(close_iterator):
                    await close_iterator()

    async def to_list(
        self,
        length: int | None = None,
    ) -> list[Document]:
        if length is not None and length < 0:
            raise ValueError("length must be non-negative or None")
        if length == 0 or self._closed:
            return []
        started_at = time.perf_counter_ns()
        try:
            documents: list[Document] = []
            while length is None or len(documents) < length:
                try:
                    documents.append(await self.__anext__())
                except StopAsyncIteration:
                    break
        except Exception as exc:
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="command",
                    command={
                        "aggregate": self._collection._collection_name,
                        "pipeline": list(self._pipeline),
                    },
                    duration_ns=time.perf_counter_ns() - started_at,
                    errmsg=str(exc),
                )
            raise
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="command",
                command={
                    "aggregate": self._collection._collection_name,
                    "pipeline": list(self._pipeline),
                },
                duration_ns=time.perf_counter_ns() - started_at,
            )
        return documents

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        active = self._active_async_iterator
        self._active_async_iterator = None
        if active is not None:
            close = getattr(active, "aclose", None)
            if callable(close):
                await close()

    async def first(self) -> Document | None:
        started_at = time.perf_counter_ns()
        async for document in self:
            profiler = getattr(self._collection, "_profile_operation", None)
            if callable(profiler):
                await profiler(
                    op="command",
                    command={
                        "aggregate": self._collection._collection_name,
                        "pipeline": list(self._pipeline),
                    },
                    duration_ns=time.perf_counter_ns() - started_at,
                )
            return document
        profiler = getattr(self._collection, "_profile_operation", None)
        if callable(profiler):
            await profiler(
                op="command",
                command={
                    "aggregate": self._collection._collection_name,
                    "pipeline": list(self._pipeline),
                },
                duration_ns=time.perf_counter_ns() - started_at,
            )
        return None

    async def explain(
        self,
        verbosity: str = SearchExplainVerbosity.EXECUTION_STATS.value,
    ) -> dict[str, object]:
        self._ensure_session_can_use_engine()
        try:
            search_verbosity = SearchExplainVerbosity(verbosity)
        except (TypeError, ValueError) as error:
            message = "verbosity must be queryPlanner or executionStats"
            raise ValueError(message) from error
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
                    details={
                        "reason": _operation_issue_message(self._operation),
                    },
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
            explanation["cxp"] = self._cxp_explain_projection()
            return explanation
        dialect = getattr(
            self._collection,
            "mongodb_dialect",
            MONGODB_DIALECT_70,
        )
        streamable_pipeline: list[object] = []
        pushdown_summary: dict[str, object]
        if self._leading_search_stage() is not None:
            operator, spec = self._leading_search_stage()
            remaining_pipeline = self._effective_pipeline()
            planning_pipeline, writeback_stage = self._split_terminal_writeback_stage(
                remaining_pipeline,
            )
            optimization = self._search_optimization_plan(
                operator,
                spec,
                planning_pipeline,
                writeback=writeback_stage is not None,
            )
            streamable_pipeline = planning_pipeline
            pushdown_summary = {
                "mode": "search",
                "totalStages": len(self._pipeline),
                "pushedDownStages": 1,
                "remainingStages": len(remaining_pipeline),
                "leadingSearchOperator": operator,
                "searchResultLimitHint": optimization.explain_limit_hint,
                "searchTopKStrategy": (
                    None
                    if optimization.strategy is _SearchOptimizationStrategy.FULL
                    else optimization.strategy.value
                ),
                "searchTopKGrowthStrategy": (
                    "adaptive-retention"
                    if optimization.strategy
                    is _SearchOptimizationStrategy.PREFIX_ITERATIVE
                    else None
                ),
                "searchDownstreamFilterPrefilter": (
                    optimization.downstream_filter_spec is not None
                ),
                "searchWriteback": writeback_stage is not None,
                "searchPlan": optimization.to_document(),
            }
            _query, request = self._build_search_request(
                operator,
                spec,
                result_limit_hint=optimization.explain_limit_hint,
                downstream_filter_spec=optimization.downstream_filter_spec,
                pipeline_plan=optimization,
            )
            engine_plan = await adapt_engine(
                self._collection._engine,
            ).explain_search(
                self._collection._db_name,
                self._collection._collection_name,
                request,
                search_verbosity,
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
                "pushedDownStages": len(self._effective_pipeline())
                - len(remaining_pipeline),
                "remainingStages": len(remaining_pipeline),
            }
            operation = self._pushdown_find_operation()
            from mongoeco.engines.semantic_core import (  # noqa: PLC0415
                compile_find_semantics_from_operation,
            )

            semantics = compile_find_semantics_from_operation(
                operation,
                dialect=dialect,
            )
            engine_plan = await self._collection._engine.explain_find_semantics(
                self._collection._db_name,
                self._collection._collection_name,
                semantics,
                context=self._session,
            )
        streaming_split = self._split_streamable_pipeline(
            streamable_pipeline,
            dialect=getattr(
                self._collection,
                "mongodb_dialect",
                MONGODB_DIALECT_70,
            ),
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
            streaming_batch_execution=bool(
                pushdown_summary["streamingEligible"],
            ),
        ).to_document()
        explanation["cxp"] = self._cxp_explain_projection()
        return explanation

    def __aiter__(self) -> AsyncIterator[Document]:
        return self

    async def __anext__(self) -> Document:
        if self._closed:
            raise StopAsyncIteration
        if self._active_async_iterator is None:
            self._active_async_iterator = self._stream_batches()
        try:
            return await self._active_async_iterator.__anext__()
        except StopAsyncIteration:
            await self.close()
            raise

    def _spill_policy(self) -> AggregationSpillPolicy | None:
        if self._allow_disk_use is False:
            return None
        policy = getattr(
            self._collection._engine,
            "aggregation_spill_policy",
            None,
        )
        if isinstance(policy, AggregationSpillPolicy):
            return policy
        if callable(policy):
            resolved = policy()
            if isinstance(resolved, AggregationSpillPolicy):
                return resolved
        return None

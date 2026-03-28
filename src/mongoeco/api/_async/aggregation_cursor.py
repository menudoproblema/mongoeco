from collections.abc import AsyncIterator
import time

from mongoeco.api._async.cursor import (
    HintSpec,
    _ensure_operation_executable,
    _resolve_planning_mode,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
)
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
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
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import AggregateExplanation, Document, QueryPlanExplanation


class AsyncAggregationCursor:
    """Cursor async mínimo para resultados de aggregate()."""

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
        if operator not in {"$search", "$vectorSearch"}:
            return None
        return operator, spec

    def _effective_pipeline(self) -> Pipeline:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            return self._pipeline
        return self._pipeline[1:]

    async def _search_documents(self) -> list[Document]:
        leading_search = self._leading_search_stage()
        if leading_search is None:
            raise OperationFailure("search stage was not present")
        operator, spec = leading_search
        search_documents = getattr(self._collection._engine, "search_documents", None)
        if not callable(search_documents):
            raise OperationFailure(f"{operator} is not supported by this engine")
        return await search_documents(
            self._collection._db_name,
            self._collection._collection_name,
            operator,
            spec,
            max_time_ms=self._max_time_ms,
            context=self._session,
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
        enforce_deadline(deadline)
        referenced_collections = await self._load_referenced_collections()
        enforce_deadline(deadline)
        if self._leading_search_stage() is not None:
            documents = await self._search_documents()
            remaining_pipeline = pipeline
        else:
            pushdown = split_pushdown_pipeline(
                pipeline,
                dialect=dialect,
            )
            documents = await self._build_pushdown_cursor(
                self._pushdown_find_operation()
            ).to_list()
            remaining_pipeline = pushdown.remaining_pipeline
        enforce_deadline(deadline)
        self._enforce_materialization_budget(
            len(documents),
            remaining_pipeline,
            dialect=dialect,
        )
        result = apply_pipeline(
            documents,
            remaining_pipeline,
            collection_resolver=referenced_collections.get,
            variables=self._let,
            dialect=dialect,
            collation=self._collation,
            spill_policy=self._spill_policy(),
        )
        enforce_deadline(deadline)
        return [DocumentCodec.to_public(document) for document in result]

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
        if self._batch_size in (None, 0):
            for document in await self._materialize():
                yield document
            return

        deadline = operation_deadline(self._max_time_ms)
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        pushdown = split_pushdown_pipeline(
            self._effective_pipeline(),
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
                yield DocumentCodec.to_public(document)

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
            return AggregateExplanation(
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
                    details={"reason": "execution blocked by deferred planning issues"},
                    planning_mode=self._operation.planning_mode,
                    planning_issues=self._operation.planning_issues,
                ),
                remaining_pipeline=list(self._pipeline),
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
        dialect = getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70)
        if self._leading_search_stage() is not None:
            operator, spec = self._leading_search_stage()
            remaining_pipeline = self._effective_pipeline()
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
            operation = self._pushdown_find_operation()
            from mongoeco.engines.semantic_core import compile_find_semantics_from_operation

            semantics = compile_find_semantics_from_operation(operation, dialect=dialect)
            engine_plan = await self._collection._engine.explain_find_semantics(
                self._collection._db_name,
                self._collection._collection_name,
                semantics,
                context=self._session,
            )
        return AggregateExplanation(
            engine_plan=engine_plan,
            remaining_pipeline=remaining_pipeline,
            hint=self._hint,
            comment=self._comment,
            max_time_ms=self._max_time_ms,
            batch_size=self._batch_size,
            allow_disk_use=self._allow_disk_use,
            let=self._let,
            streaming_batch_execution=self._batch_size not in (None, 0)
            and self._split_streamable_pipeline(
                pushdown.remaining_pipeline,
                dialect=getattr(self._collection, "mongodb_dialect", MONGODB_DIALECT_70),
            )
            is not None,
        ).to_document()

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

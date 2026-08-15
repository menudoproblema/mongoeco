from typing import Protocol, runtime_checkable

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MongoDialect
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_models import (
    SearchExecutionOutcome,
    SearchExplainVerbosity,
)
from mongoeco.engines.capabilities import EngineCapabilities
from mongoeco.engines.results import (
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import (
    EngineFindSemantics,
    EngineReadExecutionPlan,
)
from mongoeco.engines.snapshots import ReadSnapshot
from mongoeco.session import ClientSession
from mongoeco.types import (
    Document,
    DocumentId,
    Filter,
    IndexDocument,
    IndexInformation,
    IndexKeySpec,
    ProfilingCommandResult,
    Projection,
    QueryPlanExplanation,
    SearchIndexDefinition,
    SearchIndexDocument,
)


@runtime_checkable
class AsyncSessionEngine(Protocol):
    capabilities: EngineCapabilities

    def create_session_state(self, session: ClientSession) -> None: ...


@runtime_checkable
class AsyncLifecycleEngine(Protocol):
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...


@runtime_checkable
class AsyncReadSemanticsEngine(Protocol):
    def open_read_snapshot(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        operation_context: OperationContext,
    ) -> ReadSnapshot: ...
    async def count_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        operation_context: OperationContext,
    ) -> int: ...


@runtime_checkable
class AsyncCrudEngine(AsyncReadSemanticsEngine, Protocol):
    async def insert_document(  # noqa: PLR0913
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool = True,  # noqa: FBT001, FBT002
        *,
        operation_context: OperationContext,
        bypass_document_validation: bool = False,
    ) -> InsertOutcome: ...

    async def get_document(
        self,
        db_name: str,
        coll_name: str,
        doc_id: DocumentId,
        *,
        projection: Projection | None = None,
        operation_context: OperationContext,
    ) -> Document | None: ...

    async def update_with_operation(  # noqa: PLR0913
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        upsert: bool = False,  # noqa: FBT001, FBT002
        upsert_seed: Document | None = None,
        *,
        operation_context: OperationContext,
        selector_filter: Filter | None = None,
        bypass_document_validation: bool = False,
        replacement_document: Document | None = None,
    ) -> MutationOutcome: ...

    async def delete_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        *,
        operation_context: OperationContext,
        selector_filter: Filter | None = None,
    ) -> DeleteOutcome: ...

    async def merge_document(  # noqa: PLR0913
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        *,
        when_matched: str,
        when_not_matched: str,
        operation_context: OperationContext,
    ) -> MergeOutcome: ...


@runtime_checkable
class AsyncBatchInsertEngine(Protocol):
    async def insert_documents(
        self,
        db_name: str,
        coll_name: str,
        documents: list[Document],
        *,
        operation_context: OperationContext,
        bypass_document_validation: bool = False,
    ) -> tuple[InsertOutcome, ...]: ...


@runtime_checkable
class AsyncIndexAdminEngine(Protocol):
    async def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: IndexKeySpec,
        *,
        unique: bool = False,
        name: str | None = None,
        sparse: bool = False,
        hidden: bool = False,
        collation: Filter | None = None,
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
        weights: dict[str, int] | None = None,
        default_language: str | None = None,
        language_override: str | None = None,
        min_value: float | None = None,
        max_value: float | None = None,
        bucket_size: float | None = None,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str: ...
    async def list_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> list[IndexDocument]: ...
    async def index_information(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> IndexInformation: ...
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None: ...
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None: ...


@runtime_checkable
class AsyncSearchIndexAdminEngine(Protocol):
    async def create_search_index(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str: ...
    async def list_search_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        name: str | None = None,
        context: ClientSession | None = None,
    ) -> list[SearchIndexDocument]: ...
    async def update_search_index(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        definition: Document,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> None: ...
    async def drop_search_index(
        self,
        db_name: str,
        coll_name: str,
        name: str,
        *,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> None: ...


@runtime_checkable
class AsyncSearchEngine(Protocol):
    async def execute_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
    ) -> SearchExecutionOutcome: ...
    async def explain_search(
        self,
        db_name: str,
        coll_name: str,
        request: SearchRequest,
        verbosity: SearchExplainVerbosity,
    ) -> QueryPlanExplanation: ...


@runtime_checkable
class AsyncExplainSemanticsEngine(Protocol):
    async def explain_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation: ...


@runtime_checkable
class AsyncExplainEngine(AsyncExplainSemanticsEngine, Protocol):
    pass


@runtime_checkable
class AsyncReadPlanningEngine(Protocol):
    async def plan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan: ...
    async def plan_find_execution(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> EngineReadExecutionPlan: ...


@runtime_checkable
class AsyncDatabaseAdminEngine(Protocol):
    async def list_databases(
        self,
        *,
        context: ClientSession | None = None,
    ) -> list[str]: ...


@runtime_checkable
class AsyncProfilingEngine(Protocol):
    async def set_profiling_level(
        self,
        db_name: str,
        level: int,
        *,
        slow_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> ProfilingCommandResult: ...


@runtime_checkable
class AsyncNamespaceAdminEngine(Protocol):
    async def list_collections(
        self,
        db_name: str,
        *,
        context: ClientSession | None = None,
    ) -> list[str]: ...
    async def collection_options(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> dict[str, object]: ...
    async def create_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None: ...
    async def rename_collection(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None: ...
    async def drop_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None: ...


@runtime_checkable
class AsyncAdminEngine(
    AsyncDatabaseAdminEngine,
    AsyncNamespaceAdminEngine,
    Protocol,
):
    pass


@runtime_checkable
class AsyncStorageEngine(
    AsyncSessionEngine,
    AsyncLifecycleEngine,
    AsyncCrudEngine,
    AsyncIndexAdminEngine,
    AsyncSearchIndexAdminEngine,
    AsyncReadPlanningEngine,
    AsyncExplainEngine,
    AsyncAdminEngine,
    AsyncProfilingEngine,
    Protocol,
):
    """Versioned storage engine protocol."""

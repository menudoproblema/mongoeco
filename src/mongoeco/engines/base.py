from typing import AsyncIterable, Protocol, runtime_checkable

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MongoDialect
from mongoeco.engines.semantic_core import EngineFindSemantics, EngineReadExecutionPlan
from mongoeco.session import ClientSession
from mongoeco.types import (
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    IndexInformation,
    IndexDocument,
    IndexKeySpec,
    ProfilingCommandResult,
    Projection,
    QueryPlanExplanation,
    SearchIndexDefinition,
    SearchIndexDocument,
    UpdateResult,
)


@runtime_checkable
class AsyncSessionEngine(Protocol):
    def create_session_state(self, session: ClientSession) -> None: ...


@runtime_checkable
class AsyncLifecycleEngine(Protocol):
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...


@runtime_checkable
class AsyncReadSemanticsEngine(Protocol):
    def scan_find_semantics(self, db_name: str, coll_name: str, semantics: EngineFindSemantics, *, context: ClientSession | None = None) -> AsyncIterable[Document]: ...
    async def count_find_semantics(self, db_name: str, coll_name: str, semantics: EngineFindSemantics, *, context: ClientSession | None = None) -> int: ...


@runtime_checkable
class AsyncCrudEngine(AsyncReadSemanticsEngine, Protocol):
    async def put_document(self, db_name: str, coll_name: str, document: Document, overwrite: bool = True, *, context: ClientSession | None = None, bypass_document_validation: bool = False) -> bool: ...
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None: ...
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool: ...
    async def update_with_operation(self, db_name: str, coll_name: str, operation: UpdateOperation, upsert: bool = False, upsert_seed: Document | None = None, *, selector_filter: Filter | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None, bypass_document_validation: bool = False) -> UpdateResult[DocumentId]: ...
    async def delete_with_operation(self, db_name: str, coll_name: str, operation: UpdateOperation, *, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult: ...


@runtime_checkable
class AsyncIndexAdminEngine(Protocol):
    async def create_index(self, db_name: str, coll_name: str, keys: IndexKeySpec, *, unique: bool = False, name: str | None = None, sparse: bool = False, hidden: bool = False, collation: Filter | None = None, partial_filter_expression: Filter | None = None, expire_after_seconds: int | None = None, weights: dict[str, int] | None = None, default_language: str | None = None, language_override: str | None = None, max_time_ms: int | None = None, context: ClientSession | None = None) -> str: ...
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[IndexDocument]: ...
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> IndexInformation: ...
    async def drop_index(self, db_name: str, coll_name: str, index_or_name: str | IndexKeySpec, *, context: ClientSession | None = None) -> None: ...
    async def drop_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> None: ...


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
class AsyncExplainSemanticsEngine(Protocol):
    async def explain_find_semantics(self, db_name: str, coll_name: str, semantics: EngineFindSemantics, *, context: ClientSession | None = None) -> QueryPlanExplanation: ...


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
    async def list_databases(self, *, context: ClientSession | None = None) -> list[str]: ...


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
    async def list_collections(self, db_name: str, *, context: ClientSession | None = None) -> list[str]: ...
    async def collection_options(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> dict[str, object]: ...
    async def create_collection(self, db_name: str, coll_name: str, *, options: dict[str, object] | None = None, context: ClientSession | None = None) -> None: ...
    async def rename_collection(self, db_name: str, coll_name: str, new_name: str, *, context: ClientSession | None = None) -> None: ...
    async def drop_collection(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> None: ...


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
    """Protocolo Delgado (Thin Protocol) de Almacenamiento."""

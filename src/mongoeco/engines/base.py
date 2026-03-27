from typing import AsyncIterable, Protocol, runtime_checkable

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MongoDialect
from mongoeco.engines.semantic_core import EngineReadExecutionPlan
from mongoeco.core.query_plan import QueryNode
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters,
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    IndexInformation,
    IndexDocument,
    IndexKeySpec,
    Projection,
    QueryPlanExplanation,
    SortSpec,
    Update,
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
class AsyncCrudEngine(Protocol):
    async def put_document(self, db_name: str, coll_name: str, document: Document, overwrite: bool = True, *, context: ClientSession | None = None) -> bool: ...
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None: ...
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool: ...
    def scan_collection(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, plan: QueryNode | None = None, projection: Projection | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, hint: str | IndexKeySpec | None = None, comment: object | None = None, max_time_ms: int | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]: ...
    def scan_find_operation(self, db_name: str, coll_name: str, operation: FindOperation, *, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]: ...
    async def update_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, selector_filter: Filter | None = None, array_filters: ArrayFilters | None = None, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> UpdateResult[DocumentId]: ...
    async def update_with_operation(self, db_name: str, coll_name: str, operation: UpdateOperation, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, selector_filter: Filter | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> UpdateResult[DocumentId]: ...
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult: ...
    async def delete_with_operation(self, db_name: str, coll_name: str, operation: UpdateOperation, *, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult: ...
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> int: ...
    async def count_find_operation(self, db_name: str, coll_name: str, operation: FindOperation, *, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> int: ...


@runtime_checkable
class AsyncIndexAdminEngine(Protocol):
    async def create_index(self, db_name: str, coll_name: str, keys: IndexKeySpec, *, unique: bool = False, name: str | None = None, max_time_ms: int | None = None, context: ClientSession | None = None) -> str: ...
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[IndexDocument]: ...
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> IndexInformation: ...
    async def drop_index(self, db_name: str, coll_name: str, index_or_name: str | IndexKeySpec, *, context: ClientSession | None = None) -> None: ...
    async def drop_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> None: ...


@runtime_checkable
class AsyncExplainEngine(Protocol):
    async def explain_query_plan(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, plan: QueryNode | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, hint: str | IndexKeySpec | None = None, comment: object | None = None, max_time_ms: int | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> QueryPlanExplanation: ...
    async def explain_find_operation(self, db_name: str, coll_name: str, operation: FindOperation, *, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> QueryPlanExplanation: ...


@runtime_checkable
class AsyncReadPlanningEngine(Protocol):
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
    AsyncReadPlanningEngine,
    AsyncExplainEngine,
    AsyncAdminEngine,
    Protocol,
):
    """Protocolo Delgado (Thin Protocol) de Almacenamiento."""

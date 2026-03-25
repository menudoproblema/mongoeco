from mongoeco.core.query_plan import QueryNode
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import Document, Filter, Projection, SortSpec


def _validate_sort_spec(sort: SortSpec) -> None:
    if not isinstance(sort, list):
        raise TypeError("sort must be a list of (field, direction) tuples")
    for item in sort:
        if not isinstance(item, tuple) or len(item) != 2:
            raise TypeError("sort must be a list of (field, direction) tuples")
        field, direction = item
        if not isinstance(field, str):
            raise TypeError("sort fields must be strings")
        if direction not in (1, -1) or isinstance(direction, bool):
            raise ValueError("sort directions must be 1 or -1")


class AsyncCursor:
    """Cursor async mínimo y explícito sobre una colección."""

    def __init__(
        self,
        collection,
        filter_spec: Filter,
        plan: QueryNode,
        projection: Projection | None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
    ):
        self._collection = collection
        self._filter_spec = filter_spec
        self._plan = plan
        self._projection = projection
        self._sort = sort
        self._skip = skip
        self._limit = limit
        self._session = session
        self._started = False

    def _ensure_mutable(self) -> None:
        if self._started:
            raise InvalidOperation("cannot modify cursor after iteration has started")

    def _scan(self, *, limit: int | None = None):
        self._started = True
        return self._collection._engine.scan_collection(
            self._collection._db_name,
            self._collection._collection_name,
            self._filter_spec,
            plan=self._plan,
            projection=self._projection,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit if limit is None else limit,
            context=self._session,
        )

    def __aiter__(self):
        return self._scan()

    def sort(self, sort: SortSpec) -> "AsyncCursor":
        self._ensure_mutable()
        _validate_sort_spec(sort)
        self._sort = sort
        return self

    def skip(self, skip: int) -> "AsyncCursor":
        self._ensure_mutable()
        if skip < 0:
            raise ValueError("skip must be >= 0")
        self._skip = skip
        return self

    def limit(self, limit: int | None) -> "AsyncCursor":
        self._ensure_mutable()
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        self._limit = limit
        return self

    async def to_list(self) -> list[Document]:
        return [document async for document in self]

    async def first(self) -> Document | None:
        if self._limit == 0:
            return None
        async for document in self._scan(limit=1):
            return document
        return None

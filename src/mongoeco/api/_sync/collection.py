from mongoeco.session import ClientSession
from mongoeco.types import DeleteResult, Document, DocumentId, Filter, InsertOneResult, Projection, Update, UpdateResult


class Collection:
    """Adaptador sincronico sobre AsyncCollection."""

    def __init__(self, client, db_name: str, collection_name: str):
        self._client = client
        self._db_name = db_name
        self._collection_name = collection_name

    def _async_collection(self):
        self._client._ensure_connected()
        return self._client._async_client.get_database(self._db_name).get_collection(self._collection_name)

    def insert_one(self, document: Document, *, session: ClientSession | None = None) -> InsertOneResult[DocumentId]:
        return self._client._run(self._async_collection().insert_one(document, session=session))

    def find_one(self, filter_spec: Filter | None = None, projection: Projection | None = None, *, session: ClientSession | None = None) -> Document | None:
        return self._client._run(self._async_collection().find_one(filter_spec, projection, session=session))

    def find(
        self,
        filter_spec: Filter | None = None,
        projection: Projection | None = None,
        *,
        sort: list[tuple[str, int]] | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
    ) -> list[Document]:
        async_collection = self._async_collection()

        async def _collect():
            return [
                doc
                async for doc in async_collection.find(
                    filter_spec,
                    projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    session=session,
                )
            ]

        return self._client._run(_collect())

    def update_one(self, filter_spec: Filter, update_spec: Update, upsert: bool = False, *, session: ClientSession | None = None) -> UpdateResult[DocumentId]:
        return self._client._run(self._async_collection().update_one(filter_spec, update_spec, upsert, session=session))

    def delete_one(self, filter_spec: Filter, *, session: ClientSession | None = None) -> DeleteResult:
        return self._client._run(self._async_collection().delete_one(filter_spec, session=session))

    def count_documents(self, filter_spec: Filter, *, session: ClientSession | None = None) -> int:
        return self._client._run(self._async_collection().count_documents(filter_spec, session=session))

    def create_index(self, fields: list[str], *, unique: bool = False, name: str | None = None, session: ClientSession | None = None) -> str:
        return self._client._run(self._async_collection().create_index(fields, unique=unique, name=name, session=session))

    def list_indexes(self, *, session: ClientSession | None = None) -> list[dict[str, object]]:
        return self._client._run(self._async_collection().list_indexes(session=session))

from typing import TYPE_CHECKING

from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.session import ClientSession
from mongoeco.types import CollectionValidationDocument, Filter

if TYPE_CHECKING:
    from mongoeco.api._sync.client import Database


class DatabaseAdminService:
    def __init__(self, database: "Database"):
        self._database = database

    @property
    def _client(self):
        return self._database._client

    def _async_database(self):
        return self._database._async_database()

    def list_collection_names(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        async_database = self._async_database()
        return self._client._run(
            async_database.list_collection_names(filter_spec, session=session)
        )

    def list_collections(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> ListingCursor:
        async_database = self._async_database()
        return ListingCursor(
            self._client,
            async_database.list_collections(filter_spec, session=session),
        )

    def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ):
        async_database = self._async_database()
        return self._client._run_resource(
            async_database.create_collection(name, session=session, **options),
            lambda: self._database.get_collection(name),
        )

    def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        async_database = self._async_database()
        self._client._run(async_database.drop_collection(name, session=session))

    def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationDocument:
        async_database = self._async_database()
        return self._client._run(
            async_database.validate_collection(
                name_or_collection,
                scandata=scandata,
                full=full,
                background=background,
                session=session,
                comment=comment,
            )
        )

    def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        async_database = self._async_database()
        return self._client._run(
            async_database.command(command, session=session, **kwargs)
        )

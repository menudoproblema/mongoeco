from typing import TYPE_CHECKING

from mongoeco.api.public_api import (
    ARG_UNSET,
    DATABASE_LIST_COLLECTION_NAMES_SPEC,
    DATABASE_LIST_COLLECTIONS_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.api._sync.database_commands import DatabaseCommandService
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.session import ClientSession
from mongoeco.types import CollectionValidationDocument, Filter

if TYPE_CHECKING:
    from mongoeco.api._sync.client import Database

_FILTER_UNSET = ARG_UNSET


class DatabaseAdminService:
    def __init__(self, database: "Database"):
        self._database = database
        self._commands = DatabaseCommandService(self)

    @property
    def _client(self):
        return self._database._client

    def _async_database(self):
        return self._database._async_database()

    def _run_database_method(self, method_name: str, /, *args, **kwargs):
        async_database = self._async_database()
        method = getattr(async_database, method_name)
        return self._client._run(method(*args, **kwargs))

    def list_collection_names(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[str]:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTION_NAMES_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        return self._run_database_method(
            "list_collection_names",
            options.get("filter_spec", _FILTER_UNSET),
            session=options.get("session"),
        )

    def list_collections(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> ListingCursor:
        options = normalize_public_operation_arguments(
            DATABASE_LIST_COLLECTIONS_SPEC,
            explicit={"filter_spec": filter_spec, "session": session},
            extra_kwargs={"filter": filter, **kwargs},
        )
        async_database = self._async_database()
        return ListingCursor(
            self._client,
            async_database.list_collections(
                options.get("filter_spec", _FILTER_UNSET),
                session=options.get("session"),
            ),
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
        return self._run_database_method(
            "validate_collection",
            name_or_collection,
            scandata=scandata,
            full=full,
            background=background,
            session=session,
            comment=comment,
        )

    def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        return self._commands.command(command, session=session, **kwargs)

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService
    from mongoeco.api.operations import AggregateOperation, FindOperation
    from mongoeco.session import ClientSession


class DatabaseAdminRoutingService:
    def __init__(self, admin: "AsyncDatabaseAdminService") -> None:
        self._admin = admin

    async def command_list_collections(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_list_collections(spec, session=session)

    async def command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_list_databases(spec, session=session)

    async def command_create(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_create(spec, session=session)

    async def command_drop(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_drop(spec, session=session)

    async def command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_rename_collection(spec, session=session)

    async def command_count(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_count(spec, session=session)

    async def command_distinct(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_distinct(spec, session=session)

    async def command_db_hash(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_db_hash(spec, session=session)

    async def execute_distinct_command(
        self,
        collection_name: str,
        key: str,
        operation: "FindOperation",
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_distinct_command(
            collection_name,
            key,
            operation,
            session=session,
        )

    async def execute_count_command(
        self,
        collection_name: str,
        operation: "FindOperation",
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_count_command(
            collection_name,
            operation,
            session=session,
        )

    async def execute_db_hash_command(
        self,
        collections: tuple[str, ...],
        *,
        comment: object | None = None,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_db_hash_command(
            collections,
            comment=comment,
            session=session,
        )

    async def command_insert(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_insert(spec, session=session)

    async def command_update(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_update(spec, session=session)

    async def command_delete(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_delete(spec, session=session)

    async def command_find(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_find(spec, session=session)

    async def execute_find_command(
        self,
        collection_name: str,
        operation: "FindOperation",
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_find_command(
            collection_name,
            operation,
            session=session,
        )

    async def command_aggregate(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_aggregate(spec, session=session)

    async def execute_aggregate_command(
        self,
        collection_name: str,
        operation: "AggregateOperation",
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_aggregate_command(
            collection_name,
            operation,
            session=session,
        )

    async def command_explain(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.command_explain(spec, session=session)

    async def execute_find_and_modify(
        self,
        options,
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.execute_find_and_modify(options, session=session)

    async def execute_list_indexes_command(
        self,
        collection_name: str,
        *,
        comment: object | None = None,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._read_commands.execute_list_indexes_command(
            collection_name,
            comment=comment,
            session=session,
        )

    async def command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_create_indexes(spec, session=session)

    async def command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_drop_indexes(spec, session=session)

    async def command_drop_database(
        self,
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._write_commands.command_drop_database(session=session)

    async def command_current_op(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._command_service.execute_document(spec, session=session)

    async def command_kill_op(
        self,
        spec: dict[str, object],
        *,
        session: "ClientSession | None" = None,
    ) -> object:
        return await self._admin._command_service.execute_document(spec, session=session)

    async def _command_list_collections(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_list_collections(spec, session=session)

    async def _command_list_databases(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_list_databases(spec, session=session)

    async def _command_create(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_create(spec, session=session)

    async def _command_drop(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_drop(spec, session=session)

    async def _command_rename_collection(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_rename_collection(spec, session=session)

    async def _command_count(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_count(spec, session=session)

    async def _command_db_hash(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_db_hash(spec, session=session)

    async def _command_distinct(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_distinct(spec, session=session)

    async def _command_insert(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_insert(spec, session=session)

    async def _command_update(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_update(spec, session=session)

    async def _command_delete(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_delete(spec, session=session)

    async def _command_find(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_find(spec, session=session)

    async def _command_aggregate(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_aggregate(spec, session=session)

    async def _command_explain(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_explain(spec, session=session)

    async def _command_create_indexes(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_create_indexes(spec, session=session)

    async def _command_drop_indexes(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_drop_indexes(spec, session=session)

    async def _command_drop_database(self, *, session: "ClientSession | None" = None) -> object:
        return await self.command_drop_database(session=session)

    async def _command_current_op(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_current_op(spec, session=session)

    async def _command_kill_op(self, spec: dict[str, object], *, session: "ClientSession | None" = None) -> object:
        return await self.command_kill_op(spec, session=session)

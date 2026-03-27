from typing import TYPE_CHECKING

from mongoeco.api._async.database_commands import AsyncDatabaseCommandService
from mongoeco.session import ClientSession

if TYPE_CHECKING:
    from mongoeco.api._sync.database_admin import DatabaseAdminService


class DatabaseCommandService:
    def __init__(self, admin: "DatabaseAdminService"):
        self._admin = admin

    @property
    def _client(self):
        return self._admin._client

    def _async_command_service(self) -> AsyncDatabaseCommandService:
        return self._admin._async_database()._admin._commands

    def parse_raw_command(
        self,
        command: object,
        **kwargs: object,
    ) -> AsyncDatabaseCommandService.AdminCommand[dict[str, object]]:
        return self._async_command_service().parse_raw_command(command, **kwargs)

    def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        async_commands = self._async_command_service()
        parsed = async_commands.parse_raw_command(command, **kwargs)
        result = self._client._run(async_commands.execute(parsed, session=session))
        return async_commands._serialize_result(result)

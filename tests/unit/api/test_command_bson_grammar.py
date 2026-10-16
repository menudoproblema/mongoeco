"""Unknown command fields must never become aliases for valid bindings."""

import pytest

from mongoeco.api._command_bson import validate_write_fields
from mongoeco.core.server_errors import ErrorScope, error_scope
from mongoeco.errors import OperationFailure


UNKNOWN_FIELD = 40415


@pytest.mark.parametrize("command", ["update", "delete", "findAndModify"])
@pytest.mark.parametrize("value", [None, 0, [], {}])
def test_constants_is_not_a_command_level_let_alias(command, value):
    with pytest.raises(OperationFailure) as caught:
        validate_write_fields({command: "items", "constants": value}, command)
    assert caught.value.code == UNKNOWN_FIELD
    assert error_scope(caught.value) is ErrorScope.COMMAND

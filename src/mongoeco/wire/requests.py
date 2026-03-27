from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mongoeco.session import ClientSession
from mongoeco.wire.capabilities import WireCommandCapability
from mongoeco.wire.connections import WireConnectionContext


@dataclass(frozen=True, slots=True)
class WireRequestContext:
    db_name: str
    command_name: str
    command_document: dict[str, Any]
    raw_body: dict[str, Any]
    capability: WireCommandCapability
    connection: WireConnectionContext
    session: ClientSession | None = None

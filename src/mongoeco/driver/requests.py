from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mongoeco.driver.policies import ConcernPolicy, RetryPolicy, SelectionPolicy, TimeoutPolicy
from mongoeco.driver.topology import ServerDescription, TopologyDescription


@dataclass(frozen=True, slots=True)
class CommandRequest:
    database: str
    command_name: str
    payload: dict[str, Any]
    session_id: str | None = None
    read_only: bool = False


@dataclass(frozen=True, slots=True)
class RequestExecutionPlan:
    request: CommandRequest
    topology: TopologyDescription
    timeout_policy: TimeoutPolicy
    retry_policy: RetryPolicy
    selection_policy: SelectionPolicy
    concern_policy: ConcernPolicy
    candidate_servers: tuple[ServerDescription, ...] = field(default_factory=tuple)

    @property
    def primary_server(self) -> ServerDescription | None:
        return self.candidate_servers[0] if self.candidate_servers else None


@dataclass(frozen=True, slots=True)
class RequestOutcome:
    server_address: str | None
    ok: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool = False

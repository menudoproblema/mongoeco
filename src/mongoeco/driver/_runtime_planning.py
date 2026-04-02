from __future__ import annotations

from typing import Any

from mongoeco.driver.requests import CommandRequest, RequestExecutionPlan


def build_runtime_command_plan(
    *,
    database: str,
    command_name: str,
    payload: dict[str, Any],
    session,
    read_only: bool,
    topology,
    timeout_policy,
    retry_policy,
    selection_policy,
    concern_policy,
    auth_policy,
    tls_policy,
) -> RequestExecutionPlan:
    return RequestExecutionPlan(
        request=CommandRequest(
            database=database,
            command_name=command_name,
            payload=payload,
            session=session,
            read_only=read_only,
        ),
        topology=topology,
        timeout_policy=timeout_policy,
        retry_policy=retry_policy,
        selection_policy=selection_policy,
        concern_policy=concern_policy,
        auth_policy=auth_policy,
        tls_policy=tls_policy,
        dynamic_candidates=True,
        candidate_servers=selection_policy.select_servers(
            topology,
            for_writes=not read_only,
        ),
    )

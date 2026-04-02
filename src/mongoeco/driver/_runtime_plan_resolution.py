from __future__ import annotations

from dataclasses import replace

from mongoeco.driver.requests import RequestExecutionPlan
from mongoeco.driver.topology import TopologyDescription


def resolve_runtime_execution_plan(
    *,
    current_topology: TopologyDescription,
    plan: RequestExecutionPlan,
) -> RequestExecutionPlan:
    if not plan.dynamic_candidates:
        return plan
    candidate_servers = plan.selection_policy.select_servers(
        current_topology,
        for_writes=not plan.request.read_only,
    )
    if current_topology is plan.topology and candidate_servers == plan.candidate_servers:
        return plan
    return replace(
        plan,
        topology=current_topology,
        candidate_servers=candidate_servers,
    )

"""Mongoeco-owned validation of its operational driver telemetry shape."""

from __future__ import annotations

import json

from functools import lru_cache
from importlib.resources import files
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterable

    from mongoeco.telemetry_contract import TelemetrySnapshot


@lru_cache(maxsize=1)
def _specification() -> dict[str, Any]:
    resource = files('mongoeco.driver').joinpath(
        'resources/telemetry-spec-v1.json'
    )
    value = json.loads(resource.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        message = 'Invalid Mongoeco driver telemetry specification'
        raise ValueError(message)
    return value


def _selected_contracts(
    capability_names: Iterable[str],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    specs = _specification()
    selected: dict[str, dict[str, Any]] = {
        'spans': {},
        'metrics': {},
        'events': {},
    }
    issues: list[str] = []
    for capability in capability_names:
        contract = specs.get(capability)
        if contract is None:
            issues.append(f'Unknown telemetry capability: {capability}')
            continue
        for kind, names in selected.items():
            for name, signal_spec in contract[kind].items():
                previous = names.setdefault(name, signal_spec)
                if previous != signal_spec:
                    message = f'Conflicting telemetry specification: {kind}/{name}'
                    raise ValueError(message)
    return selected, issues


def _span_issues(
    snapshot: TelemetrySnapshot,
    contracts: dict[str, Any],
    *,
    reject_unknown_signals: bool,
) -> list[str]:
    issues = []
    for span in snapshot.spans:
        contract = contracts.get(span.name)
        if contract is None:
            if reject_unknown_signals:
                issues.append(f'Unknown span: {span.name}')
            continue
        issues.extend(
            f'Missing span attribute: {span.name}.{name}'
            for name in contract['required_attributes']
            if name not in span.attributes
        )
    return issues


def _metric_issues(
    snapshot: TelemetrySnapshot,
    contracts: dict[str, Any],
    *,
    reject_unknown_signals: bool,
) -> list[str]:
    issues = []
    for metric in snapshot.metrics:
        contract = contracts.get(metric.name)
        if contract is None:
            if reject_unknown_signals:
                issues.append(f'Unknown metric: {metric.name}')
            continue
        issues.extend(
            f'Missing metric label: {metric.name}.{name}'
            for name in contract['required_labels']
            if name not in metric.labels
        )
        if contract['unit'] is not None and metric.unit != contract['unit']:
            issues.append(
                f"Invalid metric unit: {metric.name}, expected {contract['unit']}"
            )
    return issues


def _event_issues(
    snapshot: TelemetrySnapshot,
    contracts: dict[str, Any],
    *,
    reject_unknown_signals: bool,
) -> list[str]:
    issues = []
    for event in snapshot.events:
        contract = contracts.get(event.event_type)
        if contract is None:
            if reject_unknown_signals:
                issues.append(f'Unknown event: {event.event_type}')
            continue
        issues.extend(
            f'Missing event payload: {event.event_type}.{name}'
            for name in contract['required_payload_keys']
            if name not in event.payload
        )
        if (
            contract['severity'] is not None
            and event.severity != contract['severity']
        ):
            issues.append(
                f"Invalid event severity: {event.event_type}, "
                f"expected {contract['severity']}"
            )
    return issues


def driver_telemetry_issues(
    snapshot: TelemetrySnapshot,
    capability_names: Iterable[str],
    *,
    reject_unknown_signals: bool = False,
) -> tuple[str, ...]:
    """Report missing signal fields and invalid units for named capabilities."""
    selected, issues = _selected_contracts(capability_names)
    issues.extend(
        _span_issues(
            snapshot,
            selected['spans'],
            reject_unknown_signals=reject_unknown_signals,
        )
    )
    issues.extend(
        _metric_issues(
            snapshot,
            selected['metrics'],
            reject_unknown_signals=reject_unknown_signals,
        )
    )
    issues.extend(
        _event_issues(
            snapshot,
            selected['events'],
            reject_unknown_signals=reject_unknown_signals,
        )
    )
    return tuple(issues)

"""Project validated Mongoeco claims into one homogeneous CXP snapshot."""

from __future__ import annotations

from dataclasses import dataclass

from cxp.exchange import CatalogStore, Document, catalog_reference

from mongoeco.cxp.exchange.documents import load_mongodb_catalog
from mongoeco.cxp.exchange.metadata import (
    AGGREGATION_OPERATION_FIELDS,
    READ_OPERATION_FIELDS,
    VECTOR_SEARCH_OPERATION_FIELDS,
    WRITE_OPERATION_FIELDS,
    validate_mongodb_metadata,
)


_VALUE_FIELDS: dict[str, frozenset[str]] = {
    "read": frozenset(
        {"async", "embedded", "queryFieldOperators", "queryTopLevelOperators", "sync"}
    ),
    "write": frozenset(
        {"async", "embedded", "supportsPipelineUpdate", "sync", "updateOperators"}
    ),
    "aggregation": frozenset(
        {
            "async",
            "embedded",
            "explainable",
            "supportedExpressionOperators",
            "supportedGroupAccumulators",
            "supportedStages",
            "supportedWindowAccumulators",
            "sync",
        }
    ),
    "search": frozenset(
        {
            "advancedAtlasLikeGaps",
            "aggregateStage",
            "exactFilterFieldMappings",
            "explainFeatures",
            "fieldMappings",
            "operators",
            "sqliteBackends",
            "structuredFieldMappings",
            "structuredParentPathOperators",
            "textSearchTier",
            "textualFieldMappings",
        }
    ),
    "vector_search": frozenset(
        {
            "aggregateStage",
            "backend",
            "explainFeatures",
            "fallback",
            "filterMode",
            "hybridFilterModes",
            "mode",
            "similarities",
        }
    ),
    "transactions": frozenset({"async", "distributed", "embedded", "mode", "sync"}),
    "change_streams": frozenset(
        {
            "boundedHistory",
            "distributed",
            "implementation",
            "persistent",
            "resumable",
            "resumableAcrossClientRestarts",
            "resumableAcrossNodes",
            "resumableAcrossProcesses",
        }
    ),
}
_OPERATION_VALUE_FIELDS = {
    "read": READ_OPERATION_FIELDS,
    "write": WRITE_OPERATION_FIELDS,
    "aggregation": AGGREGATION_OPERATION_FIELDS,
    "vector_search": VECTOR_SEARCH_OPERATION_FIELDS,
}


@dataclass(frozen=True, slots=True)
class MongoOperationClaim:
    name: str
    result_type: str


@dataclass(frozen=True, slots=True)
class MongoCapabilityClaim:
    name: str
    support: str
    metadata: dict[str, object] | None
    operations: tuple[MongoOperationClaim, ...]


@dataclass(frozen=True, slots=True)
class MongoSnapshotIdentity:
    provider_id: str
    subject_id: str
    configuration_revision: str
    observed_at: str
    source_kind: str
    source_reference: str


def _project_properties(claim: MongoCapabilityClaim) -> dict[str, object]:
    metadata = claim.metadata
    if metadata is None:
        return {}
    validate_mongodb_metadata(claim.name, metadata)
    properties: dict[str, object] = {}
    if all(value is not None for value in metadata.values()):
        properties["metadata_keys"] = sorted(metadata)
    for field in _VALUE_FIELDS.get(claim.name, frozenset()):
        if field in metadata and metadata[field] is not None:
            properties[field] = metadata[field]
    operation_fields = _OPERATION_VALUE_FIELDS.get(claim.name)
    if operation_fields is not None:
        operation_metadata = metadata.get("operationMetadata")
        if isinstance(operation_metadata, dict):
            bindings = {
                operation.name: operation.result_type for operation in claim.operations
            }
            for operation_name, operation_values in operation_metadata.items():
                if isinstance(operation_values, dict):
                    if operation_name not in bindings:
                        message = "Operation metadata lacks its exact binding"
                        raise ValueError(message)
                    result_type = operation_values.get("resultType")
                    if result_type is not None and bindings[operation_name] != (
                        f"org.mongoeco:result.{result_type}:1"
                    ):
                        message = "Operation result differs from its binding"
                        raise ValueError(message)
                    for field in operation_fields:
                        if (
                            field in operation_values
                            and operation_values[field] is not None
                        ):
                            properties[f"{operation_name}.{field}"] = operation_values[
                                field
                            ]
    return properties


def build_mongodb_snapshot(
    *,
    catalog: Document,
    identity: MongoSnapshotIdentity,
    capabilities: tuple[MongoCapabilityClaim, ...],
) -> Document:
    """Require explicit provenance, metadata coverage and operation bindings."""
    catalog.require_type("cxp.catalog")
    if catalog.payload["identity"] != {
        "namespace": "org.mongoeco",
        "name": "mongodb",
        "version": "1.1.0",
    }:
        message = "Expected the Mongoeco-owned MongoDB catalog"
        raise ValueError(message)
    if catalog.sha256 != load_mongodb_catalog().sha256:
        message = "Expected the exact Mongoeco-owned MongoDB catalog"
        raise ValueError(message)
    claims = [
        {
            "name": claim.name,
            "support": claim.support,
            "properties": _project_properties(claim),
            "operations": [
                {"name": operation.name, "result_type": operation.result_type}
                for operation in claim.operations
            ],
        }
        for claim in capabilities
    ]
    document = Document(
        {
            "document_type": "cxp.snapshot",
            "spec_version": 1,
            "payload": {
                "provider_id": identity.provider_id,
                "subject_id": identity.subject_id,
                "catalog": catalog_reference(catalog),
                "configuration_revision": identity.configuration_revision,
                "observed_at": identity.observed_at,
                "source": {
                    "kind": identity.source_kind,
                    "reference": identity.source_reference,
                },
                "capabilities": claims,
            },
        },
        expected_type="cxp.snapshot",
    )
    CatalogStore((catalog,)).validate_snapshot(document)
    return document

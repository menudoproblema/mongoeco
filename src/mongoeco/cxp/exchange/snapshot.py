"""Project validated Mongoeco claims into one homogeneous CXP snapshot."""

from __future__ import annotations

from dataclasses import dataclass

from cxp.exchange import CatalogStore, Document, catalog_reference

from mongoeco.cxp.exchange.metadata import validate_mongodb_metadata


_VALUE_FIELDS: dict[str, frozenset[str]] = {
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
    claims = []
    for claim in capabilities:
        properties: dict[str, object] = {}
        if claim.metadata is not None:
            validate_mongodb_metadata(claim.name, claim.metadata)
            if all(value is not None for value in claim.metadata.values()):
                properties["metadata_keys"] = sorted(claim.metadata)
            for field in _VALUE_FIELDS.get(claim.name, frozenset()):
                if field in claim.metadata and claim.metadata[field] is not None:
                    properties[field] = claim.metadata[field]
        claims.append(
            {
                "name": claim.name,
                "support": claim.support,
                "properties": properties,
                "operations": [
                    {"name": operation.name, "result_type": operation.result_type}
                    for operation in claim.operations
                ],
            }
        )
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

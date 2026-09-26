"""Mongoeco-owned shape checks for metadata before exchange projection."""

from __future__ import annotations

from typing import Literal

import msgspec


class MongoAggregationMetadata(msgspec.Struct, frozen=True):
    supported_stages: tuple[str, ...] = msgspec.field(name="supportedStages")
    supported_expression_operators: tuple[str, ...] = msgspec.field(
        name="supportedExpressionOperators", default=()
    )
    supported_group_accumulators: tuple[str, ...] = msgspec.field(
        name="supportedGroupAccumulators", default=()
    )
    supported_window_accumulators: tuple[str, ...] = msgspec.field(
        name="supportedWindowAccumulators", default=()
    )


class MongoSearchMetadata(msgspec.Struct, frozen=True):
    operators: tuple[str, ...]
    aggregate_stage: Literal["$search"] = msgspec.field(
        name="aggregateStage", default="$search"
    )


class MongoVectorSearchMetadata(msgspec.Struct, frozen=True):
    similarities: tuple[str, ...]
    aggregate_stage: Literal["$vectorSearch"] = msgspec.field(
        name="aggregateStage", default="$vectorSearch"
    )


class MongoCollationMetadata(msgspec.Struct, frozen=True):
    backend: dict[str, object] = msgspec.field(default_factory=dict)
    capabilities: dict[str, object] = msgspec.field(default_factory=dict)


class MongoPersistenceMetadata(msgspec.Struct, frozen=True):
    persistent: bool
    storage_engine: str = msgspec.field(name="storageEngine")


class MongoTopologyDiscoveryMetadata(msgspec.Struct, frozen=True):
    topology_type: str = msgspec.field(name="topologyType")
    server_count: int = msgspec.field(name="serverCount")
    sdam: dict[str, object] = msgspec.field(default_factory=dict)


METADATA_SCHEMAS: dict[str, type[msgspec.Struct]] = {
    "aggregation": MongoAggregationMetadata,
    "search": MongoSearchMetadata,
    "vector_search": MongoVectorSearchMetadata,
    "collation": MongoCollationMetadata,
    "persistence": MongoPersistenceMetadata,
    "topology_discovery": MongoTopologyDiscoveryMetadata,
}


def validate_mongodb_metadata(capability: str, metadata: dict[str, object]) -> None:
    """Check authored values, retaining only authored keys for compatibility."""
    if not isinstance(metadata, dict) or not all(
        isinstance(key, str) for key in metadata
    ):
        message = "MongoDB capability metadata must be a string-keyed object"
        raise ValueError(message)
    schema = METADATA_SCHEMAS.get(capability)
    if schema is not None:
        try:
            msgspec.convert(msgspec.to_builtins(metadata), type=schema, strict=True)
        except (
            TypeError,
            ValueError,
            msgspec.ValidationError,
            RecursionError,
        ) as error:
            message = f"Invalid MongoDB {capability} metadata"
            raise ValueError(message) from error

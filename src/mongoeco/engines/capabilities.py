from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


ChangeDeliveryMode = Literal[
    "none",
    "legacy-callback",
    "commit-sequence",
    "transactional-outbox",
]

_SPI_V1 = 1
_SPI_V2 = 2

_SPI_V2_REQUIRED_METHODS = frozenset(
    {
        "delete_with_operation",
        "count_find_semantics",
        "get_document",
        "insert_document",
        "merge_document",
        "update_with_operation",
    },
)
_SEQUENCED_DELIVERY_METHODS = frozenset(
    {
        "dispatch_committed_changes",
        "register_change_consumer",
        "unregister_change_consumer",
    },
)


@dataclass(frozen=True, slots=True)
class SearchEngineCapabilities:
    """Optional local Search contract implemented by an engine."""

    contract_version: str = "search-v1"
    metadata_collectors: bool = False
    highlight: bool = False
    explain_verbosity: bool = False
    operators: frozenset[str] = frozenset({"$search"})
    vector_similarities: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.contract_version != "search-v1":
            message = "unsupported Search contract version"
            raise ValueError(message)
        for field_name in (
            "metadata_collectors",
            "highlight",
            "explain_verbosity",
        ):
            if not isinstance(getattr(self, field_name), bool):
                message = f"{field_name} must be a bool"
                raise TypeError(message)
        if (
            not isinstance(self.operators, frozenset)
            or not self.operators
            or not all(
                operator in {"$search", "$vectorSearch"} for operator in self.operators
            )
        ):
            message = "Search operators must be a non-empty supported frozenset"
            raise ValueError(message)
        supported_similarities = {"cosine", "dotProduct", "euclidean"}
        if (
            not isinstance(self.vector_similarities, frozenset)
            or not self.vector_similarities <= supported_similarities
        ):
            message = "vector similarities must be a supported frozenset"
            raise ValueError(message)
        if "$vectorSearch" in self.operators and not self.vector_similarities:
            message = "vector Search capability requires declared similarities"
            raise ValueError(message)
        if "$vectorSearch" not in self.operators and self.vector_similarities:
            message = "vector similarities require the $vectorSearch capability"
            raise ValueError(message)


@dataclass(frozen=True, slots=True)
class EngineCapabilities:
    """Versioned feature contract exposed by a storage engine."""

    spi_version: int = 2
    injected_clock: bool = False
    mutation_outcomes: bool = True
    batch_inserts: bool = True
    explicit_read_snapshots: bool = False
    change_delivery: ChangeDeliveryMode = "none"
    search: SearchEngineCapabilities | None = None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.spi_version, int)
            or isinstance(self.spi_version, bool)
            or self.spi_version not in {_SPI_V1, _SPI_V2}
        ):
            message = "spi_version must be one of the supported versions: 1, 2"
            raise ValueError(message)
        if self.change_delivery not in {
            "none",
            "legacy-callback",
            "commit-sequence",
            "transactional-outbox",
        }:
            message = "change_delivery is not a supported delivery mode"
            raise ValueError(message)
        if self.search is not None and not isinstance(
            self.search,
            SearchEngineCapabilities,
        ):
            message = "search must be SearchEngineCapabilities or None"
            raise TypeError(message)
        for field_name in (
            "injected_clock",
            "mutation_outcomes",
            "batch_inserts",
            "explicit_read_snapshots",
        ):
            if not isinstance(getattr(self, field_name), bool):
                message = f"{field_name} must be a bool"
                raise TypeError(message)
        if self.spi_version == _SPI_V1 and self.mutation_outcomes:
            message = "SPI v1 cannot declare native mutation outcomes"
            raise ValueError(message)
        if self.spi_version == _SPI_V1 and self.explicit_read_snapshots:
            message = "SPI v1 cannot declare explicit read snapshots"
            raise ValueError(message)
        if self.spi_version == _SPI_V1 and self.change_delivery not in {
            "none",
            "legacy-callback",
        }:
            message = "SPI v1 cannot declare sequenced change delivery"
            raise ValueError(message)
        if self.spi_version >= _SPI_V2 and not self.mutation_outcomes:
            message = "SPI v2 requires native mutation outcomes"
            raise ValueError(message)
        if self.spi_version >= _SPI_V2 and self.change_delivery == "legacy-callback":
            message = "SPI v2 cannot declare legacy callback delivery"
            raise ValueError(message)

    @property
    def transactional_outbox(self) -> bool:
        return self.change_delivery == "transactional-outbox"

    @property
    def monotonic_commit_sequence(self) -> bool:
        return self.change_delivery in {
            "commit-sequence",
            "transactional-outbox",
        }


def resolve_engine_capabilities(engine: object) -> EngineCapabilities:
    """Resolve native capabilities or describe a legacy engine centrally."""
    declared = getattr(engine, "capabilities", None)
    if callable(declared):
        declared = declared()
    if isinstance(declared, EngineCapabilities):
        return declared
    return EngineCapabilities(
        spi_version=_SPI_V1,
        injected_clock=bool(getattr(engine, "supports_injected_clock", False)),
        mutation_outcomes=False,
        batch_inserts=callable(getattr(engine, "put_documents_bulk", None)),
        explicit_read_snapshots=False,
        change_delivery=(
            "legacy-callback"
            if bool(getattr(engine, "supports_commit_callbacks", False))
            else "none"
        ),
    )


def validate_engine_contract(
    engine: object,
    capabilities: EngineCapabilities,
) -> None:
    """Reject inconsistent SPI v2 declarations at the client boundary."""
    if capabilities.spi_version < _SPI_V2:
        return
    required = set(_SPI_V2_REQUIRED_METHODS)
    if capabilities.batch_inserts:
        required.add("insert_documents")
    if capabilities.explicit_read_snapshots:
        required.add("open_read_snapshot")
    else:
        required.add("scan_find_semantics")
    if capabilities.monotonic_commit_sequence:
        required.update(_SEQUENCED_DELIVERY_METHODS)
    if capabilities.search is not None:
        required.add("execute_search")
        if capabilities.search.explain_verbosity:
            required.add("explain_search")
    missing = sorted(
        name for name in required if not callable(getattr(engine, name, None))
    )
    if not missing:
        return
    message = (
        f"SPI v{capabilities.spi_version} engine "
        f"{type(engine).__name__} is missing required methods: "
        f"{', '.join(missing)}"
    )
    raise TypeError(message)

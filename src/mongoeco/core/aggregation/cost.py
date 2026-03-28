from dataclasses import dataclass

from mongoeco.errors import OperationFailure


@dataclass(frozen=True, slots=True)
class AggregationCostPolicy:
    max_materialized_documents: int
    require_spill_for_blocking_stages: bool = True

    def __post_init__(self) -> None:
        if self.max_materialized_documents <= 0:
            raise ValueError("max_materialized_documents must be > 0")

    def enforce_budget(
        self,
        *,
        document_count: int,
        has_materializing_stage: bool,
        spill_available: bool,
    ) -> None:
        if not has_materializing_stage:
            return
        if document_count <= self.max_materialized_documents:
            return
        if spill_available:
            return
        if self.require_spill_for_blocking_stages:
            raise OperationFailure(
                "Aggregation pipeline exceeds the configured materialization budget; allowDiskUse or spill-to-disk is required"
            )
        raise OperationFailure(
            "Aggregation pipeline exceeds the configured materialization budget"
        )

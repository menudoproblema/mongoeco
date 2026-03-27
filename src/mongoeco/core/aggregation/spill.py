import json
import os
import tempfile
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.core.codec import DocumentCodec
from mongoeco.types import Document


if TYPE_CHECKING:
    from mongoeco.core.aggregation.planning import Pipeline


BLOCKING_AGGREGATION_STAGES = frozenset(
    {
        "$sort",
        "$group",
        "$bucket",
        "$bucketAuto",
        "$facet",
        "$sortByCount",
        "$setWindowFields",
    }
)


@dataclass(frozen=True, slots=True)
class AggregationSpillPolicy:
    threshold: int
    codec: type[DocumentCodec] = DocumentCodec

    def __post_init__(self) -> None:
        if self.threshold <= 0:
            raise ValueError("aggregation spill threshold must be > 0")

    def should_spill(self, stage_operator: str, documents: list[Document]) -> bool:
        return stage_operator in BLOCKING_AGGREGATION_STAGES and len(documents) > self.threshold

    def maybe_spill(self, stage_operator: str, documents: list[Document]) -> list[Document]:
        if not self.should_spill(stage_operator, documents):
            return documents
        return self._round_trip_via_disk(documents)

    def _round_trip_via_disk(self, documents: list[Document]) -> list[Document]:
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".mongoeco-aggspill",
            delete=False,
        )
        path = handle.name
        try:
            with handle:
                for document in documents:
                    payload = json.dumps(
                        self.codec.encode(document),
                        separators=(",", ":"),
                        sort_keys=False,
                    )
                    handle.write(payload)
                    handle.write("\n")
            reloaded: list[Document] = []
            with open(path, encoding="utf-8") as spilled:
                for line in spilled:
                    reloaded.append(
                        self.codec.decode(
                            json.loads(line),
                            preserve_bson_wrappers=True,
                        )
                    )
            return reloaded
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

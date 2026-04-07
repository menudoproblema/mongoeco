from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
import heapq
from typing import TYPE_CHECKING

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.json_compat import json_dumps_compact, json_loads
from mongoeco.core.sorting import compare_documents, sort_documents
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
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

    def sort_with_spill(
        self,
        documents: list[Document],
        sort: object,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> list[Document]:
        if not self.should_spill("$sort", documents):
            return sort_documents(documents, sort, dialect=dialect, collation=collation)
        return self._external_sort(documents, sort, dialect=dialect, collation=collation)

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
                    payload = json_dumps_compact(
                        self.codec.encode(document),
                        sort_keys=False,
                    )
                    handle.write(payload)
                    handle.write("\n")
            reloaded: list[Document] = []
            with open(path, encoding="utf-8") as spilled:
                for line in spilled:
                    reloaded.append(
                        self.codec.decode(
                            json_loads(line),
                            preserve_bson_wrappers=True,
                        )
                    )
            return reloaded
        finally:
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass

    def _external_sort(
        self,
        documents: list[Document],
        sort: object,
        *,
        dialect: MongoDialect,
        collation: CollationSpec | None,
    ) -> list[Document]:
        chunk_paths: list[str] = []
        try:
            for start in range(0, len(documents), self.threshold):
                chunk = sort_documents(
                    documents[start:start + self.threshold],
                    sort,
                    dialect=dialect,
                    collation=collation,
                )
                handle = tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=".mongoeco-aggsort",
                    delete=False,
                )
                chunk_paths.append(handle.name)
                with handle:
                    for document in chunk:
                        payload = json_dumps_compact(
                            self.codec.encode(document),
                            sort_keys=False,
                        )
                        handle.write(payload)
                        handle.write("\n")

            class _HeapItem:
                __slots__ = ("document", "index")

                def __init__(self, document: Document, index: int):
                    self.document = document
                    self.index = index

                def __lt__(self, other: "_HeapItem") -> bool:
                    return compare_documents(
                        self.document,
                        other.document,
                        sort,
                        dialect=dialect,
                        collation=collation,
                    ) < 0

            streams = [open(path, encoding="utf-8") for path in chunk_paths]
            try:
                heap: list[_HeapItem] = []
                for index, stream in enumerate(streams):
                    line = stream.readline()
                    if not line:
                        continue
                    document = self.codec.decode(json_loads(line), preserve_bson_wrappers=True)
                    heapq.heappush(heap, _HeapItem(document, index))
                merged: list[Document] = []
                while heap:
                    item = heapq.heappop(heap)
                    merged.append(item.document)
                    line = streams[item.index].readline()
                    if line:
                        document = self.codec.decode(json_loads(line), preserve_bson_wrappers=True)
                        heapq.heappush(heap, _HeapItem(document, item.index))
                return merged
            finally:
                for stream in streams:
                    stream.close()
        finally:
            for path in chunk_paths:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass

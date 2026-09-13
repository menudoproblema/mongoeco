"""Exact bounded selection with the same stable tie order as full ranking."""

from __future__ import annotations

import heapq
import math

from typing import TYPE_CHECKING

import numpy as np


if TYPE_CHECKING:
    from collections.abc import Callable


def top_score_indexes(
    scores: np.ndarray,
    limit: int | None,
    *,
    tie_key: Callable[[int], str] | None = None,
) -> list[int]:
    """Partition scores, then order only winners; never cut a tie arbitrarily.

    NaN has no total order. Preserve the former stable NumPy-then-public-sort
    behavior for that exceptional case instead of selecting an arbitrary top-k.
    Equal public keys retain the original row order, including at the boundary.
    """
    size = len(scores)
    count = size if limit is None or limit <= 0 else min(limit, size)
    if count == 0:
        return []
    if count == size or np.isnan(scores).any():
        indexes = [int(index) for index in np.argsort(-scores, kind="stable")]
        if tie_key is not None:
            indexes.sort(key=lambda index: (-float(scores[index]), tie_key(index)))
        return indexes[:count]
    cutoff = np.partition(scores, size - count)[size - count]
    above = [int(index) for index in np.flatnonzero(scores > cutoff)]
    equal = np.flatnonzero(scores == cutoff)
    remaining = count - len(above)
    boundary = (
        [int(index) for index in equal[:remaining]]
        if tie_key is None
        else heapq.nsmallest(remaining, map(int, equal), key=tie_key)
    )
    indexes = sorted(above + boundary)
    indexes.sort(
        key=(
            (lambda index: -float(scores[index]))
            if tie_key is None
            else (lambda index: (-float(scores[index]), tie_key(index)))
        )
    )
    return indexes


def top_scored_rows(
    rows: list[tuple[float, int]],
    limit: int,
    tie_keys: tuple[str, ...],
) -> list[tuple[float, int]]:
    """Select after residual evaluation, without copying every matching payload.

    The caller supplies the former stable score order. Keep the full-sort
    fallback for NaN because heap ordering would not preserve that behavior.
    """

    def key(item: tuple[float, int]) -> tuple[float, str]:
        return -item[0], tie_keys[item[1]]

    if any(math.isnan(score) for score, _row in rows):
        return sorted(rows, key=key)[:limit]
    return heapq.nsmallest(limit, rows, key=key)

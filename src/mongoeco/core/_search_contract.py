from __future__ import annotations

TEXT_SEARCH_OPERATOR_NAMES: tuple[str, ...] = (
    "text",
    "phrase",
    "autocomplete",
    "wildcard",
    "exists",
    "near",
    "compound",
)

TEXT_SEARCH_INDEX_CAPABILITIES: tuple[str, ...] = TEXT_SEARCH_OPERATOR_NAMES

SEARCH_STAGE_OPERATORS: tuple[str, ...] = ("$search", "$vectorSearch")

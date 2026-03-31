from functools import lru_cache
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

try:
    import icu as _icu
except Exception:  # pragma: no cover - optional dependency
    _icu = None

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.types import CollationDocument


@dataclass(frozen=True, slots=True)
class CollationSpec:
    locale: str = "simple"
    strength: int = 3
    case_level: bool = False
    numeric_ordering: bool = False

    def to_document(self) -> CollationDocument:
        return {
            "locale": self.locale,
            "strength": self.strength,
            "caseLevel": self.case_level,
            "numericOrdering": self.numeric_ordering,
        }


def normalize_collation(collation: object | None) -> CollationSpec | None:
    if collation is None:
        return None
    if not isinstance(collation, dict):
        raise TypeError("collation must be a document")
    locale = collation.get("locale", "simple")
    if not isinstance(locale, str) or not locale:
        raise TypeError("collation locale must be a non-empty string")
    if locale not in {"simple", "en"}:
        raise ValueError("only 'simple' and 'en' collations are supported")
    strength = collation.get("strength", 3)
    if not isinstance(strength, int) or isinstance(strength, bool) or strength not in (1, 2, 3):
        raise ValueError("collation strength must be 1, 2 or 3")
    case_level = collation.get("caseLevel", False)
    if not isinstance(case_level, bool):
        raise TypeError("collation caseLevel must be a boolean")
    numeric_ordering = collation.get("numericOrdering", False)
    if not isinstance(numeric_ordering, bool):
        raise TypeError("collation numericOrdering must be a boolean")
    return CollationSpec(
        locale=locale,
        strength=strength,
        case_level=case_level,
        numeric_ordering=numeric_ordering,
    )


def compare_with_collation(
    left: Any,
    right: Any,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> int:
    if collation is None or not isinstance(left, str) or not isinstance(right, str):
        return dialect.policy.compare_values(left, right)
    if _can_use_icu_collation(collation):
        return _compare_with_icu(left, right, collation)

    primary_left = _collation_primary_key(left, collation)
    primary_right = _collation_primary_key(right, collation)
    if primary_left < primary_right:
        return -1
    if primary_left > primary_right:
        return 1

    if collation.case_level or collation.strength >= 3:
        if left < right:
            return -1
        if left > right:
            return 1
    return 0


def values_equal_with_collation(
    left: Any,
    right: Any,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if collation is None:
        return dialect.policy.values_equal(left, right)
    return compare_with_collation(left, right, dialect=dialect, collation=collation) == 0


def icu_collation_available() -> bool:
    return _icu is not None


_NUMERIC_SEGMENT_RE = re.compile(r"\d+|\D+")


def _collation_primary_key(value: str, collation: CollationSpec) -> tuple[object, ...]:
    normalized = value
    if collation.locale != "simple":
        if collation.strength == 1:
            normalized = _strip_accents(normalized)
            normalized = normalized.casefold()
        elif collation.strength == 2:
            normalized = normalized.casefold()
    if not collation.numeric_ordering:
        return (normalized,)
    parts: list[object] = []
    for segment in _NUMERIC_SEGMENT_RE.findall(normalized):
        if segment.isdigit():
            parts.append((0, int(segment)))
        else:
            parts.append((1, segment))
    return tuple(parts)


def _can_use_icu_collation(collation: CollationSpec) -> bool:
    return _icu is not None and collation.locale != "simple"


@lru_cache(maxsize=32)
def _get_icu_collator(
    locale: str,
    strength: int,
    case_level: bool,
    numeric_ordering: bool,
):
    if _icu is None:  # pragma: no cover - guarded by caller
        raise RuntimeError("ICU collation backend is unavailable")
    collator = _icu.Collator.createInstance(_icu.Locale(locale))
    strength_map = {
        1: _icu.Collator.PRIMARY,
        2: _icu.Collator.SECONDARY,
        3: _icu.Collator.TERTIARY,
    }
    collator.setStrength(strength_map[strength])
    if hasattr(_icu, "UCollAttribute") and hasattr(_icu, "UCollAttributeValue"):
        collator.setAttribute(
            _icu.UCollAttribute.NORMALIZATION_MODE,
            _icu.UCollAttributeValue.ON,
        )
        collator.setAttribute(
            _icu.UCollAttribute.CASE_LEVEL,
            _icu.UCollAttributeValue.ON if case_level else _icu.UCollAttributeValue.OFF,
        )
        collator.setAttribute(
            _icu.UCollAttribute.NUMERIC_COLLATION,
            _icu.UCollAttributeValue.ON if numeric_ordering else _icu.UCollAttributeValue.OFF,
        )
        if strength == 1:
            collator.setAttribute(
                _icu.UCollAttribute.ALTERNATE_HANDLING,
                _icu.UCollAttributeValue.SHIFTED,
            )
    return collator


def _compare_with_icu(left: str, right: str, collation: CollationSpec) -> int:
    collator = _get_icu_collator(
        collation.locale,
        collation.strength,
        collation.case_level,
        collation.numeric_ordering,
    )
    comparison = collator.compare(left, right)
    if comparison < 0:
        return -1
    if comparison > 0:
        return 1
    return 0


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")

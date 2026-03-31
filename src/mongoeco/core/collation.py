from functools import lru_cache
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

try:
    import icu as _icu
except Exception:  # pragma: no cover - optional dependency
    _icu = None
try:
    import pyuca as _pyuca
except Exception:  # pragma: no cover - optional dependency
    _pyuca = None

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.types import CollationDocument


@dataclass(frozen=True, slots=True)
class CollationSpec:
    locale: str = "simple"
    strength: int = 3
    case_level: bool = False
    numeric_ordering: bool = False
    backwards: bool = False
    alternate: str = "non-ignorable"
    max_variable: str = "punct"
    normalization: bool = False

    def to_document(self) -> CollationDocument:
        document: CollationDocument = {
            "locale": self.locale,
            "strength": self.strength,
            "caseLevel": self.case_level,
            "numericOrdering": self.numeric_ordering,
        }
        if self.backwards:
            document["backwards"] = True
        if self.alternate != "non-ignorable":
            document["alternate"] = self.alternate
        if self.max_variable != "punct":
            document["maxVariable"] = self.max_variable
        if self.normalization:
            document["normalization"] = True
        return document


@dataclass(frozen=True, slots=True)
class CollationBackendInfo:
    selected_backend: str
    available_backends: tuple[str, ...]
    unicode_available: bool
    advanced_options_available: bool

    def to_document(self) -> dict[str, object]:
        return {
            "selectedBackend": self.selected_backend,
            "availableBackends": list(self.available_backends),
            "unicodeAvailable": self.unicode_available,
            "advancedOptionsAvailable": self.advanced_options_available,
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
    backwards = collation.get("backwards", False)
    if not isinstance(backwards, bool):
        raise TypeError("collation backwards must be a boolean")
    alternate = collation.get("alternate", "non-ignorable")
    if alternate not in {"non-ignorable", "shifted"}:
        raise ValueError("collation alternate must be 'non-ignorable' or 'shifted'")
    max_variable = collation.get("maxVariable", "punct")
    if max_variable not in {"punct", "space"}:
        raise ValueError("collation maxVariable must be 'punct' or 'space'")
    normalization = collation.get("normalization", False)
    if not isinstance(normalization, bool):
        raise TypeError("collation normalization must be a boolean")
    return CollationSpec(
        locale=locale,
        strength=strength,
        case_level=case_level,
        numeric_ordering=numeric_ordering,
        backwards=backwards,
        alternate=alternate,
        max_variable=max_variable,
        normalization=normalization,
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
    if _requires_icu_backend(collation) and not _can_use_icu_collation(collation):
        raise ValueError("requested collation options require an ICU backend")
    if _can_use_icu_collation(collation):
        return _compare_with_icu(left, right, collation)
    if _can_use_pyuca_collation(collation):
        return _compare_with_pyuca(left, right, collation)

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


def unicode_collation_available() -> bool:
    return _icu is not None or _pyuca is not None


def collation_backend_info() -> CollationBackendInfo:
    available: list[str] = []
    if _icu is not None:
        available.append("icu")
    if _pyuca is not None:
        available.append("pyuca")
    if _icu is not None:
        selected = "icu"
    elif _pyuca is not None:
        selected = "pyuca"
    else:
        selected = "none"
    return CollationBackendInfo(
        selected_backend=selected,
        available_backends=tuple(available),
        unicode_available=bool(available),
        advanced_options_available=_icu is not None,
    )


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


def _can_use_pyuca_collation(collation: CollationSpec) -> bool:
    return _pyuca is not None and collation.locale != "simple"


def _requires_icu_backend(collation: CollationSpec) -> bool:
    return (
        collation.backwards
        or collation.alternate != "non-ignorable"
        or collation.max_variable != "punct"
        or collation.normalization
    )


@lru_cache(maxsize=32)
def _get_icu_collator(
    locale: str,
    strength: int,
    case_level: bool,
    numeric_ordering: bool,
    backwards: bool,
    alternate: str,
    max_variable: str,
    normalization: bool,
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
            _icu.UCollAttributeValue.ON if normalization else _icu.UCollAttributeValue.OFF,
        )
        collator.setAttribute(
            _icu.UCollAttribute.CASE_LEVEL,
            _icu.UCollAttributeValue.ON if case_level else _icu.UCollAttributeValue.OFF,
        )
        collator.setAttribute(
            _icu.UCollAttribute.NUMERIC_COLLATION,
            _icu.UCollAttributeValue.ON if numeric_ordering else _icu.UCollAttributeValue.OFF,
        )
        if hasattr(_icu.UCollAttribute, "FRENCH_COLLATION"):
            collator.setAttribute(
                _icu.UCollAttribute.FRENCH_COLLATION,
                _icu.UCollAttributeValue.ON if backwards else _icu.UCollAttributeValue.OFF,
            )
        if hasattr(_icu.UCollAttribute, "ALTERNATE_HANDLING"):
            collator.setAttribute(
                _icu.UCollAttribute.ALTERNATE_HANDLING,
                (
                    _icu.UCollAttributeValue.SHIFTED
                    if alternate == "shifted"
                    else _icu.UCollAttributeValue.NON_IGNORABLE
                ),
            )
        if alternate == "shifted" and hasattr(_icu.UCollAttribute, "MAX_VARIABLE"):
            max_variable_value = getattr(
                _icu.UCollAttributeValue,
                "SPACE" if max_variable == "space" else "PUNCT",
                None,
            )
            if max_variable_value is not None:
                collator.setAttribute(_icu.UCollAttribute.MAX_VARIABLE, max_variable_value)
    return collator


def _compare_with_icu(left: str, right: str, collation: CollationSpec) -> int:
    collator = _get_icu_collator(
        collation.locale,
        collation.strength,
        collation.case_level,
        collation.numeric_ordering,
        collation.backwards,
        collation.alternate,
        collation.max_variable,
        collation.normalization,
    )
    comparison = collator.compare(left, right)
    if comparison < 0:
        return -1
    if comparison > 0:
        return 1
    return 0


@lru_cache(maxsize=1)
def _get_pyuca_collator():
    if _pyuca is None:  # pragma: no cover - guarded by caller
        raise RuntimeError('pyuca collation backend is unavailable')
    return _pyuca.Collator()


def _compare_with_pyuca(left: str, right: str, collation: CollationSpec) -> int:
    left_key = _pyuca_collation_key(left, collation)
    right_key = _pyuca_collation_key(right, collation)
    if left_key < right_key:
        return -1
    if left_key > right_key:
        return 1
    return 0


def _pyuca_collation_key(value: str, collation: CollationSpec) -> tuple[object, ...]:
    if not collation.numeric_ordering:
        return _truncate_uca_key(_get_pyuca_collator().sort_key(value), collation)
    parts: list[object] = []
    for segment in _NUMERIC_SEGMENT_RE.findall(value):
        if segment.isdigit():
            parts.append((0, int(segment)))
        else:
            parts.append((1, _truncate_uca_key(_get_pyuca_collator().sort_key(segment), collation)))
    return tuple(parts)


def _truncate_uca_key(key: tuple[int, ...], collation: CollationSpec) -> tuple[object, ...]:
    levels = _split_uca_levels(key)
    truncated: list[object] = []
    if levels:
        truncated.append(levels[0])
    if collation.strength >= 2 and len(levels) >= 2:
        truncated.append(levels[1])
    if (collation.case_level or collation.strength >= 3) and len(levels) >= 3:
        truncated.append(levels[2])
    return tuple(truncated)


def _split_uca_levels(key: tuple[int, ...]) -> tuple[tuple[int, ...], ...]:
    levels: list[tuple[int, ...]] = []
    current: list[int] = []
    for value in key:
        if value == 0:
            levels.append(tuple(current))
            current = []
            continue
        current.append(value)
    if current:
        levels.append(tuple(current))
    return tuple(levels)


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")

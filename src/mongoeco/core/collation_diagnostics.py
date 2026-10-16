"""ICU 57 root sort keys used in MongoDB's duplicate-key diagnostics.

Derived from Unicode ICU 57.1 data and sort-key algorithms; see the bundled
ICU-LICENSE.txt. This codec does not select or change the storage comparator.
"""

import json
import re
import unicodedata

from functools import lru_cache
from importlib.resources import files

from pyuca.collator import Collator_8_0_0
from pyuca.trie import Trie


_COMMON_WEIGHT = 0x0500
_LEVEL_SEPARATOR = 0x0100
_PRIMARY_TERMINATOR = 0x01000000
_MERGE_SEPARATOR = 0x02000000
_MERGE_LEAD = 2
_NUMERIC_SMALL_LIMIT = 74
_NUMERIC_MEDIUM_LIMIT = 10234
_NUMERIC_LARGE_LIMIT = 1042490
_CASE_COMMON_MAX = 7
_SECONDARY_STRENGTH = 2
_TERTIARY_STRENGTH = 3


class _RootCollator(Collator_8_0_0):
    def __init__(self, data):
        self.table = Trie()
        self.implicit_weights = []
        self.han = {ord(char): position for position, char in enumerate(data["han"])}
        self.han_lead = 254 - ((len(self.han) * 2 + 4 + 254 * 254 - 1) // (254 * 254))
        for text, rows in data["mappings"]:
            weights = []
            for row in rows:
                if row[0].startswith("U+"):
                    weights.append(
                        [
                            self.implicit_weight(int(row[0].replace("U+", "0x"), 0))[0][
                                0
                            ],
                            _COMMON_WEIGHT,
                            int(row[1].ljust(4, "0"), 16)
                            if len(row) > 1
                            else _COMMON_WEIGHT,
                        ]
                    )
                else:
                    weights.append(
                        [
                            int(value.ljust(length * 2, "0"), 16) if value else 0
                            for value, length in zip(row, (4, 2, 2), strict=True)
                        ]
                    )
            self.table.add([ord(char) for char in text], weights)
        for prefix, suffix, rows in data["contexts"]:
            weights = self.collation_elements(prefix)
            weights += [
                [
                    int(value.ljust(length * 2, "0"), 16) if value else 0
                    for value, length in zip(row, (4, 2, 2), strict=True)
                ]
                for row in rows
            ]
            self.table.add([ord(char) for char in prefix + suffix], weights)
        for cp, primary, secondary, tertiary in [
            (0xFFFD, 0xFFFD0000, _COMMON_WEIGHT, _COMMON_WEIGHT),
            (0xFFFF, 0xFFFF0000, _COMMON_WEIGHT, _COMMON_WEIGHT),
            (0xFFFE, _MERGE_SEPARATOR, _COMMON_WEIGHT, _COMMON_WEIGHT),
        ]:
            self.table.add([cp], [[primary, secondary, tertiary]])

    def implicit_weight(self, cp):
        if cp in self.han:
            offset = self.han[cp] * 2
            # Han primaries reserve two bytes, each with the range 02..FF.
            primary = (
                ((self.han_lead + offset // (254 * 254)) << 24)
                | ((2 + (offset // 254) % 254) << 16)
                | ((2 + offset % 254) << 8)
            )
        else:
            value = cp + 1
            primary = (
                (254 << 24)
                | ((4 + value // (18 * 254)) << 16)
                | ((2 + (value // 18) % 254) << 8)
                | (2 + value % 18 * 14)
            )
        return [[primary, _COMMON_WEIGHT, _COMMON_WEIGHT]]


@lru_cache(maxsize=1)
def _root():
    data = json.loads(
        files("mongoeco.core")
        .joinpath("resources/icu57_root.json")
        .read_text(encoding="utf-8")
    )
    return _RootCollator(data), frozenset(data["compressible"])


def _numeric_elements(text):
    text = text.lstrip("0") or "0"
    result = []
    for start in range(0, len(text), 254):
        segment = text[start : start + 254]
        value = int(segment)
        if value < _NUMERIC_SMALL_LIMIT:
            primaries = [bytes((15, value + 2))]
        elif value < _NUMERIC_MEDIUM_LIMIT:
            value -= _NUMERIC_SMALL_LIMIT
            primaries = [bytes((15, 76 + value // 254, 2 + value % 254))]
        elif value < _NUMERIC_LARGE_LIMIT:
            value -= _NUMERIC_MEDIUM_LIMIT
            primaries = [
                bytes(
                    (
                        15,
                        116 + value // (254 * 254),
                        2 + (value // 254) % 254,
                        2 + value % 254,
                    )
                )
            ]
        else:
            pairs = (len(segment) + 1) // 2
            segment = (
                segment.rstrip("0")
                if len(segment.rstrip("0")) % 2 == len(segment) % 2
                else segment.rstrip("0") + "0"
            )
            offset = len(segment) % 2 or 2
            numbers = [int(segment[:offset])] + [
                int(segment[i : i + 2]) for i in range(offset, len(segment), 2)
            ]
            encoded = [11 + 2 * number for number in numbers]
            encoded[-1] -= 1
            primaries = []
            first = [15, 128 + pairs]
            while encoded:
                count = 4 - len(first)
                primaries.append(bytes(first + encoded[:count]))
                encoded = encoded[count:]
                first = [15]
        result.extend(
            [
                int.from_bytes(primary.ljust(4, b"\0"), "big"),
                _COMMON_WEIGHT,
                _COMMON_WEIGHT,
            ]
            for primary in primaries
        )
    return result


def _weight_bytes(weight, size):
    return weight.to_bytes(size, "big").rstrip(b"\0")


def _primary_level(elements, compressible):
    result = bytearray()
    previous = 0
    for primary, _, _ in elements:
        if primary <= _PRIMARY_TERMINATOR:
            continue
        lead = primary >> 24
        if lead not in compressible or lead != previous >> 24:
            if previous:
                if primary >= previous:
                    result.append(255)
                elif lead > _MERGE_LEAD:
                    result.append(3)
            result.append(lead)
            previous = primary if lead in compressible else 0
        result.extend(_weight_bytes(primary, 4)[1:])
    return bytes(result)


def _compressed_level(weights, *, tertiary=False):
    result = bytearray()
    common = 0
    maximum, middle, high = (97, 0x65, 0xC5) if tertiary else (33, 0x25, 0x45)
    for raw_weight in [*weights, _LEVEL_SEPARATOR]:
        weight = raw_weight & 0x3F3F if tertiary else raw_weight
        if not weight:
            continue
        if weight == _COMMON_WEIGHT:
            common += 1
            continue
        if common:
            remaining = common - 1
            while remaining >= maximum:
                result.append(middle)
                remaining -= maximum
            result.append(
                5 + remaining if weight < _COMMON_WEIGHT else high - remaining
            )
            common = 0
        if weight > _COMMON_WEIGHT and tertiary:
            weight += 0xC000
        result.extend(_weight_bytes(weight, 2))
    return bytes(result[:-1])


def _case_level(elements, strength):
    nibbles = []
    common = 0
    for primary, secondary, tertiary in [
        *elements,
        [0, _LEVEL_SEPARATOR, _LEVEL_SEPARATOR],
    ]:
        if strength == 1 and primary == 0 and secondary != _LEVEL_SEPARATOR:
            continue
        if strength != 1 and secondary == 0:
            continue
        case = tertiary >> 8
        if not case & 0xC0 and case > 1:
            common += 1
            continue
        if common and (case > 1 or nibbles):
            remaining = common - 1
            while remaining >= _CASE_COMMON_MAX:
                nibbles.append(7)
                remaining -= 7
            nibbles.append(1 + remaining if case <= 1 else 13 - remaining)
        common = 0
        if case > 1:
            nibbles.append(13 + (case >> 6))
    return bytes(
        (nibbles[i] << 4) | (nibbles[i + 1] if i + 1 < len(nibbles) else 0)
        for i in range(0, len(nibbles), 2)
    )


def _secondary_weights(elements, backwards):
    if not backwards:
        return [row[1] for row in elements]
    weights, segment = [], []
    for primary, secondary, _ in elements:
        if primary == _MERGE_SEPARATOR:
            weights.extend(reversed(segment))
            weights.append(0x0200)
            segment = []
        else:
            segment.append(secondary)
    weights.extend(reversed(segment))
    return weights


def comparison_key(value, spec):
    collator, compressible = _root()
    normalized = unicodedata.normalize("NFD", value)
    elements = []
    for part in (
        re.findall(r"\d+|\D+", normalized) if spec.numeric_ordering else [normalized]
    ):
        if (
            spec.numeric_ordering
            and part
            and all(unicodedata.category(char) == "Nd" for char in part)
        ):
            elements.extend(
                _numeric_elements(
                    "".join(str(unicodedata.decimal(char)) for char in part)
                )
            )
        else:
            elements.extend(collator.collation_elements(part))
    if spec.alternate == "shifted":
        # Variable groups end before symbols (punct) or punctuation (space).
        maximum = 0x0C000000 if spec.max_variable == "punct" else 0x05000000
        kept, shifted = [], False
        for row in elements:
            if row[0]:
                shifted = _MERGE_SEPARATOR < row[0] < maximum
            if not shifted:
                kept.append(row)
        elements = kept
    result = _primary_level(elements, compressible)
    if spec.strength >= _SECONDARY_STRENGTH:
        result += b"\x01" + _compressed_level(
            _secondary_weights(elements, spec.backwards)
        )
    if spec.case_level:
        result += b"\x01" + _case_level(elements, spec.strength)
    if spec.strength >= _TERTIARY_STRENGTH:
        result += b"\x01" + _compressed_level(
            [row[2] for row in elements], tertiary=True
        )
    return result

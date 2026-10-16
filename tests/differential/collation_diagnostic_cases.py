"""Native ICU key specimens, independent of Mongoeco and its collation codec."""

COLLATION_KEY_OPTIONS = tuple(
    {"locale": "en", "strength": strength, **extra}
    for strength in (1, 2, 3)
    for extra in (
        {},
        {"caseLevel": True},
        {"numericOrdering": True},
        {"caseLevel": True, "numericOrdering": True},
        {"backwards": True},
        {"alternate": "shifted"},
        {"alternate": "shifted", "maxVariable": "space"},
        {"normalization": True},
    )
    if not (strength == 1 and extra.get("backwards"))
)
COLLATION_KEY_VALUES = (
    "TAKEN",
    "taken",
    "Taken",
    "café",
    "CAFE",
    "æ",
    "ß",
    "abé",
    "éab",
    "a" * 40,
    "a" * 110,
    "a12",
    "a0012",
    "a73",
    "a74",
    "a10233",
    "a10234",
    "a1042489",
    "a1042490",
    "a12345678901234567890",
    "a" + "1234567890" * 30,
    "",
    "\u03b1β",
    "\u0430\u0431",
    "\u03b1a",
    "a\u03b1",
    "\u0430\u0431\u03b1",
    "中",
    "a b",
    "a-b",
    "a ́b",
    "١٢",
    "\u0378",
    "l·l",
    "L·l",
    "\ufffe",
    "\uffff",
)

"""Finite read windows, repeated executions, count and static failures."""

from bson import Regex


DOCUMENTS = [
    {"_id": "a", "group": "A", "name": "Ada", "n": 1},
    {"_id": "b", "group": "A", "name": "ada", "n": 2},
    {"_id": "c", "group": "A", "name": "Ana", "n": 3},
    {"_id": "d", "group": "B", "name": "Bob", "n": 4},
]
READ_CASES = {
    "window": {"filter": {"group": "A"}, "skip": 1, "limit": 2},
    "empty": {"filter": {"group": "missing"}},
    "regex": {"filter": {"name": {"$regex": "^a", "$options": "i"}}},
    "collation": {
        "filter": {"name": "ada"},
        "options": {"collation": {"locale": "en", "strength": 2}},
    },
    "projection_error": {"filter": {}, "projection": {"group": 1, "name": 0}},
}


for name, query in {
    "regex_error": {"name": {"$regex": "["}},
    "regex_literal_error": {"name": Regex("[")},
    "regex_not_error": {"name": {"$not": {"$regex": "["}}},
    "regex_in_error": {"name": {"$in": [Regex("[")]}},
    "regex_nin_error": {"name": {"$nin": [Regex("[")]}},
    "regex_all_error": {"name": {"$all": [Regex("[")]}},
    "regex_no_match_error": {"group": "missing", "name": {"$regex": "["}},
}.items():
    READ_CASES[name] = {"filter": query, "invalid_regex": True}
READ_CASES["regex_empty_error"] = {
    "filter": {"name": {"$regex": "["}},
    "documents": [],
    "invalid_regex": True,
}

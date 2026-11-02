"""Bounded PCRE2 witnesses, independent of local regex helpers."""

from functools import partial

from tests.support_write_parity import SEMANTIC_ERRORS


SEED = [{"_id": 1, "text": "éab"}]
PATTERNS = (
    ("keep_reset", r"a\Kb", "ab"),
    ("branch_reset", r"(?|a(b)|c(d))", "cd"),
    ("property", r"\p{L}+", "áβ"),
    ("named_group", r"(?<word>ab)", "ab"),
    ("inline_options", r"(?i:ab)c", "ABc"),
    ("optional_capture", r"(a)?b", "b"),
    ("word_ascii", r"\w+", "á word"),
    ("word_ucp", r"(*UCP)\w+", "á word"),
    ("digit_ascii", r"\d+", "\u06612"),
    ("digit_ucp", r"(*UCP)\d+", "\u06612"),
    ("offset", "ab", "éab"),
    ("casefold", "(?i)ss", "ß"),
    ("empty", "(?=a)", "aa"),
)


def observe(collection, pattern, text):
    collection.delete_many({})
    collection.insert_one({"_id": 1, "text": text})
    output = {}
    for operator in ("$regexMatch", "$regexFind", "$regexFindAll"):
        output[operator] = list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "value": {operator: {"input": "$text", "regex": pattern}},
                        }
                    }
                ]
            )
        )
    output["query"] = list(collection.find({"text": {"$regex": pattern}}, {"_id": 1}))
    collection.database.command(
        {
            "collMod": collection.name,
            "validator": {
                "$jsonSchema": {"properties": {"text": {"pattern": pattern}}}
            },
        }
    )
    try:
        collection.insert_one({"_id": 2, "text": text})
    except SEMANTIC_ERRORS as error:
        output["schema"] = {"code": error.code}
    else:
        output["schema"] = {"ok": True}
    return output


def resource_limits(collection):
    pattern = r"(*LIMIT_MATCH=1)(a|aa)+$"
    text = "a" * 20 + "b"
    collection.delete_many({})
    collection.insert_one({"_id": 1, "text": text})
    observed = {
        "query": list(collection.find({"text": {"$regex": pattern}}, {"_id": 1}))
    }
    for operator in ("$regexMatch", "$regexFind", "$regexFindAll"):
        phase = "invoke"
        try:
            cursor = collection.aggregate(
                [
                    {
                        "$project": {
                            "value": {operator: {"input": "$text", "regex": pattern}}
                        }
                    }
                ]
            )
            phase = "consume"
            observed[operator] = list(cursor)
        except SEMANTIC_ERRORS as error:
            if getattr(error, "code", None) is None:
                raise
            observed[operator] = {"code": error.code, "phase": phase}
    collection.database.command(
        {
            "collMod": collection.name,
            "validator": {
                "$jsonSchema": {"properties": {"text": {"pattern": pattern}}}
            },
        }
    )
    try:
        collection.insert_one({"_id": 2, "text": text})
    except SEMANTIC_ERRORS as error:
        if getattr(error, "code", None) is None:
            raise
        observed["schema"] = {"code": error.code}
    else:
        observed["schema"] = {"ok": True}
    return observed


SEMANTIC_PCRE2_CASES = tuple(
    ("semantic_pcre2_" + name, partial(observe, pattern=pattern, text=text))
    for name, pattern, text in PATTERNS
)
SEMANTIC_PCRE2_CASES += (("semantic_pcre2_resource_limits", resource_limits),)

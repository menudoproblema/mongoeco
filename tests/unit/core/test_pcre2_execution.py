"""Native adapter behavior, alongside public process-selection tests."""

import re

import pytest

from mongoeco.core import _regex_engine as engine
from mongoeco.errors import OperationFailure


REGEX_EXECUTION_ERROR = 51156


def test_native_execution_matches_retained_oracles(monkeypatch):
    pytest.importorskip("pcre2")
    # Exercise the pure adapter, never the query caches of the running process.
    monkeypatch.setattr(engine, "_BACKEND", "pcre2")
    for pattern, text, flags in (
        ("ab", "AB", re.IGNORECASE),
        ("^b", "a\nb", re.MULTILINE),
        ("a.b", "a\nb", re.DOTALL),
        ("a b", "ab", re.VERBOSE),
    ):
        assert engine.compile_pattern(pattern, flags).search(text) is not None
    assert engine.compile_pattern(r"\w+").search("á word").group() == "word"
    assert engine.compile_pattern(r"(*UCP)\w+").search("á word").group() == "á"
    match = engine.compile_pattern(r"a\Kb").search("éab")
    assert (match.group(), match.start(), match.groups()) == ("b", 2, ())
    assert engine.compile_pattern(r"(a)?b").search("b").groups() == (None,)
    assert [m.start() for m in engine.compile_pattern("(?=a)").finditer("aa")] == [0, 1]
    with pytest.raises(re.error):
        engine.compile_pattern("[")
    pattern = r"(*LIMIT_MATCH=1)(a|aa)+$"
    text = "a" * 20 + "b"
    assert engine.compile_pattern(pattern).search(text) is None
    with pytest.raises(OperationFailure) as failure:
        engine.compile_pattern(pattern, operator="$regexFind").search(text)
    assert failure.value.code == REGEX_EXECUTION_ERROR
    with pytest.raises(OperationFailure) as failure:
        list(engine.compile_pattern(pattern, operator="$regexFindAll").finditer(text))
    assert failure.value.code == REGEX_EXECUTION_ERROR

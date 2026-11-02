"""The optional backend is selected explicitly, before importing the runtime."""

import os
import subprocess
import sys

from pathlib import Path

import pytest


def _run(code, backend, *, extra_path=""):
    env = dict(os.environ, MONGOECO_REGEX_BACKEND=backend)
    env["PYTHONPATH"] = os.pathsep.join(
        item for item in (extra_path, str(Path("src").resolve())) if item
    )
    return subprocess.run(  # noqa: S603 - fixed interpreter and test-owned source
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def test_default_python_backend_does_not_require_pcre2():
    result = _run(
        "from mongoeco.core._filtering_support import compile_regex; "
        "assert compile_regex('a', 'i').search('A'); "
        "assert compile_regex(r'\\w+', '').search('á').group() == 'á'",
        "python",
    )
    assert result.returncode == 0, result.stderr


def test_invalid_backend_is_explicit_configuration_error():
    result = _run(
        "from mongoeco.core._filtering_support import compile_regex; "
        "compile_regex('a', '')",
        "invalid",
    )
    assert result.returncode != 0
    assert "Unknown MONGOECO_REGEX_BACKEND" in result.stderr


def test_missing_explicit_backend_does_not_fall_back():
    result = _run(
        "import sys; sys.modules['pcre2'] = None; "
        "from mongoeco.core._filtering_support import compile_regex; "
        "compile_regex('a', '')",
        "pcre2",
    )
    assert result.returncode != 0
    assert "install mongoeco[pcre2]" in result.stderr


@pytest.mark.skipif(
    not os.environ.get("MONGOECO_TEST_PCRE2_PATH"),
    reason="optional PCRE2 backend profile is not enabled",
)
def test_pcre2_public_consumers_and_contextual_errors():
    code = r"""from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.types import Regex
from mongoeco.errors import OperationFailure
from mongoeco.core._filtering_support import compile_regex

assert compile_regex(r"\w+", "").search("á word").group() == "word"
assert compile_regex(r"(*UCP)\w+", "").search("á word").group() == "á"
for factory in (MemoryEngine, SQLiteEngine):
    with MongoClient(factory()) as client:
        collection = client.db.rows
        collection.insert_one({"_id": 1, "text": "éab"})
        assert collection.find_one({"text": {"$regex": r"a\Kb"}})["_id"] == 1
        for pattern in (r"a\Kb", Regex(r"a\Kb")):
            rows = list(
                collection.aggregate(
                    [
                        {
                            "$project": {
                                "_id": 0,
                                "found": {
                                    "$regexFind": {"input": "$text", "regex": pattern}
                                },
                            }
                        }
                    ]
                )
            )
            assert rows == [{"found": {"match": "b", "idx": 2, "captures": []}}], rows
        rows = list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "found": {
                                "$regexFindAll": {"input": "aa", "regex": "(?=a)"}
                            },
                        }
                    }
                ]
            )
        )
        assert rows == [
            {
                "found": [
                    {"match": "", "idx": 0, "captures": []},
                    {"match": "", "idx": 1, "captures": []},
                ]
            }
        ]
        for operation, code in (
            (lambda: collection.find_one({"text": {"$regex": "["}}), 51091),
            (
                lambda: list(
                    collection.aggregate(
                        [
                            {
                                "$project": {
                                    "v": {
                                        "$regexFind": {"input": "$text", "regex": "["}
                                    }
                                }
                            }
                        ]
                    )
                ),
                51111,
            ),
        ):
            try:
                operation()
            except OperationFailure as error:
                assert error.code == code, (error.code, code)
            else:
                raise AssertionError("invalid regex accepted")
        schema = client.db.create_collection(
            "schema",
            validator={"$jsonSchema": {"properties": {"text": {"pattern": r"\p{L}+"}}}},
        )
        schema.insert_one({"text": "áβ"})
        try:
            schema.insert_one({"text": "123"})
        except OperationFailure as error:
            assert error.code == 121
        else:
            raise AssertionError("invalid schema document accepted")
"""
    result = _run(code, "pcre2", extra_path=os.environ["MONGOECO_TEST_PCRE2_PATH"])
    assert result.returncode == 0, result.stdout + result.stderr

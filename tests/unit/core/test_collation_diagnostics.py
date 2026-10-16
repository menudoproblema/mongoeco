"""ICU diagnostic keys match unmodified native captures, including opaque bytes."""

import hashlib
import json
import os
import subprocess
import sys

from functools import lru_cache
from pathlib import Path

import pytest

from mongoeco.core.collation import (
    CollationSpec,
    index_collation_document,
    index_collation_key,
)


DUPLICATE_KEY_CODE = 11000


@lru_cache
def vectors(target):
    root = Path(__file__).parents[2]
    data = json.loads(
        (
            root / f"fixtures/collation_keys_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for key, path in (
        ("collation_catalog_sha256", "differential/collation_diagnostic_cases.py"),
        ("collation_observer_sha256", "support_collation_diagnostics.py"),
    ):
        assert data[key] == hashlib.sha256((root / path).read_bytes()).hexdigest()
    return data["observations"]


def specimen_spec(options):
    return CollationSpec(
        locale=options["locale"],
        strength=options["strength"],
        case_level=options.get("caseLevel", False),
        numeric_ordering=options.get("numericOrdering", False),
        backwards=options.get("backwards", False),
        alternate=options.get("alternate", "non-ignorable"),
        max_variable=options.get("maxVariable", "punct"),
        normalization=options.get("normalization", False),
    )


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
def test_all_opaque_collation_keys_match_native_byte_for_byte(target):
    for row in vectors(target):
        spec = specimen_spec(row["options"])
        assert row["code"] == DUPLICATE_KEY_CODE
        assert (
            index_collation_key(row["value"], spec).hex()
            == row["details"]["keyValue"]["key"]
        ), row
        assert index_collation_document(spec) == row["details"]["collation"]
        assert row["details"]["hexEncoded"] == [True]


def test_diagnostic_resource_loads_without_a_utf8_process_locale():
    expected = next(
        row["details"]["keyValue"]["key"]
        for row in vectors("9.0")
        if row["value"] == "taken" and row["options"] == {"locale": "en", "strength": 2}
    )
    script = (
        "from mongoeco.core.collation import CollationSpec,index_collation_key;"
        "print(index_collation_key('taken',CollationSpec(locale='en',strength=2)).hex())"
    )
    result = subprocess.run(  # noqa: S603 - trusted script and current interpreter
        [sys.executable, "-B", "-c", script],
        env={
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUTF8": "0",
            "PYTHONCOERCECLOCALE": "0",
            "LC_ALL": "C",
        },
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.stdout.strip() == expected

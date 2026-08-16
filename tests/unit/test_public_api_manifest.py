from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from unittest.mock import patch

import pytest

from mongoeco.compat import (
    compare_public_api_manifests,
    public_api as public_api_module,
    public_api_manifest,
)


def test_public_api_manifest_captures_contract_surfaces() -> None:
    manifest = public_api_manifest()
    assert manifest["schemaVersion"] == "mongoeco-public-api/v1"
    assert manifest["contracts"]["engineSpi"] == [1, 2]
    assert manifest["contracts"]["search"] == ["search-v1"]
    assert set(manifest["modules"]) == {
        "mongoeco",
        "mongoeco.api",
        "mongoeco.compat",
        "mongoeco.cxp",
        "mongoeco.engines",
        "mongoeco.conformance",
    }
    engine = manifest["modules"]["mongoeco.engines"]
    assert "EngineCapabilities" in engine["exports"]
    capabilities = engine["symbols"]["EngineCapabilities"]
    assert capabilities["dataclass"] is True
    assert {field["name"] for field in capabilities["fields"]} >= {
        "spi_version",
        "explicit_read_snapshots",
        "change_delivery",
    }
    search = engine["symbols"]["SearchRequest"]
    assert search["dataclass"] is True


def test_public_api_manifest_comparison_classifies_breaking_dimensions() -> None:
    before = public_api_manifest()
    after = deepcopy(before)
    symbols = after["modules"]["mongoeco.engines"]["symbols"]
    symbols.pop("SearchRequest")
    outcome = symbols["SearchExecutionOutcome"]["callable"]
    outcome["parameters"][0]["default"] = "changed"
    outcome["async"] = not outcome["async"]
    after["contracts"]["search"] = ["search-v2"]
    classifications = {
        change["classification"]
        for change in compare_public_api_manifests(before, after)
    }
    assert classifications >= {
        "removal",
        "default-change",
        "async-change",
        "schema-change",
    }


def test_public_api_manifest_comparison_classifies_additions() -> None:
    before = public_api_manifest()
    after = deepcopy(before)
    after["modules"]["mongoeco.engines"]["symbols"]["FutureType"] = {"kind": "class"}
    changes = compare_public_api_manifests(before, after)
    assert any(change["classification"] == "addition-compatible" for change in changes)


def test_symbol_introspection_covers_properties_attributes_and_factories() -> None:
    class Surface:
        value = 1

        @property
        def label(self) -> str:
            return "label"

        @staticmethod
        def build() -> None:
            return None

    @dataclass
    class WithFactory:
        values: list[str] = field(default_factory=list)

    members = public_api_module._class_members(Surface)
    assert members["value"] == {"kind": "attribute"}
    assert members["label"] == {"kind": "property"}
    assert members["build"]["kind"] == "method"
    fields = public_api_module._dataclass_fields(WithFactory)
    assert fields[0]["default"].startswith("<factory:")
    assert public_api_module._dataclass_fields(Surface) == []

    with patch.object(
        public_api_module.inspect, "getattr_static", side_effect=AttributeError
    ):
        assert public_api_module._class_members(Surface) == {}


def test_public_api_comparator_covers_every_structural_dimension() -> None:
    before = public_api_manifest()
    after = deepcopy(before)
    before["modules"]["removed.module"] = {"symbols": {}}
    after["modules"]["added.module"] = {"symbols": {}}

    symbols = after["modules"]["mongoeco.engines"]["symbols"]
    capabilities = symbols["EngineCapabilities"]
    capabilities["kind"] = "value"

    search = symbols["SearchRequest"]
    search["fields"][0]["annotation"] = {"rendered": "str", "nullable": False}
    search["fields"][1]["default"] = "changed"

    outcome = symbols["SearchExecutionOutcome"]["callable"]
    outcome["parameters"] = outcome["parameters"][:-1]
    outcome["return"] = {"rendered": "int", "nullable": False}

    status = symbols["SearchExecutionState"]
    status["enumValues"] = []

    request_before = before["modules"]["mongoeco.engines"]["symbols"]["SearchRequest"]
    request_before["protocol"] = not request_before["protocol"]
    request_before["members"] = {"legacy": {"kind": "method"}}

    after["packageVersion"] = "future"
    after["resources"] = []
    changes = compare_public_api_manifests(before, after)
    classifications = {change["classification"] for change in changes}
    assert classifications >= {
        "addition-compatible",
        "removal",
        "signature-change",
        "default-change",
        "type-change",
        "manual-review",
    }


def test_public_api_comparator_rejects_malformed_module_documents() -> None:
    with pytest.raises(TypeError, match="module documents"):
        compare_public_api_manifests({"modules": []}, {"modules": {}})

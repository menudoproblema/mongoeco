"""Native-reference checks reject stale corpora and unrecorded SDK differences."""

import hashlib
import json

from copy import deepcopy

import pytest

from scripts.capture_error_phases import check_reference


@pytest.fixture
def capture_inputs(tmp_path):
    reference = {
        "inputs_sha256": {"corpus": "original"},
        "observations": {"case": [{"method": "command", "result": {"n": 1}}]},
    }
    baseline = tmp_path / "native.json"
    baseline.write_text(json.dumps(reference))
    result = {**deepcopy(reference), "sdk": "4.9.2"}
    result["observations"]["case"][0]["result"]["n"] = 2
    delta = {
        "targets": {
            "7.0": {
                "sdk": "4.9.2",
                "baseline_fixture_sha256": hashlib.sha256(
                    baseline.read_bytes()
                ).hexdigest(),
                "inputs_sha256": reference["inputs_sha256"],
                "compared_scenarios": 1,
                "overrides": result["observations"],
            }
        }
    }
    path = tmp_path / "sdk-delta.json"
    path.write_text(json.dumps(delta))
    return result, baseline, path


def test_sdk_delta_checks_do_not_mutate_or_weaken_the_native_observation(
    capture_inputs,
):
    result, baseline, path = capture_inputs
    original = deepcopy(result)
    check_reference(result, "7.0", path, baseline=baseline)
    assert result == original
    result["observations"]["case"][0]["result"]["n"] = 0
    with pytest.raises(RuntimeError, match="reference changed"):
        check_reference(result, "7.0", path, baseline=baseline)


@pytest.mark.parametrize("field", ["sdk", "inputs_sha256", "observations"])
def test_sdk_delta_checks_reject_stale_metadata_and_missing_results(
    capture_inputs, field
):
    result, baseline, path = capture_inputs
    result[field] = {} if field != "sdk" else "4.19.0"
    with pytest.raises(RuntimeError):
        check_reference(result, "7.0", path, baseline=baseline)


def test_sdk_delta_checks_reject_a_changed_baseline(capture_inputs):
    result, baseline, path = capture_inputs
    baseline.write_text(baseline.read_text() + "\n")
    with pytest.raises(RuntimeError, match="baseline changed"):
        check_reference(result, "7.0", path, baseline=baseline)


def test_native_reference_checks_keep_error_codes(tmp_path):
    result = {
        "inputs_sha256": {},
        "observations": {"case": [{"error": {"code": 14}}]},
    }
    check = tmp_path / "native.json"
    check.write_text(json.dumps(result))
    check_reference(result, "7.0", check)
    result["observations"]["case"][0]["error"].pop("code")
    with pytest.raises(RuntimeError, match="reference changed"):
        check_reference(result, "7.0", check)

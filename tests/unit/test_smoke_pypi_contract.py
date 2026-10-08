from __future__ import annotations

import importlib.util
import io
import json

from pathlib import Path
from unittest.mock import patch

import pytest


_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "smoke_pypi_contract.py"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("smoke_pypi_contract", _SCRIPT_PATH)
    if spec is None or spec.loader is None:
        message = f"Unable to load smoke script from {_SCRIPT_PATH}"
        raise RuntimeError(message)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_pypi_install_uses_the_public_index_without_cache() -> None:
    module = _load_module()
    commands: list[list[str]] = []

    with patch.object(module, "_run", side_effect=commands.append):
        module._install_from_pypi(Path("pip"), "mongoeco==4.7.0")

    assert commands == [
        [
            "pip",
            "install",
            "--index-url",
            "https://pypi.org/simple",
            "--no-cache-dir",
            "mongoeco==4.7.0",
        ],
    ]


def test_latest_published_version_is_selected_from_pypi_independently_of_git():
    module = _load_module()
    payload = {
        "info": {"name": "mongoeco", "version": "4.8.1"},
        "urls": [{"yanked": False}],
    }
    response = io.BytesIO(json.dumps(payload).encode())
    with (
        patch.object(module, "urlopen", return_value=response) as request,
        patch.object(module.subprocess, "run", side_effect=AssertionError("Git read")),
    ):
        assert module._latest_published_version() == "4.8.1"
    request.assert_called_once_with("https://pypi.org/pypi/mongoeco/json", timeout=30)


@pytest.mark.parametrize(
    "payload",
    [
        {"info": {"name": "other", "version": "4.8.1"}, "urls": [{"yanked": False}]},
        {"info": {"name": "mongoeco", "version": ""}, "urls": [{"yanked": False}]},
        {"info": {"name": "mongoeco", "version": 49}, "urls": [{"yanked": False}]},
        {"info": {"name": "mongoeco", "version": "4.8.1"}, "urls": []},
        {"info": {"name": "mongoeco", "version": "4.8.1"}, "urls": [{"yanked": True}]},
    ],
)
def test_latest_version_metadata_failures_are_not_hidden(payload):
    module = _load_module()
    with (
        patch.object(
            module, "urlopen", return_value=io.BytesIO(json.dumps(payload).encode())
        ),
        pytest.raises(
            ValueError, match=r"Invalid Mongoeco|no non-yanked distributions"
        ),
    ):
        module._latest_published_version()


def test_latest_version_network_failures_remain_gate_failures():
    module = _load_module()
    with (
        patch.object(module, "urlopen", side_effect=TimeoutError("PyPI unavailable")),
        pytest.raises(TimeoutError, match="PyPI unavailable"),
    ):
        module._latest_published_version()

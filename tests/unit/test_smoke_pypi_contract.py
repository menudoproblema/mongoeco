from __future__ import annotations

import importlib.util

from pathlib import Path
from unittest.mock import patch


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

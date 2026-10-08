#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from urllib.request import urlopen


_PYPI_SIMPLE_INDEX = "https://pypi.org/simple"
_PYPI_PROJECT_METADATA = "https://pypi.org/pypi/mongoeco/json"


def _latest_published_version() -> str:
    with urlopen(_PYPI_PROJECT_METADATA, timeout=30) as response:
        metadata = json.load(response)
    info = metadata["info"]
    version = info["version"]
    if info["name"] != "mongoeco" or not isinstance(version, str) or not version:
        message = "Invalid Mongoeco project metadata from PyPI"
        raise ValueError(message)
    if not any(not artifact["yanked"] for artifact in metadata["urls"]):
        message = "Latest Mongoeco release has no non-yanked distributions"
        raise ValueError(message)
    return version


def _run(command: list[str], *, cwd: Path | None = None) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _install_from_pypi(pip_bin: Path, *requirements: str) -> None:
    _run(
        [
            str(pip_bin),
            "install",
            "--index-url",
            _PYPI_SIMPLE_INDEX,
            "--no-cache-dir",
            *requirements,
        ],
    )


def _contract_smoke_script() -> str:
    return """
import importlib
import importlib.util
import mongoeco
import mongoeco.compat as compat
import mongoeco.cxp.exchange as exchange

assert mongoeco.__version__ == EXPECTED_VERSION, (
    f"version mismatch: expected {EXPECTED_VERSION}, got {mongoeco.__version__}"
)

required_exchange_symbols = {
    "load_mongodb_catalog",
    "load_mongodb_declared_snapshot",
    "load_mongodb_profile",
    "mongodb_catalog_store",
}
for name in required_exchange_symbols:
    assert hasattr(exchange, name), f"missing exchange symbol: {name}"
    assert name in exchange.__all__, name

for module_name in (
    "mongoeco.cxp.capabilities",
    "mongoeco.cxp.catalogs",
    "mongoeco.cxp.descriptors",
    "mongoeco.cxp.handshake",
    "cxp.capabilities",
    "cxp.catalogs",
    "cxp.descriptors",
    "cxp.handshake",
):
    assert importlib.util.find_spec(module_name) is None, module_name

catalog = exchange.load_mongodb_catalog()
compat_catalog = compat.export_exchange_catalog()
assert catalog.document_type == "cxp.catalog"
assert compat_catalog["catalog_sha256"] == catalog.sha256
assert "mongodb-core" in compat_catalog["profiles"]

for name in (
    "export_exchange_catalog",
    "export_full_compat_catalog",
):
    assert hasattr(compat, name), f"missing mongoeco.compat symbol: {name}"
    assert name in compat.__all__, f"missing mongoeco.compat __all__ symbol: {name}"

print("ok", mongoeco.__version__)
print("module", mongoeco.__file__)
print("cxp_catalog_sha256", catalog.sha256)
"""


def _published_47_contract_smoke_script() -> str:
    # The already published 4.7.0 artifact retains its own CXP 4 contract.
    return """
import importlib
import mongoeco
import mongoeco.compat as compat
import mongoeco.cxp as cxp

assert mongoeco.__version__ == "4.7.0"
for name in (
    "MONGODB_INTERFACE",
    "MONGODB_CATALOG",
    "export_cxp_capability_catalog",
    "export_cxp_operation_catalog",
    "export_cxp_profile_catalog",
    "export_cxp_profile_support_catalog",
):
    assert hasattr(cxp, name), name
    assert name in cxp.__all__, name
for module_name in (
    "mongoeco.cxp.descriptors",
    "mongoeco.cxp.contracts",
    "mongoeco.cxp.handshake",
    "mongoeco.cxp.telemetry",
):
    importlib.import_module(module_name)
catalog = cxp.export_cxp_capability_catalog()
assert catalog["interface"] == "database/mongodb"
assert compat.export_cxp_catalog()["interface"] == catalog["interface"]
assert "profiles" in catalog and "profileSupport" in catalog
print("historical contract", mongoeco.__version__)
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Instala mongoeco desde PyPI en un venv limpio y valida "
            "smoke de imports/contrato CXP publicado."
        ),
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--version",
        help="Version exacta de mongoeco a instalar desde PyPI.",
    )
    selection.add_argument(
        "--latest",
        action="store_true",
        help="Selecciona la versión publicada desde PyPI, independiente de tags Git.",
    )
    parser.add_argument(
        "--venv",
        help="Ruta del virtualenv temporal. Si se omite, crea uno efimero.",
    )
    parser.add_argument(
        "--keep-venv",
        action="store_true",
        help="Conserva el venv al terminar.",
    )
    args = parser.parse_args()
    version = _latest_published_version() if args.latest else args.version
    sys.stdout.write(f"Verifying published Mongoeco {version} from PyPI\n")
    sys.stdout.flush()

    if args.venv:
        venv_root = Path(args.venv).expanduser().resolve()
        keep_venv = True
    else:
        venv_root = Path(tempfile.mkdtemp(prefix="mongoeco-pypi-contract-smoke-"))
        keep_venv = args.keep_venv

    python_bin = venv_root / "bin" / "python"
    pip_bin = venv_root / "bin" / "pip"

    try:
        if venv_root.exists():
            shutil.rmtree(venv_root)
        _run([sys.executable, "-m", "venv", str(venv_root)])
        _install_from_pypi(pip_bin, "--upgrade", "pip")
        if version == "4.7.0":
            # Preserve evidence for the historical release without carrying its
            # protocol implementation into the current package. Its published
            # dependency metadata does not exclude incompatible CXP 5.
            _install_from_pypi(pip_bin, "mongoeco==4.7.0", "cxp[exchange]==4.3.0")
            script = _published_47_contract_smoke_script()
        else:
            _install_from_pypi(pip_bin, f"mongoeco=={version}")
            script = f"EXPECTED_VERSION = {version!r}\n{_contract_smoke_script()}"
        _run([str(python_bin), "-c", script], cwd=Path("/tmp"))
    finally:
        if not keep_venv:
            shutil.rmtree(venv_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

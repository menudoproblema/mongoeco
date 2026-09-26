#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


_PYPI_SIMPLE_INDEX = "https://pypi.org/simple"


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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Instala mongoeco desde PyPI en un venv limpio y valida "
            "smoke de imports/contrato CXP publicado."
        ),
    )
    parser.add_argument(
        "--version",
        required=True,
        help="Version exacta de mongoeco a instalar desde PyPI.",
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
        _install_from_pypi(pip_bin, f"mongoeco=={args.version}")
        script = f"EXPECTED_VERSION = {args.version!r}\n{_contract_smoke_script()}"
        _run([str(python_bin), "-c", script], cwd=Path("/tmp"))
    finally:
        if not keep_venv:
            shutil.rmtree(venv_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

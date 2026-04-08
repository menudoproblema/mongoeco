#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def _run(command: list[str], *, cwd: Path | None = None) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _contract_smoke_script() -> str:
    return """
import importlib
import mongoeco
import mongoeco.compat as compat
import mongoeco.cxp as cxp

assert mongoeco.__version__ == EXPECTED_VERSION, (
    f"version mismatch: expected {EXPECTED_VERSION}, got {mongoeco.__version__}"
)

# Superficie principal de mongoeco.cxp: fachada acotada.
required_cxp_symbols = {
    "MONGODB_INTERFACE",
    "MONGODB_CATALOG",
    "MONGODB_CORE_PROFILE",
    "MONGODB_TEXT_SEARCH_PROFILE",
    "MONGODB_SEARCH_PROFILE",
    "MONGODB_PLATFORM_PROFILE",
    "MONGODB_AGGREGATE_RICH_PROFILE",
    "MongoAggregationMetadata",
    "MongoSearchMetadata",
    "MongoVectorSearchMetadata",
    "MongoCollationMetadata",
    "MongoPersistenceMetadata",
    "MongoTopologyDiscoveryMetadata",
    "export_cxp_capability_catalog",
    "export_cxp_operation_catalog",
    "export_cxp_profile_catalog",
    "export_cxp_profile_support_catalog",
}
for name in required_cxp_symbols:
    assert hasattr(cxp, name), f"missing mongoeco.cxp symbol: {name}"
    assert name in cxp.__all__, f"missing mongoeco.cxp __all__ symbol: {name}"

# Evitar reexports anchos/legacy en la fachada.
for legacy_name in (
    "Capability",
    "CapabilityMatrix",
    "CapabilityMetadata",
    "MONGODB_FIND",
    "MONGODB_UPDATE_ONE",
    "MONGODB_SEARCH",
):
    assert not hasattr(cxp, legacy_name), f"legacy symbol leaked in mongoeco.cxp: {legacy_name}"

# Submodulos explicitos de mongoeco.cxp deben seguir disponibles.
for module_name in (
    "mongoeco.cxp.types",
    "mongoeco.cxp.descriptors",
    "mongoeco.cxp.contracts",
    "mongoeco.cxp.handshake",
    "mongoeco.cxp.telemetry",
    "mongoeco.cxp.integration",
):
    importlib.import_module(module_name)

assert cxp.MONGODB_INTERFACE == "database/mongodb"
assert cxp.MONGODB_CATALOG.interface == "database/mongodb"

cxp_capability_catalog = cxp.export_cxp_capability_catalog()
compat_catalog = compat.export_cxp_catalog()

assert cxp_capability_catalog["interface"] == "database/mongodb"
assert compat_catalog["interface"] == "database/mongodb"
assert cxp_capability_catalog["interface"] == compat_catalog["interface"]
assert "profiles" in cxp_capability_catalog
assert "profileSupport" in cxp_capability_catalog
assert "capabilities" in compat_catalog
assert "operations" in compat_catalog
assert "profileSupport" in compat_catalog

# compat sigue siendo la proyeccion de contrato para tooling.
for name in (
    "export_cxp_catalog",
    "export_cxp_operation_catalog",
    "export_cxp_profile_catalog",
    "export_cxp_profile_support_catalog",
    "export_full_compat_catalog",
):
    assert hasattr(compat, name), f"missing mongoeco.compat symbol: {name}"
    assert name in compat.__all__, f"missing mongoeco.compat __all__ symbol: {name}"

print("ok", mongoeco.__version__)
print("module", mongoeco.__file__)
print("cxp_interface", cxp.MONGODB_INTERFACE)
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
        default="3.3.0",
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
        _run([str(pip_bin), "install", "--upgrade", "pip"])
        _run([str(pip_bin), "install", f"mongoeco=={args.version}"])
        script = f"EXPECTED_VERSION = {args.version!r}\n{_contract_smoke_script()}"
        _run([str(python_bin), "-c", script], cwd=Path("/tmp"))
    finally:
        if not keep_venv:
            shutil.rmtree(venv_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

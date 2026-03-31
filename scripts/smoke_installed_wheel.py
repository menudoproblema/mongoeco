#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DIST_ROOT = PROJECT_ROOT / "dist"


def _resolve_wheel(path: str | None) -> Path:
    if path is not None:
        wheel = Path(path).expanduser().resolve()
        if not wheel.is_file():
            raise FileNotFoundError(f"Wheel no encontrado: {wheel}")
        return wheel
    candidates = sorted(DIST_ROOT.glob("mongoeco-*.whl"))
    if not candidates:
        raise FileNotFoundError("No hay wheels en dist/. Ejecuta primero `python3 -m build`.")
    return candidates[-1]


def _run(command: list[str], *, cwd: Path | None = None) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def _smoke_script() -> str:
    return """
import asyncio
import mongoeco
from mongoeco import AsyncMongoClient
from mongoeco.core.collation import compare_with_collation, normalize_collation, unicode_collation_available
from mongoeco.engines.memory import MemoryEngine

async def main() -> None:
    async with AsyncMongoClient(MemoryEngine()) as client:
        collection = client.test.events
        await collection.insert_one({"_id": "1", "kind": "view", "count": 1})
        found = await collection.find_one({"$jsonSchema": {"required": ["kind"]}})
        docs = await collection.find({}, sort=[("count", 1)]).to_list()
        spec = normalize_collation({"locale": "en", "strength": 1, "numericOrdering": True})
        assert unicode_collation_available()
        assert compare_with_collation("Álvaro", "alvaro", collation=spec) == 0
        assert compare_with_collation("file2", "file10", collation=spec) < 0
        print("version", mongoeco.__version__)
        print("module", mongoeco.__file__)
        print("unicode_collation", unicode_collation_available())
        print("find_one", found)
        print("docs", docs)

asyncio.run(main())
"""


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Instala un wheel de mongoeco en un venv limpio y ejecuta un smoke real.",
    )
    parser.add_argument(
        "--wheel",
        help="Ruta al wheel. Si se omite, usa el ultimo mongoeco-*.whl de dist/.",
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

    wheel = _resolve_wheel(args.wheel)
    if args.venv:
        venv_root = Path(args.venv).expanduser().resolve()
        keep_venv = True
    else:
        venv_root = Path(tempfile.mkdtemp(prefix="mongoeco-wheel-smoke-"))
        keep_venv = args.keep_venv

    python_bin = venv_root / "bin" / "python"
    pip_bin = venv_root / "bin" / "pip"

    try:
        if venv_root.exists():
            shutil.rmtree(venv_root)
        _run([sys.executable, "-m", "venv", str(venv_root)])
        _run([str(pip_bin), "install", "--upgrade", "pip"])
        _run([str(pip_bin), "install", str(wheel)])
        _run([str(python_bin), "-c", _smoke_script()], cwd=Path("/tmp"))
    finally:
        if not keep_venv:
            shutil.rmtree(venv_root, ignore_errors=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

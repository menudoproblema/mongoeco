"""Run an anonymous consumer suite with only its client preparation substituted."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import zipfile

from pathlib import Path
from xml.etree import ElementTree as ET


PREPARATION = """
import os
from pathlib import Path
import mongoeco
import pytest

def pytest_sessionstart():
    actual = Path(mongoeco.__file__).resolve()
    assert actual == Path(os.environ["MONGOECO_CANARY_IMPORT"])

@pytest.fixture(autouse=True)
def _prepare_database(request, monkeypatch, mongoeco_async_client):
    monkeypatch.setattr(
        request.module, os.environ['MONGOECO_CANARY_FACTORY'],
        lambda: mongoeco_async_client,
    )
"""


def verify_wheel(wheel: Path, module: Path) -> list[str]:
    if "site-packages" not in module.parts:
        message = "canary requires the installed wheel"
        raise RuntimeError(message)
    with zipfile.ZipFile(wheel) as archive:
        runtime_files = [
            name for name in archive.namelist() if name.startswith("mongoeco/")
        ]
        for name in runtime_files:
            if (module.parent.parent / name).read_bytes() != archive.read(name):
                message = "installed runtime differs from the declared wheel"
                raise RuntimeError(message)
    return runtime_files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer-root", type=Path, required=True)
    parser.add_argument("--test-file", type=Path, required=True)
    parser.add_argument("--factory-name", required=True)
    parser.add_argument("--expected-source-sha256", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, choices=(0, 2, 4), default=0)
    args = parser.parse_args()
    import pymongo  # noqa: PLC0415 - optional canary dependency
    import pytest  # noqa: PLC0415 - optional canary dependency

    import mongoeco  # noqa: PLC0415 - inspect the installed artifact, never add src

    runtime_files = verify_wheel(args.wheel, Path(mongoeco.__file__).resolve())
    source = (args.consumer_root / args.test_file).resolve()
    original = source.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    if digest != args.expected_source_sha256:
        message = "consumer source fingerprint differs from the declared input"
        raise RuntimeError(message)
    tree = ast.parse(original)
    inventory = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]
    cases = len(inventory)
    results = []
    with tempfile.TemporaryDirectory(prefix="mongoeco-pytest-consumer-") as name:
        scratch = Path(name)
        (scratch / "canary_preparation.py").write_text(PREPARATION)
        for backend in ("memory", "sqlite"):
            report = scratch / f"{backend}.xml"
            command = [
                sys.executable,
                "-m",
                "pytest",
                str(source),
                "-p",
                "anyio.pytest_plugin",
                "-p",
                "mongoeco.pytest_anyio",
                "-p",
                "canary_preparation",
                "--mongoeco-backend=" + backend,
                "--junitxml=" + str(report),
                "-q",
            ]
            if args.workers:
                command.extend(["-p", "xdist.plugin", "-n", str(args.workers)])
            with (scratch / f"{backend}.log").open("w") as log:
                result = subprocess.run(  # noqa: S603 - explicit argument vector
                    command,
                    cwd=args.consumer_root,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env={
                        **os.environ,
                        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
                        "PYTHONPATH": os.pathsep.join(
                            (str(scratch), str(args.consumer_root.resolve()))
                        ),
                        "MONGOECO_CANARY_FACTORY": args.factory_name,
                        "MONGOECO_CANARY_IMPORT": str(
                            Path(mongoeco.__file__).resolve()
                        ),
                    },
                    check=False,
                )
            if result.returncode:
                # Retain identifying failure output only in a private temporary file.
                with tempfile.NamedTemporaryFile(
                    prefix="mongoeco-private-failure-", suffix=".log", delete=False
                ) as private:
                    private.write((scratch / f"{backend}.log").read_bytes())
                message = (
                    "consumer canary failed; inspect the private local failure log"
                )
                raise RuntimeError(message)
            suites = ET.parse(report).getroot().iter("testsuite")  # noqa: S314 - own pytest output
            totals = dict.fromkeys(("tests", "failures", "errors", "skipped"), 0)
            for suite in suites:
                for key in totals:
                    totals[key] += int(suite.get(key, "0"))
            if totals != {"tests": cases, "failures": 0, "errors": 0, "skipped": 0}:
                message = "consumer inventory was skipped, expanded or reduced"
                raise RuntimeError(message)
            results.append({"backend": backend, **totals})
    if source.read_bytes() != original:
        message = "consumer source changed during canary"
        raise RuntimeError(message)
    args.output.write_text(
        json.dumps(
            {
                "schema": "mongoeco-pytest-consumer-canary/v1",
                "identity": "anonymous-consumer",
                "wheel_sha256": hashlib.sha256(args.wheel.read_bytes()).hexdigest(),
                "runtime_files_verified": len(runtime_files),
                "source_sha256": digest,
                "business_ast_sha256": hashlib.sha256(
                    ast.dump(tree).encode()
                ).hexdigest(),
                "source_unchanged": True,
                "preparation_only": True,
                "test_inventory": cases,
                "async_tests": sum(
                    isinstance(node, ast.AsyncFunctionDef) for node in inventory
                ),
                "python": sys.version.split()[0],
                "pymongo": pymongo.version,
                "pytest": pytest.__version__,
                "workers": args.workers,
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"Anonymous canary: {cases} tests per backend passed")  # noqa: T201 - public receipt


if __name__ == "__main__":
    main()

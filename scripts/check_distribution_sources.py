"""Reject stale, missing or modified package files in built distributions."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tarfile
import zipfile

from pathlib import Path


def package_sources(root: Path) -> dict[str, bytes]:
    git = shutil.which("git")
    if git is None:
        message = "Git is required to identify tracked package sources"
        raise RuntimeError(message)
    paths = subprocess.check_output(  # noqa: S603 - resolved Git executable and fixed arguments
        [git, "ls-files", "src/mongoeco"], cwd=root, text=True
    ).splitlines()
    if not paths:
        message = "no tracked Mongoeco package sources"
        raise ValueError(message)
    return {path.removeprefix("src/"): (root / path).read_bytes() for path in paths}


def check_distribution(path: Path, expected: dict[str, bytes]) -> None:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            actual = {
                name: archive.read(name)
                for name in archive.namelist()
                if name.startswith("mongoeco/") and not name.endswith("/")
            }
    else:
        with tarfile.open(path) as archive:
            actual = {
                member.name.split("/src/", 1)[1]: archive.extractfile(member).read()
                for member in archive.getmembers()
                if member.isfile() and "/src/mongoeco/" in member.name
            }
    missing = sorted(expected.keys() - actual.keys())
    unexpected = sorted(actual.keys() - expected.keys())
    changed = sorted(
        name
        for name in expected.keys() & actual.keys()
        if actual[name] != expected[name]
    )
    if missing or unexpected or changed:
        message = (
            f"{path.name}: missing={missing}; unexpected={unexpected}; "
            f"changed={changed}"
        )
        raise ValueError(message)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("distributions", nargs="+", type=Path)
    args = parser.parse_args()
    sources = package_sources(Path(__file__).resolve().parents[1])
    for distribution in args.distributions:
        check_distribution(distribution, sources)
    print(f"distribution sources verified: {len(sources)} package files")  # noqa: T201 - CLI result


if __name__ == "__main__":
    main()

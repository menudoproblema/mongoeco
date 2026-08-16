#!/usr/bin/env python3
"""Normalize a setuptools sdist into a reproducible gzip/tar archive."""

from __future__ import annotations

import argparse
import copy
import gzip
import os
import posixpath
import tarfile
import unicodedata

from pathlib import Path


def normalize_sdist(path: Path, *, epoch: int) -> None:
    """Rewrite ``path`` atomically with deterministic archive metadata."""
    if epoch < 0:
        message = "epoch must be non-negative"
        raise ValueError(message)
    temporary = path.with_name(f".{path.name}.normalized")
    try:
        with (
            tarfile.open(path, "r:gz") as source,
            temporary.open("wb") as raw_output,
            gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_output,
                mtime=epoch,
            ) as compressed_output,
            tarfile.open(
                fileobj=compressed_output,
                mode="w",
                format=tarfile.PAX_FORMAT,
            ) as target,
        ):
            members = sorted(source.getmembers(), key=lambda item: item.name)
            _validate_archive_structure(path, members)
            member_names: set[str] = set()
            for member in members:
                canonical_name = _validate_member(member)
                if canonical_name in member_names:
                    msg = f"duplicate sdist member path: {canonical_name}"
                    raise ValueError(msg)
                member_names.add(canonical_name)
                normalized = copy.copy(member)
                normalized.mtime = epoch
                normalized.uid = 0
                normalized.gid = 0
                normalized.uname = ""
                normalized.gname = ""
                normalized.pax_headers = {}
                normalized.mode = _canonical_mode(member)
                payload = source.extractfile(member) if member.isfile() else None
                try:
                    target.addfile(normalized, payload)
                finally:
                    if payload is not None:
                        payload.close()
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _validate_member(member: tarfile.TarInfo) -> str:
    normalized_name = posixpath.normpath(member.name)
    if (
        not member.name
        or member.name == "."
        or "\\" in member.name
        or "\x00" in member.name
        or member.name.startswith("/")
        or normalized_name == ".."
        or normalized_name.startswith("../")
    ):
        msg = f"unsafe sdist member path: {member.name}"
        raise ValueError(msg)
    if member.name != normalized_name:
        msg = f"non-canonical sdist member path: {member.name}"
        raise ValueError(msg)
    if unicodedata.normalize("NFC", member.name) != member.name:
        msg = f"non-NFC sdist member path: {member.name}"
        raise ValueError(msg)
    if not (member.isfile() or member.isdir()):
        msg = f"unsupported sdist member type: {member.name}"
        raise ValueError(msg)
    return normalized_name


def _validate_archive_structure(
    path: Path,
    members: list[tarfile.TarInfo],
) -> None:
    expected_root = path.name.removesuffix(".tar.gz")
    if not expected_root or expected_root == path.name:
        msg = "sdist path must end in .tar.gz"
        raise ValueError(msg)
    canonical_names = [_validate_member(member) for member in members]
    roots = {name.partition("/")[0] for name in canonical_names}
    if roots != {expected_root}:
        msg = f"sdist must contain only the expected root: {expected_root}"
        raise ValueError(msg)
    if f"{expected_root}/PKG-INFO" not in canonical_names:
        msg = "sdist root must contain PKG-INFO"
        raise ValueError(msg)
    casefold_names: dict[str, str] = {}
    for name in canonical_names:
        folded = name.casefold()
        previous = casefold_names.get(folded)
        if previous is not None and previous != name:
            msg = f"case-insensitive sdist path collision: {previous}, {name}"
            raise ValueError(msg)
        casefold_names[folded] = name


def _canonical_mode(member: tarfile.TarInfo) -> int:
    if member.isdir():
        return 0o755
    return 0o644


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sdist", type=Path)
    parser.add_argument(
        "--epoch",
        type=int,
        default=None,
        help="Unix timestamp; defaults to SOURCE_DATE_EPOCH.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    epoch = args.epoch
    if epoch is None:
        raw_epoch = os.getenv("SOURCE_DATE_EPOCH")
        if raw_epoch is None:
            message = "--epoch or SOURCE_DATE_EPOCH is required"
            raise SystemExit(message)
        try:
            epoch = int(raw_epoch)
        except ValueError as error:
            message = "SOURCE_DATE_EPOCH must be an integer"
            raise SystemExit(message) from error
    normalize_sdist(args.sdist, epoch=epoch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

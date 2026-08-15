#!/usr/bin/env python3
"""Normalize a setuptools sdist into a reproducible gzip/tar archive."""

from __future__ import annotations

import argparse
import copy
import gzip
import os
import posixpath
import tarfile

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
            member_names: set[str] = set()
            for member in members:
                _validate_member(member)
                if member.name in member_names:
                    msg = f"duplicate sdist member path: {member.name}"
                    raise ValueError(msg)
                member_names.add(member.name)
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


def _validate_member(member: tarfile.TarInfo) -> None:
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
    if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
        msg = f"unsupported sdist member type: {member.name}"
        raise ValueError(msg)
    if member.issym() or member.islnk():
        linkname = member.linkname
        base = posixpath.dirname(member.name) if member.issym() else ""
        target = posixpath.normpath(posixpath.join(base, linkname))
        if (
            not linkname
            or "\\" in linkname
            or "\x00" in linkname
            or linkname.startswith("/")
            or target == ".."
            or target.startswith("../")
        ):
            msg = f"unsafe sdist link target: {member.linkname}"
            raise ValueError(msg)


def _canonical_mode(member: tarfile.TarInfo) -> int:
    if member.isdir():
        return 0o755
    if member.issym() or member.islnk():
        return 0o777
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

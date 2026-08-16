import gzip
import io
import tarfile
import unittest

from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.normalize_sdist import normalize_sdist


def _write_sdist(path: Path, *, archive_mtime: int, member_mtime: int) -> None:
    payload = b"release payload\n"
    with (
        path.open("wb") as raw_output,
        gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            mtime=archive_mtime,
        ) as compressed_output,
        tarfile.open(fileobj=compressed_output, mode="w") as archive,
    ):
        member = tarfile.TarInfo("mongoeco-4.5.0/PKG-INFO")
        member.size = len(payload)
        member.mtime = member_mtime
        member.uid = 501
        member.gid = 20
        member.uname = "builder"
        member.gname = "staff"
        archive.addfile(member, io.BytesIO(payload))


def _write_members(path: Path, members: list[tarfile.TarInfo]) -> None:
    with tarfile.open(path, "w:gz") as archive:
        for member in members:
            payload = io.BytesIO(b"x" * member.size) if member.isfile() else None
            archive.addfile(member, payload)


def _pkg_info() -> tarfile.TarInfo:
    member = tarfile.TarInfo("mongoeco-4.5.0/PKG-INFO")
    member.size = 1
    return member


class NormalizeSdistTests(unittest.TestCase):
    def test_normalization_is_reproducible_across_source_metadata(self) -> None:
        with TemporaryDirectory() as directory:
            first_dir = Path(directory) / "first"
            second_dir = Path(directory) / "second"
            first_dir.mkdir()
            second_dir.mkdir()
            first = first_dir / "mongoeco-4.5.0.tar.gz"
            second = second_dir / "mongoeco-4.5.0.tar.gz"
            _write_sdist(first, archive_mtime=100, member_mtime=200)
            _write_sdist(second, archive_mtime=300, member_mtime=400)

            normalize_sdist(first, epoch=42)
            normalize_sdist(second, epoch=42)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            with tarfile.open(first, "r:gz") as archive:
                member = archive.getmember("mongoeco-4.5.0/PKG-INFO")
                self.assertEqual(member.mtime, 42)
                self.assertEqual(member.uid, 0)
                self.assertEqual(member.gid, 0)
                self.assertEqual(member.mode, 0o644)
                extracted = archive.extractfile(member)
                self.assertIsNotNone(extracted)
                self.assertEqual(extracted.read(), b"release payload\n")

    def test_negative_epoch_is_rejected_without_touching_archive(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mongoeco-4.5.0.tar.gz"
            _write_sdist(path, archive_mtime=100, member_mtime=200)
            original = path.read_bytes()

            with self.assertRaisesRegex(ValueError, "non-negative"):
                normalize_sdist(path, epoch=-1)

            self.assertEqual(path.read_bytes(), original)

    def test_members_are_sorted_and_modes_are_canonical(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "mongoeco-4.5.0.tar.gz"
            file_member = tarfile.TarInfo("mongoeco-4.5.0/z.py")
            file_member.size = 1
            file_member.mode = 0o777
            directory_member = tarfile.TarInfo("mongoeco-4.5.0")
            directory_member.type = tarfile.DIRTYPE
            directory_member.mode = 0o700
            first_member = tarfile.TarInfo("mongoeco-4.5.0/a.py")
            first_member.size = 1
            _write_members(
                path,
                [file_member, directory_member, first_member, _pkg_info()],
            )

            normalize_sdist(path, epoch=42)

            with tarfile.open(path, "r:gz") as archive:
                self.assertEqual(
                    archive.getnames(),
                    sorted(archive.getnames()),
                )
                self.assertEqual(archive.getmember("mongoeco-4.5.0").mode, 0o755)
                self.assertEqual(archive.getmember("mongoeco-4.5.0/z.py").mode, 0o644)

    def test_unsafe_or_ambiguous_members_are_rejected_atomically(self) -> None:
        cases = []
        for name in ("../escape", "/absolute", r"windows\\path"):
            member = tarfile.TarInfo(name)
            member.size = 1
            cases.append((name, [member], "unsafe"))

        symlink = tarfile.TarInfo("mongoeco-4.5.0/link")
        symlink.type = tarfile.SYMTYPE
        symlink.linkname = "../../escape"
        cases.append(("unsafe-link", [symlink], "unsupported"))

        ambiguous_symlink = tarfile.TarInfo("mongoeco-4.5.0/link")
        ambiguous_symlink.type = tarfile.SYMTYPE
        ambiguous_symlink.linkname = "directory/../target"
        cases.append(("ambiguous-link", [ambiguous_symlink], "unsupported"))

        device = tarfile.TarInfo("mongoeco-4.5.0/device")
        device.type = tarfile.CHRTYPE
        cases.append(("device", [device], "unsupported"))

        duplicate_a = tarfile.TarInfo("mongoeco-4.5.0/duplicate")
        duplicate_a.size = 1
        duplicate_b = tarfile.TarInfo("mongoeco-4.5.0/duplicate")
        duplicate_b.size = 1
        cases.append(("duplicate", [duplicate_a, duplicate_b], "duplicate"))

        for name in (
            "mongoeco-4.5.0/package/../module.py",
            "mongoeco-4.5.0/./module.py",
            "mongoeco-4.5.0//module.py",
        ):
            member = tarfile.TarInfo(name)
            member.size = 1
            cases.append((name, [member], "non-canonical"))

        for label, members, expected in cases:
            with self.subTest(case=label), TemporaryDirectory() as directory:
                path = Path(directory) / "mongoeco-4.5.0.tar.gz"
                _write_members(path, [*members, _pkg_info()])
                original = path.read_bytes()

                with self.assertRaisesRegex(ValueError, expected):
                    normalize_sdist(path, epoch=42)

                self.assertEqual(path.read_bytes(), original)

    def test_archive_structure_is_single_root_portable_and_link_free(self) -> None:
        cases: list[tuple[str, list[tarfile.TarInfo], str]] = []
        second_root = tarfile.TarInfo("unexpected/file.py")
        second_root.size = 1
        cases.append(("multiple-roots", [_pkg_info(), second_root], "expected root"))

        wrong_root = tarfile.TarInfo("wrong/PKG-INFO")
        wrong_root.size = 1
        cases.append(("wrong-root", [wrong_root], "expected root"))

        missing_metadata = tarfile.TarInfo("mongoeco-4.5.0/module.py")
        missing_metadata.size = 1
        cases.append(("missing-pkg-info", [missing_metadata], "PKG-INFO"))

        case_alias = tarfile.TarInfo("mongoeco-4.5.0/pkg-info")
        case_alias.size = 1
        cases.append(("case-alias", [_pkg_info(), case_alias], "collision"))

        cyclic_link = tarfile.TarInfo("mongoeco-4.5.0/loop")
        cyclic_link.type = tarfile.SYMTYPE
        cyclic_link.linkname = "loop"
        cases.append(("cyclic-link", [_pkg_info(), cyclic_link], "unsupported"))

        for label, members, expected in cases:
            with self.subTest(case=label), TemporaryDirectory() as directory:
                path = Path(directory) / "mongoeco-4.5.0.tar.gz"
                _write_members(path, members)
                original = path.read_bytes()

                with self.assertRaisesRegex(ValueError, expected):
                    normalize_sdist(path, epoch=42)

                self.assertEqual(path.read_bytes(), original)

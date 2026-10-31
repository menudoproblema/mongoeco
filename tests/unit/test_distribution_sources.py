"""Source conservation catches a reproducible wheel carrying stale build output."""

import io
import tarfile
import zipfile

from pathlib import Path

import pytest

from scripts.check_distribution_sources import check_distribution, package_sources


SOURCES = {"mongoeco/__init__.py": b"VERSION = 1\n", "mongoeco/py.typed": b""}


def artifact(path, files, kind):
    if kind == "wheel":
        target = path / "mongoeco-1-py3-none-any.whl"
        with zipfile.ZipFile(target, "w") as archive:
            for name, content in files.items():
                archive.writestr(name, content)
    else:
        target = path / "mongoeco-1.tar.gz"
        with tarfile.open(target, "w:gz") as archive:
            for name, content in files.items():
                info = tarfile.TarInfo("mongoeco-1/src/" + name)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
    return target


@pytest.mark.parametrize("kind", ["wheel", "sdist"])
@pytest.mark.parametrize("mutation", ["unchanged", "stale", "missing", "changed"])
def test_package_bytes_and_inventory_must_match_source(tmp_path, kind, mutation):
    files = dict(SOURCES)
    if mutation == "stale":
        files["mongoeco/deleted_module.py"] = b"old compiler\n"
    elif mutation == "missing":
        del files["mongoeco/py.typed"]
    elif mutation == "changed":
        files["mongoeco/__init__.py"] = b"VERSION = 0\n"
    path = artifact(tmp_path, files, kind)
    if mutation == "unchanged":
        check_distribution(path, SOURCES)
    else:
        with pytest.raises(
            ValueError,
            match={
                "stale": "unexpected=.*deleted_module",
                "missing": "missing=.*py.typed",
                "changed": "changed=.*__init__",
            }[mutation],
        ):
            check_distribution(path, SOURCES)


def test_source_inventory_is_the_tracked_package():
    root = Path(__file__).resolve().parents[2]
    sources = package_sources(root)
    assert (
        sources["mongoeco/__init__.py"]
        == (root / "src/mongoeco/__init__.py").read_bytes()
    )
    assert "mongoeco/core/aggregation/compiled_aggregation.py" not in sources
    assert "mongoeco/py.typed" in sources

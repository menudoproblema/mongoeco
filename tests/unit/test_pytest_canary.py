"""An installed-wheel check must reject source imports and runtime mismatches."""

import zipfile

import pytest

from scripts.run_pytest_consumer_canary import verify_wheel


def test_canary_rejects_source_import(tmp_path):
    with pytest.raises(RuntimeError, match="requires the installed wheel"):
        verify_wheel(tmp_path / "unused.whl", tmp_path / "src/mongoeco/__init__.py")


def test_canary_compares_every_declared_runtime_file(tmp_path):
    module = tmp_path / "site-packages/mongoeco/__init__.py"
    module.parent.mkdir(parents=True)
    module.write_text("runtime")
    wheel = tmp_path / "artifact.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("mongoeco/__init__.py", "runtime")
    assert verify_wheel(wheel, module) == ["mongoeco/__init__.py"]
    module.write_text("different")
    with pytest.raises(RuntimeError, match="differs from the declared wheel"):
        verify_wheel(wheel, module)

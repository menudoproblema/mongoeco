"""A changed observer input must invalidate its evidence before publication."""

import os
import shutil
import subprocess
import sys

from pathlib import Path

import pytest

from scripts._capture_identity import SourceIdentity
from scripts.capture_wire_read_parity import SOURCES


def test_capture_identity_rejects_changed_and_removed_sources(tmp_path):
    source = tmp_path / "helper.py"
    source.write_text("VALUE = 1\n")
    identity = SourceIdentity(tmp_path, ["helper.py"])
    expected = identity.verify()
    source.write_text("VALUE = 2\n")
    with pytest.raises(RuntimeError, match="sources changed"):
        identity.verify()
    source.write_text("VALUE = 1\n")
    assert identity.verify() == expected
    source.unlink()
    with pytest.raises(FileNotFoundError):
        identity.verify()


def test_observer_rejects_imported_helper_drift_without_writing_receipt(tmp_path):
    root = Path(__file__).parents[2]
    for source in SOURCES:
        destination = tmp_path / source
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / source, destination)
    (tmp_path / "tests/__init__.py").touch()
    (tmp_path / "tests/support_wire_reads.py").write_text("""
from pathlib import Path
CASES = ((False, False, 'document'),)
def case_id(*case): return 'drift'
async def observe(*args, **kwargs):
    path = Path(__file__)
    path.write_text(path.read_text() + '\\n# changed after import\\n')
    return {'ok': 1}
""")
    (tmp_path / "scripts/pymongo.py").write_text("""
version = 'controlled'
class MongoClient:
    def __init__(self, *args, **kwargs): self.admin = self
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def server_info(self): return {'version': '7.0.43'}
    def command(self, command):
        return {'featureCompatibilityVersion': {'version': '7.0'}}
    def list_database_names(self): return []
""")
    output = tmp_path / "receipt.json"
    result = subprocess.run(  # noqa: S603 - controlled Python fixture process
        [
            sys.executable,
            str(tmp_path / "scripts/capture_wire_read_parity.py"),
            "--target",
            "7.0",
            "--output",
            str(output),
        ],
        cwd=tmp_path,
        env={**os.environ, "MONGOECO_REAL_MONGODB_URI": "controlled"},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert "sources changed during execution" in result.stderr
    assert not output.exists()

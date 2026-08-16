import subprocess
import json
import os
import sys
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class OptionalBsonImportTests(unittest.TestCase):
    def test_core_api_imports_without_bson_installed(self) -> None:
        script = textwrap.dedent(
            """
            import asyncio
            import importlib.abc
            import sys


            class _BsonBlocker(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "bson" or fullname.startswith("bson."):
                        raise ModuleNotFoundError("blocked for test")
                    return None


            sys.meta_path.insert(0, _BsonBlocker())

            from mongoeco import AsyncMongoClient
            from mongoeco.engines.memory import MemoryEngine


            async def main() -> None:
                async with AsyncMongoClient(MemoryEngine()) as client:
                    collection = client.demo.users
                    await collection.insert_one({"_id": "1", "name": "Ada"})
                    document = await collection.find_one({"name": "Ada"})
                    assert document == {"_id": "1", "name": "Ada"}, document


            asyncio.run(main())
            """
        )
        process = subprocess.run(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=_clean_import_environment(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            process.returncode,
            0,
            msg=f"stdout:\n{process.stdout}\n\nstderr:\n{process.stderr}",
        )

    def test_object_id_public_contract_is_stable_without_bson(self) -> None:
        regular = _object_id_contract(block_bson=False)
        without_bson = _object_id_contract(block_bson=True)

        self.assertEqual(without_bson, regular)


def _object_id_contract(*, block_bson: bool) -> dict[str, object]:
    blocker = (
        """
        import importlib.abc

        class _BsonBlocker(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "bson" or fullname.startswith("bson."):
                    raise ModuleNotFoundError("blocked for test")
                return None

        sys.meta_path.insert(0, _BsonBlocker())
        """
        if block_bson
        else ""
    )
    script = textwrap.dedent(
        f"""
        import json
        import sys
        {blocker}
        from mongoeco.compat import public_api_manifest

        symbol = public_api_manifest()["modules"]["mongoeco"]["symbols"][
            "ObjectId"
        ]
        contract = {{
            "callable": symbol["callable"],
            "members": symbol["members"],
        }}
        print(json.dumps(contract, sort_keys=True))
        """
    )
    process = subprocess.run(  # noqa: S603 - fixed interpreter and script
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=_clean_import_environment(),
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        message = f"stdout:\n{process.stdout}\n\nstderr:\n{process.stderr}"
        raise AssertionError(message)
    return json.loads(process.stdout)


def _clean_import_environment() -> dict[str, str]:
    env = dict(os.environ)
    if env.get("MONGOECO_TEST_INSTALLED_ARTIFACT") == "1":
        env.pop("PYTHONPATH", None)
    else:
        env["PYTHONPATH"] = str(ROOT / "src")
    return env

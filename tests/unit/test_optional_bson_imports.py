import subprocess
import json
import os
import sys
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


class OptionalBsonImportTests(unittest.TestCase):
    def test_uuid_and_objectid_validation_without_the_optional_sdk(self):
        script = textwrap.dedent(
            '''
            import asyncio
            import importlib.abc
            import sys

            class BlockSDK(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname.split(".")[0] in {"bson", "pymongo"}:
                        raise ModuleNotFoundError("SDK deliberately unavailable")
                    return None

            sys.meta_path.insert(0, BlockSDK())
            from mongoeco import MongoClient, AsyncMongoClient
            from mongoeco.engines import MemoryEngine, SQLiteEngine
            from mongoeco.types import Binary, CodecOptions, ObjectId
            from mongoeco.core._public_materialization import _InvalidBSON

            for whitespace in ["  ", "\\t\\t", "\\n\\n"]:
                text = "00" * 11 + whitespace
                assert not ObjectId.is_valid(text)
                try:
                    ObjectId(text)
                except ValueError:
                    pass
                else:
                    raise AssertionError("invalid ObjectId accepted")
            assert len(ObjectId("A0" * 12).binary) == 12

            async def check():
                for engine in [MemoryEngine, SQLiteEngine]:
                    async with AsyncMongoClient(
                        engine(), pymongo_profile="4.20",
                        codec_options=CodecOptions(uuid_representation="standard"),
                    ) as client:
                        collection = client.test.items
                        await collection.insert_one({"_id": "x", "v": Binary(b"ab", 4)})
                        try:
                            await collection.find_one()
                        except _InvalidBSON:
                            pass
                        else:
                            raise AssertionError("invalid requested UUID accepted")
                        raw = collection.with_options(codec_options=CodecOptions())
                        assert bytes((await raw.find_one())["v"]) == b"ab"
                    with MongoClient(
                        engine(),
                        codec_options=CodecOptions(uuid_representation="pythonLegacy"),
                    ) as client:
                        collection = client.test.items
                        collection.insert_one({"_id": "x", "v": [Binary(b"ab", 3)]})
                        try:
                            collection.find_one()
                        except _InvalidBSON:
                            pass
                        else:
                            raise AssertionError("invalid legacy UUID accepted")
            asyncio.run(check())
            '''
        )
        process = subprocess.run(  # noqa: S603 - fixed interpreter and test program
            [sys.executable, "-c", script], cwd=ROOT,
            env=_clean_import_environment(), capture_output=True, text=True,
            check=False,
        )
        self.assertEqual(process.returncode, 0, process.stdout + process.stderr)

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
                async with AsyncMongoClient(
                    MemoryEngine(), pymongo_profile="4.19"
                ) as client:
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

import copy
import json
from pathlib import Path
import unittest
import uuid

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from tests.differential._real_parity_base import _normalize
from tests.differential.cases import REAL_PARITY_CASES


class DifferentialReplayGoldenTests(unittest.TestCase):
    def test_local_engines_match_replay_golden_fixture(self):
        fixture = json.loads(
            Path("tests/fixtures/differential_replay_golden.json").read_text(
                encoding="utf-8"
            )
        )
        target_dialect = fixture["targetDialect"]
        expected_by_case = fixture["cases"]
        self.assertIsInstance(expected_by_case, dict)
        self.assertEqual(
            sorted(expected_by_case),
            sorted(case.name for case in REAL_PARITY_CASES),
        )

        cases = {
            case.name: case
            for case in REAL_PARITY_CASES
        }
        for case_name, expected in expected_by_case.items():
            case = cases[case_name]
            for engine_name, engine_factory in (
                ("memory", MemoryEngine),
                ("sqlite", SQLiteEngine),
            ):
                with self.subTest(case=case_name, engine=engine_name):
                    database_name = f"mongoeco_replay_{case_name}_{uuid.uuid4().hex}"
                    with MongoClient(
                        engine_factory(),
                        mongodb_dialect=target_dialect,
                    ) as client:
                        collection = client.get_database(database_name).get_collection(
                            "cases"
                        )
                        for document in copy.deepcopy(case.seed_documents):
                            collection.insert_one(document)
                        actual = case.action(collection)

                    self.assertEqual(_normalize(actual), _normalize(expected))

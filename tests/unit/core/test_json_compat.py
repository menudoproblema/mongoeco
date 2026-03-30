import os
import unittest

from mongoeco.core import json_compat
from mongoeco.core.json_compat import get_json_backend_name, json_dumps_compact, json_loads


class JsonCompatTests(unittest.TestCase):
    def setUp(self) -> None:
        self._original_backend = os.environ.get("MONGOECO_JSON_BACKEND")
        json_compat._resolve_json_backend_name.cache_clear()

    def tearDown(self) -> None:
        if self._original_backend is None:
            os.environ.pop("MONGOECO_JSON_BACKEND", None)
        else:
            os.environ["MONGOECO_JSON_BACKEND"] = self._original_backend
        json_compat._resolve_json_backend_name.cache_clear()

    def test_json_dumps_compact_matches_expected_shape(self):
        payload = {"b": 1, "a": [True, "x"]}

        self.assertEqual(
            json_dumps_compact(payload, sort_keys=False),
            '{"b":1,"a":[true,"x"]}',
        )
        self.assertEqual(
            json_dumps_compact(payload, sort_keys=True),
            '{"a":[true,"x"],"b":1}',
        )

    def test_json_loads_round_trips_strings(self):
        self.assertEqual(
            json_loads('{"a":[1,2],"b":true}'),
            {"a": [1, 2], "b": True},
        )

    def test_default_backend_is_stdlib(self):
        os.environ.pop("MONGOECO_JSON_BACKEND", None)
        json_compat._resolve_json_backend_name.cache_clear()

        self.assertEqual(get_json_backend_name(), "stdlib")

    def test_auto_backend_uses_orjson_when_available(self):
        os.environ["MONGOECO_JSON_BACKEND"] = "auto"
        json_compat._resolve_json_backend_name.cache_clear()

        expected = "orjson" if json_compat.orjson is not None else "stdlib"
        self.assertEqual(get_json_backend_name(), expected)

    def test_explicit_orjson_backend_requires_dependency(self):
        os.environ["MONGOECO_JSON_BACKEND"] = "orjson"
        json_compat._resolve_json_backend_name.cache_clear()

        if json_compat.orjson is None:
            with self.assertRaises(RuntimeError):
                get_json_backend_name()
            return

        self.assertEqual(get_json_backend_name(), "orjson")

    def test_invalid_backend_raises_value_error(self):
        os.environ["MONGOECO_JSON_BACKEND"] = "fastest"
        json_compat._resolve_json_backend_name.cache_clear()

        with self.assertRaises(ValueError):
            get_json_backend_name()

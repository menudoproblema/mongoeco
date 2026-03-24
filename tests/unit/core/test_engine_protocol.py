import unittest

from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.memory import MemoryEngine


class AsyncStorageEngineProtocolTests(unittest.TestCase):
    def test_memory_engine_satisfies_runtime_protocol(self):
        self.assertIsInstance(MemoryEngine(), AsyncStorageEngine)

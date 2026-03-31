import importlib.util
import pathlib
import unittest


class PackageInitTests(unittest.TestCase):
    def test_src_package_init_executes_and_exports_public_symbols(self):
        init_path = pathlib.Path(__file__).resolve().parents[2] / "src" / "mongoeco" / "__init__.py"
        spec = importlib.util.spec_from_file_location("_mongoeco_src_init", init_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None

        spec.loader.exec_module(module)

        self.assertIn("AsyncMongoClient", module.__all__)
        self.assertIn("CollectionInvalid", module.__all__)
        self.assertIn("CollationBackendInfo", module.__all__)
        self.assertIn("collation_backend_info", module.__all__)
        self.assertIn("WriteConcern", module.__all__)
        self.assertIn("__version__", module.__all__)

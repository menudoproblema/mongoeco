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
        self.assertIn("ChangeStreamBackendInfo", module.__all__)
        self.assertIn("CollationBackendInfo", module.__all__)
        self.assertIn("CollationCapabilitiesInfo", module.__all__)
        self.assertIn("collation_backend_info", module.__all__)
        self.assertIn("collation_capabilities_info", module.__all__)
        self.assertIn("SdamCapabilitiesInfo", module.__all__)
        self.assertIn("sdam_capabilities_info", module.__all__)
        self.assertIn("MONGODB_INTERFACE", module.__all__)
        self.assertIn("MONGODB_CATALOG", module.__all__)
        self.assertIn("export_cxp_catalog", module.__all__)
        self.assertIn("export_cxp_operation_catalog", module.__all__)
        self.assertIn("export_cxp_profile_catalog", module.__all__)
        self.assertIn("export_cxp_profile_support_catalog", module.__all__)
        self.assertNotIn("CxpProvider", module.__all__)
        self.assertNotIn("AsyncCxpProvider", module.__all__)
        self.assertIn("WriteConcern", module.__all__)
        self.assertIn("__version__", module.__all__)

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
        self.assertIn("MongoClientOptions", module.__all__)
        self.assertIn("MongoUri", module.__all__)
        self.assertIn("parse_mongo_uri", module.__all__)
        self.assertNotIn("CxpProvider", module.__all__)
        self.assertNotIn("AsyncCxpProvider", module.__all__)
        self.assertNotIn("DriverRuntime", module.__all__)
        self.assertNotIn("ConnectionPool", module.__all__)
        self.assertNotIn("PreparedRequestExecution", module.__all__)
        self.assertNotIn("MONGODB_SEARCH", module.__all__)
        self.assertNotIn("MONGODB_VECTOR_SEARCH", module.__all__)
        self.assertNotIn("CollectionInvalid", module.__all__)
        self.assertNotIn("ChangeStreamBackendInfo", module.__all__)
        self.assertNotIn("CollationBackendInfo", module.__all__)
        self.assertNotIn("CollationCapabilitiesInfo", module.__all__)
        self.assertNotIn("collation_backend_info", module.__all__)
        self.assertNotIn("collation_capabilities_info", module.__all__)
        self.assertNotIn("SdamCapabilitiesInfo", module.__all__)
        self.assertNotIn("sdam_capabilities_info", module.__all__)
        self.assertNotIn("MONGODB_INTERFACE", module.__all__)
        self.assertNotIn("MONGODB_CATALOG", module.__all__)
        self.assertNotIn("export_cxp_catalog", module.__all__)
        self.assertNotIn("export_cxp_operation_catalog", module.__all__)
        self.assertNotIn("export_cxp_profile_catalog", module.__all__)
        self.assertNotIn("export_cxp_profile_support_catalog", module.__all__)
        self.assertNotIn("MongoDialect80", module.__all__)
        self.assertNotIn("PyMongoProfile413", module.__all__)
        self.assertIn("WriteConcern", module.__all__)
        self.assertIn("__version__", module.__all__)

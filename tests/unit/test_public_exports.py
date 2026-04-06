import os
import pathlib
import subprocess
import sys
import textwrap
import unittest


class PublicExportsTests(unittest.TestCase):
    @staticmethod
    def _repo_root() -> pathlib.Path:
        return pathlib.Path(__file__).resolve().parents[2]

    def test_public_packages_export_declared_symbols(self):
        package_names = [
            'mongoeco',
            'mongoeco.api',
            'mongoeco.api._async',
            'mongoeco.api._sync',
            'mongoeco.compat',
            'mongoeco.core.aggregation',
            'mongoeco.driver',
            'mongoeco.engines',
            'mongoeco.wire',
        ]
        pythonpath_entries = [str(self._repo_root() / 'src')]
        if existing_pythonpath := os.environ.get('PYTHONPATH'):
            pythonpath_entries.append(existing_pythonpath)
        env = os.environ | {'PYTHONPATH': os.pathsep.join(pythonpath_entries)}

        for package_name in package_names:
            with self.subTest(package=package_name):
                result = subprocess.run(
                    [
                        sys.executable,
                        '-c',
                        textwrap.dedent(
                            '''
                            import importlib
                            import sys

                            module = importlib.import_module(sys.argv[1])
                            exported_names = getattr(module, '__all__', ())
                            assert exported_names, sys.argv[1]

                            for exported_name in exported_names:
                                value = getattr(module, exported_name)
                                assert value is not None, (
                                    sys.argv[1],
                                    exported_name,
                                )
                            '''
                        ),
                        package_name,
                    ],
                    cwd=self._repo_root(),
                    capture_output=True,
                    text=True,
                    env=env,
                    check=False,
                )

                if result.returncode != 0:
                    self.fail(
                        f'Public exports failed for {package_name}\n'
                        f'stdout:\n{result.stdout}\n'
                        f'stderr:\n{result.stderr}'
                    )

    def test_async_and_sync_api_packages_export_public_cursor_types(self):
        from mongoeco.api import _async as async_api
        from mongoeco.api import _sync as sync_api

        self.assertEqual(async_api.AsyncListingCursor.__name__, "AsyncListingCursor")
        self.assertEqual(async_api.AsyncSearchIndexCursor.__name__, "AsyncSearchIndexCursor")
        self.assertEqual(async_api.AsyncRawBatchCursor.__name__, "AsyncRawBatchCursor")
        self.assertEqual(sync_api.ListingCursor.__name__, "ListingCursor")
        self.assertEqual(sync_api.SearchIndexCursor.__name__, "SearchIndexCursor")
        self.assertEqual(sync_api.RawBatchCursor.__name__, "RawBatchCursor")

    def test_compat_package_keeps_a_curated_public_surface(self):
        import mongoeco.compat as compat_module

        self.assertIn("export_cxp_catalog", compat_module.__all__)
        self.assertIn("export_full_compat_catalog", compat_module.__all__)
        self.assertNotIn("export_local_runtime_subset_catalog", compat_module.__all__)
        self.assertNotIn("DATABASE_COMMAND_SUPPORT_CATALOG", compat_module.__all__)
        self.assertNotIn("DATABASE_COMMAND_OPTION_SUPPORT_CATALOG", compat_module.__all__)
        self.assertNotIn("MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED", compat_module.__all__)
        self.assertNotIn("PYMONGO_CAP_UPDATE_ONE_SORT", compat_module.__all__)

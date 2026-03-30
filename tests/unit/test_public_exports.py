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

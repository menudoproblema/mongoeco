import os
import pathlib
import subprocess
import sys
import textwrap
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[3] / 'src'))

from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.memory import MemoryEngine


class AsyncStorageEngineProtocolTests(unittest.TestCase):
    def test_memory_engine_satisfies_runtime_protocol(self):
        self.assertIsInstance(MemoryEngine(), AsyncStorageEngine)

    def test_engine_protocol_method_annotations_resolve(self):
        repo_root = pathlib.Path(__file__).resolve().parents[3]
        pythonpath_entries = [str(repo_root / 'src')]
        if existing_pythonpath := os.environ.get('PYTHONPATH'):
            pythonpath_entries.append(existing_pythonpath)
        env = os.environ | {'PYTHONPATH': os.pathsep.join(pythonpath_entries)}

        result = subprocess.run(
            [
                sys.executable,
                '-c',
                textwrap.dedent(
                    '''
                    import inspect
                    import typing

                    import mongoeco.engines.base as engine_base

                    protocol_classes = [
                        member
                        for _, member in inspect.getmembers(
                            engine_base,
                            inspect.isclass,
                        )
                        if member.__module__ == engine_base.__name__
                        and member.__name__.startswith('Async')
                        and member.__name__.endswith('Engine')
                    ]

                    for protocol_class in protocol_classes:
                        for method_name, method in protocol_class.__dict__.items():
                            if method_name.startswith('_'):
                                continue
                            if not inspect.isfunction(method):
                                continue

                            hints = typing.get_type_hints(
                                method,
                                globalns=vars(engine_base),
                                localns={
                                    protocol_class.__name__: protocol_class,
                                },
                            )
                            assert hints, (
                                protocol_class.__name__,
                                method_name,
                            )
                    '''
                ),
            ],
            cwd=repo_root,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

        if result.returncode != 0:
            self.fail(
                'Engine protocol annotations did not resolve in a clean '
                f'interpreter.\nstdout:\n{result.stdout}\n'
                f'stderr:\n{result.stderr}'
            )

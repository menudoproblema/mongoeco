import os
from pathlib import Path
import subprocess
import sys
import textwrap
import unittest


class DatabaseCommandAnnotationTests(unittest.TestCase):
    @staticmethod
    def _repo_root() -> Path:
        return Path(__file__).resolve().parents[2]

    def test_execute_document_annotations_resolve_in_a_clean_interpreter(self):
        repo_root = self._repo_root()
        pythonpath_entries = [str(repo_root / "src")]
        if existing_pythonpath := os.environ.get("PYTHONPATH"):
            pythonpath_entries.append(existing_pythonpath)
        env = os.environ | {"PYTHONPATH": os.pathsep.join(pythonpath_entries)}

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent(
                    """
                    import typing

                    import mongoeco.api._async.database_commands as database_commands
                    from mongoeco.api._async.database_commands import AsyncDatabaseCommandService

                    typing.get_type_hints(
                        AsyncDatabaseCommandService.execute_document,
                        globalns=vars(database_commands),
                        localns={
                            "AsyncDatabaseCommandService": AsyncDatabaseCommandService,
                        },
                    )
                    """
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
                "AsyncDatabaseCommandService annotations did not resolve in a clean "
                f"interpreter.\nstdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )


if __name__ == "__main__":
    unittest.main()

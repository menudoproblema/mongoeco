import os
import sys

from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[1] / 'src'
TEST_INSTALLED_ARTIFACT = (
    os.environ.get('MONGOECO_TEST_INSTALLED_ARTIFACT') == '1'
)
if TEST_INSTALLED_ARTIFACT:
    import mongoeco

    module_path = Path(mongoeco.__file__).resolve()
    if 'site-packages' not in module_path.parts:
        message = (
            'tests must import the installed artifact, '
            f'found {module_path}'
        )
        raise RuntimeError(message)
elif str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

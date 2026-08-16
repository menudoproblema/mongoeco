import os
import sys

from pathlib import Path

from hypothesis import HealthCheck, settings


SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
TEST_INSTALLED_ARTIFACT = os.environ.get("MONGOECO_TEST_INSTALLED_ARTIFACT") == "1"
if TEST_INSTALLED_ARTIFACT:
    import mongoeco

    module_path = Path(mongoeco.__file__).resolve()
    if "site-packages" not in module_path.parts:
        message = f"tests must import the installed artifact, found {module_path}"
        raise RuntimeError(message)
elif str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


settings.register_profile(
    "ci",
    deadline=None,
    derandomize=True,
    max_examples=30,
    suppress_health_check=(HealthCheck.too_slow,),
)
settings.register_profile(
    "deep",
    deadline=None,
    derandomize=True,
    max_examples=300,
    suppress_health_check=(HealthCheck.too_slow,),
)
settings.load_profile(os.environ.get("MONGOECO_HYPOTHESIS_PROFILE", "ci"))

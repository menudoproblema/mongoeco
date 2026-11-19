"""The new SDK profile inherits the retained 4.19 API contract."""

from unittest.mock import patch

import pytest

from mongoeco.compat import (
    DEFAULT_PYMONGO_PROFILE,
    PYMONGO_PROFILE_419,
    PYMONGO_PROFILE_420,
    PyMongoProfile420,
    resolve_pymongo_profile,
    resolve_pymongo_profile_resolution,
)


def test_explicit_profile_and_instance_preserve_the_inherited_contract():
    assert resolve_pymongo_profile("4.20") is PYMONGO_PROFILE_420
    assert resolve_pymongo_profile(PyMongoProfile420()) == PYMONGO_PROFILE_420
    assert PYMONGO_PROFILE_420.capabilities == PYMONGO_PROFILE_419.capabilities
    assert PYMONGO_PROFILE_420.behavior_flags() == PYMONGO_PROFILE_419.behavior_flags()
    assert DEFAULT_PYMONGO_PROFILE == "4.9"


@pytest.mark.parametrize("mode", ["auto-installed", "strict-auto-installed"])
@pytest.mark.parametrize("version", ["4.20.0", "4.20.12"])
def test_installed_minor_resolves_exactly(mode, version):
    with patch(
        "mongoeco.compat.registry.importlib_metadata.version", return_value=version
    ):
        resolution = resolve_pymongo_profile_resolution(mode)
    assert resolution.resolved_profile is PYMONGO_PROFILE_420
    assert resolution.resolution_mode == "auto-exact"
    assert resolution.installed_version == version

"""The 4.19 profile is explicit, discoverable and independent of MongoDB dialects."""

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import (
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    PYMONGO_PROFILE_418,
    PYMONGO_PROFILE_419,
    PYMONGO_PROFILES,
    PyMongoProfile419,
    export_pymongo_profile_catalog,
    resolve_pymongo_profile,
    resolve_pymongo_profile_resolution,
)


def test_profile_exports_and_catalog_preserve_previous_contracts():
    assert resolve_pymongo_profile("4.19") is PYMONGO_PROFILE_419
    assert resolve_pymongo_profile(PyMongoProfile419()) == PYMONGO_PROFILE_419
    assert PYMONGO_PROFILES["4.19"] is PYMONGO_PROFILE_419
    assert PYMONGO_PROFILE_419.capabilities > PYMONGO_PROFILE_418.capabilities
    assert PYMONGO_PROFILE_419.supports("uri.srv_host_validator")
    assert PYMONGO_PROFILE_419.supports("uri.rejects_percent_encoded_hostnames")
    assert PYMONGO_PROFILE_419.behavior_flags() == PYMONGO_PROFILE_418.behavior_flags()
    assert export_pymongo_profile_catalog()["4.19"]["label"] == "PyMongo 4.19"
    assert DEFAULT_PYMONGO_PROFILE == "4.9"
    assert DEFAULT_MONGODB_DIALECT == "7.0"


@pytest.mark.parametrize("mode", ["auto-installed", "strict-auto-installed"])
@pytest.mark.parametrize("installed", ["4.19.0", "4.19.12"])
def test_installed_419_resolves_exactly(mode, installed):
    with patch(
        "mongoeco.compat.registry.importlib_metadata.version", return_value=installed
    ):
        resolution = resolve_pymongo_profile_resolution(mode)
    assert resolution.resolved_profile is PYMONGO_PROFILE_419
    assert resolution.installed_version == installed
    assert resolution.resolution_mode == "auto-exact"


def test_unknown_future_minor_keeps_distinct_flexible_and_strict_policies():
    with patch(
        "mongoeco.compat.registry.importlib_metadata.version", return_value="4.20.0"
    ):
        resolution = resolve_pymongo_profile_resolution("auto-installed")
        assert resolution.resolved_profile is PYMONGO_PROFILE_419
        assert resolution.resolution_mode == "auto-compatible-minor-fallback"
        with pytest.raises(ValueError, match="strict-auto-installed"):
            resolve_pymongo_profile_resolution("strict-auto-installed")


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
@pytest.mark.parametrize("profile", ["4.9", "4.18", "4.19"])
def test_validator_cannot_be_smuggled_through_uri(surface, profile):
    from mongoeco.errors import ConfigurationError  # noqa: PLC0415 - local assertion

    with pytest.raises(ConfigurationError, match="callable keyword"):
        surface(
            uri="mongodb://localhost/?srv_host_validator=allow", pymongo_profile=profile
        )


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_419_local_profile_does_not_require_optional_dns_driver(surface, dialect):
    import asyncio  # noqa: PLC0415 - close both public surfaces

    with patch(
        "mongoeco.driver.discovery.resolve_srv_public",
        side_effect=AssertionError("unexpected DNS"),
    ):
        client = surface(
            uri="mongodb://user%40example:secret%25@localhost/",
            pymongo_profile="4.19",
            mongodb_dialect=dialect,
        )
    try:
        assert client.pymongo_profile is PYMONGO_PROFILE_419
        assert client.mongodb_dialect.key == dialect
    finally:
        if surface is AsyncMongoClient:
            asyncio.run(client.close())
        else:
            client.close()

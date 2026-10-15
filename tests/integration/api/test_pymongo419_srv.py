"""Compare public DNS validation with the released PyMongo 4.19 parser."""

import asyncio

from unittest.mock import patch

import dns.resolver
import dns.rrset
import pymongo
import pytest

from pymongo.uri_parser import parse_uri

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.driver.runtime import DriverRuntime
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import ConfigurationError, InvalidURI
from mongoeco.types import ReadConcern, ReadPreference, WriteConcern


pytestmark = pytest.mark.skipif(
    pymongo.version_tuple[:2] < (4, 19), reason="requires PyMongo 4.19"
)


def dns_response(hosts):
    def resolve(name, record_type, **_kwargs):
        if record_type == "TXT":
            raise dns.resolver.NoAnswer
        return dns.rrset.from_text(
            str(name),
            60,
            "IN",
            "SRV",
            *(f"0 0 27018 {host}." for host in hosts),
        )

    return resolve


def close_client(client):
    if isinstance(client, AsyncMongoClient):
        asyncio.run(client.close())
    else:
        client.close()


def runtime(**options):
    return DriverRuntime(
        uri="mongodb+srv://cluster.example.net/",
        write_concern=WriteConcern(),
        read_concern=ReadConcern(),
        read_preference=ReadPreference(),
        pymongo_profile="4.19",
        **options,
    )


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
def test_callback_accepts_each_cross_domain_host_like_native_parser(factory, surface):
    hosts = ["one.other.net", "two.private.test"]
    uri = "mongodb+srv://cluster.example.net/"
    native_calls, local_calls = [], []

    def accept(calls):
        def validator(host):
            calls.append(host)
            return host in hosts

        return validator

    with patch("dns.resolver.resolve", side_effect=dns_response(hosts)):
        native = parse_uri(uri, srv_host_validator=accept(native_calls))
        client = surface(
            factory(),
            uri=uri,
            pymongo_profile="4.19",
            srv_host_validator=accept(local_calls),
        )
    try:
        resolution = client.srv_resolution
        assert sorted(seed.host for seed in resolution.resolved_seeds) == sorted(
            host for host, _ in native["nodelist"]
        )
        assert sorted(local_calls) == sorted(native_calls) == sorted(hosts)
        assert client.pymongo_profile.key == "4.19"
    finally:
        close_client(client)


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
@pytest.mark.parametrize("outcome", [False, None, 1, "yes", "raise"])
def test_rejected_callback_results_and_exceptions_match_native(surface, outcome):
    def validator(_host):
        if outcome == "raise":
            message = "policy unavailable"
            raise RuntimeError(message)
        return outcome

    uri = "mongodb+srv://cluster.example.net/"
    with patch("dns.resolver.resolve", side_effect=dns_response(["one.other.net"])):
        with pytest.raises(pymongo.errors.ConfigurationError) as native:
            parse_uri(uri, srv_host_validator=validator)
        with pytest.raises(ConfigurationError) as local:
            surface(uri=uri, pymongo_profile="4.19", srv_host_validator=validator)
    assert str(local.value) == str(native.value)
    assert isinstance(local.value, pymongo.errors.ConfigurationError)


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
@pytest.mark.parametrize(
    "suffix", ["internal", "test", "localhost", ".INTERNAL.", "bücher.example"]
)
def test_private_single_label_suffix_matches_native(surface, suffix):
    domain = suffix.strip(".").lower().encode("idna").decode("ascii")
    uri = f"mongodb+srv://cluster.example.net/?srvAllowedHostsSuffix={suffix}"
    with patch("dns.resolver.resolve", side_effect=dns_response([f"node.{domain}"])):
        expected = parse_uri(uri)
        client = surface(uri=uri, pymongo_profile="4.19")
    try:
        resolution = client.srv_resolution
        assert [
            (seed.host, seed.port) for seed in resolution.resolved_seeds
        ] == expected["nodelist"]
    finally:
        close_client(client)


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
@pytest.mark.parametrize(
    "uri", ["mongodb://host%2Cother/", "mongodb+srv://host%2Eexample.net/"]
)
def test_percent_encoded_hostnames_fail_before_dns_with_native_error(surface, uri):
    with patch("dns.resolver.resolve", side_effect=AssertionError("unexpected DNS")):
        with pytest.raises(pymongo.errors.InvalidURI) as native:
            parse_uri(uri)
        with pytest.raises(InvalidURI) as local:
            surface(uri=uri, pymongo_profile="4.19")
    assert str(local.value) == str(native.value)
    assert isinstance(local.value, pymongo.errors.InvalidURI)


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
def test_invalid_options_fail_before_dns(surface):
    with patch("dns.resolver.resolve", side_effect=AssertionError("unexpected DNS")):
        with pytest.raises(ValueError, match="must be a callable"):
            surface(
                uri="mongodb+srv://cluster.example.net/",
                pymongo_profile="4.19",
                srv_host_validator=1,
            )
        with pytest.raises(ConfigurationError, match="mongodb\\+srv"):
            surface(pymongo_profile="4.19", srv_host_validator=lambda _: True)
        with pytest.raises(ConfigurationError, match="Cannot specify both"):
            surface(
                uri="mongodb+srv://cluster.example.net/?srvAllowedHostsSuffix=example.net",
                pymongo_profile="4.19",
                srv_host_validator=lambda _: True,
            )
        with pytest.raises(ConfigurationError, match=r"profile 4\.19"):
            surface(
                uri="mongodb+srv://cluster.example.net/",
                pymongo_profile="4.18",
                srv_host_validator=lambda _: True,
            )


def test_simulated_dns_cannot_bypass_callback_validation():
    for injected in (
        {"srv_records": (("simulation.invalid", 27017),)},
        {"srv_resolver": dns_response(["simulation.invalid"])},
    ):
        with pytest.raises(ConfigurationError, match="public resolver"):
            runtime(srv_host_validator=lambda _: True, **injected)


def test_public_419_resolution_requires_the_optional_419_driver():
    with (
        patch("pymongo.version_tuple", (4, 18, 2)),
        pytest.raises(ConfigurationError, match=r"PyMongo >= 4\.19"),
    ):
        runtime(srv_host_validator=lambda _: True)


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
def test_one_rejected_host_fails_the_whole_discovery_before_max_hosts(surface):
    uri = "mongodb+srv://cluster.example.net/?srvMaxHosts=1"

    def validator(host):
        return host == "one.internal"

    with patch(
        "dns.resolver.resolve",
        side_effect=dns_response(["one.internal", "two.other.net"]),
    ):
        with pytest.raises(pymongo.errors.ConfigurationError) as native:
            parse_uri(uri, srv_host_validator=validator)
        with pytest.raises(ConfigurationError) as local:
            surface(uri=uri, pymongo_profile="4.19", srv_host_validator=validator)
    assert str(local.value) == str(native.value)


@pytest.mark.parametrize("suffix", ["com", "co.uk", "production"])
def test_public_or_unreserved_single_label_suffix_is_rejected(suffix):
    uri = f"mongodb+srv://cluster.example.net/?srvAllowedHostsSuffix={suffix}"
    with patch("dns.resolver.resolve", side_effect=AssertionError("unexpected DNS")):
        with pytest.raises(pymongo.errors.ConfigurationError) as native:
            parse_uri(uri)
        with pytest.raises(ConfigurationError) as local:
            MongoClient(uri=uri, pymongo_profile="4.19")
    assert str(local.value) == str(native.value)

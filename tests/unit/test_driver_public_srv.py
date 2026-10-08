"""Exercise the public PyMongo resolver through controlled DNS answers."""

import asyncio

from unittest.mock import patch

import dns.name
import dns.resolver
import dns.rrset
import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.driver import DriverRuntime
from mongoeco.errors import ConfigurationError
from mongoeco.types import ReadConcern, ReadPreference, WriteConcern


HAS_PUBLIC_SUFFIX = pymongo.version_tuple[:2] >= (4, 18)


def build_runtime(uri, **options):
    return DriverRuntime(
        uri=uri,
        write_concern=WriteConcern(),
        read_concern=ReadConcern(),
        read_preference=ReadPreference(),
        **options,
    )


def controlled_dns(host="node.example.net.", *, txt=None, fail=False):
    def resolve(name, record_type, **kwargs):
        if fail:
            message = "controlled DNS failure"
            raise dns.resolver.NoNameservers(message)
        if record_type == "SRV":
            return dns.rrset.from_text(str(name), 60, "IN", "SRV", f"0 0 27018 {host}")
        if txt is None:
            raise dns.resolver.NoAnswer
        return dns.rrset.from_text(str(name), 60, "IN", "TXT", f'"{txt}"')

    return resolve


@pytest.mark.skipif(
    not HAS_PUBLIC_SUFFIX, reason="requires the real PyMongo 4.18 resolver"
)
@pytest.mark.parametrize("suffix", [None, "example.net", ".EXAMPLE.NET."])
def test_public_srv_validates_hosts_and_materializes_dns_options(suffix):
    uri = "mongodb+srv://ada:password@cluster.example.net/?tls=false"
    if suffix:
        uri += f"&srvAllowedHostsSuffix={suffix}"
    with patch(
        "dns.resolver.resolve",
        side_effect=controlled_dns(txt="authSource=identity&replicaSet=rs0"),
    ):
        runtime = build_runtime(uri, pymongo_profile="4.18")
    assert runtime.srv_resolution.resolved_seeds[0].address == "node.example.net:27018"
    assert runtime.auth_policy.source == "identity"
    assert runtime.topology.set_name == "rs0"
    assert not runtime.tls_policy.enabled


@pytest.mark.skipif(
    not HAS_PUBLIC_SUFFIX, reason="requires the real PyMongo 4.18 resolver"
)
@pytest.mark.parametrize(
    ["suffix", "host", "fail"],
    [
        (None, "node.other.net.", False),
        ("example.net", "node.other.net.", False),
        ("com", "node.example.com.", False),
        ("co.uk", "node.example.co.uk.", False),
        (None, "node.example.net.", True),
    ],
)
def test_public_srv_rejects_invalid_hosts_suffixes_and_dns_errors(suffix, host, fail):
    uri = "mongodb+srv://cluster.example.net/"
    if suffix:
        uri += f"?srvAllowedHostsSuffix={suffix}"
    with (
        patch("dns.resolver.resolve", side_effect=controlled_dns(host, fail=fail)),
        pytest.raises(ConfigurationError),
    ):
        build_runtime(uri, pymongo_profile="4.18")


@pytest.mark.parametrize("profile", ["4.9", "4.17", "4.18"])
def test_injected_seeds_do_not_claim_suffix_validation(profile):
    uri = "mongodb+srv://cluster.example.net/?srvAllowedHostsSuffix=example.net"
    for injected in (
        {"srv_records": (("node.example.net", 27017),)},
        {"srv_resolver": controlled_dns()},
    ):
        with pytest.raises(ConfigurationError, match="public resolver"):
            build_runtime(uri, pymongo_profile=profile, **injected)
    runtime = build_runtime(
        "mongodb+srv://cluster.example.net/",
        pymongo_profile=profile,
        srv_records=(("simulation.invalid", 27017),),
    )
    assert runtime.srv_resolution.resolved_seeds[0].host == "simulation.invalid"


@pytest.mark.parametrize("surface", [AsyncMongoClient, MongoClient])
def test_local_client_does_not_need_dns_and_plain_uri_rejects_suffix(surface):
    with patch("dns.resolver.resolve", side_effect=AssertionError("unexpected DNS")):
        client = surface(pymongo_profile="4.18")
        assert client.pymongo_profile.key == "4.18"
        if surface is AsyncMongoClient:
            asyncio.run(client.close())
        else:
            client.close()
        with pytest.raises(ConfigurationError, match="mongodb\\+srv"):
            surface(
                uri="mongodb://localhost/?srvAllowedHostsSuffix=example.net",
                pymongo_profile="4.18",
            )


def test_old_profile_dns_fallback_is_preserved_but_suffix_is_rejected():
    with patch("dns.resolver.resolve", side_effect=controlled_dns(fail=True)):
        runtime = build_runtime(
            "mongodb+srv://cluster.example.net/", pymongo_profile="4.17"
        )
        assert (
            runtime.srv_resolution.resolved_seeds[0].address
            == "cluster.example.net:27017"
        )
    with pytest.raises(ConfigurationError, match=r"profile 4\.18"):
        build_runtime(
            "mongodb+srv://cluster.example.net/?srvAllowedHostsSuffix=example.net",
            pymongo_profile="4.17",
        )


def test_public_srv_requires_supported_optional_driver():
    with (
        patch("pymongo.version_tuple", (4, 17, 0)),
        pytest.raises(ConfigurationError, match=r"PyMongo >= 4\.18"),
    ):
        build_runtime("mongodb+srv://cluster.example.net/", pymongo_profile="4.18")
    with (
        patch.dict("sys.modules", {"pymongo.uri_parser": None}),
        pytest.raises(ConfigurationError, match=r"PyMongo >= 4\.18"),
    ):
        build_runtime("mongodb+srv://cluster.example.net/", pymongo_profile="4.18")


@pytest.mark.skipif(
    not HAS_PUBLIC_SUFFIX, reason="requires the real PyMongo 4.18 resolver"
)
@pytest.mark.parametrize("txt", [None, "authSource=from_txt&replicaSet=from_txt"])
@pytest.mark.parametrize(
    "spelling",
    [
        "authSource=from_uri&replicaSet=from_uri",
        "authsource=from_uri&replicaset=from_uri",
    ],
)
def test_public_srv_explicit_uri_options_take_precedence_over_txt(txt, spelling):
    uri = f"mongodb+srv://ada:password@cluster.example.net/?tls=false&{spelling}"
    with patch("dns.resolver.resolve", side_effect=controlled_dns(txt=txt)):
        runtime = build_runtime(uri, pymongo_profile="4.18")
    assert runtime.auth_policy.source == "from_uri"
    assert runtime.topology.set_name == "from_uri"
    assert runtime.srv_resolution.effective_options.auth.source == "from_uri"
    assert runtime.srv_resolution.effective_options.replica_set == "from_uri"


@pytest.mark.skipif(
    not HAS_PUBLIC_SUFFIX, reason="requires the real PyMongo 4.18 resolver"
)
@pytest.mark.parametrize(
    ["uri_value", "txt_value", "expected"],
    [("true", None, True), ("false", "true", False), ("true", "false", True)],
)
def test_public_srv_explicit_load_balanced_is_preserved(uri_value, txt_value, expected):
    uri = f"mongodb+srv://cluster.example.net/?tls=false&loadBalanced={uri_value}"
    txt = None if txt_value is None else f"loadBalanced={txt_value}"
    with patch("dns.resolver.resolve", side_effect=controlled_dns(txt=txt)):
        runtime = build_runtime(uri, pymongo_profile="4.18")
    assert runtime.srv_resolution.effective_options.load_balanced is expected

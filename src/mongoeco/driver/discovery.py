from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Sequence

from mongoeco.driver.uri import MongoClientOptions, MongoUri, MongoUriSeed
from mongoeco.errors import ConfigurationError


@dataclass(frozen=True, slots=True)
class SrvResolution:
    original_hostname: str
    service_name: str
    resolved_seeds: tuple[MongoUriSeed, ...]
    max_hosts: int | None = None
    txt_options: dict[str, str] | None = None
    effective_options: MongoClientOptions | None = None


def resolve_srv_public(uri: MongoUri) -> SrvResolution | None:
    """Delegate DNS and suffix validation to the driver's public API."""
    if uri.scheme != "mongodb+srv":
        return None
    missing_driver_message = "PyMongo >= 4.18 is required for public SRV resolution"
    try:
        import pymongo  # noqa: PLC0415 - optional dependency

        from pymongo.errors import (  # noqa: PLC0415 - optional dependency
            ConfigurationError as DriverConfigurationError,
        )
        from pymongo.uri_parser import parse_uri  # noqa: PLC0415
    except ImportError as exc:
        raise ConfigurationError(missing_driver_message) from exc
    if pymongo.version_tuple[:2] < (4, 18):
        raise ConfigurationError(missing_driver_message)
    try:
        result = parse_uri(
            uri.original,
            connect_timeout=uri.options.connect_timeout_ms / 1000,
            srv_service_name=uri.options.srv_service_name,
            srv_max_hosts=uri.options.srv_max_hosts,
            srv_allowed_hosts_suffix=uri.options.srv_allowed_hosts_suffix,
        )
    except DriverConfigurationError as exc:
        raise ConfigurationError(str(exc)) from exc
    options = result["options"]
    effective_options = replace(
        uri.options,
        replica_set=options.get("replicaSet"),
        auth=replace(uri.options.auth, source=options.get("authSource")),
        load_balanced=bool(options.get("loadBalanced", False)),
    )
    return SrvResolution(
        original_hostname=uri.seeds[0].host,
        service_name=uri.options.srv_service_name or "mongodb",
        resolved_seeds=tuple(
            MongoUriSeed(host, port) for host, port in result["nodelist"]
        ),
        max_hosts=uri.options.srv_max_hosts,
        effective_options=effective_options,
    )


def resolve_srv_seeds(
    uri: MongoUri,
    *,
    srv_records: Sequence[MongoUriSeed | tuple[str, int | None]] | None = None,
) -> SrvResolution | None:
    if uri.scheme != "mongodb+srv":
        return None
    hostname = uri.seeds[0].host
    service_name = uri.options.srv_service_name or "mongodb"
    if srv_records is None:
        resolved = (MongoUriSeed(hostname, 27017),)
    else:
        if not srv_records:
            raise ValueError("mongodb+srv resolution requires at least one seed")
        normalized: list[MongoUriSeed] = []
        for seed in srv_records:
            if isinstance(seed, MongoUriSeed):
                normalized.append(
                    MongoUriSeed(
                        seed.host, seed.port if seed.port is not None else 27017
                    )
                )
            else:
                host, port = seed
                normalized.append(MongoUriSeed(host, 27017 if port is None else port))
        resolved = tuple(normalized)
    max_hosts = uri.options.srv_max_hosts
    if max_hosts is not None:
        resolved = resolved[:max_hosts]
    return SrvResolution(
        original_hostname=hostname,
        service_name=service_name,
        resolved_seeds=resolved,
        max_hosts=max_hosts,
    )


def resolve_srv_dns(
    uri: MongoUri,
    *,
    resolver=None,
) -> SrvResolution | None:
    if uri.scheme != "mongodb+srv":
        return None
    if resolver is None:
        try:
            import dns.resolver  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "dnspython is required for real SRV DNS resolution"
            ) from exc

        resolver = dns.resolver.resolve
    hostname = uri.seeds[0].host
    service_name = uri.options.srv_service_name or "mongodb"
    srv_name = f"_{service_name}._tcp.{hostname}"
    try:
        srv_answers = resolver(srv_name, "SRV")
    except Exception:  # noqa: BLE001
        # Keep client construction stable for local/test use when real DNS SRV is
        # unavailable; callers that need strict DNS behavior can inject a resolver.
        return resolve_srv_seeds(uri)
    txt_options: dict[str, str] = {}
    try:
        txt_answers = resolver(hostname, "TXT")
    except Exception:  # noqa: BLE001
        txt_answers = ()
    resolved: list[MongoUriSeed] = []
    for answer in srv_answers:
        target = getattr(answer, "target", None)
        port = getattr(answer, "port", None)
        if target is None:
            continue
        host = str(target).rstrip(".")
        resolved.append(MongoUriSeed(host, port))
    for answer in txt_answers:
        strings = getattr(answer, "strings", ())
        if not strings:
            continue
        for item in strings:
            raw = (
                item.decode("utf-8")
                if isinstance(item, (bytes, bytearray))
                else str(item)
            )
            for part in raw.split("&"):
                if "=" not in part:
                    continue
                key, value = part.split("=", 1)
                txt_options[key] = value
    max_hosts = uri.options.srv_max_hosts
    if max_hosts is not None:
        resolved = resolved[:max_hosts]
    return SrvResolution(
        original_hostname=hostname,
        service_name=service_name,
        resolved_seeds=tuple(resolved),
        max_hosts=max_hosts,
        txt_options=txt_options or None,
    )


def materialize_srv_uri(
    uri: MongoUri,
    *,
    resolution: SrvResolution | None,
) -> MongoUri:
    if resolution is None:
        return uri
    effective_uri = replace(uri, seeds=resolution.resolved_seeds)
    if resolution.effective_options is not None:
        return replace(effective_uri, options=resolution.effective_options)
    if not resolution.txt_options:
        return effective_uri
    options = effective_uri.options
    txt = resolution.txt_options
    return replace(
        effective_uri,
        options=replace(
            options,
            replica_set=options.replica_set or txt.get("replicaSet"),
            read_preference=options.read_preference or txt.get("readPreference"),
            raw_options={**txt, **options.raw_options},
        ),
    )

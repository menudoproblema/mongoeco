from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Sequence

from mongoeco.driver.uri import MongoUri, MongoUriSeed


@dataclass(frozen=True, slots=True)
class SrvResolution:
    original_hostname: str
    service_name: str
    resolved_seeds: tuple[MongoUriSeed, ...]
    max_hosts: int | None = None


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
                normalized.append(MongoUriSeed(seed.host, seed.port if seed.port is not None else 27017))
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


def materialize_srv_uri(
    uri: MongoUri,
    *,
    resolution: SrvResolution | None,
) -> MongoUri:
    if resolution is None:
        return uri
    return replace(uri, seeds=resolution.resolved_seeds)

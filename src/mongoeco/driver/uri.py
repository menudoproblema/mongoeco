from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import parse_qsl, unquote, urlsplit

from mongoeco.types import ReadPreference, WriteConcern, ReadConcern


type MongoScheme = Literal["mongodb", "mongodb+srv"]


@dataclass(frozen=True, slots=True)
class MongoAuthOptions:
    source: str | None = None
    mechanism: str | None = None


@dataclass(frozen=True, slots=True)
class MongoTlsOptions:
    enabled: bool = False
    allow_invalid_certificates: bool = False
    ca_file: str | None = None
    certificate_key_file: str | None = None


@dataclass(frozen=True, slots=True)
class MongoUriSeed:
    host: str
    port: int | None = None

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must be a non-empty string")
        if self.port is not None and not (0 < self.port <= 65535):
            raise ValueError("port must be between 1 and 65535")

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}" if self.port is not None else self.host


@dataclass(frozen=True, slots=True)
class MongoClientOptions:
    app_name: str | None = None
    replica_set: str | None = None
    auth: MongoAuthOptions = field(default_factory=MongoAuthOptions)
    tls: MongoTlsOptions = field(default_factory=MongoTlsOptions)
    direct_connection: bool | None = None
    load_balanced: bool = False
    retry_reads: bool = True
    retry_writes: bool = True
    server_selection_timeout_ms: int = 30_000
    connect_timeout_ms: int = 20_000
    socket_timeout_ms: int | None = None
    max_pool_size: int = 100
    min_pool_size: int = 0
    max_idle_time_ms: int | None = None
    compressors: tuple[str, ...] = ()
    read_preference: str | None = None
    read_preference_tags: tuple[dict[str, str], ...] | None = None
    max_staleness_seconds: int | None = None
    read_concern_level: str | None = None
    write_concern_w: int | str | None = None
    write_concern_journal: bool | None = None
    write_concern_wtimeout_ms: int | None = None
    srv_service_name: str | None = None
    srv_max_hosts: int | None = None
    raw_options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MongoUri:
    original: str
    scheme: MongoScheme
    seeds: tuple[MongoUriSeed, ...]
    username: str | None = None
    password: str | None = None
    default_database: str | None = None
    options: MongoClientOptions = field(default_factory=MongoClientOptions)

    def __post_init__(self) -> None:
        if not self.seeds:
            raise ValueError("MongoUri requires at least one seed")

    @property
    def normalized(self) -> str:
        authority = ",".join(seed.address for seed in self.seeds)
        path = f"/{self.default_database}" if self.default_database else "/"
        return f"{self.scheme}://{authority}{path}"


def parse_mongo_uri(uri: str | None) -> MongoUri:
    if uri is None:
        return MongoUri(
            original="mongodb://localhost/",
            scheme="mongodb",
            seeds=(MongoUriSeed("localhost", 27017),),
        )
    if not isinstance(uri, str) or not uri:
        raise TypeError("uri must be a non-empty string or None")

    parsed = urlsplit(uri)
    if parsed.scheme not in {"mongodb", "mongodb+srv"}:
        raise ValueError("uri scheme must be mongodb:// or mongodb+srv://")
    if not parsed.netloc:
        raise ValueError("uri must include at least one host")

    username: str | None = None
    password: str | None = None
    hostinfo = parsed.netloc
    if "@" in hostinfo:
        credentials, hostinfo = hostinfo.rsplit("@", 1)
        if ":" in credentials:
            username_raw, password_raw = credentials.split(":", 1)
            username = unquote(username_raw) if username_raw else None
            password = unquote(password_raw) if password_raw else None
        else:
            username = unquote(credentials) if credentials else None

    seeds = _parse_seeds(hostinfo, scheme=parsed.scheme)  # type: ignore[arg-type]
    default_database = parsed.path.lstrip("/") or None
    option_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    options = _parse_client_options(option_pairs)
    return MongoUri(
        original=uri,
        scheme=parsed.scheme,  # type: ignore[arg-type]
        seeds=seeds,
        username=username,
        password=password,
        default_database=default_database,
        options=options,
    )


def _parse_seeds(hostinfo: str, *, scheme: MongoScheme) -> tuple[MongoUriSeed, ...]:
    raw_seeds = [item for item in hostinfo.split(",") if item]
    if not raw_seeds:
        raise ValueError("uri must include at least one host")
    seeds: list[MongoUriSeed] = []
    for raw_seed in raw_seeds:
        if raw_seed.startswith("["):
            raise ValueError("IPv6 literal hosts are not supported yet")
        if ":" in raw_seed:
            host, raw_port = raw_seed.rsplit(":", 1)
            if scheme == "mongodb+srv":
                raise ValueError("mongodb+srv URIs must not specify ports")
            seeds.append(MongoUriSeed(host=host, port=int(raw_port)))
        else:
            seeds.append(MongoUriSeed(host=raw_seed, port=None if scheme == "mongodb+srv" else 27017))
    return tuple(seeds)


def _parse_client_options(option_pairs: list[tuple[str, str]]) -> MongoClientOptions:
    raw_options = {key: value for key, value in option_pairs}
    grouped_options: dict[str, list[str]] = {}
    for key, value in option_pairs:
        grouped_options.setdefault(key, []).append(value)

    def _get_bool(name: str, default: bool | None = None) -> bool | None:
        raw = raw_options.get(name)
        if raw is None:
            return default
        normalized = raw.lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
        raise ValueError(f"{name} must be a boolean option")

    def _get_int(name: str, default: int | None = None) -> int | None:
        raw = raw_options.get(name)
        if raw is None:
            return default
        value = int(raw)
        if value < 0:
            raise ValueError(f"{name} must be >= 0")
        return value

    def _parse_tag_sets() -> tuple[dict[str, str], ...] | None:
        raw_items = grouped_options.get("readPreferenceTags")
        if not raw_items:
            return None
        tag_sets: list[dict[str, str]] = []
        for raw_item in raw_items:
            if raw_item == "":
                tag_sets.append({})
                continue
            pairs = [part for part in raw_item.split(",") if part]
            tag_set: dict[str, str] = {}
            for pair in pairs:
                if ":" not in pair:
                    raise ValueError("readPreferenceTags items must be key:value pairs")
                key, value = pair.split(":", 1)
                if not key:
                    raise ValueError("readPreferenceTags keys must be non-empty")
                tag_set[key] = value
            tag_sets.append(tag_set)
        return tuple(tag_sets)

    def _parse_write_concern_w() -> int | str | None:
        raw = raw_options.get("w")
        if raw is None:
            return None
        try:
            return int(raw)
        except ValueError:
            if not raw:
                raise ValueError("w must be a non-empty string or integer")
            return raw

    compressors_raw = raw_options.get("compressors")
    compressors = tuple(item for item in (compressors_raw.split(",") if compressors_raw else []) if item)

    max_pool_size = _get_int("maxPoolSize", 100)
    min_pool_size = _get_int("minPoolSize", 0)
    assert max_pool_size is not None
    assert min_pool_size is not None
    if min_pool_size > max_pool_size:
        raise ValueError("minPoolSize must be <= maxPoolSize")

    tls_enabled = _get_bool("tls", None)
    if tls_enabled is None:
        tls_enabled = _get_bool("ssl", None)
    if tls_enabled is None:
        tls_enabled = False
    assert tls_enabled is not None
    retry_reads = _get_bool("retryReads", True)
    retry_writes = _get_bool("retryWrites", True)
    assert retry_reads is not None and retry_writes is not None
    direct_connection = _get_bool("directConnection", None)
    load_balanced = _get_bool("loadBalanced", False)
    assert load_balanced is not None
    if direct_connection and load_balanced:
        raise ValueError("directConnection and loadBalanced are mutually exclusive")

    auth = MongoAuthOptions(
        source=raw_options.get("authSource"),
        mechanism=raw_options.get("authMechanism"),
    )
    tls = MongoTlsOptions(
        enabled=tls_enabled,
        allow_invalid_certificates=_get_bool("tlsAllowInvalidCertificates", False) or False,
        ca_file=raw_options.get("tlsCAFile"),
        certificate_key_file=raw_options.get("tlsCertificateKeyFile"),
    )

    return MongoClientOptions(
        app_name=raw_options.get("appName"),
        replica_set=raw_options.get("replicaSet"),
        auth=auth,
        tls=tls,
        direct_connection=direct_connection,
        load_balanced=load_balanced,
        retry_reads=retry_reads,
        retry_writes=retry_writes,
        server_selection_timeout_ms=_get_int("serverSelectionTimeoutMS", 30_000) or 30_000,
        connect_timeout_ms=_get_int("connectTimeoutMS", 20_000) or 20_000,
        socket_timeout_ms=_get_int("socketTimeoutMS", None),
        max_pool_size=max_pool_size,
        min_pool_size=min_pool_size,
        max_idle_time_ms=_get_int("maxIdleTimeMS", None),
        compressors=compressors,
        read_preference=raw_options.get("readPreference"),
        read_preference_tags=_parse_tag_sets(),
        max_staleness_seconds=_get_int("maxStalenessSeconds", None),
        read_concern_level=raw_options.get("readConcernLevel"),
        write_concern_w=_parse_write_concern_w(),
        write_concern_journal=_get_bool("journal", None),
        write_concern_wtimeout_ms=_get_int("wtimeoutMS", None),
        srv_service_name=raw_options.get("srvServiceName"),
        srv_max_hosts=_get_int("srvMaxHosts", None),
        raw_options=raw_options,
    )


def build_read_preference_from_uri(uri: MongoUri, default: ReadPreference) -> ReadPreference:
    options = uri.options
    if options.read_preference is None and options.read_preference_tags is None and options.max_staleness_seconds is None:
        return default
    return ReadPreference(
        options.read_preference or default.mode,
        tag_sets=options.read_preference_tags if options.read_preference_tags is not None else default.tag_sets,
        max_staleness_seconds=(
            options.max_staleness_seconds
            if options.max_staleness_seconds is not None
            else default.max_staleness_seconds
        ),
    )


def build_read_concern_from_uri(uri: MongoUri, default: ReadConcern) -> ReadConcern:
    level = uri.options.read_concern_level
    if level is None:
        return default
    return ReadConcern(level)


def build_write_concern_from_uri(uri: MongoUri, default: WriteConcern) -> WriteConcern:
    options = uri.options
    if (
        options.write_concern_w is None
        and options.write_concern_journal is None
        and options.write_concern_wtimeout_ms is None
    ):
        return default
    return WriteConcern(
        w=options.write_concern_w if options.write_concern_w is not None else default.w,
        j=(
            options.write_concern_journal
            if options.write_concern_journal is not None
            else default.j
        ),
        wtimeout=(
            options.write_concern_wtimeout_ms
            if options.write_concern_wtimeout_ms is not None
            else default.wtimeout
        ),
    )

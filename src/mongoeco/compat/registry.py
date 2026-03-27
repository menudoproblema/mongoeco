import importlib.metadata as importlib_metadata
from dataclasses import dataclass

from mongoeco.compat.catalog import (
    AUTO_INSTALLED_PYMONGO_PROFILE,
    DEFAULT_MONGODB_DIALECT,
    DEFAULT_PYMONGO_PROFILE,
    MONGODB_DIALECT_ALIASES,
    PYMONGO_PROFILE_ALIASES,
    STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
)
from mongoeco.compat.base import (
    MongoDialect,
    MONGODB_DIALECTS,
    PyMongoProfile,
    PYMONGO_PROFILE_49,
    PYMONGO_PROFILE_411,
    PYMONGO_PROFILE_413,
    PYMONGO_PROFILES,
    SUPPORTED_MONGODB_MAJORS,
    SUPPORTED_PYMONGO_MAJORS,
)


MongoDialectInput = MongoDialect | str | None
PyMongoProfileInput = PyMongoProfile | str | None

_KNOWN_PYMONGO_PROFILE_VERSIONS = {
    tuple(map(int, profile.key.split('.', 1))): profile
    for profile in PYMONGO_PROFILES.values()
}
_KNOWN_PYMONGO_PROFILE_MINORS_BY_MAJOR = {
    major: sorted(
        minor
        for (profile_major, minor) in _KNOWN_PYMONGO_PROFILE_VERSIONS
        if profile_major == major
    )
    for major in SUPPORTED_PYMONGO_MAJORS
}


@dataclass(frozen=True, slots=True)
class MongoDialectResolution:
    requested: str | None
    detected_server_version: str | None
    resolved_dialect: MongoDialect
    resolution_mode: str


@dataclass(frozen=True, slots=True)
class PyMongoProfileResolution:
    requested: str | None
    installed_version: str | None
    resolved_profile: PyMongoProfile
    resolution_mode: str


def resolve_mongodb_dialect(value: MongoDialectInput = None) -> MongoDialect:
    """Resuelve un dialecto efectivo a partir de un alias o una instancia."""

    return resolve_mongodb_dialect_resolution(value).resolved_dialect


def resolve_mongodb_dialect_resolution(
    value: MongoDialectInput = None,
) -> MongoDialectResolution:
    """Resuelve un dialecto junto con metadatos de la política aplicada."""

    if value is None:
        return MongoDialectResolution(
            requested=None,
            detected_server_version=None,
            resolved_dialect=MONGODB_DIALECTS[DEFAULT_MONGODB_DIALECT],
            resolution_mode='default',
        )
    if isinstance(value, MongoDialect):
        return MongoDialectResolution(
            requested=value.key,
            detected_server_version=None,
            resolved_dialect=value,
            resolution_mode='explicit-instance',
        )
    canonical = MONGODB_DIALECT_ALIASES.get(value)
    if canonical is not None:
        return MongoDialectResolution(
            requested=value,
            detected_server_version=None,
            resolved_dialect=MONGODB_DIALECTS[canonical],
            resolution_mode='explicit-alias',
        )
    if isinstance(value, str):
        major, _, _minor = value.partition('.')
        if major.isdigit() and int(major) not in SUPPORTED_MONGODB_MAJORS:
            supported = ', '.join(str(item) for item in sorted(SUPPORTED_MONGODB_MAJORS))
            raise ValueError(
                f'Unsupported MongoDB major version: {value}. '
                f'Supported majors: {supported}'
            )
    raise ValueError(f'Unsupported MongoDB dialect: {value}')


def resolve_pymongo_profile(value: PyMongoProfileInput = None) -> PyMongoProfile:
    """Resuelve un perfil efectivo a partir de un alias o de la instalacion local."""

    return resolve_pymongo_profile_resolution(value).resolved_profile


def resolve_pymongo_profile_resolution(
    value: PyMongoProfileInput = None,
) -> PyMongoProfileResolution:
    """Resuelve un perfil junto con metadatos de la politica aplicada."""

    if value is None:
        return PyMongoProfileResolution(
            requested=None,
            installed_version=None,
            resolved_profile=PYMONGO_PROFILES[DEFAULT_PYMONGO_PROFILE],
            resolution_mode='default',
        )
    if isinstance(value, PyMongoProfile):
        return PyMongoProfileResolution(
            requested=value.key,
            installed_version=None,
            resolved_profile=value,
            resolution_mode='explicit-instance',
        )
    canonical = PYMONGO_PROFILE_ALIASES.get(value)
    if canonical is not None:
        return PyMongoProfileResolution(
            requested=value,
            installed_version=None,
            resolved_profile=PYMONGO_PROFILES[canonical],
            resolution_mode='explicit-alias',
        )
    if value == AUTO_INSTALLED_PYMONGO_PROFILE:
        return detect_installed_pymongo_profile_resolution(strict=False)
    if value == STRICT_AUTO_INSTALLED_PYMONGO_PROFILE:
        return detect_installed_pymongo_profile_resolution(strict=True)
    raise ValueError(f'Unsupported PyMongo profile: {value}')


def detect_installed_pymongo_profile() -> PyMongoProfile:
    """Mapea la version instalada de pymongo a un perfil estable de compatibilidad."""

    return detect_installed_pymongo_profile_resolution(strict=False).resolved_profile


def detect_installed_pymongo_profile_resolution(
    *,
    strict: bool = False,
) -> PyMongoProfileResolution:
    """Resuelve la instalacion local con politica flexible o estricta."""

    try:
        installed = importlib_metadata.version('pymongo')
    except importlib_metadata.PackageNotFoundError as exc:
        raise ValueError(
            'pymongo_profile auto-installed requiere tener pymongo instalado'
        ) from exc

    major, _, remainder = installed.partition('.')
    minor, _, _patch = remainder.partition('.')
    if not major.isdigit() or (minor and not minor.isdigit()):
        raise ValueError(f'Unsupported installed PyMongo version: {installed}')
    major_number = int(major)
    if major_number not in SUPPORTED_PYMONGO_MAJORS:
        supported = ', '.join(str(item) for item in sorted(SUPPORTED_PYMONGO_MAJORS))
        raise ValueError(
            f'Unsupported installed PyMongo major version: {installed}. '
            f'Supported majors: {supported}'
        )
    minor_number = int(minor or '0')
    if (major_number, minor_number) < min(_KNOWN_PYMONGO_PROFILE_VERSIONS):
        raise ValueError(f'Unsupported installed PyMongo version: {installed}')
    exact_profile = _KNOWN_PYMONGO_PROFILE_VERSIONS.get((major_number, minor_number))
    if strict:
        if exact_profile is None:
            raise ValueError(
                'Unsupported installed PyMongo version for strict-auto-installed: '
                f'{installed}'
            )
        return PyMongoProfileResolution(
            requested=STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
            installed_version=installed,
            resolved_profile=exact_profile,
            resolution_mode='auto-exact',
        )
    if exact_profile is not None:
        resolved = exact_profile
        resolution_mode = 'auto-exact'
    else:
        compatible_minor = max(
            minor
            for minor in _KNOWN_PYMONGO_PROFILE_MINORS_BY_MAJOR[major_number]
            if minor <= minor_number
        )
        resolved = _KNOWN_PYMONGO_PROFILE_VERSIONS[(major_number, compatible_minor)]
        resolution_mode = 'auto-compatible-minor-fallback'
    return PyMongoProfileResolution(
        requested=AUTO_INSTALLED_PYMONGO_PROFILE,
        installed_version=installed,
        resolved_profile=resolved,
        resolution_mode=resolution_mode,
    )

from __future__ import annotations

from dataclasses import dataclass

from mongoeco.driver.uri import MongoUri


@dataclass(frozen=True, slots=True)
class AuthPolicy:
    username: str | None
    password: str | None
    source: str | None
    mechanism: str | None
    mechanism_properties: dict[str, str]
    external: bool = False

    @property
    def enabled(self) -> bool:
        return self.username is not None or self.mechanism is not None


@dataclass(frozen=True, slots=True)
class TlsPolicy:
    enabled: bool
    verify_certificates: bool
    ca_file: str | None = None
    certificate_key_file: str | None = None


def build_auth_policy(uri: MongoUri) -> AuthPolicy:
    mechanism = uri.options.auth.mechanism
    username = uri.username
    password = uri.password
    source = uri.options.auth.source
    if mechanism == "MONGODB-X509":
        if password is not None:
            raise ValueError("MONGODB-X509 must not include a password")
        source = "$external"
    elif mechanism in {"PLAIN", "GSSAPI"}:
        source = source or "$external"
    elif username is not None:
        source = source or uri.default_database or "admin"
    external = source == "$external"
    if mechanism in {"SCRAM-SHA-1", "SCRAM-SHA-256", "PLAIN", "GSSAPI"} and username is None:
        raise ValueError(f"{mechanism} requires a username")
    return AuthPolicy(
        username=username,
        password=password,
        source=source,
        mechanism=mechanism,
        mechanism_properties=dict(uri.options.auth.mechanism_properties),
        external=external,
    )


def build_tls_policy(uri: MongoUri) -> TlsPolicy:
    tls = uri.options.tls
    if tls.certificate_key_file and not tls.enabled:
        raise ValueError("tlsCertificateKeyFile requires TLS to be enabled")
    if tls.ca_file and not tls.enabled:
        raise ValueError("tlsCAFile requires TLS to be enabled")
    if uri.options.auth.mechanism == "MONGODB-X509" and not tls.enabled:
        raise ValueError("MONGODB-X509 requires TLS to be enabled")
    return TlsPolicy(
        enabled=tls.enabled,
        verify_certificates=not tls.allow_invalid_certificates,
        ca_file=tls.ca_file,
        certificate_key_file=tls.certificate_key_file,
    )

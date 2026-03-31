from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
import unicodedata
from dataclasses import dataclass

from mongoeco.errors import OperationFailure


_SCRAM_DIGESTS = {
    "SCRAM-SHA-1": "sha1",
    "SCRAM-SHA-256": "sha256",
}


@dataclass(frozen=True, slots=True)
class ScramClientStart:
    nonce: str
    first_bare: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class ScramServerFirst:
    nonce: str
    salt: bytes
    iterations: int
    message: str


@dataclass(frozen=True, slots=True)
class ScramClientFinal:
    payload: bytes
    expected_server_signature: str


def generate_scram_nonce() -> str:
    return base64.b64encode(secrets.token_bytes(18)).decode("ascii").rstrip("=")


def build_scram_client_start(*, username: str, nonce: str | None = None) -> ScramClientStart:
    actual_nonce = nonce or generate_scram_nonce()
    first_bare = f"n={_scram_escape_username(username)},r={actual_nonce}"
    return ScramClientStart(
        nonce=actual_nonce,
        first_bare=first_bare,
        payload=f"n,,{first_bare}".encode("utf-8"),
    )


def parse_scram_client_start(payload: bytes, *, mechanism: str) -> tuple[str, str, str]:
    _digest_name_for_mechanism(mechanism)
    message = _decode_scram_payload(payload)
    if not message.startswith("n,,"):
        raise OperationFailure("SCRAM client-first message must use gs2 header 'n,,'")
    first_bare = message[3:]
    attributes = _parse_scram_attributes(first_bare)
    username = _scram_unescape_username(attributes.get("n"))
    nonce = attributes.get("r")
    if not username:
        raise OperationFailure("SCRAM client-first message requires username")
    if not nonce:
        raise OperationFailure("SCRAM client-first message requires nonce")
    return username, nonce, first_bare


def build_scram_server_first(*, client_nonce: str, mechanism: str, salt: bytes, iterations: int, server_nonce: str | None = None) -> ScramServerFirst:
    _digest_name_for_mechanism(mechanism)
    if iterations <= 0:
        raise OperationFailure("SCRAM iterations must be positive")
    combined_nonce = client_nonce + (server_nonce or generate_scram_nonce())
    message = f"r={combined_nonce},s={base64.b64encode(salt).decode('ascii')},i={iterations}"
    return ScramServerFirst(
        nonce=combined_nonce,
        salt=salt,
        iterations=iterations,
        message=message,
    )


def parse_scram_server_first(message: str, *, mechanism: str, client_nonce: str) -> ScramServerFirst:
    _digest_name_for_mechanism(mechanism)
    attributes = _parse_scram_attributes(message)
    nonce = attributes.get("r")
    salt_text = attributes.get("s")
    iterations_text = attributes.get("i")
    if not nonce or not nonce.startswith(client_nonce):
        raise OperationFailure("SCRAM server-first nonce must extend the client nonce")
    if not salt_text:
        raise OperationFailure("SCRAM server-first message requires salt")
    if not iterations_text:
        raise OperationFailure("SCRAM server-first message requires iterations")
    try:
        salt = base64.b64decode(salt_text.encode("ascii"), validate=True)
    except Exception as exc:  # noqa: BLE001
        raise OperationFailure("SCRAM server-first salt must be valid base64") from exc
    try:
        iterations = int(iterations_text)
    except ValueError as exc:
        raise OperationFailure("SCRAM server-first iterations must be an integer") from exc
    if iterations <= 0:
        raise OperationFailure("SCRAM server-first iterations must be positive")
    return ScramServerFirst(
        nonce=nonce,
        salt=salt,
        iterations=iterations,
        message=message,
    )


def build_scram_client_final(
    *,
    password: str,
    mechanism: str,
    client_first_bare: str,
    server_first_message: str,
    combined_nonce: str,
) -> ScramClientFinal:
    digest_name = _digest_name_for_mechanism(mechanism)
    channel_binding = "biws"
    client_final_without_proof = f"c={channel_binding},r={combined_nonce}"
    auth_message = ",".join((client_first_bare, server_first_message, client_final_without_proof))
    client_nonce = _parse_scram_attributes(client_first_bare).get("r")
    if not client_nonce:
        raise OperationFailure("SCRAM client-first message requires nonce")
    server_first = parse_scram_server_first(
        server_first_message,
        mechanism=mechanism,
        client_nonce=client_nonce,
    )
    salted_password = _salted_password(
        password,
        mechanism,
        server_first.salt,
        server_first.iterations,
    )
    client_key = hmac.digest(salted_password, b"Client Key", digest_name)
    stored_key = hashlib.new(digest_name, client_key).digest()
    client_signature = hmac.digest(stored_key, auth_message.encode("utf-8"), digest_name)
    client_proof = bytes(left ^ right for left, right in zip(client_key, client_signature))
    server_key = hmac.digest(salted_password, b"Server Key", digest_name)
    server_signature = hmac.digest(server_key, auth_message.encode("utf-8"), digest_name)
    payload = (
        f"{client_final_without_proof},p={base64.b64encode(client_proof).decode('ascii')}"
    ).encode("utf-8")
    return ScramClientFinal(
        payload=payload,
        expected_server_signature=base64.b64encode(server_signature).decode("ascii"),
    )


def verify_scram_client_final(
    payload: bytes,
    *,
    password: str,
    mechanism: str,
    client_first_bare: str,
    server_first_message: str,
) -> str:
    digest_name = _digest_name_for_mechanism(mechanism)
    message = _decode_scram_payload(payload)
    attributes = _parse_scram_attributes(message)
    proof_text = attributes.get("p")
    channel_binding = attributes.get("c")
    nonce = attributes.get("r")
    if channel_binding != "biws":
        raise OperationFailure("SCRAM channel binding data must be 'biws'")
    if not nonce:
        raise OperationFailure("SCRAM client-final message requires nonce")
    if not proof_text:
        raise OperationFailure("SCRAM client-final message requires proof")
    try:
        proof = base64.b64decode(proof_text.encode("ascii"), validate=True)
    except Exception as exc:  # noqa: BLE001
        raise OperationFailure("SCRAM client proof must be valid base64") from exc
    final_without_proof = ",".join(part for part in message.split(",") if not part.startswith("p="))
    client_nonce = _parse_scram_attributes(client_first_bare).get("r")
    if not client_nonce:
        raise OperationFailure("SCRAM client-first message requires nonce")
    server_first = parse_scram_server_first(
        server_first_message,
        mechanism=mechanism,
        client_nonce=client_nonce,
    )
    auth_message = ",".join((client_first_bare, server_first.message, final_without_proof))
    salted_password = _salted_password(password, mechanism, server_first.salt, server_first.iterations)
    client_key = hmac.digest(salted_password, b"Client Key", digest_name)
    stored_key = hashlib.new(digest_name, client_key).digest()
    client_signature = hmac.digest(stored_key, auth_message.encode("utf-8"), digest_name)
    expected_proof = bytes(left ^ right for left, right in zip(client_key, client_signature))
    if not hmac.compare_digest(proof, expected_proof):
        raise OperationFailure("Authentication failed")
    server_key = hmac.digest(salted_password, b"Server Key", digest_name)
    server_signature = hmac.digest(server_key, auth_message.encode("utf-8"), digest_name)
    return base64.b64encode(server_signature).decode("ascii")


def verify_scram_server_final(payload: bytes, *, expected_server_signature: str) -> None:
    message = _decode_scram_payload(payload)
    attributes = _parse_scram_attributes(message)
    server_signature = attributes.get("v")
    error = attributes.get("e")
    if error:
        raise OperationFailure(f"SCRAM authentication failed: {error}")
    if not server_signature:
        raise OperationFailure("SCRAM server-final message requires verifier")
    if not hmac.compare_digest(server_signature, expected_server_signature):
        raise OperationFailure("SCRAM server signature did not match the conversation transcript")


def _digest_name_for_mechanism(mechanism: str) -> str:
    digest_name = _SCRAM_DIGESTS.get(mechanism)
    if digest_name is None:
        raise OperationFailure(f"Unsupported SCRAM mechanism: {mechanism}")
    return digest_name


def _salted_password(password: str, mechanism: str, salt: bytes, iterations: int) -> bytes:
    digest_name = _digest_name_for_mechanism(mechanism)
    return hashlib.pbkdf2_hmac(
        digest_name,
        _normalize_password(password).encode("utf-8"),
        salt,
        iterations,
    )


def _normalize_password(password: str) -> str:
    return unicodedata.normalize("NFKC", password)


def _parse_scram_attributes(message: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for part in message.split(","):
        if not part:
            continue
        name, separator, value = part.partition("=")
        if not separator or len(name) != 1:
            raise OperationFailure("SCRAM message contains an invalid attribute")
        result[name] = value
    return result


def _scram_escape_username(username: str) -> str:
    return username.replace("=", "=3D").replace(",", "=2C")


def _scram_unescape_username(username: str | None) -> str:
    if username is None:
        return ""
    return username.replace("=2C", ",").replace("=3D", "=")


def _decode_scram_payload(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OperationFailure("SCRAM payload must be valid UTF-8") from exc

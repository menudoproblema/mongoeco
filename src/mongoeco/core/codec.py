import binascii
import datetime
import uuid
from typing import Any

from mongoeco.types import ObjectId


class DocumentCodec:
    """
    Normaliza documentos usando un formato interno reversible (Extended JSON style).
    Asegura que un datetime guardado en SQLite vuelva como datetime al usuario.
    """

    _MARKER = "$mongoeco"
    _TYPE = "type"
    _VALUE = "value"

    @staticmethod
    def _tagged_value(value_type: str, value: Any) -> dict[str, Any]:
        return {
            DocumentCodec._MARKER: {
                DocumentCodec._TYPE: value_type,
                DocumentCodec._VALUE: value,
            }
        }

    @staticmethod
    def _is_tagged_value(data: Any) -> bool:
        if not isinstance(data, dict) or set(data) != {DocumentCodec._MARKER}:
            return False

        payload = data[DocumentCodec._MARKER]
        return (
            isinstance(payload, dict)
            and set(payload) == {DocumentCodec._TYPE, DocumentCodec._VALUE}
            and isinstance(payload[DocumentCodec._TYPE], str)
        )

    @staticmethod
    def encode(data: Any) -> Any:
        if isinstance(data, dict):
            encoded = {k: DocumentCodec.encode(v) for k, v in data.items()}
            if DocumentCodec._is_tagged_value(encoded):
                return DocumentCodec._tagged_value("dict", encoded)
            return encoded
        if isinstance(data, list):
            return [DocumentCodec.encode(v) for v in data]

        if isinstance(data, datetime.datetime):
            return DocumentCodec._tagged_value("datetime", data.isoformat())

        if isinstance(data, uuid.UUID):
            return DocumentCodec._tagged_value("uuid", str(data))

        if isinstance(data, ObjectId):
            return DocumentCodec._tagged_value("objectid", str(data))

        if isinstance(data, bytes):
            return DocumentCodec._tagged_value("bytes", binascii.hexlify(data).decode("ascii"))

        return data

    @staticmethod
    def decode(data: Any) -> Any:
        if DocumentCodec._is_tagged_value(data):
            payload = data[DocumentCodec._MARKER]
            value_type = payload[DocumentCodec._TYPE]
            value = payload[DocumentCodec._VALUE]

            if value_type == "datetime":
                return datetime.datetime.fromisoformat(value)
            if value_type == "uuid":
                return uuid.UUID(value)
            if value_type == "objectid":
                return ObjectId(value)
            if value_type == "bytes":
                return binascii.unhexlify(value)
            if value_type == "dict":
                return {k: DocumentCodec.decode(v) for k, v in value.items()}
            raise ValueError(f"Unsupported tagged value type: {value_type}")

        if isinstance(data, dict):
            return {k: DocumentCodec.decode(v) for k, v in data.items()}

        if isinstance(data, list):
            return [DocumentCodec.decode(v) for v in data]

        return data

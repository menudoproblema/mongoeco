import binascii
import datetime
import decimal
import uuid
from typing import Any

try:
    from bson.code import Code as BsonCode
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
except Exception:  # pragma: no cover - optional dependency
    BsonCode = None
    BsonMaxKey = None
    BsonMinKey = None

from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    unwrap_bson_numeric,
    validate_bson_value,
)
from mongoeco.types import (
    Binary,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    SON,
    Timestamp,
    UndefinedType,
    UNDEFINED,
    is_object_id_like,
    normalize_object_id,
)


class DocumentCodec:
    """
    Normaliza documentos usando un formato interno reversible (Extended JSON style).
    Asegura que un datetime guardado en SQLite vuelva como datetime al usuario.
    """

    _MARKER = "$mongoeco"
    _TYPE = "type"
    _VALUE = "value"
    _BSON_WRAPPER_TYPES = (BsonInt32, BsonInt64, BsonDouble, BsonDecimal128)

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
        if (
            not isinstance(data, dict)
            or len(data) != 1
            or DocumentCodec._MARKER not in data
        ):
            return False

        payload = data[DocumentCodec._MARKER]
        return (
            isinstance(payload, dict)
            and len(payload) == 2
            and DocumentCodec._TYPE in payload
            and DocumentCodec._VALUE in payload
            and isinstance(payload[DocumentCodec._TYPE], str)
        )

    @staticmethod
    def encode(data: Any) -> Any:
        if isinstance(data, SON):
            return DocumentCodec._tagged_value(
                "son",
                [[key, DocumentCodec.encode(value)] for key, value in data.items()],
            )
        if isinstance(data, dict):
            encoded = {k: DocumentCodec.encode(v) for k, v in data.items()}
            if DocumentCodec._is_tagged_value(encoded):
                return DocumentCodec._tagged_value("dict", encoded)
            return encoded
        if isinstance(data, list):
            return [DocumentCodec.encode(v) for v in data]

        validate_bson_value(data)

        if isinstance(data, datetime.datetime):
            return DocumentCodec._tagged_value("datetime", data.isoformat())

        if BsonMinKey is not None and isinstance(data, BsonMinKey):
            return DocumentCodec._tagged_value("minkey", True)

        if BsonMaxKey is not None and isinstance(data, BsonMaxKey):
            return DocumentCodec._tagged_value("maxkey", True)

        if isinstance(data, decimal.Decimal):
            return DocumentCodec._tagged_value("decimal", str(data))

        if isinstance(data, uuid.UUID):
            return DocumentCodec._tagged_value("uuid", str(data))

        if is_object_id_like(data):
            return DocumentCodec._tagged_value(
                "objectid",
                str(normalize_object_id(data)),
            )

        if isinstance(data, Binary):
            return DocumentCodec._tagged_value(
                "binary",
                {
                    "hex": binascii.hexlify(bytes(data)).decode("ascii"),
                    "subtype": data.subtype,
                },
            )

        if isinstance(data, Regex):
            return DocumentCodec._tagged_value(
                "regex",
                {
                    "pattern": data.pattern,
                    "flags": data.flags,
                },
            )

        if isinstance(data, Timestamp):
            return DocumentCodec._tagged_value(
                "timestamp",
                {"time": data.time, "inc": data.inc},
            )

        if isinstance(data, Decimal128):
            return DocumentCodec._tagged_value("decimal128_public", str(data.value))

        if BsonCode is not None and isinstance(data, BsonCode):
            return DocumentCodec._tagged_value(
                "code",
                {
                    "code": str(data),
                    "scope": DocumentCodec.encode(data.scope) if data.scope is not None else None,
                },
            )

        if isinstance(data, DBRef):
            return DocumentCodec._tagged_value(
                "dbref",
                {
                    "collection": data.collection,
                    "id": DocumentCodec.encode(data.id),
                    "database": data.database,
                    "extras": DocumentCodec.encode(data.extras),
                },
            )

        if isinstance(data, bytes):
            return DocumentCodec._tagged_value("bytes", binascii.hexlify(data).decode("ascii"))

        if isinstance(data, UndefinedType):
            return DocumentCodec._tagged_value("undefined", True)

        if isinstance(data, BsonInt32):
            return DocumentCodec._tagged_value("int32", data.value)

        if isinstance(data, BsonInt64):
            return DocumentCodec._tagged_value("int64", data.value)

        if isinstance(data, BsonDouble):
            return DocumentCodec._tagged_value("double", data.value)

        if isinstance(data, BsonDecimal128):
            return DocumentCodec._tagged_value("decimal128", str(data.value))

        return data

    @staticmethod
    def decode(data: Any, *, preserve_bson_wrappers: bool = False) -> Any:
        if DocumentCodec._is_tagged_value(data):
            payload = data[DocumentCodec._MARKER]
            value_type = payload[DocumentCodec._TYPE]
            value = payload[DocumentCodec._VALUE]

            if value_type == "datetime":
                return datetime.datetime.fromisoformat(value)
            if value_type == "minkey":
                if BsonMinKey is None:
                    raise ValueError("MinKey requires bson support")
                return BsonMinKey()
            if value_type == "maxkey":
                if BsonMaxKey is None:
                    raise ValueError("MaxKey requires bson support")
                return BsonMaxKey()
            if value_type == "decimal":
                return decimal.Decimal(value)
            if value_type == "uuid":
                return uuid.UUID(value)
            if value_type == "objectid":
                return ObjectId(value)
            if value_type == "binary":
                return Binary(binascii.unhexlify(value["hex"]), subtype=int(value["subtype"]))
            if value_type == "regex":
                return Regex(value["pattern"], value["flags"])
            if value_type == "timestamp":
                return Timestamp(int(value["time"]), int(value["inc"]))
            if value_type == "decimal128_public":
                return Decimal128(value)
            if value_type == "code":
                if BsonCode is None:
                    raise ValueError("Code requires bson support")
                scope = value["scope"]
                return BsonCode(
                    value["code"],
                    DocumentCodec.decode(scope, preserve_bson_wrappers=preserve_bson_wrappers)
                    if scope is not None
                    else None,
                )
            if value_type == "dbref":
                return DBRef(
                    collection=str(value["collection"]),
                    id=DocumentCodec.decode(value["id"], preserve_bson_wrappers=preserve_bson_wrappers),
                    database=str(value["database"]) if value["database"] is not None else None,
                    extras=DocumentCodec.decode(value["extras"], preserve_bson_wrappers=preserve_bson_wrappers),
                )
            if value_type == "bytes":
                return binascii.unhexlify(value)
            if value_type == "undefined":
                return UNDEFINED
            if value_type == "int32":
                return BsonInt32(int(value)) if preserve_bson_wrappers else int(value)
            if value_type == "int64":
                return BsonInt64(int(value)) if preserve_bson_wrappers else int(value)
            if value_type == "double":
                return BsonDouble(float(value)) if preserve_bson_wrappers else float(value)
            if value_type == "decimal128":
                return BsonDecimal128(decimal.Decimal(value)) if preserve_bson_wrappers else decimal.Decimal(value)
            if value_type == "dict":
                return {
                    k: DocumentCodec.decode(v, preserve_bson_wrappers=preserve_bson_wrappers)
                    for k, v in value.items()
                }
            if value_type == "son":
                return SON(
                    (
                        str(key),
                        DocumentCodec.decode(item, preserve_bson_wrappers=preserve_bson_wrappers),
                    )
                    for key, item in value
                )
            raise ValueError(f"Unsupported tagged value type: {value_type}")

        if isinstance(data, dict):
            flat_dict: dict[Any, Any] = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    break
                if isinstance(value, list):
                    if any(isinstance(item, dict | list) for item in value):
                        break
                    flat_dict[key] = list(value)
                    continue
                flat_dict[key] = value
            else:
                return flat_dict
            decoded: dict[Any, Any] = {}
            for key, value in data.items():
                if isinstance(value, dict):
                    decoded[key] = DocumentCodec.decode(
                        value,
                        preserve_bson_wrappers=preserve_bson_wrappers,
                    )
                    continue
                if isinstance(value, list):
                    decoded[key] = DocumentCodec._decode_list_fast(
                        value,
                        preserve_bson_wrappers=preserve_bson_wrappers,
                    )
                    continue
                decoded[key] = value
            return decoded

        if isinstance(data, list):
            return DocumentCodec._decode_list_fast(
                data,
                preserve_bson_wrappers=preserve_bson_wrappers,
            )

        return data

    @staticmethod
    def _decode_list_fast(data: list[Any], *, preserve_bson_wrappers: bool) -> list[Any]:
        if not data:
            return []
        if not any(isinstance(value, dict | list) for value in data):
            return list(data)
        decoded_items: list[Any] = []
        for value in data:
            if isinstance(value, dict | list):
                decoded_items.append(
                    DocumentCodec.decode(
                        value,
                        preserve_bson_wrappers=preserve_bson_wrappers,
                    )
                )
                continue
            decoded_items.append(value)
        return decoded_items

    @staticmethod
    def to_public(data: Any) -> Any:
        return DocumentCodec._to_public_copy_on_write(data)

    @staticmethod
    def _to_public_copy_on_write(data: Any) -> Any:
        if isinstance(data, dict):
            flat_changed = False
            flat_items: list[tuple[Any, Any]] = []
            for key, value in data.items():
                if isinstance(value, DocumentCodec._BSON_WRAPPER_TYPES):
                    flat_changed = True
                    flat_items.append((key, unwrap_bson_numeric(value)))
                    continue
                if isinstance(value, list):
                    public_list, list_changed, contains_nested = DocumentCodec._to_public_flat_list(value)
                    if contains_nested:
                        break
                    if list_changed:
                        flat_changed = True
                        flat_items.append((key, public_list))
                    else:
                        flat_items.append((key, value))
                    continue
                if isinstance(value, dict):
                    break
                flat_items.append((key, value))
            else:
                if not flat_changed:
                    return data
                return {key: value for key, value in flat_items}

            converted_items: list[tuple[Any, Any]] = []
            changed = False
            for key, value in data.items():
                public_value = DocumentCodec._to_public_copy_on_write(value)
                if public_value is not value:
                    changed = True
                converted_items.append((key, public_value))
            if not changed:
                return data
            return {key: value for key, value in converted_items}

        if isinstance(data, list):
            flat_items, flat_changed, contains_nested = DocumentCodec._to_public_flat_list(data)
            if not contains_nested:
                return flat_items if flat_changed else data

            converted_items: list[Any] = []
            changed = False
            for value in data:
                public_value = DocumentCodec._to_public_copy_on_write(value)
                if public_value is not value:
                    changed = True
                converted_items.append(public_value)
            if not changed:
                return data
            return converted_items

        return unwrap_bson_numeric(data)

    @staticmethod
    def _to_public_flat_list(data: list[Any]) -> tuple[list[Any], bool, bool]:
        converted_items: list[Any] = []
        changed = False
        for value in data:
            if isinstance(value, DocumentCodec._BSON_WRAPPER_TYPES):
                changed = True
                converted_items.append(unwrap_bson_numeric(value))
                continue
            if isinstance(value, dict | list):
                return converted_items, changed, True
            converted_items.append(value)
        return converted_items, changed, False

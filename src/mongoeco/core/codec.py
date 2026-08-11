import binascii
import datetime
import decimal
import re
import uuid

from typing import Any

from mongoeco._types.concerns import CodecOptions, UuidRepresentation


try:
    from bson.binary import Binary as BsonBinary
    from bson.code import Code as BsonCode
    from bson.dbref import DBRef as BsonDBRef
    from bson.decimal128 import Decimal128 as BsonDecimal128Public
    from bson.max_key import MaxKey as BsonMaxKey
    from bson.min_key import MinKey as BsonMinKey
    from bson.objectid import ObjectId as BsonObjectId
    from bson.regex import Regex as BsonRegex
    from bson.son import SON as BsonSON
    from bson.timestamp import Timestamp as BsonTimestamp
except Exception:  # pragma: no cover - optional dependency
    BsonBinary = None
    BsonCode = None
    BsonDBRef = None
    BsonDecimal128Public = None
    BsonMaxKey = None
    BsonMinKey = None
    BsonObjectId = None
    BsonRegex = None
    BsonSON = None
    BsonTimestamp = None

from mongoeco.core.bson_scalars import (
    BsonDecimal128,
    BsonDouble,
    BsonInt32,
    BsonInt64,
    normalize_utc_bson_datetime,
    unwrap_bson_numeric,
    validate_bson_value,
)
from mongoeco.types import (
    SON,
    UNDEFINED,
    Binary,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    Timestamp,
    UndefinedType,
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
    def to_internal(data: Any) -> Any:
        """Apply the BSON command boundary before engine evaluation."""
        return DocumentCodec.decode(DocumentCodec.encode(data))

    @staticmethod
    def encode(data: Any) -> Any:
        if BsonSON is not None and isinstance(data, BsonSON):
            validate_bson_value(data)
            return DocumentCodec._tagged_value(
                "son",
                [[key, DocumentCodec.encode(value)] for key, value in data.items()],
            )
        if isinstance(data, SON):
            validate_bson_value(data)
            return DocumentCodec._tagged_value(
                "son",
                [[key, DocumentCodec.encode(value)] for key, value in data.items()],
            )
        if isinstance(data, dict):
            validate_bson_value(data)
            encoded = {k: DocumentCodec.encode(v) for k, v in data.items()}
            if DocumentCodec._is_tagged_value(encoded):
                return DocumentCodec._tagged_value("dict", encoded)
            return encoded
        if isinstance(data, (list, tuple)):
            return [DocumentCodec.encode(v) for v in data]
        if isinstance(data, bytearray):
            data = bytes(data)
        if isinstance(data, (set, frozenset)):
            raise TypeError("set values are not BSON-serializable")

        validate_bson_value(data)

        if isinstance(data, datetime.datetime):
            data = normalize_utc_bson_datetime(data)
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

        if BsonBinary is not None and isinstance(data, BsonBinary):
            return DocumentCodec._tagged_value(
                "binary",
                {
                    "hex": binascii.hexlify(bytes(data)).decode("ascii"),
                    "subtype": data.subtype,
                },
            )

        if isinstance(data, Binary):
            return DocumentCodec._tagged_value(
                "binary",
                {
                    "hex": binascii.hexlify(bytes(data)).decode("ascii"),
                    "subtype": data.subtype,
                },
            )

        if BsonRegex is not None and isinstance(data, BsonRegex):
            flags = int(data.flags)
            rendered_flags = "".join(
                flag
                for flag, mask in (
                    ("i", re.IGNORECASE),
                    ("l", re.LOCALE),
                    ("m", re.MULTILINE),
                    ("s", re.DOTALL),
                    ("u", re.UNICODE),
                    ("x", re.VERBOSE),
                )
                if flags & mask
            )
            return DocumentCodec._tagged_value(
                "regex",
                {
                    "pattern": data.pattern,
                    "flags": rendered_flags,
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

        if BsonTimestamp is not None and isinstance(data, BsonTimestamp):
            return DocumentCodec._tagged_value(
                "timestamp",
                {"time": data.time, "inc": data.inc},
            )

        if BsonDecimal128Public is not None and isinstance(
            data, BsonDecimal128Public
        ):
            return DocumentCodec._tagged_value(
                "decimal128_public",
                str(data),
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

        if BsonDBRef is not None and isinstance(data, BsonDBRef):
            as_document = data.as_doc()
            extras = {
                key: value
                for key, value in as_document.items()
                if key not in {"$ref", "$id", "$db"}
            }
            return DocumentCodec._tagged_value(
                "dbref",
                {
                    "collection": data.collection,
                    "id": DocumentCodec.encode(data.id),
                    "database": data.database,
                    "extras": DocumentCodec.encode(extras),
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
    def apply_codec_options(
        data: Any,
        *,
        codec_options: CodecOptions | None,
    ) -> Any:
        if codec_options is None:
            return data
        return DocumentCodec._apply_codec_options_recursive(data, codec_options=codec_options)

    @staticmethod
    def _apply_codec_options_recursive(
        data: Any,
        *,
        codec_options: CodecOptions,
    ) -> Any:
        if isinstance(data, dict):
            converted = {
                key: DocumentCodec._apply_codec_options_recursive(value, codec_options=codec_options)
                for key, value in data.items()
            }
            if codec_options.document_class is dict:
                return converted
            return codec_options.document_class(converted)
        if isinstance(data, list):
            return [
                DocumentCodec._apply_codec_options_recursive(value, codec_options=codec_options)
                for value in data
            ]
        if isinstance(data, tuple):
            return tuple(
                DocumentCodec._apply_codec_options_recursive(value, codec_options=codec_options)
                for value in data
            )
        return DocumentCodec._apply_codec_scalar_options(data, codec_options=codec_options)

    @staticmethod
    def _apply_codec_scalar_options(
        value: Any,
        *,
        codec_options: CodecOptions,
    ) -> Any:
        transformed = value
        if isinstance(transformed, datetime.datetime):
            transformed = DocumentCodec._apply_datetime_codec_options(
                transformed,
                codec_options=codec_options,
            )
        elif isinstance(transformed, uuid.UUID):
            transformed = DocumentCodec._apply_uuid_codec_options(
                transformed,
                codec_options=codec_options,
            )

        for value_type, decoder in codec_options.type_registry:
            if isinstance(transformed, value_type):
                transformed = decoder(transformed)
        return transformed

    @staticmethod
    def _apply_datetime_codec_options(
        value: datetime.datetime,
        *,
        codec_options: CodecOptions,
    ) -> datetime.datetime:
        if value.tzinfo is None:
            if not codec_options.tz_aware:
                return value
            aware_utc = value.replace(tzinfo=datetime.timezone.utc)
        else:
            aware_utc = value.astimezone(datetime.timezone.utc)
            if not codec_options.tz_aware:
                return aware_utc.replace(tzinfo=None)
        target_tz = codec_options.tzinfo or datetime.timezone.utc
        return aware_utc.astimezone(target_tz)

    @staticmethod
    def _apply_uuid_codec_options(
        value: uuid.UUID,
        *,
        codec_options: CodecOptions,
    ) -> uuid.UUID | Binary:
        uuid_representation = codec_options.uuid_representation
        if uuid_representation is UuidRepresentation.STANDARD:
            return value
        if uuid_representation is UuidRepresentation.UNSPECIFIED:
            return Binary(value.bytes, subtype=4)
        return Binary(value.bytes, subtype=3)

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
    def to_pymongo(data: Any) -> Any:
        """Materialize internal BSON values as the selected PyMongo surface."""
        if BsonObjectId is None:
            return data
        if BsonCode is not None and isinstance(data, BsonCode):
            scope = data.scope
            return BsonCode(
                str(data),
                DocumentCodec.to_pymongo(scope) if scope is not None else None,
            )
        if type(data) is ObjectId:
            return BsonObjectId(data.binary)
        if type(data) is Binary:
            return BsonBinary(bytes(data), subtype=data.subtype)
        if type(data) is Decimal128:
            return BsonDecimal128Public(str(data.value))
        if type(data) is Regex:
            return BsonRegex(data.pattern, data.flags)
        if type(data) is Timestamp:
            return BsonTimestamp(data.time, data.inc)
        if type(data) is DBRef:
            extras = DocumentCodec.to_pymongo(data.extras)
            return BsonDBRef(
                data.collection,
                DocumentCodec.to_pymongo(data.id),
                data.database,
                **extras,
            )
        if type(data) is SON:
            return BsonSON(
                (key, DocumentCodec.to_pymongo(value))
                for key, value in data.items()
            )
        if isinstance(data, dict):
            return {
                key: DocumentCodec.to_pymongo(value)
                for key, value in data.items()
            }
        if isinstance(data, list):
            return [DocumentCodec.to_pymongo(value) for value in data]
        if isinstance(data, tuple):
            return tuple(DocumentCodec.to_pymongo(value) for value in data)
        return data

    @staticmethod
    def _to_public_copy_on_write(data: Any) -> Any:
        if type(data) is SON:
            return SON(
                (
                    key,
                    DocumentCodec._to_public_copy_on_write(value),
                )
                for key, value in data.items()
            )
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

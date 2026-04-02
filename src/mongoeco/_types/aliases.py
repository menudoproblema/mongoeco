import datetime
import uuid
from typing import Any, Literal

from mongoeco._types.bson import Binary, DBRef, Decimal128, ObjectId, Regex, SON, Timestamp, UndefinedType

type Document = dict[str, Any]
type Filter = dict[str, Any]
type UpdateDocument = dict[str, Any]
type UpdatePipelineStage = dict[str, Any]
type UpdatePipeline = list[UpdatePipelineStage]
type Update = UpdateDocument | UpdatePipeline
type ArrayFilters = list[Filter]
type Projection = dict[str, Any]
type BsonScalar = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | datetime.datetime
    | Binary
    | Decimal128
    | Regex
    | Timestamp
    | ObjectId
    | DBRef
    | UndefinedType
)
type BsonValue = BsonScalar | list["BsonValue"] | dict[str, "BsonValue"] | SON
type BsonBindings = dict[str, BsonValue]
type BitwiseMaskOperand = int | bytes | uuid.UUID | list[int]
type SortDirection = Literal[1, -1]
type SpecialIndexDirection = Literal["text", "hashed", "2dsphere", "2d"]
type IndexDirection = SortDirection | SpecialIndexDirection
type SortSpec = list[tuple[str, SortDirection]]
type CollationDocument = dict[str, object]
type IndexKeySpec = list[tuple[str, IndexDirection]]
type IndexDocument = dict[str, object]
type IndexInformationEntry = dict[str, object]
type IndexInformation = dict[str, IndexInformationEntry]
type SearchIndexDocument = dict[str, object]
type CollectionOptionsDocument = dict[str, object]
type DocumentScalarId = ObjectId | str | bytes | int | float | bool | None | UndefinedType
type DocumentId = DocumentScalarId | list["DocumentId"] | dict[str, "DocumentId"]
type HintSpec = str | SortSpec

from __future__ import annotations


SUPPORTED_QUERY_FIELD_OPERATORS = frozenset(
    {
        "$eq", "$cmp", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$all",
        "$size", "$mod", "$regex", "$options", "$not", "$elemMatch", "$exists", "$type",
        "$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear",
        "$geoWithin", "$geoIntersects", "$near", "$nearSphere",
    }
)

SUPPORTED_QUERY_TOP_LEVEL_OPERATORS = frozenset({"$and", "$or", "$nor", "$expr", "$jsonSchema", "$comment", "$text", "$where"})

SUPPORTED_UPDATE_OPERATORS = frozenset(
    {"$set", "$unset", "$inc", "$min", "$max", "$mul", "$bit", "$rename", "$currentDate",
     "$setOnInsert", "$push", "$addToSet", "$pull", "$pullAll", "$pop"}
)

# Argument counts belong to their expression family, never the query grammar.
CONTROL_EXPRESSION_ARGUMENTS = {
    **dict.fromkeys(("$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in"), (2, 2)),
    "$and": (0, None),
    "$or": (0, None),
    "$not": (1, 1),
    "$ifNull": (2, None),
}
SCALAR_EXPRESSION_ARGUMENTS = {"$cmp": (2, 2)}
ARRAY_ACCESS_EXPRESSION_ARGUMENTS = {
    "$size": (1, 1),
    "$first": (1, 1),
    "$last": (1, 1),
    "$arrayElemAt": (2, 2),
}

SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS = (
    frozenset(
        {
            "$literal",
            "$convert",
            "$cond",
            "$switch",
            "$abs",
            "$add",
            "$multiply",
            "$subtract",
            "$divide",
            "$mod",
            "$sum",
            "$avg",
            "$min",
            "$max",
            "$exp",
            "$ln",
            "$log",
            "$log10",
            "$pow",
            "$round",
            "$sqrt",
            "$stdDevPop",
            "$stdDevSamp",
            "$median",
            "$percentile",
            "$floor",
            "$ceil",
            "$trunc",
            "$range",
            "$slice",
            "$firstN",
            "$lastN",
            "$maxN",
            "$minN",
            "$allElementsTrue",
            "$anyElementTrue",
            "$objectToArray",
            "$zip",
            "$isArray",
            "$bitAnd",
            "$bitNot",
            "$bitOr",
            "$bitXor",
            "$bsonSize",
            "$concat",
            "$ltrim",
            "$replaceOne",
            "$replaceAll",
            "$reverseArray",
            "$rtrim",
            "$setDifference",
            "$setEquals",
            "$setIntersection",
            "$setIsSubset",
            "$strcasecmp",
            "$substr",
            "$substrBytes",
            "$substrCP",
            "$strLenBytes",
            "$strLenCP",
            "$trim",
            "$split",
            "$toBool",
            "$toDate",
            "$toDecimal",
            "$toInt",
            "$toDouble",
            "$toLong",
            "$toObjectId",
            "$toUUID",
            "$toLower",
            "$toUpper",
            "$toString",
            "$let",
            "$concatArrays",
            "$setUnion",
            "$map",
            "$filter",
            "$reduce",
            "$arrayToObject",
            "$indexOfArray",
            "$indexOfBytes",
            "$indexOfCP",
            "$regexMatch",
            "$regexFind",
            "$regexFindAll",
            "$sortArray",
            "$dateTrunc",
            "$dateAdd",
            "$dateSubtract",
            "$dateDiff",
            "$dateFromString",
            "$dateFromParts",
            "$dateToParts",
            "$dateToString",
            "$year",
            "$month",
            "$dayOfMonth",
            "$dayOfWeek",
            "$dayOfYear",
            "$hour",
            "$minute",
            "$second",
            "$millisecond",
            "$isoDayOfWeek",
            "$rand",
            "$setField",
            "$unsetField",
            "$week",
            "$isoWeek",
            "$isoWeekYear",
            "$mergeObjects",
            "$getField",
            "$isNumber",
            "$type",
            "$binarySize",
        }
    )
    | frozenset(CONTROL_EXPRESSION_ARGUMENTS)
    | frozenset(ARRAY_ACCESS_EXPRESSION_ARGUMENTS)
    | frozenset(SCALAR_EXPRESSION_ARGUMENTS)
)

SUPPORTED_AGGREGATION_STAGES = frozenset(
    {
        "$match", "$project", "$unset", "$sample", "$sort", "$skip", "$limit", "$addFields", "$set",
        "$unwind", "$group", "$bucket", "$bucketAuto", "$lookup", "$unionWith", "$replaceRoot",
        "$replaceWith", "$redact", "$facet", "$count", "$sortByCount", "$setWindowFields", "$documents",
        "$densify", "$fill", "$merge", "$geoNear", "$collStats", "$indexStats", "$currentOp", "$planCacheStats", "$listSessions",
    }
)

SUPPORTED_GROUP_ACCUMULATORS = frozenset(
    {"$sum", "$count", "$min", "$max", "$first", "$last", "$firstN", "$lastN", "$maxN", "$minN",
     "$top", "$bottom", "$topN", "$bottomN", "$avg", "$push", "$addToSet", "$mergeObjects",
     "$stdDevPop", "$stdDevSamp", "$median", "$percentile"}
)

SUPPORTED_WINDOW_ACCUMULATORS = frozenset(
    {"$sum", "$count", "$min", "$max", "$avg", "$stdDevPop", "$stdDevSamp", "$push", "$addToSet",
     "$first", "$last", "$firstN", "$lastN", "$maxN", "$minN", "$top", "$bottom", "$topN",
     "$bottomN", "$median", "$percentile", "$documentNumber", "$rank", "$denseRank", "$shift",
     "$locf", "$linearFill", "$expMovingAvg", "$derivative", "$integral"}
)

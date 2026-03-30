# Compat Catalog

## Defaults
- `mongodb_dialect`: `7.0`
- `pymongo_profile`: `4.9`
- `pymongo_auto_profile`: `auto-installed`
- `pymongo_strict_auto_profile`: `strict-auto-installed`

## Hooks
- `mongodb_dialect`: `null_query_matches_undefined`
- `pymongo_profile`: `supports_update_one_sort`

## Supported Majors
- `mongodb`: 7, 8
- `pymongo`: 4

## MongoDB Dialects
### `7.0`
- `server_version`: `7.0`
- `label`: `MongoDB 7.0`
- `aliases`: `7`, `7.0`
- `behavior_flags`: `{'null_query_matches_undefined': True}`
- `policy_spec`: `{'null_query_matches_undefined': True, 'expression_truthiness': 'mongo-default', 'projection_flag_mode': 'bool-or-binary-int', 'update_path_sort_mode': 'numeric-then-lex', 'equality_mode': 'bson-structural', 'comparison_mode': 'bson-total-order'}`
- `capabilities`: `query.null_matches_undefined`
- `query_field_operators`: `$all`, `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$cmp`, `$elemMatch`, `$eq`, `$exists`, `$gt`, `$gte`, `$in`, `$lt`, `$lte`, `$mod`, `$ne`, `$nin`, `$not`, `$options`, `$regex`, `$size`, `$type`
- `query_top_level_operators`: `$and`, `$expr`, `$jsonSchema`, `$nor`, `$or`
- `update_operators`: `$addToSet`, `$bit`, `$currentDate`, `$inc`, `$max`, `$min`, `$mul`, `$pop`, `$pull`, `$pullAll`, `$push`, `$rename`, `$set`, `$setOnInsert`, `$unset`
- `aggregation_expression_operators`: `$abs`, `$add`, `$allElementsTrue`, `$and`, `$anyElementTrue`, `$arrayElemAt`, `$arrayToObject`, `$binarySize`, `$bitAnd`, `$bitNot`, `$bitOr`, `$bitXor`, `$bsonSize`, `$ceil`, `$cmp`, `$concat`, `$concatArrays`, `$cond`, `$convert`, `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$divide`, `$eq`, `$exp`, `$filter`, `$first`, `$firstN`, `$floor`, `$getField`, `$gt`, `$gte`, `$hour`, `$ifNull`, `$in`, `$indexOfArray`, `$indexOfBytes`, `$indexOfCP`, `$isArray`, `$isNumber`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$lastN`, `$let`, `$literal`, `$ln`, `$log`, `$log10`, `$lt`, `$lte`, `$ltrim`, `$map`, `$maxN`, `$median`, `$mergeObjects`, `$millisecond`, `$minN`, `$minute`, `$mod`, `$month`, `$multiply`, `$ne`, `$objectToArray`, `$or`, `$percentile`, `$pow`, `$rand`, `$range`, `$reduce`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$reverseArray`, `$round`, `$rtrim`, `$second`, `$setDifference`, `$setEquals`, `$setField`, `$setIntersection`, `$setIsSubset`, `$setUnion`, `$size`, `$slice`, `$sortArray`, `$split`, `$sqrt`, `$stdDevPop`, `$stdDevSamp`, `$strLenBytes`, `$strLenCP`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$subtract`, `$switch`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toLower`, `$toObjectId`, `$toString`, `$toUUID`, `$toUpper`, `$trim`, `$trunc`, `$type`, `$unsetField`, `$week`, `$year`, `$zip`
- `aggregation_stages`: `$addFields`, `$bucket`, `$bucketAuto`, `$count`, `$documents`, `$facet`, `$group`, `$limit`, `$lookup`, `$match`, `$project`, `$replaceRoot`, `$replaceWith`, `$sample`, `$set`, `$setWindowFields`, `$skip`, `$sort`, `$sortByCount`, `$unionWith`, `$unset`, `$unwind`
- `group_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$mergeObjects`, `$min`, `$minN`, `$percentile`, `$push`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `window_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$denseRank`, `$documentNumber`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$min`, `$minN`, `$percentile`, `$push`, `$rank`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`

### `8.0`
- `server_version`: `8.0`
- `label`: `MongoDB 8.0`
- `aliases`: `8`, `8.0`
- `behavior_flags`: `{'null_query_matches_undefined': False}`
- `policy_spec`: `{'null_query_matches_undefined': False, 'expression_truthiness': 'mongo-default', 'projection_flag_mode': 'bool-or-binary-int', 'update_path_sort_mode': 'numeric-then-lex', 'equality_mode': 'bson-structural', 'comparison_mode': 'bson-total-order'}`
- `capabilities`: _empty_
- `query_field_operators`: `$all`, `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$cmp`, `$elemMatch`, `$eq`, `$exists`, `$gt`, `$gte`, `$in`, `$lt`, `$lte`, `$mod`, `$ne`, `$nin`, `$not`, `$options`, `$regex`, `$size`, `$type`
- `query_top_level_operators`: `$and`, `$expr`, `$jsonSchema`, `$nor`, `$or`
- `update_operators`: `$addToSet`, `$bit`, `$currentDate`, `$inc`, `$max`, `$min`, `$mul`, `$pop`, `$pull`, `$pullAll`, `$push`, `$rename`, `$set`, `$setOnInsert`, `$unset`
- `aggregation_expression_operators`: `$abs`, `$add`, `$allElementsTrue`, `$and`, `$anyElementTrue`, `$arrayElemAt`, `$arrayToObject`, `$binarySize`, `$bitAnd`, `$bitNot`, `$bitOr`, `$bitXor`, `$bsonSize`, `$ceil`, `$cmp`, `$concat`, `$concatArrays`, `$cond`, `$convert`, `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$divide`, `$eq`, `$exp`, `$filter`, `$first`, `$firstN`, `$floor`, `$getField`, `$gt`, `$gte`, `$hour`, `$ifNull`, `$in`, `$indexOfArray`, `$indexOfBytes`, `$indexOfCP`, `$isArray`, `$isNumber`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$lastN`, `$let`, `$literal`, `$ln`, `$log`, `$log10`, `$lt`, `$lte`, `$ltrim`, `$map`, `$maxN`, `$median`, `$mergeObjects`, `$millisecond`, `$minN`, `$minute`, `$mod`, `$month`, `$multiply`, `$ne`, `$objectToArray`, `$or`, `$percentile`, `$pow`, `$rand`, `$range`, `$reduce`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$reverseArray`, `$round`, `$rtrim`, `$second`, `$setDifference`, `$setEquals`, `$setField`, `$setIntersection`, `$setIsSubset`, `$setUnion`, `$size`, `$slice`, `$sortArray`, `$split`, `$sqrt`, `$stdDevPop`, `$stdDevSamp`, `$strLenBytes`, `$strLenCP`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$subtract`, `$switch`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toLower`, `$toObjectId`, `$toString`, `$toUUID`, `$toUpper`, `$trim`, `$trunc`, `$type`, `$unsetField`, `$week`, `$year`, `$zip`
- `aggregation_stages`: `$addFields`, `$bucket`, `$bucketAuto`, `$count`, `$documents`, `$facet`, `$group`, `$limit`, `$lookup`, `$match`, `$project`, `$replaceRoot`, `$replaceWith`, `$sample`, `$set`, `$setWindowFields`, `$skip`, `$sort`, `$sortByCount`, `$unionWith`, `$unset`, `$unwind`
- `group_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$mergeObjects`, `$min`, `$minN`, `$percentile`, `$push`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `window_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$denseRank`, `$documentNumber`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$min`, `$minN`, `$percentile`, `$push`, `$rank`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`

## PyMongo Profiles
### `4.9`
- `driver_series`: `4.x`
- `label`: `PyMongo 4.9`
- `aliases`: `4`, `4.9`
- `behavior_flags`: `{'supports_update_one_sort': False}`
- `capabilities`: _empty_

### `4.11`
- `driver_series`: `4.x`
- `label`: `PyMongo 4.11`
- `aliases`: `4.11`
- `behavior_flags`: `{'supports_update_one_sort': True}`
- `capabilities`: `update_one.sort`

### `4.13`
- `driver_series`: `4.x`
- `label`: `PyMongo 4.13`
- `aliases`: `4.13`
- `behavior_flags`: `{'supports_update_one_sort': True}`
- `capabilities`: `update_one.sort`

## Operation Options
### `find`
- `hint`: `status`="effective", `note`="Validated against existing indexes and applied to read planning/explain where engines can honor it."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata and surfaced by explain()."
- `max_time_ms`: `status`="effective", `note`="Enforced as a local deadline during read execution and explain()."
- `batch_size`: `status`="effective", `note`="Async and sync find cursors now fetch local batches before yielding results, even though engines remain in-process."

### `count_documents`
- `hint`: `status`="effective", `note`="Applied through the underlying find() path used to count matching documents."
- `comment`: `status`="effective", `note`="Propagated through the underlying read path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Enforced through the underlying find() path used to count documents."

### `distinct`
- `hint`: `status`="effective", `note`="Applied through the underlying find() path used to enumerate distinct values."
- `comment`: `status`="effective", `note`="Propagated through the underlying read path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Enforced through the underlying find() path used to enumerate distinct values."

### `estimated_document_count`
- `comment`: `status`="effective", `note`="Propagated through the underlying full-collection read path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Enforced through the underlying full-collection read path."

### `aggregate`
- `hint`: `status`="effective", `note`="Applied through the pushdown find() path used by aggregate() and surfaced in explain()."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata and propagated through aggregate explain/materialization."
- `max_time_ms`: `status`="effective", `note`="Applied to referenced collection loads, pushdown reads and final pipeline materialization."
- `batch_size`: `status`="effective", `note`="Positive batch sizes trigger chunked execution for streamable aggregate pipelines; global stages still fall back to full materialization."
- `allow_disk_use`: `status`="effective", `note`="Controls whether the aggregation cursor may use the configured spill-to-disk policy for blocking stages."
- `let`: `status`="effective", `note`="Propagated into aggregate expression evaluation and subpipelines."

### `update_one`
- `array_filters`: `status`="effective", `note`="Applied during update execution for supported filtered positional paths."
- `hint`: `status`="effective", `note`="Applied through hinted document selection before single-document update execution."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write operation."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."
- `sort`: `status`="effective", `note`="Implemented with profile-aware validation since PyMongo 4.11."

### `update_many`
- `array_filters`: `status`="effective", `note`="Applied during per-document update execution for supported filtered positional paths."
- `hint`: `status`="effective", `note`="Applied through hinted _id preselection before per-document updates."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write operation."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."

### `replace_one`
- `hint`: `status`="effective", `note`="Applied through hinted document selection before replacement."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write operation."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."
- `sort`: `status`="effective", `note`="Implemented with profile-aware validation since PyMongo 4.11."

### `delete_one`
- `hint`: `status`="effective", `note`="Applied through hinted document selection before delete."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write operation."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."

### `delete_many`
- `hint`: `status`="effective", `note`="Applied through hinted _id preselection before per-document deletes."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write operation."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."

### `find_one_and_update`
- `array_filters`: `status`="effective", `note`="Propagated to the underlying update_one() execution for supported filtered positional paths."
- `hint`: `status`="effective", `note`="Applied through hinted document selection and post-update fetch."
- `comment`: `status`="effective", `note`="Propagated through the underlying read selection path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Propagated through the underlying read selection path and enforced there."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."
- `sort`: `status`="effective", `note`="Implemented through update_one()/find semantics with profile-aware validation."

### `find_one_and_replace`
- `hint`: `status`="effective", `note`="Applied through hinted document selection and post-replacement fetch."
- `comment`: `status`="effective", `note`="Propagated through the underlying read selection path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Propagated through the underlying read selection path and enforced there."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."
- `sort`: `status`="effective", `note`="Implemented through replace_one()/find semantics with profile-aware validation."

### `find_one_and_delete`
- `sort`: `status`="effective", `note`="Implemented through find() selection semantics before delete."
- `hint`: `status`="effective", `note`="Applied through hinted document selection before delete."
- `comment`: `status`="effective", `note`="Propagated through the underlying read selection path and session metadata."
- `max_time_ms`: `status`="effective", `note`="Propagated through the underlying read selection path and enforced there."
- `let`: `status`="effective", `note`="Command-level let variables are available through $expr in write filters and selection paths."

### `bulk_write`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the batch write operation."
- `let`: `status`="effective", `note`="Command-level let variables flow into per-operation write filters through $expr when requests do not override them."

### `list_indexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."

### `create_index`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."
- `max_time_ms`: `status`="effective", `note`="Enforced as a local deadline during index build and multikey backfill."

### `create_indexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."
- `max_time_ms`: `status`="effective", `note`="Enforced as a local deadline across the whole index batch."

### `drop_index`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."

### `drop_indexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."

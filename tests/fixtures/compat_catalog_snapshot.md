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
- `query_field_operators`: `$all`, `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$cmp`, `$elemMatch`, `$eq`, `$exists`, `$geoIntersects`, `$geoWithin`, `$gt`, `$gte`, `$in`, `$lt`, `$lte`, `$mod`, `$ne`, `$near`, `$nearSphere`, `$nin`, `$not`, `$options`, `$regex`, `$size`, `$type`
- `query_top_level_operators`: `$and`, `$comment`, `$expr`, `$jsonSchema`, `$nor`, `$or`, `$text`, `$where`
- `update_operators`: `$addToSet`, `$bit`, `$currentDate`, `$inc`, `$max`, `$min`, `$mul`, `$pop`, `$pull`, `$pullAll`, `$push`, `$rename`, `$set`, `$setOnInsert`, `$unset`
- `aggregation_expression_operators`: `$abs`, `$add`, `$all`, `$allElementsTrue`, `$and`, `$anyElementTrue`, `$arrayElemAt`, `$arrayToObject`, `$binarySize`, `$bitAnd`, `$bitNot`, `$bitOr`, `$bitXor`, `$bsonSize`, `$ceil`, `$cmp`, `$concat`, `$concatArrays`, `$cond`, `$convert`, `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$divide`, `$elemMatch`, `$eq`, `$exists`, `$exp`, `$filter`, `$first`, `$firstN`, `$floor`, `$getField`, `$gt`, `$gte`, `$hour`, `$ifNull`, `$in`, `$indexOfArray`, `$indexOfBytes`, `$indexOfCP`, `$isArray`, `$isNumber`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$lastN`, `$let`, `$literal`, `$ln`, `$log`, `$log10`, `$lt`, `$lte`, `$ltrim`, `$map`, `$maxN`, `$median`, `$mergeObjects`, `$millisecond`, `$minN`, `$minute`, `$mod`, `$month`, `$multiply`, `$ne`, `$nin`, `$objectToArray`, `$or`, `$percentile`, `$pow`, `$rand`, `$range`, `$reduce`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$reverseArray`, `$round`, `$rtrim`, `$second`, `$setDifference`, `$setEquals`, `$setField`, `$setIntersection`, `$setIsSubset`, `$setUnion`, `$size`, `$slice`, `$sortArray`, `$split`, `$sqrt`, `$stdDevPop`, `$stdDevSamp`, `$strLenBytes`, `$strLenCP`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$subtract`, `$switch`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toLower`, `$toObjectId`, `$toString`, `$toUUID`, `$toUpper`, `$trim`, `$trunc`, `$type`, `$unsetField`, `$week`, `$year`, `$zip`
- `aggregation_stages`: `$addFields`, `$bucket`, `$bucketAuto`, `$collStats`, `$count`, `$densify`, `$documents`, `$facet`, `$fill`, `$geoNear`, `$group`, `$indexStats`, `$limit`, `$lookup`, `$match`, `$merge`, `$project`, `$redact`, `$replaceRoot`, `$replaceWith`, `$sample`, `$set`, `$setWindowFields`, `$skip`, `$sort`, `$sortByCount`, `$unionWith`, `$unset`, `$unwind`
- `group_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$mergeObjects`, `$min`, `$minN`, `$percentile`, `$push`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `window_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$denseRank`, `$derivative`, `$documentNumber`, `$expMovingAvg`, `$first`, `$firstN`, `$integral`, `$last`, `$lastN`, `$linearFill`, `$locf`, `$max`, `$maxN`, `$median`, `$min`, `$minN`, `$percentile`, `$push`, `$rank`, `$shift`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`

### `8.0`
- `server_version`: `8.0`
- `label`: `MongoDB 8.0`
- `aliases`: `8`, `8.0`
- `behavior_flags`: `{'null_query_matches_undefined': False}`
- `policy_spec`: `{'null_query_matches_undefined': False, 'expression_truthiness': 'mongo-default', 'projection_flag_mode': 'bool-or-binary-int', 'update_path_sort_mode': 'numeric-then-lex', 'equality_mode': 'bson-structural', 'comparison_mode': 'bson-total-order'}`
- `capabilities`: _empty_
- `query_field_operators`: `$all`, `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$cmp`, `$elemMatch`, `$eq`, `$exists`, `$geoIntersects`, `$geoWithin`, `$gt`, `$gte`, `$in`, `$lt`, `$lte`, `$mod`, `$ne`, `$near`, `$nearSphere`, `$nin`, `$not`, `$options`, `$regex`, `$size`, `$type`
- `query_top_level_operators`: `$and`, `$comment`, `$expr`, `$jsonSchema`, `$nor`, `$or`, `$text`, `$where`
- `update_operators`: `$addToSet`, `$bit`, `$currentDate`, `$inc`, `$max`, `$min`, `$mul`, `$pop`, `$pull`, `$pullAll`, `$push`, `$rename`, `$set`, `$setOnInsert`, `$unset`
- `aggregation_expression_operators`: `$abs`, `$add`, `$all`, `$allElementsTrue`, `$and`, `$anyElementTrue`, `$arrayElemAt`, `$arrayToObject`, `$binarySize`, `$bitAnd`, `$bitNot`, `$bitOr`, `$bitXor`, `$bsonSize`, `$ceil`, `$cmp`, `$concat`, `$concatArrays`, `$cond`, `$convert`, `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$divide`, `$elemMatch`, `$eq`, `$exists`, `$exp`, `$filter`, `$first`, `$firstN`, `$floor`, `$getField`, `$gt`, `$gte`, `$hour`, `$ifNull`, `$in`, `$indexOfArray`, `$indexOfBytes`, `$indexOfCP`, `$isArray`, `$isNumber`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$lastN`, `$let`, `$literal`, `$ln`, `$log`, `$log10`, `$lt`, `$lte`, `$ltrim`, `$map`, `$maxN`, `$median`, `$mergeObjects`, `$millisecond`, `$minN`, `$minute`, `$mod`, `$month`, `$multiply`, `$ne`, `$nin`, `$objectToArray`, `$or`, `$percentile`, `$pow`, `$rand`, `$range`, `$reduce`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$reverseArray`, `$round`, `$rtrim`, `$second`, `$setDifference`, `$setEquals`, `$setField`, `$setIntersection`, `$setIsSubset`, `$setUnion`, `$size`, `$slice`, `$sortArray`, `$split`, `$sqrt`, `$stdDevPop`, `$stdDevSamp`, `$strLenBytes`, `$strLenCP`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$subtract`, `$switch`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toLower`, `$toObjectId`, `$toString`, `$toUUID`, `$toUpper`, `$trim`, `$trunc`, `$type`, `$unsetField`, `$week`, `$year`, `$zip`
- `aggregation_stages`: `$addFields`, `$bucket`, `$bucketAuto`, `$collStats`, `$count`, `$densify`, `$documents`, `$facet`, `$fill`, `$geoNear`, `$group`, `$indexStats`, `$limit`, `$lookup`, `$match`, `$merge`, `$project`, `$redact`, `$replaceRoot`, `$replaceWith`, `$sample`, `$set`, `$setWindowFields`, `$skip`, `$sort`, `$sortByCount`, `$unionWith`, `$unset`, `$unwind`
- `group_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$mergeObjects`, `$min`, `$minN`, `$percentile`, `$push`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `window_accumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$denseRank`, `$derivative`, `$documentNumber`, `$expMovingAvg`, `$first`, `$firstN`, `$integral`, `$last`, `$lastN`, `$linearFill`, `$locf`, `$max`, `$maxN`, `$median`, `$min`, `$minN`, `$percentile`, `$push`, `$rank`, `$shift`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`

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

## Database Commands
### `aggregate`
- `family`: `admin_read`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `allowDiskUse`, `batchSize`, `comment`, `hint`, `let`, `maxTimeMS`
- `note`: `Compiled through database admin routing and exposed by both database.command(...) and the local wire passthrough.`

### `buildInfo`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local metadata sourced from the dialect/runtime descriptor.`

### `collStats`
- `family`: `admin_stats`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: `scale`
- `note`: `Served from local collection stats snapshots.`

### `connectionStatus`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: `showPrivileges`
- `note`: `Static local auth/runtime shape, with wire auth info patched from the active connection.`

### `count`
- `family`: `admin_read`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `comment`, `hint`, `limit`, `maxTimeMS`, `query`, `skip`
- `note`: `Compiled through the same find-selection path used by direct count command routing.`

### `create`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Namespace administration routed through database admin services.`

### `createIndexes`
- `family`: `admin_index`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`, `maxTimeMS`
- `note`: `Index administration routed through local collection/index services, including local-only per-index metadata such as hidden indexes.`

### `currentOp`
- `family`: `admin_control`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`
- `note`: `Exposes the local active-operation registry for embedded-runtime API and wire parity; no distributed semantics are attempted.`

### `dbHash`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `collections`, `comment`
- `note`: `Computes stable local collection hashes for embedded-runtime verification across documents, indexes and collection options.`

### `dbStats`
- `family`: `admin_stats`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: `scale`
- `note`: `Served from local database stats snapshots.`

### `delete`
- `family`: `admin_write`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `comment`, `let`, `ordered`
- `note`: `Write command orchestration routes each delete spec through the same write semantics as the public API.`

### `distinct`
- `family`: `admin_read`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `comment`, `hint`, `maxTimeMS`, `query`
- `note`: `Compiled through the same read-selection path used by direct distinct execution.`

### `drop`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Namespace administration routed through database admin services.`

### `dropDatabase`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Implemented as local namespace lifecycle cleanup plus runtime invalidation.`

### `dropIndexes`
- `family`: `admin_index`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`
- `note`: `Index administration routed through local collection/index services.`

### `explain`
- `family`: `admin_explain`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`, `maxTimeMS`, `verbosity`
- `note`: `Delegates to routed find/aggregate/update/delete/count/distinct/findAndModify explain builders and preserves the explained command shape.`

### `find`
- `family`: `admin_read`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `batchSize`, `comment`, `filter`, `hint`, `let`, `limit`, `maxTimeMS`, `projection`, `skip`, `sort`
- `note`: `Compiled through the same find operation path as the public collection surface.`

### `findAndModify`
- `family`: `admin_find_and_modify`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `arrayFilters`, `bypassDocumentValidation`, `comment`, `hint`, `let`, `maxTimeMS`, `sort`
- `note`: `Routes through explicit find-and-modify orchestration shared by database.command(...) and wire passthrough.`

### `getCmdLineOpts`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local process metadata.`

### `hello`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local handshake metadata.`

### `hostInfo`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local host/process metadata.`

### `insert`
- `family`: `admin_write`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `bypassDocumentValidation`, `comment`, `ordered`
- `note`: `Write command orchestration routes each insert batch through the same collection semantics as the public API.`

### `isMaster`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Legacy handshake alias sharing the local hello metadata source of truth.`

### `ismaster`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Legacy handshake alias sharing the local hello metadata source of truth.`

### `listCollections`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `authorizedCollections`, `comment`, `filter`, `nameOnly`
- `note`: `Namespace listing routed through local snapshots with filter/nameOnly support.`

### `listCommands`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static inventory of the supported raw command surface, including admin family, wire availability and explain availability metadata.`

### `listDatabases`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`, `filter`, `nameOnly`
- `note`: `Database listing routed through local snapshots with filter/nameOnly support.`

### `listIndexes`
- `family`: `admin_index`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`
- `note`: `Index metadata surfaced through local collection/index services.`

### `killOp`
- `family`: `admin_control`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `comment`, `op`
- `note`: `Best-effort local cancellation over operations registered as killable in the embedded runtime; no remote or cluster cancellation exists.`

### `ping`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local health/availability response.`

### `profile`
- `family`: `admin_control`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: `slowms`
- `note`: `Local profiling control and introspection routed through engine profiling support, including current level and recorded entry counts.`

### `renameCollection`
- `family`: `admin_namespace`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Namespace administration routed through database admin services.`

### `serverStatus`
- `family`: `admin_status`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local runtime metadata enriched with engine information, embedded-runtime counters and profiling summary.`

### `update`
- `family`: `admin_write`
- `supports_wire`: `True`
- `supports_explain`: `True`
- `supports_comment`: `True`
- `supported_options`: `arrayFilters`, `bypassDocumentValidation`, `comment`, `let`, `ordered`
- `note`: `Write command orchestration routes each update spec through the same write semantics as the public API.`

### `validate`
- `family`: `admin_validate`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `True`
- `supported_options`: `background`, `comment`, `full`, `scandata`
- `note`: `Validation snapshots routed through local collection validation support.`

### `whatsmyuri`
- `family`: `admin_introspection`
- `supports_wire`: `True`
- `supports_explain`: `False`
- `supports_comment`: `False`
- `supported_options`: _empty_
- `note`: `Static local connection metadata.`

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

## Database Command Options
### `aggregate`
- `hint`: `status`="effective", `note`="Propagated through command routing into aggregate pushdown/explain."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata and surfaced through explain/materialization."
- `maxTimeMS`: `status`="effective", `note`="Enforced against local aggregate execution and explain paths."
- `allowDiskUse`: `status`="effective", `note`="Applied to blocking aggregate stages through the same local spill policy as the public API."
- `let`: `status`="effective", `note`="Propagated into aggregate expression evaluation and subpipelines."
- `batchSize`: `status`="effective", `note`="Materialized into the command cursor surface for streamable pipelines."

### `collStats`
- `scale`: `status`="effective", `note`="Applied to size-oriented metrics in the local collection stats snapshot."

### `connectionStatus`
- `showPrivileges`: `status`="effective", `note`="Controls whether the local auth status document includes the privileges array."

### `count`
- `query`: `status`="effective", `note`="Compiled into the same local find-selection path used to execute the count command."
- `skip`: `status`="effective", `note`="Applied before materializing the local count result."
- `limit`: `status`="effective", `note`="Applied before materializing the local count result."
- `hint`: `status`="effective", `note`="Applied through the compiled selection path used by the count command."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the command execution."
- `maxTimeMS`: `status`="effective", `note`="Enforced through the compiled selection path used by the count command."

### `createIndexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."
- `maxTimeMS`: `status`="effective", `note`="Enforced as a local deadline across the command index batch."

### `currentOp`
- `comment`: `status`="effective", `note`="Accepted for command parity and recorded in local admin profiling metadata when supplied."

### `dbHash`
- `collections`: `status`="effective", `note`="Limits hashing to the selected local collections while preserving a stable collection-order contract."
- `comment`: `status`="effective", `note`="Accepted for command parity and recorded in command profiling metadata."

### `dbStats`
- `scale`: `status`="effective", `note`="Applied to size-oriented metrics in the local database stats snapshot."

### `delete`
- `ordered`: `status`="effective", `note`="Controls short-circuiting when one delete specification fails."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write command."
- `let`: `status`="effective", `note`="Propagated into per-delete filters through $expr when supplied on individual specs."

### `distinct`
- `query`: `status`="effective", `note`="Compiled into the same local selection path used to enumerate distinct values."
- `hint`: `status`="effective", `note`="Applied through the underlying read selection path used by the command."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the command execution."
- `maxTimeMS`: `status`="effective", `note`="Enforced through the underlying distinct selection path."

### `dropIndexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."

### `explain`
- `verbosity`: `status`="effective", `note`="Accepted and surfaced in explain responses for supported routed commands, including find/aggregate/update/delete/count/distinct/findAndModify."
- `comment`: `status`="effective", `note`="Propagated into the explained command where that command supports comment."
- `maxTimeMS`: `status`="effective", `note`="Propagated into the explained command where that command supports maxTimeMS."

### `find`
- `filter`: `status`="effective", `note`="Compiled into the same local find operation shape as the collection API."
- `projection`: `status`="effective", `note`="Applied to command cursor materialization using the same projection semantics as the collection API."
- `sort`: `status`="effective", `note`="Applied to command read planning and result materialization."
- `skip`: `status`="effective", `note`="Applied before command cursor materialization."
- `limit`: `status`="effective", `note`="Applied before command cursor materialization."
- `hint`: `status`="effective", `note`="Applied to command read planning and surfaced in explain."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the command execution."
- `maxTimeMS`: `status`="effective", `note`="Enforced during command read execution and explain."
- `batchSize`: `status`="effective", `note`="Materialized into the command cursor surface."
- `let`: `status`="effective", `note`="Propagated into command-level $expr evaluation."

### `findAndModify`
- `arrayFilters`: `status`="effective", `note`="Applied through the underlying write path for supported filtered positional updates."
- `hint`: `status`="effective", `note`="Applied through hinted selection and post-write fetch."
- `maxTimeMS`: `status`="effective", `note`="Propagated through the underlying selection path and enforced there."
- `let`: `status`="effective", `note`="Propagated into command-level $expr evaluation for the write filter."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the command execution."
- `bypassDocumentValidation`: `status`="effective", `note`="Validated and propagated through replacement/update command routing."
- `sort`: `status`="effective", `note`="Applied through the underlying find-and-modify selection path."

### `insert`
- `ordered`: `status`="effective", `note`="Controls short-circuiting when one insert document fails."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write command."
- `bypassDocumentValidation`: `status`="effective", `note`="Validated and propagated through command routing."

### `listCollections`
- `filter`: `status`="effective", `note`="Applied to the local namespace snapshot before cursor materialization."
- `nameOnly`: `status`="effective", `note`="Controls the fields exposed by the listCollections cursor."
- `authorizedCollections`: `status`="effective", `note`="Accepted for wire/API parity and preserved in the normalized command options."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the admin read command."

### `listDatabases`
- `filter`: `status`="effective", `note`="Applied to the local database snapshot before materialization."
- `nameOnly`: `status`="effective", `note`="Controls the fields exposed by the listDatabases response."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the admin read command."

### `listIndexes`
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for index administration."

### `killOp`
- `op`: `status`="effective", `note`="Identifies the locally registered operation to cancel on a best-effort basis."
- `comment`: `status`="effective", `note`="Accepted for command parity and recorded in local admin profiling metadata when supplied."

### `profile`
- `slowms`: `status`="effective", `note`="Propagated through the local profiling control path when a profiling level update is requested; status queries also surface current level and entry counts."

### `update`
- `ordered`: `status`="effective", `note`="Controls short-circuiting when one update specification fails."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the write command."
- `let`: `status`="effective", `note`="Propagated into per-update filters through $expr when supplied on individual specs."
- `bypassDocumentValidation`: `status`="effective", `note`="Validated and propagated through replacement/update command routing."
- `arrayFilters`: `status`="effective", `note`="Applied through per-update write execution for supported filtered positional paths."

### `validate`
- `scandata`: `status`="effective", `note`="Controls whether storage-engine level scan metadata is requested in the validation snapshot."
- `full`: `status`="effective", `note`="Controls whether the validation snapshot requests the expanded pass."
- `background`: `status`="effective", `note`="Validated and surfaced in the validation snapshot contract."
- `comment`: `status`="effective", `note`="Recorded in engine session metadata for the validation command."

## CXP
- `interface`: `database/mongodb`
### Profiles
#### `mongodb-core`
- `description`: `Reusable profile for providers that expose the MongoDB core set.`
- `recommendedFor`: `core-tests`, `general-resources`
#### `mongodb-text-search`
- `description`: `Reusable profile for tests and resources that only need text search.`
- `recommendedFor`: `text-search-tests`, `search-without-vector-search`
#### `mongodb-search`
- `description`: `Reusable profile for tests and resources with search and vector search.`
- `recommendedFor`: `full-search-tests`, `search-with-vector-search`
#### `mongodb-platform`
- `description`: `Reusable profile for MongoDB runtimes with platform surface.`
- `recommendedFor`: `platform-tests`, `runtime-conformance`
#### `mongodb-aggregate-rich`
- `description`: `Reusable profile for tests that need a richer aggregation subset.`
- `recommendedFor`: `aggregation-rich-tests`, `subset-sensitive-tooling`
### `read`
- `description`: `Read-only collection operations.`
- `tiers`: `core`, `search`, `platform`
- `operations`: `find`, `find_one`, `count_documents`, `estimated_document_count`, `distinct`
- `embedded`: `True`
- `sync`: `True`
- `async`: `True`
- `queryFieldOperators`: `$all`, `$bitsAllClear`, `$bitsAllSet`, `$bitsAnyClear`, `$bitsAnySet`, `$cmp`, `$elemMatch`, `$eq`, `$exists`, `$geoIntersects`, `$geoWithin`, `$gt`, `$gte`, `$in`, `$lt`, `$lte`, `$mod`, `$ne`, `$near`, `$nearSphere`, `$nin`, `$not`, `$options`, `$regex`, `$size`, `$type`
- `queryTopLevelOperators`: `$and`, `$comment`, `$expr`, `$jsonSchema`, `$nor`, `$or`, `$text`, `$where`
- `operationMetadata`: `{'find': {'supportedOptions': ['batch_size', 'comment', 'hint', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsProjection': True, 'acceptsSort': True, 'acceptsSkip': True, 'acceptsLimit': True, 'acceptsBatchSize': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsMaxTimeMs': True, 'acceptsLet': True, 'acceptsCollation': True}, 'find_one': {'supportedOptions': [], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'document', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsProjection': True, 'acceptsSort': False, 'acceptsSkip': False, 'acceptsLimit': False, 'acceptsBatchSize': False, 'acceptsHint': False, 'acceptsComment': False, 'acceptsMaxTimeMs': False, 'acceptsLet': False, 'acceptsCollation': True}, 'count_documents': {'supportedOptions': ['comment', 'hint', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'count', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsMaxTimeMs': True, 'acceptsCollation': True}, 'estimated_document_count': {'supportedOptions': ['comment', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'count', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': False, 'acceptsComment': True, 'acceptsMaxTimeMs': True}, 'distinct': {'supportedOptions': ['comment', 'hint', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'array', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsFieldPath': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsMaxTimeMs': True, 'acceptsCollation': True}}`

### `write`
- `description`: `Mutation operations over collections.`
- `tiers`: `core`, `search`, `platform`
- `operations`: `insert_one`, `insert_many`, `update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`, `bulk_write`
- `embedded`: `True`
- `sync`: `True`
- `async`: `True`
- `updateOperators`: `$addToSet`, `$bit`, `$currentDate`, `$inc`, `$max`, `$min`, `$mul`, `$pop`, `$pull`, `$pullAll`, `$push`, `$rename`, `$set`, `$setOnInsert`, `$unset`
- `supportsPipelineUpdate`: `True`
- `operationMetadata`: `{'insert_one': {'supportedOptions': [], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'insert_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsDocument': True, 'acceptsComment': False}, 'insert_many': {'supportedOptions': [], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'insert_many_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsDocuments': True, 'supportsOrderedExecution': True, 'acceptsComment': False, 'acceptsOrderedExecution': True}, 'update_one': {'supportedOptions': ['array_filters', 'comment', 'hint', 'let', 'sort'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'update_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsUpdateDocument': True, 'supportsUpsert': True, 'acceptsSort': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsCollation': True, 'acceptsArrayFilters': True, 'supportsPipelineUpdate': True, 'supportedUpdateOperators': ['$addToSet', '$bit', '$currentDate', '$inc', '$max', '$min', '$mul', '$pop', '$pull', '$pullAll', '$push', '$rename', '$set', '$setOnInsert', '$unset']}, 'update_many': {'supportedOptions': ['array_filters', 'comment', 'hint', 'let'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'update_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsUpdateDocument': True, 'supportsUpsert': True, 'acceptsSort': False, 'acceptsHint': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsCollation': True, 'acceptsArrayFilters': True, 'supportsPipelineUpdate': True, 'supportedUpdateOperators': ['$addToSet', '$bit', '$currentDate', '$inc', '$max', '$min', '$mul', '$pop', '$pull', '$pullAll', '$push', '$rename', '$set', '$setOnInsert', '$unset']}, 'replace_one': {'supportedOptions': ['comment', 'hint', 'let', 'sort'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'update_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsReplacementDocument': True, 'supportsUpsert': True, 'acceptsSort': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsCollation': True, 'supportsReplacementDocument': True}, 'delete_one': {'supportedOptions': ['comment', 'hint', 'let'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'delete_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsCollation': True}, 'delete_many': {'supportedOptions': ['comment', 'hint', 'let'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'delete_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsFilter': True, 'acceptsHint': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsCollation': True}, 'bulk_write': {'supportedOptions': ['comment', 'let'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': False, 'resultType': 'bulk_write_result', 'collectionScoped': True, 'supportsSession': True, 'acceptsWriteModels': True, 'supportsOrderedExecution': True, 'acceptsComment': True, 'acceptsLet': True, 'acceptsOrderedExecution': True}}`

### `transactions`
- `description`: `Client sessions and transaction orchestration.`
- `tiers`: `platform`
- `operations`: `start_session`, `with_transaction`
- `embedded`: `True`
- `sync`: `True`
- `async`: `True`
- `distributed`: `False`
- `mode`: `local`

### `change_streams`
- `description`: `Observe collection or database changes.`
- `tiers`: `platform`
- `operations`: `watch`
- `implementation`: `local`
- `distributed`: `False`
- `persistent`: `False`
- `resumable`: `True`
- `resumableAcrossClientRestarts`: `False`
- `resumableAcrossProcesses`: `False`
- `resumableAcrossNodes`: `False`
- `boundedHistory`: `True`

### `aggregation`
- `description`: `General-purpose aggregation pipeline support.`
- `tiers`: `core`, `search`, `platform`
- `operations`: `aggregate`
- `supportedStages`: `$addFields`, `$bucket`, `$bucketAuto`, `$collStats`, `$count`, `$densify`, `$documents`, `$facet`, `$fill`, `$geoNear`, `$group`, `$indexStats`, `$limit`, `$lookup`, `$match`, `$merge`, `$project`, `$redact`, `$replaceRoot`, `$replaceWith`, `$sample`, `$set`, `$setWindowFields`, `$skip`, `$sort`, `$sortByCount`, `$unionWith`, `$unset`, `$unwind`
- `supportedExpressionOperators`: `$abs`, `$add`, `$all`, `$allElementsTrue`, `$and`, `$anyElementTrue`, `$arrayElemAt`, `$arrayToObject`, `$binarySize`, `$bitAnd`, `$bitNot`, `$bitOr`, `$bitXor`, `$bsonSize`, `$ceil`, `$cmp`, `$concat`, `$concatArrays`, `$cond`, `$convert`, `$dateAdd`, `$dateDiff`, `$dateFromParts`, `$dateFromString`, `$dateSubtract`, `$dateToParts`, `$dateToString`, `$dateTrunc`, `$dayOfMonth`, `$dayOfWeek`, `$dayOfYear`, `$divide`, `$elemMatch`, `$eq`, `$exists`, `$exp`, `$filter`, `$first`, `$firstN`, `$floor`, `$getField`, `$gt`, `$gte`, `$hour`, `$ifNull`, `$in`, `$indexOfArray`, `$indexOfBytes`, `$indexOfCP`, `$isArray`, `$isNumber`, `$isoDayOfWeek`, `$isoWeek`, `$isoWeekYear`, `$lastN`, `$let`, `$literal`, `$ln`, `$log`, `$log10`, `$lt`, `$lte`, `$ltrim`, `$map`, `$maxN`, `$median`, `$mergeObjects`, `$millisecond`, `$minN`, `$minute`, `$mod`, `$month`, `$multiply`, `$ne`, `$nin`, `$objectToArray`, `$or`, `$percentile`, `$pow`, `$rand`, `$range`, `$reduce`, `$regexFind`, `$regexFindAll`, `$regexMatch`, `$replaceAll`, `$replaceOne`, `$reverseArray`, `$round`, `$rtrim`, `$second`, `$setDifference`, `$setEquals`, `$setField`, `$setIntersection`, `$setIsSubset`, `$setUnion`, `$size`, `$slice`, `$sortArray`, `$split`, `$sqrt`, `$stdDevPop`, `$stdDevSamp`, `$strLenBytes`, `$strLenCP`, `$strcasecmp`, `$substr`, `$substrBytes`, `$substrCP`, `$subtract`, `$switch`, `$toBool`, `$toDate`, `$toDecimal`, `$toDouble`, `$toInt`, `$toLong`, `$toLower`, `$toObjectId`, `$toString`, `$toUUID`, `$toUpper`, `$trim`, `$trunc`, `$type`, `$unsetField`, `$week`, `$year`, `$zip`
- `supportedGroupAccumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$first`, `$firstN`, `$last`, `$lastN`, `$max`, `$maxN`, `$median`, `$mergeObjects`, `$min`, `$minN`, `$percentile`, `$push`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `supportedWindowAccumulators`: `$addToSet`, `$avg`, `$bottom`, `$bottomN`, `$count`, `$denseRank`, `$derivative`, `$documentNumber`, `$expMovingAvg`, `$first`, `$firstN`, `$integral`, `$last`, `$lastN`, `$linearFill`, `$locf`, `$max`, `$maxN`, `$median`, `$min`, `$minN`, `$percentile`, `$push`, `$rank`, `$shift`, `$stdDevPop`, `$stdDevSamp`, `$sum`, `$top`, `$topN`
- `embedded`: `True`
- `sync`: `True`
- `async`: `True`
- `explainable`: `True`
- `operationMetadata`: `{'aggregate': {'supportedOptions': ['allow_disk_use', 'batch_size', 'comment', 'hint', 'let', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'supportsCollectionScope': True, 'supportsDatabaseScope': True, 'supportsSession': True, 'acceptsPipeline': True, 'supportedStages': ['$addFields', '$bucket', '$bucketAuto', '$collStats', '$count', '$densify', '$documents', '$facet', '$fill', '$geoNear', '$group', '$indexStats', '$limit', '$lookup', '$match', '$merge', '$project', '$redact', '$replaceRoot', '$replaceWith', '$sample', '$set', '$setWindowFields', '$skip', '$sort', '$sortByCount', '$unionWith', '$unset', '$unwind'], 'supportedExpressionOperators': ['$abs', '$add', '$all', '$allElementsTrue', '$and', '$anyElementTrue', '$arrayElemAt', '$arrayToObject', '$binarySize', '$bitAnd', '$bitNot', '$bitOr', '$bitXor', '$bsonSize', '$ceil', '$cmp', '$concat', '$concatArrays', '$cond', '$convert', '$dateAdd', '$dateDiff', '$dateFromParts', '$dateFromString', '$dateSubtract', '$dateToParts', '$dateToString', '$dateTrunc', '$dayOfMonth', '$dayOfWeek', '$dayOfYear', '$divide', '$elemMatch', '$eq', '$exists', '$exp', '$filter', '$first', '$firstN', '$floor', '$getField', '$gt', '$gte', '$hour', '$ifNull', '$in', '$indexOfArray', '$indexOfBytes', '$indexOfCP', '$isArray', '$isNumber', '$isoDayOfWeek', '$isoWeek', '$isoWeekYear', '$lastN', '$let', '$literal', '$ln', '$log', '$log10', '$lt', '$lte', '$ltrim', '$map', '$maxN', '$median', '$mergeObjects', '$millisecond', '$minN', '$minute', '$mod', '$month', '$multiply', '$ne', '$nin', '$objectToArray', '$or', '$percentile', '$pow', '$rand', '$range', '$reduce', '$regexFind', '$regexFindAll', '$regexMatch', '$replaceAll', '$replaceOne', '$reverseArray', '$round', '$rtrim', '$second', '$setDifference', '$setEquals', '$setField', '$setIntersection', '$setIsSubset', '$setUnion', '$size', '$slice', '$sortArray', '$split', '$sqrt', '$stdDevPop', '$stdDevSamp', '$strLenBytes', '$strLenCP', '$strcasecmp', '$substr', '$substrBytes', '$substrCP', '$subtract', '$switch', '$toBool', '$toDate', '$toDecimal', '$toDouble', '$toInt', '$toLong', '$toLower', '$toObjectId', '$toString', '$toUUID', '$toUpper', '$trim', '$trunc', '$type', '$unsetField', '$week', '$year', '$zip'], 'supportedGroupAccumulators': ['$addToSet', '$avg', '$bottom', '$bottomN', '$count', '$first', '$firstN', '$last', '$lastN', '$max', '$maxN', '$median', '$mergeObjects', '$min', '$minN', '$percentile', '$push', '$stdDevPop', '$stdDevSamp', '$sum', '$top', '$topN'], 'supportedWindowAccumulators': ['$addToSet', '$avg', '$bottom', '$bottomN', '$count', '$denseRank', '$derivative', '$documentNumber', '$expMovingAvg', '$first', '$firstN', '$integral', '$last', '$lastN', '$linearFill', '$locf', '$max', '$maxN', '$median', '$min', '$minN', '$percentile', '$push', '$rank', '$shift', '$stdDevPop', '$stdDevSamp', '$sum', '$top', '$topN'], 'supportsLeadingSearchStage': True, 'supportsLeadingVectorSearchStage': True}}`

### `search`
- `description`: `Atlas Search via aggregate pipelines.`
- `tiers`: `search`
- `operations`: `aggregate`
- `operators`: `text`, `phrase`, `autocomplete`, `wildcard`, `exists`, `in`, `equals`, `range`, `near`, `compound`, `regex`
- `aggregateStage`: `$search`
- `fieldMappings`: `autocomplete`, `boolean`, `date`, `document`, `embeddedDocuments`, `number`, `objectId`, `string`, `token`, `uuid`
- `structuredFieldMappings`: `document`, `embeddedDocuments`
- `textualFieldMappings`: `autocomplete`, `string`, `token`
- `exactFilterFieldMappings`: `boolean`, `date`, `number`, `objectId`, `uuid`
- `structuredParentPathOperators`: `text`, `phrase`, `autocomplete`, `wildcard`, `regex`, `exists`
- `explainFeatures`: `pathSummary`, `resolvedLeafPaths`, `structuredParentPathResolution`, `querySemantics`, `stageOptions`, `countPreview`, `highlightPreview`, `facetPreview`
- `operatorSemantics`: `{'text': {'matchingMode': 'tokenized-any-term', 'scope': 'local-text-tier'}, 'phrase': {'matchingMode': 'ordered-token-window', 'supportsSlop': True, 'scope': 'local-text-tier'}, 'autocomplete': {'matchingMode': 'token-prefix', 'tokenization': 'classic-text-local', 'tokenOrder': ['any', 'sequential'], 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'wildcard': {'matchingMode': 'glob-local', 'patternSyntax': 'fnmatch-like', 'allowAnalyzedField': True, 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'regex': {'matchingMode': 'python-regex-local', 'supportsFlags': True, 'supportedFlags': ['i', 'm', 's', 'x'], 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'exists': {'matchingMode': 'field-presence', 'scope': 'local-text-tier'}, 'in': {'matchingMode': 'exact-membership', 'scope': 'local-filter-tier'}, 'equals': {'matchingMode': 'exact-equality', 'scope': 'local-filter-tier'}, 'range': {'matchingMode': 'range-comparison', 'scope': 'local-filter-tier'}, 'near': {'matchingMode': 'distance-ranking', 'scope': 'local-filter-tier'}}`
- `textSearchTier`: `closed-local-tier`
- `stageOptions`: `{'count': ['total', 'lowerBound'], 'highlight': {'supportsPath': True, 'supportsMaxChars': True, 'resultField': 'searchHighlights'}, 'facet': {'supportsPath': True, 'supportsNumBuckets': True, 'previewOnly': True}}`
- `advancedAtlasLikeGaps`: `fullFacetCollectorParity`, `fullHighlightParity`, `countMetaParity`, `advancedAutocompleteSemantics`, `advancedWildcardSemantics`, `advancedRegexOptions`
- `sqliteBackends`: `fts5`, `fts5-glob`, `fts5-path`, `fts5-prefilter`, `python`
- `operationMetadata`: `{'aggregate': {'supportedOptions': ['allow_disk_use', 'batch_size', 'comment', 'hint', 'let', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'supportsCollectionScope': True, 'supportsDatabaseScope': False, 'supportsSession': True, 'acceptsPipeline': True, 'aggregateStage': '$search', 'operators': ['text', 'phrase', 'autocomplete', 'wildcard', 'exists', 'in', 'equals', 'range', 'near', 'compound', 'regex'], 'stageOptions': {'count': ['total', 'lowerBound'], 'highlight': {'supportsPath': True, 'supportsMaxChars': True, 'resultField': 'searchHighlights'}, 'facet': {'supportsPath': True, 'supportsNumBuckets': True, 'previewOnly': True}}, 'requiresLeadingStage': True}}`
- `note`: `The local textual $search tier is closed in its documented subset; remaining gaps are advanced Atlas-like features.`

### `vector_search`
- `description`: `Atlas Vector Search via aggregate pipelines.`
- `tiers`: `search`
- `operations`: `aggregate`
- `similarities`: `cosine`, `dotProduct`, `euclidean`
- `aggregateStage`: `$vectorSearch`
- `backend`: `usearch`
- `mode`: `local-ann-with-exact-baseline`
- `filterMode`: `post-candidate-with-adaptive-candidate-expansion`
- `fallback`: `exact`
- `hybridFilterModes`: `candidate-prefilter`, `candidate-prefilter+post-candidate`, `post-candidate`
- `explainFeatures`: `pathSummary`, `resolvedLeafPaths`, `querySemantics`, `scoreBreakdown`, `candidatePlan`, `hybridRetrieval`, `vectorBackend`
- `operationMetadata`: `{'aggregate': {'supportedOptions': ['allow_disk_use', 'batch_size', 'comment', 'hint', 'let', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'supportsCollectionScope': True, 'supportsDatabaseScope': False, 'supportsSession': True, 'acceptsPipeline': True, 'aggregateStage': '$vectorSearch', 'similarities': ['cosine', 'dotProduct', 'euclidean'], 'scoreField': 'vectorSearchScore', 'hybridFilterModes': ['candidate-prefilter', 'candidate-prefilter+post-candidate', 'post-candidate'], 'explainFeatures': ['pathSummary', 'resolvedLeafPaths', 'querySemantics', 'scoreBreakdown', 'candidatePlan', 'hybridRetrieval', 'vectorBackend'], 'requiresLeadingStage': True}}`
- `note`: `SQLiteEngine uses a local usearch ANN backend when the vector index is materialized; MemoryEngine remains the exact semantic baseline.`

### `collation`
- `description`: `Locale-aware comparison and sorting support.`
- `tiers`: `platform`
- `operations`: _empty_
- `backend`: `{'selectedBackend': 'pyuca', 'availableBackends': ['pyuca'], 'unicodeAvailable': True, 'advancedOptionsAvailable': False}`
- `capabilities`: `{'supportedLocales': ['simple', 'en'], 'supportedStrengths': [1, 2, 3], 'supportsCaseLevel': True, 'supportsNumericOrdering': True, 'optionalIcuBackend': True, 'fallbackBackend': 'pyuca', 'advancedOptionsRequireIcu': ['backwards', 'alternate', 'maxVariable', 'normalization']}`
- `operationMetadata`: `{'find': {'supportsCollation': True, 'scope': 'collection-query', 'behavior': 'filter-and-sort'}, 'find_one': {'supportsCollation': True, 'scope': 'collection-query', 'behavior': 'filter'}, 'count_documents': {'supportsCollation': True, 'scope': 'collection-query', 'behavior': 'filter'}, 'distinct': {'supportsCollation': True, 'scope': 'collection-query', 'behavior': 'dedup'}, 'update_one': {'supportsCollation': True, 'scope': 'collection-write', 'behavior': 'filter-match'}, 'update_many': {'supportsCollation': True, 'scope': 'collection-write', 'behavior': 'filter-match'}, 'replace_one': {'supportsCollation': True, 'scope': 'collection-write', 'behavior': 'filter-match'}, 'delete_one': {'supportsCollation': True, 'scope': 'collection-write', 'behavior': 'filter-match'}, 'delete_many': {'supportsCollation': True, 'scope': 'collection-write', 'behavior': 'filter-match'}, 'serverStatus': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'metadataPath': 'mongoeco.collation'}}`

### `persistence`
- `description`: `Persistence guarantees and storage-engine metadata.`
- `tiers`: `platform`
- `operations`: _empty_
- `persistent`: `True`
- `storageEngine`: `runtime-dependent`
- `operationMetadata`: `{'serverStatus': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'metadataPaths': ['storageEngine.name', 'mongoeco.engineRuntime']}, 'listDatabases': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'behavior': 'list-visible-databases'}}`

### `topology_discovery`
- `description`: `Expose topology and SDAM-style discovery data.`
- `tiers`: `platform`
- `operations`: _empty_
- `topologyType`: `unknown`
- `serverCount`: `1`
- `sdam`: `{'fullSdam': False, 'topologyVersionAware': True, 'helloMemberDiscovery': True, 'serverHealthTracking': True, 'electionMetadataAware': True, 'longPollingHello': False, 'distributedMonitoring': False}`
- `operationMetadata`: `{'hello': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'metadataPaths': ['helloOk', 'isWritablePrimary', 'maxWireVersion']}, 'isMaster': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'legacyAliasOf': 'hello'}, 'serverStatus': {'supportsCapabilityInspection': True, 'inspectionSurface': 'database.command', 'metadataPath': 'mongoeco.sdam'}, 'sdam_capabilities': {'supportsCapabilityInspection': True, 'inspectionSurface': 'mongoeco.client', 'metadataPath': 'sdam_capabilities()'}}`

## CXP Extensions
### `classicText`
- `supportsTextScore`: `True`
- `supportsMetaProjection`: `True`
- `supportsSortByTextScore`: `True`
- `projectsFromCapability`: `search`
- `note`: `The classic $text subset remains local, token-based and intentionally lighter than MongoDB server full-text behavior.`

### `geospatial`
- `semantics`: `planar-local`
- `storedGeometries`: `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`, `legacy [x, y]`
- `queryOperators`: `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`
- `aggregationStages`: `$geoNear`
- `projectsFromCapability`: `read`
- `note`: `The embedded runtime uses planar local geometry operations. $nearSphere and 2dsphere remain Mongo-like names over local planar distance.`

## Local Runtime Subsets
### `vectorSearch`
- `similarities`: `cosine`, `dotProduct`, `euclidean`
- `aggregateStage`: `$vectorSearch`
- `backend`: `usearch`
- `mode`: `local-ann-with-exact-baseline`
- `filterMode`: `post-candidate-with-adaptive-candidate-expansion`
- `fallback`: `exact`
- `hybridFilterModes`: `candidate-prefilter`, `candidate-prefilter+post-candidate`, `post-candidate`
- `explainFeatures`: `pathSummary`, `resolvedLeafPaths`, `querySemantics`, `scoreBreakdown`, `candidatePlan`, `hybridRetrieval`, `vectorBackend`
- `operationMetadata`: `{'aggregate': {'supportedOptions': ['allow_disk_use', 'batch_size', 'comment', 'hint', 'let', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'supportsCollectionScope': True, 'supportsDatabaseScope': False, 'supportsSession': True, 'acceptsPipeline': True, 'aggregateStage': '$vectorSearch', 'similarities': ['cosine', 'dotProduct', 'euclidean'], 'scoreField': 'vectorSearchScore', 'hybridFilterModes': ['candidate-prefilter', 'candidate-prefilter+post-candidate', 'post-candidate'], 'explainFeatures': ['pathSummary', 'resolvedLeafPaths', 'querySemantics', 'scoreBreakdown', 'candidatePlan', 'hybridRetrieval', 'vectorBackend'], 'requiresLeadingStage': True}}`
- `note`: `SQLiteEngine uses a local usearch ANN backend when the vector index is materialized; MemoryEngine remains the exact semantic baseline.`

### `search`
- `operators`: `text`, `phrase`, `autocomplete`, `wildcard`, `exists`, `in`, `equals`, `range`, `near`, `compound`, `regex`
- `aggregateStage`: `$search`
- `fieldMappings`: `autocomplete`, `boolean`, `date`, `document`, `embeddedDocuments`, `number`, `objectId`, `string`, `token`, `uuid`
- `structuredFieldMappings`: `document`, `embeddedDocuments`
- `textualFieldMappings`: `autocomplete`, `string`, `token`
- `exactFilterFieldMappings`: `boolean`, `date`, `number`, `objectId`, `uuid`
- `structuredParentPathOperators`: `text`, `phrase`, `autocomplete`, `wildcard`, `regex`, `exists`
- `explainFeatures`: `pathSummary`, `resolvedLeafPaths`, `structuredParentPathResolution`, `querySemantics`, `stageOptions`, `countPreview`, `highlightPreview`, `facetPreview`
- `operatorSemantics`: `{'text': {'matchingMode': 'tokenized-any-term', 'scope': 'local-text-tier'}, 'phrase': {'matchingMode': 'ordered-token-window', 'supportsSlop': True, 'scope': 'local-text-tier'}, 'autocomplete': {'matchingMode': 'token-prefix', 'tokenization': 'classic-text-local', 'tokenOrder': ['any', 'sequential'], 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'wildcard': {'matchingMode': 'glob-local', 'patternSyntax': 'fnmatch-like', 'allowAnalyzedField': True, 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'regex': {'matchingMode': 'python-regex-local', 'supportsFlags': True, 'supportedFlags': ['i', 'm', 's', 'x'], 'atlasParity': 'subset', 'scope': 'local-text-tier'}, 'exists': {'matchingMode': 'field-presence', 'scope': 'local-text-tier'}, 'in': {'matchingMode': 'exact-membership', 'scope': 'local-filter-tier'}, 'equals': {'matchingMode': 'exact-equality', 'scope': 'local-filter-tier'}, 'range': {'matchingMode': 'range-comparison', 'scope': 'local-filter-tier'}, 'near': {'matchingMode': 'distance-ranking', 'scope': 'local-filter-tier'}}`
- `textSearchTier`: `closed-local-tier`
- `stageOptions`: `{'count': ['total', 'lowerBound'], 'highlight': {'supportsPath': True, 'supportsMaxChars': True, 'resultField': 'searchHighlights'}, 'facet': {'supportsPath': True, 'supportsNumBuckets': True, 'previewOnly': True}}`
- `advancedAtlasLikeGaps`: `fullFacetCollectorParity`, `fullHighlightParity`, `countMetaParity`, `advancedAutocompleteSemantics`, `advancedWildcardSemantics`, `advancedRegexOptions`
- `sqliteBackends`: `fts5`, `fts5-glob`, `fts5-path`, `fts5-prefilter`, `python`
- `operationMetadata`: `{'aggregate': {'supportedOptions': ['allow_disk_use', 'batch_size', 'comment', 'hint', 'let', 'max_time_ms'], 'acceptedNoopOptions': [], 'unsupportedOptions': [], 'supportsExplain': True, 'resultType': 'cursor', 'supportsCollectionScope': True, 'supportsDatabaseScope': False, 'supportsSession': True, 'acceptsPipeline': True, 'aggregateStage': '$search', 'operators': ['text', 'phrase', 'autocomplete', 'wildcard', 'exists', 'in', 'equals', 'range', 'near', 'compound', 'regex'], 'stageOptions': {'count': ['total', 'lowerBound'], 'highlight': {'supportsPath': True, 'supportsMaxChars': True, 'resultField': 'searchHighlights'}, 'facet': {'supportsPath': True, 'supportsNumBuckets': True, 'previewOnly': True}}, 'requiresLeadingStage': True}}`
- `note`: `The local textual $search tier is closed in its documented subset; remaining gaps are advanced Atlas-like features.`

### `classicText`
- `supportsTextScore`: `True`
- `supportsMetaProjection`: `True`
- `supportsSortByTextScore`: `True`
- `projectsFromCapability`: `search`
- `note`: `The classic $text subset remains local, token-based and intentionally lighter than MongoDB server full-text behavior.`

### `geospatial`
- `semantics`: `planar-local`
- `storedGeometries`: `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`, `legacy [x, y]`
- `queryOperators`: `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`
- `aggregationStages`: `$geoNear`
- `projectsFromCapability`: `read`
- `note`: `The embedded runtime uses planar local geometry operations. $nearSphere and 2dsphere remain Mongo-like names over local planar distance.`

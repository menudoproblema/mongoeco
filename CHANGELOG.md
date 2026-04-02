# Changelog

Todos los cambios relevantes de este proyecto se documentan en este
archivo.

El formato sigue las recomendaciones de Keep a Changelog y el proyecto
usa Semantic Versioning.

## [Unreleased]

### Added

- El catálogo de compatibilidad exporta ya una matriz separada de
  `database_command_options`, para declarar la surface efectiva de
  `database.command(...)` y del proxy wire dentro del alcance soportado
  de `MongoDB 8.0` y `PyMongo 4.x`.
- El catálogo de compatibilidad exporta ahora también `database_commands`,
  un inventario declarativo de los comandos crudos soportados, su familia
  administrativa y si forman parte de la surface wire local.
- `listCommands` expone ahora también metadatos observables del producto por
  comando (`adminFamily`, `supportsWire`, `supportsExplain`,
  `supportsComment`, `supportedOptions`, `note`) para tooling e
  introspección local.
- La surface administrativa local añade ahora `dbHash` tanto en
  `database.command(...)` como en el proxy wire, y `serverStatus`
  incorpora contadores embebidos y resumen de profiling para observabilidad
  local.
- El runtime embebido añade ahora `currentOp` y `killOp` con semántica local
  y best-effort, visibles tanto desde `database.command(...)` como desde la
  surface wire administrativa.
- La agregación local soporta ya `$densify`, `$fill` y `$merge` en su subset
  documentado, y los pipeline-style updates quedan cerrados end-to-end también
  en las rutas administrativas de `update` y `findAndModify`.
- El runtime embebido soporta ya un subset geoespacial local y explícito:
  `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere` y `$geoNear`
  sobre datos `Point` GeoJSON o pares legacy `[x, y]`. En `SQLiteEngine`
  esa semántica queda visible como fallback Python honesto, con
  `pushdown_hints` específicos en `explain()`.
- El runtime embebido soporta ya un subset local explícito de `$text`
  clásico con `textScore`, proyección `$meta`, ordenación por score y
  `explain()` consistente entre API directa, engines y proxy wire.
- La surface básica de `find` queda cerrada también para proyección avanzada
  en su subconjunto local útil (`$slice`, `$elemMatch` y proyección
  posicional), incluyendo `database.command(...)`.
- La agregación local soporta ya también `$collStats` como stage inicial de
  introspección.

### Fixed

- El executor wire valida ya de forma temprana que el nombre del comando
  sea un string no vacio, devolviendo un `OperationFailure` estable en
  vez de dejar que requests malformed fallen mas tarde en routing o
  ejecucion.
- El executor wire valida tambien de forma temprana payloads malformed
  de familias passthrough comunes, como nombres de coleccion vacios en
  `find`/`listIndexes`, `explain` sin documento de comando o batches
  vacios/invalidos en `insert`/`update`/`delete`/`createIndexes`,
  evitando errores tardios y dejando mensajes publicos mas estables.
- El runtime wire endurece tambien la validacion temprana de
  `count`, `distinct`, `validate` y `explain(verbosity=...)`, y las
  respuestas de `explain` dejan ya de filtrar objetos internos como
  `EngineIndexRecord` que PyMongo no podia codificar al cruzar el proxy.
- El proxy wire valida ahora tambien de forma temprana varios comandos
  menos transitados de introspeccion y control (`connectionStatus`,
  `collStats`, `dbStats`, `profile`, `listCollections`,
  `listDatabases`) y endurece las familias `auth`/`session`/`cursor`
  (`authenticate`, `saslContinue`, `endSessions`, `getMore`,
  `killCursors`, `commitTransaction`, `abortTransaction`) para que los
  payloads malformed fallen antes y con mensajes publicos mas estables.
- La validacion temprana del proxy wire endurece ahora tambien varios
  shapes de `wire/admin` que antes podian caer tarde dentro de
  `database.command(...)`, incluyendo `find`, `count`, `distinct`,
  `aggregate`, `createIndexes`, `dropIndexes`, `listIndexes`,
  `findAndModify`, `listCollections` y `listDatabases`.
- `explain` cubre ahora tambien `count`, `distinct` y `findAndModify`,
  ademas de `find`, `aggregate`, `update` y `delete`, reutilizando el
  mismo routing administrativo y devolviendo shapes serializables por
  wire.
- `collStats` y `dbStats` incluyen ya `scaleFactor` en las respuestas
  administrativas locales para reflejar mejor la escala efectiva usada
  al materializar los snapshots.
- El comando `profile` devuelve ahora también el nivel actual y el número
  de entradas registradas, y `explain` materializa `command` /
  `explained_command` de forma más uniforme en la surface administrativa.
- `serverStatus.mongoeco` expone ahora también bloques estructurados de
  `collation` y `sdam`, y `validate` devuelve warnings explícitos cuando se
  usan flags aceptados solo por compatibilidad (`scandata`, `full`,
  `background`) que no cambian el comportamiento del runtime embebido.
- `collStats.totalIndexSize` y `dbStats.indexSize` reflejan ya tamaños locales
  reales de metadata de índices, `listIndexes` expone `ns` por documento, y
  `explain` materializa también `collection` y `namespace` de forma uniforme
  en todas las rutas administrativas soportadas.
- `serverStatus.mongoeco` expone ahora tambien resumen local de
  `changeStreams`, y `profile` devuelve ademas `namespaceVisible`,
  `trackedDatabases` y `visibleNamespaces` para reforzar la observabilidad del
  runtime embebido.
- `serverStatus.mongoeco` resume ahora tambien la surface administrativa
  declarada (`adminFamilies`, `explainableCommandCount`), y `validate`
  anade warnings TTL cuando detecta indices `expireAfterSeconds` cuyos
  documentos actuales no contienen ningun valor fecha usable.
- `serverStatus.mongoeco` expone ahora tambien `engineRuntime` con diagnostico
  estructurado de planner/search/caches por engine, los explains de search
  materializan detalles de lifecycle/backend (`backendAvailable`,
  `backendMaterialized`, `physicalName`, `readyAtEpoch`, `fts5Available`) y en
  SQLite `explain` deja visible un bloque `pushdown` comun para distinguir SQL
  puro, plan hibrido y fallback Python.
- `aggregate(...).explain()` expone ahora tambien un bloque top-level
  `pushdown` con recuento de stages empujados/restantes y elegibilidad de
  streaming, y `serverStatus.mongoeco.engineRuntime` en SQLite resume ademas
  search indexes declarados/pendientes y caches fisicas relevantes.
- SQLite empuja ya tambien `$size` simple a SQL cuando la ruta es segura, y
  `find(...).explain()` materializa `planning_issues` del engine para los
  fallbacks hibridos o Python, en vez de dejar solo `fallback_reason`.
- SQLite empuja ahora tambien `$mod` entero sobre campos escalares cuando el
  path no mezcla arrays ni valores `real`, y `find(...).explain()` mantiene el
  fallback a Python con `planning_issues` del engine cuando esa ruta segura no
  aplica.
- SQLite empuja ahora tambien un subconjunto seguro de `$regex` anclado por
  patrones literales sobre campos string escalares (`literal`, `^literal`,
  `literal$`, `^literal$`, `^literal.*`) y conserva fallback explicito a Python
  cuando hay arrays u opciones regex que cambian la semantica.
- Dentro de ese subconjunto, SQLite acepta ya tambien `$options: "i"` cuando
  el patron y los valores del field son ASCII, manteniendo fallback explicito
  en cuanto aparece texto no ASCII o una semantica regex mas amplia.
- SQLite empuja ahora tambien `$all` sobre arrays escalares simples,
  `$elemMatch` muy acotado sobre arrays escalares top-level y comparaciones de
  rango sobre paths que mezclan escalares y arrays cuando el contenido sigue
  siendo homogeneo en un mismo tipo comparable.
- `find(...).explain()` en SQLite expone ya tambien `pushdown_hints` cuando una
  query cae a fallback, para dejar visible que familia de operador esta
  bloqueando el pushdown y cual seria la siguiente extension natural. Esa
  clasificacion cubre ya tambien bloqueos estructurales como `sort`,
  `collation`, `array-comparison` o `array-traversal`, no solo operadores
  explicitos de la query.
- `serverStatus.opcounters` refleja ya actividad local real del runtime
  embebido (`insert`, `query`, `update`, `delete`, `getmore`, `command`) en
  lugar de quedar fijado a ceros.
- `vectorSearch` local acepta ya `filter` y similitudes `cosine`,
  `dotProduct` y `euclidean`, manteniendo el contrato como exact search local
  y reflejando esa surface ampliada en explain, compatibilidad declarada y
  tests.
- Los índices `hidden` quedan ya soportados como metadata administrativa local
  real: se preservan en `create_index`, `createIndexes`, `listIndexes` e
  `index_information()`, y el planner rechaza de forma estable los `hint`
  contra índices ocultos.
- La precarga de snapshots para `$collStats` deja ya de activarse fuera de las
  pipelines que realmente usan ese stage, evitando regresiones colaterales en
  agregaciones con valores BSON no serializables por el helper de stats.

## [3.0.0] - 2026-04-01

### Added

- Se exponen ya capacidades explícitas para las tres superficies más
  dependientes de contrato: `change_stream_backend_info()` en cliente,
  base de datos y colección; `sdam_capabilities_info()` /
  `client.sdam_capabilities()` para el subconjunto SDAM soportado; y
  `collation_capabilities_info()` para el alcance de collation soportado
  con `PyICU` opcional y `pyuca` como fallback.
- Los subpaquetes `mongoeco.api._async` y `mongoeco.api._sync` exportan
  ahora también los cursores públicos de listado, search indexes y raw
  BSON batches, alineando sus `__all__` con la superficie real devuelta
  por clientes y colecciones.

### Fixed

- Se endurece la aritmetica BSON y la validacion de `$mod`: `bson_divide`
  y `bson_mod` rechazan ya divisores cero con `OperationFailure`, y los
  filtros `$mod` dejan de aceptar divisores `NaN` o infinitos.
- Se corrige la semantica base de proyeccion: una proyeccion vacia `{}`
  vuelve a devolver el documento completo.
- `insert_many()` deja de truncar silenciosamente la verificacion de
  resultados cuando un engine bulk devuelve un numero de respuestas
  distinto al de documentos enviados.
- El parser wire acepta ya los flags validos reconocidos de `OP_MSG` y
  `OP_QUERY`, en lugar de rechazar cualquier request con flags de
  protocolo soportados.
- El transporte wire del driver usa ya `saslStart` y `saslContinue`
  para autenticacion SCRAM, y el proxy wire entiende ahora esa
  conversacion multi-step moderna ademas del comando legacy
  `authenticate`.
- El bridge wire, el codec interno y la semantica de `$type` preservan
  ya `MinKey`, `MaxKey` y `Code/CodeWithScope`, evitando perder esos
  valores BSON especiales en round-trips y comparaciones basicas.
- El fast path de ordenacion SQL en SQLite clasifica ya `Binary`,
  `Timestamp` y `Regex` en los mismos brackets BSON que el runtime
  Python, reduciendo desajustes de orden en sorts pushdown.
- Los tipos BSON publicos endurecen ya su semantica observable:
  `Binary` distingue el `subtype`, `Regex` normaliza el orden de flags,
  `Timestamp` recupera orden total por `(time, inc)` y `Decimal128`
  trata `NaN` como igual a `NaN`.
- La validacion de queries y pipelines endurece varios edge cases:
  `$size` rechaza ya valores fuera de `int32`, los field paths de
  filtros no aceptan nombres vacios, segmentos vacios ni `null bytes`,
  los operadores bitwise aceptan wrappers BSON y se limitan a 64 bits,
  `$lookup let` valida nombres de variable al estilo MongoDB,
  `$replaceRoot` falla antes ante `newRoot` claramente invalido y los
  updates sobre arrays con segmentos no numericos dejan ya de fallar en
  silencio.
- Las comparaciones de rango sobre campos array dejan ya de tratar el
  array completo como un escalar BSON frente a objetivos escalares;
  `find()` y los paths de filtrado comunes vuelven a decidir por los
  elementos del array, evitando falsos positivos como `{"a": [2]}`
  matcheando `{"a": {"$gt": 5}}`.
- `$strcasecmp` trata ya operandos `null` o ausentes como cadenas
  vacias, alineando las comparaciones con el comportamiento observado en
  MongoDB real en lugar de devolver `null`.
- `watch(session=...)` deja de ignorar sesiones explicitamente y falla
  ahora con un error claro en cliente, base de datos y coleccion.
- La topologia local de `replica set` deja de inventar un primario antes
  del handshake y usa los seeds como candidatos provisionales hasta que
  llegue discovery real.
- `$addToSet` en agregacion respeta ya `collation` al deduplicar,
  tanto en el camino interpretado como en el compilado.
- El seed de upsert extrae ya igualdades seedables desde `$and`
  top-level y desde condiciones `$in` con un unico valor.
- `$dateFromParts` valida ya de forma explicita los rangos de `hour`,
  `minute`, `second` y `millisecond`, devolviendo errores estables en
  vez de propagar `ValueError` genericos del constructor de `datetime`.
- La compilacion de queries corta ya filtros con anidacion logica
  excesiva en `$and`/`$or`/`$nor`, evitando `RecursionError` tardios.
- El driver exige ya `maxStalenessSeconds >= 90` y deja de considerar
  nodos `STANDALONE` o `MONGOS` como primarios validos dentro de una
  topologia `replica set`.
- La compilacion de pipelines valida ya las especificaciones de
  `$addFields` y `$set` antes de ejecutar documentos, evitando que
  claves invalidas se acepten y fallen tarde en runtime.
- Se validan ya como no soportados los valores `NaN` e infinitos en
  ventanas `range` de `$setWindowFields`, evitando comparaciones
  inconsistentes sobre el campo de ordenacion.
- La creacion de rutas sobre documentos faltantes deja de inferir
  arrays solo porque el siguiente segmento sea numerico, evitando
  estructuras ambiguas al escribir paths como `"a.0.b"` desde un padre
  inexistente.
- `$elemMatch` deja de reutilizar planes compilados cuando el dialecto
  de ejecucion no coincide con el de compilacion, evitando congelar
  semanticas como `null` vs `undefined` entre MongoDB 7 y 8.
- `$group` rechaza ya claves `_id` no BSON no hashables en vez de
  agruparlas por `repr()`, eliminando colisiones silenciosas entre
  objetos Python arbitrarios.
- El catálogo de compatibilidad permite ya declarar inventarios de
  operadores de query y update por dialecto, en lugar de forzar un
  conjunto global estático para todas las versiones de MongoDB.
- `commit_transaction()` y `abort_transaction()` ya no limpian el estado
  local de la sesion si el hook del engine falla, evitando ocultar
  transacciones que han quedado en estado ambiguo.
- `create_collection()` y el comando `create` validan ya las opciones de
  colecciones `capped`: exigen `size > 0` cuando `capped=True` y
  rechazan tambien `max <= 0`.
- El comando `listIndexes` deja de exponer el campo no estandar
  `fields` en su salida wire, alineando mejor la respuesta con MongoDB.
- Los documentos publicos de `list_indexes()` e `IndexDefinition` dejan
  tambien de exponer el campo no estandar `fields`; esa metadata queda
  reservada a los registros internos del engine.
- `drop_database()` aprovecha ya fast paths nativos de engine cuando
  existen, y los engines permiten aliases con el mismo key pattern si
  la definicion es identica; en esos casos `drop_index()` por key
  pattern falla solo cuando hay ambiguedad real y pide usar el nombre.
- El codec valida ya que las claves BSON sean strings, rechaza sets no
  serializables, acepta tuplas como arrays BSON y normaliza `bytearray`
  a `bytes`; ademas, `$abs` detecta overflow de `int64`, `$jsonSchema`
  reutiliza el schema compilado y SQLite indexa ya elementos
  `Decimal128` dentro de arrays multikey.
- La compilacion interna de updates deja ya de aceptar en silencio
  documentos de update invalidos en modo `STRICT`, mientras que
  `RELAXED` los reporta como `planning_issues`; ademas, `$switch`
  tolera ahora claves extra dentro de cada branch siempre que existan
  `case` y `then`.
- La collation prefiere ya un backend Unicode real (`ICU` cuando esta
  disponible y `pyuca` en caso contrario), y el paquete base incorpora
  `pyuca` como dependencia runtime para evitar caer al fallback
  aproximado anterior en entornos sin `PyICU`.
- La collation expone ya metadata de capacidad en runtime para distinguir
  entre backend `ICU`, fallback `pyuca` y ausencia de backend Unicode, y
  documenta de forma explicita que las opciones avanzadas de tailoring
  solo quedan soportadas cuando `PyICU` esta disponible.
- La collation `simple` vuelve a quedar cerrada sobre el comparador BSON
  base: `caseLevel`, `numericOrdering` y el resto de knobs Unicode ya no
  se aceptan bajo `locale="simple"`.
- `$jsonSchema` valida ya operadores lógicos (`allOf`, `anyOf`,
  `oneOf`, `not`), `$bucket` y `$bucketAuto` heredan la `collation`
  del pipeline, y las conversiones de agregación endurecen su
  compatibilidad con MongoDB: `$convert` aplica `onError` ante fallos
  de conversión no envueltos y `$toInt`/`$toLong` truncan `double`
  finitos hacia cero en vez de rechazarlos por fraccionales.
- Los modulos wire y de transporte dejan ya de importar `bson` de forma
  ansiosa en un interprete limpio: el paquete puede exponerse e
  importarse sin extras wire instalados, y solo falla con un error claro
  cuando se ejecuta funcionalidad que realmente necesita `pymongo` o
  `bson`.
- `create_index()` acepta ya key patterns especiales como `"text"`,
  `"hashed"`, `"2d"` y `"2dsphere"` en metadata publica y round-trips de
  indices. Mientras no exista un planner especializado para ellos,
  `mongoeco` evita reutilizarlos como indices ordenados normales para
  `hint` o aceleracion B-tree.
- Los change streams aceptan ya stages adicionales compatibles en
  pipeline (`$addFields`, `$set`, `$unset`, `$replaceRoot`,
  `$replaceWith`) y se cierran automaticamente tras recibir
  `invalidate`.
- Los change streams creados desde `AsyncCollection` o `AsyncDatabase`
  directos comparten ya un `ChangeStreamHub` persistente y el historial
  retenido en memoria queda acotado, con errores explicitos cuando un
  cursor o un resume token apuntan a eventos ya expirados.
- El tamaño de retención del historial local de change streams pasa a
  poder configurarse desde los clientes async/sync y desde constructores
  directos de base de datos o colección.
- Las fachadas async/sync preservan ya correctamente la configuracion
  derivada de colección y base de datos: `rename()` mantiene concerns,
  codec, planning y parametros locales de change streams, y los
  accesores de `database` / propiedades de runtime dejan de perder o
  esconder ajustes de journal e historial tras caer por `__getattr__`.
- Los change streams locales pueden persistir ahora su historial retenido
  a un journal en fichero mediante `change_stream_journal_path`, lo que
  permite reanudar cursores con `resume_after` o `start_after` tras
  recrear clientes o colecciones dentro del mismo entorno local.
- Cuando el journal de change streams está activado, `mongoeco` conserva
  ahora un log incremental y lo compacta periódicamente contra un
  snapshot retenido, evitando reescrituras completas del historial en
  cada evento publicado.
- La persistencia local de change streams añade ahora checksum por
  entrada incremental, ignora una cola truncada si el último append
  quedó a medias y expone knobs públicos de durabilidad y rotación
  (`change_stream_journal_fsync`, `change_stream_journal_max_bytes`).
- Los change streams locales exponen ahora `change_stream_state()` en
  cliente, base de datos y colección para inspeccionar offsets
  retenidos, estado del journal y progreso de compactación.
- `watch()` acepta ya `fullDocument` (`default`, `updateLookup`,
  `whenAvailable`, `required`), los resume tokens dejan de exponerse
  como enteros decimales simples y `drop_database()` insiste hasta
  vaciar las colecciones visibles del database.
- Las sesiones validan ya `writeConcern(w=0)` al abrir transacciones,
  reintentan `commit`/`abort` cuando el error llega etiquetado como
  transitorio o con resultado ambiguo, y exponen estado causal basico
  (`cluster_time` / `operation_time`) que se actualiza en operaciones
  locales y respuestas wire.
- El driver local arranca ya seeds únicos no directos como topología
  provisional `UNKNOWN` en lugar de fijarlos a `single`, usa selección
  provisional mientras no haya handshake y `refresh_topology()` descubre
  miembros adicionales de replica set desde `hello`, marcando además
  incompatibilidades por familias mezcladas o `setName` conflictivos.
- El discovery del driver aprovecha además `primary`, `me`,
  `arbiterOnly` y `topologyVersion`: los arbiters pasan a modelarse como
  miembros explícitos del replica set, el monitor puede descubrir seeds
  adicionales desde respuestas secundarias y deja de degradar el estado
  local si recibe un `hello` con versión topológica más vieja.
- El driver normaliza ya fallos reales de red wire a
  `ConnectionFailure`, haciendo efectivos los retryable reads/writes
  también ante errores de `connect`/`read`/`write`; además, cada server
  mantiene estado de salud local (`healthy`, `recovering`, `degraded`,
  `unreachable`) y esa señal se usa para priorizar candidatos más sanos.
- La ejecución de comandos del driver vuelve a resolver `candidate_servers`
  contra la topología vigente en el momento de ejecutar, evitando que un
  `RequestExecutionPlan` preparado con seeds o miembros ya desfasados siga
  enviando tráfico a candidatos obsoletos tras un `refresh_topology()`.

## [2.2.0] - 2026-03-31

### Fixed

- Se corrige la semantica de operadores de actualizacion sobre arrays y
  subdocumentos para acercarla a MongoDB real: `$pull` con dicts
  parciales, `collation` en `$addToSet`/`$pull`/`$pullAll`,
  `arrayFilters`, `UndefinedType` en expresiones de control y varios
  edge cases de query/update.
- Se normaliza el tratamiento de `ObjectId`, `Decimal128` y otros
  wrappers BSON a traves de codec, comparacion, wire bridge y motores,
  evitando diferencias entre `mongoeco` y objetos BSON externos y
  eliminando fallbacks silenciosos que enmascaraban errores reales.
- Se corrigen varias rutas de agregacion y query para respetar mejor la
  semantica MongoDB, incluyendo `missing` en `$getField`, regex en
  `$eq`, `$comment` top-level, validacion de `$and/$or/$nor`,
  validacion de `$not`, soporte de `timestamp` en `$currentDate` y
  validaciones de `$rename`/`$bit`.
- El motor en memoria detecta ya conflictos MVCC entre commits
  concurrentes en lugar de sobrescribir silenciosamente cambios.
- Las excepciones publicas de `mongoeco.errors` quedan alineadas con la
  jerarquia de `pymongo.errors`, de modo que `except
  pymongo.errors.X` captura tambien las equivalentes de `mongoeco`
  cuando PyMongo esta instalado.

### Added

- Se anade soporte para updates por pipeline de agregacion en
  `update_one`, `update_many`, `find_one_and_update` y `bulk_write`,
  reutilizando el runtime de agregacion por documento y con fallback
  Python en SQLite cuando no hay traduccion SQL.
- Se amplia la proyeccion avanzada de `find` y `find_one` con soporte
  para `$slice`, `$elemMatch` y proyeccion posicional `"field.$"`,
  incluyendo los caminos manuales de `find_one_and_update` y
  `find_one_and_delete`.
- Se anade soporte para indices TTL con `expireAfterSeconds` en la API
  publica, metadatos de indices, `list_indexes()` e
  `index_information()`, junto con purga oportunista de documentos
  vencidos en los motores `MemoryEngine` y `SQLiteEngine`.
- Se amplia la cobertura sobre `types.py`, helpers de indices,
  validacion de TTL, proyeccion posicional, API de indices y contratos
  de errores compatibles con PyMongo.

### Changed

- La validacion de indices unicos en SQLite incorpora un fast path sobre
  `scalar_index_entries` para indices simples de un campo, reduciendo el
  coste de validacion frente al escaneo completo.

## [2.1.0] - 2026-03-31

### Fixed

- Se corrige la resolucion de anotaciones en
  `mongoeco.api._async.database_commands` para evitar errores en
  Python 3.13+ al combinar forward refs internas con el operador `|`.
- Se corrige un `NameError` en `mongoeco.core.aggregation.runtime`:
  `_subtract_values` dependia de `_require_numeric` sin importarlo,
  rompiendo la ruta interna de resta numero-numero.
- Se elimina la dependencia ansiosa de `bson` sobre la superficie base
  del paquete: `mongoeco`, `AsyncMongoClient` y el quick start con
  `MemoryEngine` vuelven a funcionar desde un wheel instalado en un
  entorno limpio sin extras wire.

### Added

- Se anade soporte para la forma generada por joins correlacionados que
  fijan condiciones de campo con `$and` y `$or` dentro de `$expr`,
  junto con pruebas de unidad e integracion para pipelines `$lookup`.
- Se anade soporte para reutilizar operadores de `query_filter`
  (`$exists`, `$all`, `$nin` y `$elemMatch`) dentro de `$expr` en
  pipelines `$lookup`, reutilizando la semantica de `QueryEngine` y
  actualizando los snapshots de compatibilidad.
- Se blindan con pruebas los joins correlacionados de lista que usan
  `$in` dentro de `$lookup`, incluyendo la variante con rutas
  variables punteadas sobre listas de subdocumentos.
- Se anade una prueba en interprete limpio para validar que las
  anotaciones de `AsyncDatabaseCommandService` se resuelven
  correctamente.
- Se amplian las pruebas de `admin_parsing` y `core.search` para cubrir
  validaciones, normalizacion de entradas y edge cases de busqueda
  textual y vectorial.
- Se anaden pruebas especificas para `driver.transports`,
  `engines.virtual_indexes` y los adaptadores `raw_batch_cursor`,
  elevando la cobertura de esos modulos y reforzando caminos de error,
  roundtrips wire y helpers internos de implicacion.
- Se amplia la cobertura de `change_streams` con pruebas de offsets,
  reanudacion, espera bloqueante, iteracion async y validacion de
  pipelines.
- Se refuerzan `engines.virtual_indexes`, `core.filtering` y
  `api._async.database_admin` con pruebas adicionales sobre helpers de
  implicacion, claves hashables especiales, compilacion de comandos y
  ramas de error en comandos administrativos.
- Se reorganiza la suite para facilitar mantenimiento y nuevas tandas
  de cobertura: la infraestructura sync compartida de integracion se
  mueve a `tests/support.py`, `test_aggregation.py` se divide por
  familias funcionales y `test_architecture.py` se separa por
  responsabilidades.
- Se amplian las pruebas de agregacion sobre `stages`, `runtime` y
  `scalar_expressions`, cubriendo el camino interpretado, optimizaciones
  de ventana para `sort`, helpers BSON y conversiones escalares
  internas.
- Se reorganizan las pruebas de `filtering` separando consultas y
  helpers internos en ficheros distintos, y se amplian los casos de
  cobertura sobre comparacion, tipos BSON, bitwise, membership y
  resolucion de rutas anidadas.
- Se amplian las pruebas de `bson_scalars` sobre overflows, division y
  modulo, rewrap interno, helpers de metadata numerica y rutas bitwise
  con wrappers BSON.
- Se amplia la cobertura de `json_compat`, `driver.topology_monitor`,
  `core.sorting`, `wire.protocol`, `driver.uri` y `core.operators`,
  reforzando ramas de error, roundtrips wire, parsing de URI y helpers
  internos de actualizacion.
- Se anade una prueba de regresion que bloquea `bson` en un subprocess y
  valida que la API base sigue importando y ejecutando operaciones
  simples sin depender de imports ansiosos del runtime wire.
- Se blindan los filtros top-level con `$jsonSchema` dentro de
  `$and`, `$or` y `$nor`, y se fija con pruebas el fallback Python de
  SQLite para esa condicion no traducible a SQL.
- Se anade `scripts/smoke_installed_wheel.py` para reproducir el smoke
  del wheel instalado en un entorno limpio sin reconstruir los comandos
  manualmente.
- Se anade soporte de lectura por subcampo sobre `DBRef` en filtros y
  joins, incluyendo acceso a `"$ref"`, `"$id"`, `"$db"` y `extras`
  mediante dot-paths.

### Changed

- Se endurece el parser de `OP_MSG` para exigir un body kind 0 valido y
  se simplifica `core.sorting` eliminando comparadores internos no
  usados, priorizando correccion del protocolo y mantenibilidad frente a
  cobertura artificial.
- Se desacoplan `raw batches` y el transporte wire de la importacion
  base del paquete moviendo esos imports a resolucion perezosa, lo que
  mantiene `pymongo` como dependencia opcional para esas rutas.

## [2.0.1] - 2026-03-30

### Fixed

- Se corrige un `NameError` al importar `mongoeco.engines.base` en
  Python 3.13+ porque `AsyncIndexAdminEngine` usaba `IndexKeySpec` sin
  importarlo en el modulo.

### Added

- Se anade un smoke test que importa todos los modulos bajo
  `src/mongoeco` en interpretes limpios para detectar errores de
  importacion antes de publicar una nueva version.
- Se anaden pruebas que fuerzan la resolucion de anotaciones en los
  protocolos `Async*Engine` y validan que los paquetes publicos exportan
  simbolos resolubles desde `__all__`.

### Changed

- Se prepara la matriz de CI para Python `3.13` y `3.14`.

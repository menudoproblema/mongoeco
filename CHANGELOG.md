# Changelog

Todos los cambios relevantes de este proyecto se documentan en este
archivo.

El formato sigue las recomendaciones de Keep a Changelog y el proyecto
usa Semantic Versioning.

## [Unreleased]

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

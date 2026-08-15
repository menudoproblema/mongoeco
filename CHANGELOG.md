# Changelog

Todos los cambios relevantes de este proyecto se documentan en este
archivo.

El formato sigue las recomendaciones de Keep a Changelog y el proyecto
usa Semantic Versioning.

## [Unreleased]

## [4.4.0] - 2026-08-15

### Fixed

- Los cursores `find` sync poseen desde su creacion el mismo cursor async y
  `OperationContext` canonicos. `$$NOW`, filtros, proyecciones, `let` y sesion
  quedan capturados una vez, y `clone`, `rewind`, `explain` y cierre ya no
  recompilan una operacion divergente al consumirla. Sus finalizadores delegan
  el cleanup sin bloquear el lock del runner, evitando el deadlock entre el
  recolector y el helper sync cuando se abandona un cursor parcialmente
  consumido.
- Los documentos BSON internos son profundamente inmutables. Planes, comandos
  y snapshots no pueden cambiar por aliasing despues de cruzar la frontera del
  codec, pero la materializacion publica sigue devolviendo `dict` y `list`
  ordinarios e independientes. La propiedad alcanza tambien `DBRef.id`,
  `DBRef.extras`, scopes de `bson.Code` y el subtipo de `Binary`.
- SQLite separa lease, heartbeat, checkpoints y compactacion de la conexion de
  datos cuando usa un fichero. Un fallo del heartbeat se propaga y evita
  confirmar el checkpoint; la entrega queda formalizada como at-least-once.
  Los consumidores locales incorporan identidad de proceso, owner y TTL para
  evitar colisiones por reutilizacion de `id()` y recuperar registros
  efimeros tras cierres abruptos. El TTL se renueva mientras vive el engine,
  no elimina registros con un lease activo y la serializacion/reanudacion se
  prueba entre procesos, incluido el replay posterior a un crash.
- El supervisor de cleanup de snapshots mantiene un registro acotado. Un
  snapshot cuyo cierre supera el timeout permanece `CLOSING`, no `CLOSED`,
  hasta que el cleanup termina o falla de forma observable. El mismo plazo se
  aplica al agotamiento natural sin romper el fast path sync no suspendente.
- Filtros, proyecciones, updates, pipelines y bindings `let`, tambien desde
  comandos administrativos, cruzan una unica frontera BSON recursiva. Los
  datetimes aware se convierten a UTC y precision de milisegundos sin mutar la
  entrada; `findAndModify` y los explains conservan ademas collation y `let`.
- `update_many` y `delete_many` derivan un ordinal de evento por documento.
  SQLite conserva la identidad `(operation_id, event_index)` despues de
  compactar la outbox dentro de la ventana `maxEntries`, evitando perder
  eventos hermanos, duplicar replays recientes o mantener un ledger sin
  limite.
- SPI v2 exige `OperationContext`, una estrategia de lectura declarada e
  imagenes atomicas en outcomes aplicados. Los engines pueden conservar el
  fallback `scan_find_semantics` publicado en 4.3.0 o declarar snapshots
  explicitos. Las capabilities son la unica fuente de verdad, solo se aceptan
  versiones SPI conocidas y no se sobrescriben mediante flags heredados de
  SPI v1. Engines sin batch nativo degradan a inserciones individuales con
  identidad de evento estable.
- `$merge` publica su outcome para engines externos con
  `change_delivery='none'`; los estados imposibles de outcomes y secuencias de
  commit se rechazan en la frontera comun.
- `ReadSnapshot` cierra fuentes que fallan al crear o avanzar el iterador,
  preserva la excepcion original, verifica politica e identidad y protege el
  cleanup ante cancelaciones repetidas sin introducir suspensiones
  artificiales en la fachada sync.
- `OperationContext` valida sus tipos, congela bindings anidados y es la unica
  autoridad de dialecto, collation, `let` y reloj. MongoEco rechaza planes con
  un contexto divergente y evita renormalizar BSON ya interno.
- La outbox SQLite verifica tipo y hash del payload en replays idempotentes,
  incluso despues de compactar la fila viva. Los outcomes de una transaccion
  abierta pueden diferir la secuencia hasta el commit efectivo.
- El dispatcher SQLite drena todos los lotes confirmados al iniciar la entrega
  y nunca invoca consumidores externos bajo el lock del engine. La identidad
  idempotente usa un hash del efecto completo, tambien para huecos sin payload,
  y el esquema de outbox se migra de forma versionada, atomica y serializada.
  Los leases persistentes serializan cada consumidor entre instancias, esperan
  contencion sin perder solicitudes y protegen checkpoints con una generacion;
  un gate compartido por ruta rechaza ademas la reentrada entre engines del
  mismo proceso y no se retira mientras queden owners o waiters activos.
- La materializacion publica elimina siempre los marcadores privados del codec,
  incluso en documentos y arrays anidados. Los bindings bloquean tambien
  operadores in-place como `|=` y los snapshots aplican un plazo de cleanup
  finito sin dejar excepciones de tareas sin observar.
- Las operaciones y semanticas ligadas rechazan dialecto, collation o variables
  que contradigan su `OperationContext`; el adapter valida igualmente llamadas
  SPI realizadas mediante keywords, sin depender de indices posicionales. El
  binding reutiliza planes equivalentes y recompila cuando cambia una entrada
  semantica, sin volver a normalizar valores BSON ya internos.
- La evaluacion de aggregation usa frames explicitos para `ROOT`, `CURRENT` y
  bindings lexicos, evitando que etapas recursivas como `$redact` pierdan la
  raiz al descender. Los snapshots exponen lifecycle y errores de cleanup
  supervisado incluso cuando el cierre supera su timeout.
- CI deriva el artefacto publicado y la base del ratchet desde las etiquetas,
  fija acciones por SHA y usa constraints reproducibles solo durante la
  instalacion controlada. Wheel y sdist se construyen una vez; una etiqueta
  `v*` publica exactamente esos artifacts mediante Trusted Publishing despues
  de superar ambas versiones de Python y verificar etiqueta y version.
- La suite completa de CI se ejecuta contra el wheel inmutable descargado, con
  verificacion SHA-256 y de import desde `site-packages`. El ratchet compara
  diagnosticos Ruff completos entre la etiqueta base y HEAD, por lo que detecta
  tambien regresiones estructurales reportadas fuera de la linea editada.
- Las fronteras privadas SQLite con muchos parametros usan argumentos
  keyword-only y la lectura directa recibe un unico `OperationContext`,
  evitando los nuevos `PLR0917`/`RUF036` sin ocultarlos en el baseline.

## [4.3.0] - 2026-08-12

### Added

- `mongoeco.engines` exporta el contrato publico SPI v2: capabilities,
  `OperationContext`, outcomes tipados y snapshots explicitos. Se anade una
  guia de migracion para engines externos y un contrato ejecutable comun para
  Memory y SQLite.
- El commit log de Memory y la outbox SQLite aplican retencion configurable por
  checkpoints. SQLite persiste consumidores durables y expone diagnosticos de
  secuencia, suelo compactado y entradas pendientes.
- CI incorpora cobertura minima del 99% y un ratchet Ruff que rechaza errores
  introducidos en lineas nuevas sin bloquear por deuda historica ajena.

### Deprecated

- El SPI v1 de engines queda deprecado y emite `DeprecationWarning` una vez por
  clase. `LegacyEngineAdapter` se conserva durante 4.x y se retirara en 5.0.0.

### Changed

- Los engines integrados declaran un SPI v2 mediante `EngineCapabilities` y
  retornan outcomes estables (`MutationOutcome`, `DeleteOutcome`,
  `InsertOutcome` y `MergeOutcome`). Los engines v1 quedan aislados en
  `LegacyEngineAdapter` en vez de dispersar flags privados y `hasattr` por la
  API.
- Cada operacion compilada cruza una sola frontera inmutable
  `OperationContext`, que conserva dialecto, collation, `let`/`$$NOW`, codec,
  sesion y politica de change events. Las lecturas por cursor poseen un
  `ReadSnapshot` explicito con politica y lifecycle cerrable.
- `MemoryEngine` asigna una secuencia monotona al commit efectivo y
  `SQLiteEngine` persiste escritura y change event/hueco en un outbox dentro de
  la misma transaccion. El dispatcher entrega por secuencia, tolera replay y
  usa el journal del hub como checkpoint durable tras reinicios.
- `$merge` delega cada documento en una primitiva atomica del engine: la
  comprobacion de identidad, la politica `whenMatched`/`whenNotMatched`, la
  validacion, los indices y el change event pertenecen a una sola escritura.
- La composicion streaming de `$skip` y `$limit` conserva el orden de los
  stages y produce la misma ventana que la ejecucion materializada.
- Las mutaciones seleccionadas resuelven seleccion, revalidacion, escritura e
  imagenes `before`/`after` dentro de una unica frontera atomica del engine.
  Memory usa el lock de coleccion y SQLite una transaccion de escritura.
- Los cursores con batching mantienen una sola fuente estable hasta agotarla;
  `rewind()` retira la fuente anterior y `close()` libera tambien fuentes
  parciales. Los cursores async de agregacion, metadata y change streams son
  awaitables sin perder su uso directo.
- La publicacion de change events ocurre en el mismo orden que las mutaciones
  confirmadas y antes del profiling. Un fallo posterior del journal no
  reclasifica la escritura como fallida: degrada explicitamente el stream y
  queda visible en `change_stream_state()`.

### Fixed

- `find_one` ya no omite el contexto/snapshot cuando degrada del lookup directo
  al scan, y la normalizacion de collation acepta de forma idempotente un
  `CollationSpec` ya normalizado.
- Los abortos SQLite revierten conjuntamente documento y fila de outbox; las
  transacciones Memory no consumen secuencia ni publican eventos hasta su
  commit. Un replay repetido no duplica eventos ya presentes en el journal.
- La secuencia confirmada sobrevive a la compactacion total de la outbox y los
  inserts idempotentes no consumen huecos de autoincremento. Se rechazan tanto
  consumidores por detras del suelo podado como checkpoints por delante del
  ultimo commit, en vez de fabricar continuidad.
- Los indices de Memory usan claves de igualdad BSON, por lo que numeros
  equivalentes de distintos tipos numericos conservan el mismo resultado antes
  y despues de crear un indice. SQLite degrada a evaluacion Python cuando un
  decimal no puede representarse con seguridad en su pushdown numerico.
- Los hints sobre indices parciales con collation solo son elegibles cuando la
  operacion declara exactamente la misma collation; la inferencia de
  implicacion usa ese mismo comparador.
- Los fallos de fuentes async, raw batches y productores SQLite cierran la
  fuente exactamente una vez y conservan la excepcion original, incluso tras
  producir un lote parcial.
- `bulk_write` valida y copia `array_filters` durante la preparacion, pero
  delega su unica normalizacion BSON a la frontera comun de compilacion.
- Los upserts de `find_one_and_update` y `find_one_and_replace` publican un
  evento `insert` tambien con `ReturnDocument.BEFORE`; updates y replacements
  sin cambios ya no generan change events.
- Los raw batches dejan de reabrir desde el principio una fuente agotada y los
  cursores con `limit` exacto cierran su fuente sin dejar productores activos.
- La inferencia de indices parciales usa igualdad y orden BSON en vez de
  igualdad Python, y admite valores no hashables en `$in` sin alterar
  resultados por una seleccion de indice insegura. La pertenencia al indice
  respeta tambien la collation declarada por el propio indice.
- El reloj de ejecucion de TTL, creacion de indices y comandos wire queda
  acotado al comando y se restaura al finalizar, sin filtrar estado temporal a
  operaciones posteriores.
- `count` administrativo cruza la misma frontera `OperationContext` que el
  resto de lecturas, conservando dialecto, sesion, variables y reloj.
- `insert_many` comparte el contexto temporal de toda la operacion;
  `update_many(..., upsert=True)` no reentra por la API publica, y `$merge`
  usa primitivas atomicas para reemplazar, fusionar o insertar el destino.
- `findAndModify` construye valor y metadata desde un unico outcome atomico;
  los cursores raw y de agregacion cierran sus fuentes, y el productor SQLite
  aplica backpressure sin bloquear al cancelar o agotar parcialmente.
- El adaptador de write models PyMongo concentra y valida su layout privado,
  con un error de compatibilidad explicito si una version futura lo cambia.

## [4.2.1] - 2026-08-11

### Fixed

- `array_filters` atraviesa una unica vez la misma frontera BSON que los
  documentos persistidos en `update_one`, `update_many`,
  `find_one_and_update` y `bulk_write`, para superficies sync y async. Los
  `datetime` anidados se convierten a UTC naive con precision de milisegundos
  sin mutar el argumento del caller.
- `find_one_and_update` revalida dentro del lock del engine tanto la identidad
  preseleccionada como el filtro original completo. Un competidor que pierde
  una condicion CAS devuelve no-match, no modifica el documento y no publica
  un change event.
- La misma revalidacion atomica cubre `update_one` con `sort` o `hint`,
  `update_many`, `replace_one`, `find_one_and_replace`, `delete_one`,
  `delete_many` y `find_one_and_delete` en Memory y SQLite, preservando los
  documentos legacy sin `_id`.

## [4.2.0] - 2026-08-11

### Added

- Las expresiones de agregacion `$min`, `$max`, `$sum` y `$avg` funcionan en
  proyecciones y updates por pipeline, incluida la semantica distinta de
  operandos array unicos y listas de expresiones.
- `bulk_write` acepta directamente los seis modelos oficiales de PyMongo:
  `InsertOne`, `UpdateOne`, `UpdateMany`, `ReplaceOne`, `DeleteOne` y
  `DeleteMany`, preservando `collation`, `array_filters`, `hint` y `sort`.
- Los cursores async admiten `sort(key, direction)`, consumo incremental con
  `to_list(length=...)` y cierre explicito; la superficie sync conserva paridad
  para esas firmas.

### Changed

- La frontera publica devuelve tipos BSON oficiales de PyMongo, incluidos
  `bson.ObjectId` en documentos y resultados de escritura.
- Todas las entradas de coleccion normalizan `datetime` a UTC naive y precision
  BSON de milisegundos antes de filtrar, agregar o persistir.
- `aggregate()` captura `$$NOW` al crear el cursor, de modo que consumirlo mas
  tarde no cambia el instante del comando.

### Fixed

- Las selecciones internas de escrituras mantienen tipos BSON internos; esto
  evita perder la identidad de almacenamiento en SQLite despues de exponer un
  `_id` publico como `bson.ObjectId`.
- Los modelos bulk oficiales con `pymongo.collation.Collation` se adaptan a un
  documento interno sin acoplar el core a tipos del driver.
- Los `_id` generados mutan los documentos de entrada con `bson.ObjectId` y los
  upserts parciales de `BulkWriteError.details` tampoco filtran IDs internos.
- Proyecciones, filtros parciales de indices y pipelines de change streams
  atraviesan la misma frontera BSON que el resto de operaciones. SQLite
  persiste la metadata de esos indices con el codec reversible.
- La metadata publica de indices, los eventos de change streams y el scope de
  `bson.Code` materializan recursivamente los tipos BSON oficiales; los flags
  BSON `l` y `u` de `bson.Regex` se conservan en round trips.

## [4.1.1] - 2026-08-11

### Fixed

- `MemoryEngine` ya no descarta coincidencias de igualdad en rutas anidadas
  cuando existe un indice sobre esa ruta. La seleccion de candidatos conserva
  las semanticas de indices parciales, sparse, ocultos, multikey y collation.
- Los indices unicos de Memory y SQLite comparan correctamente entradas
  multikey, rutas anidadas que atraviesan arrays, collations e indices
  compuestos; tambien rechazan arrays paralelos en un indice compuesto.
- SQLite valida la unicidad logica por base y coleccion, evitando que un indice
  fisico compartido imponga una restriccion entre namespaces distintos.

## [4.1.0] - 2026-07-26

### Added

- `AsyncMongoClient` y `MongoClient` aceptan `now_factory`, una fuente de hora
  opcional para tests deterministas. Se propaga a bases, colecciones,
  `with_options()`, comandos, cursores, subpipelines y lotes clásicos.
- Los engines locales Memory y SQLite declaran soporte de reloj inyectable. TTL
  se evalúa con el mismo instante de la operación, por lo que puede probarse
  sin esperar al reloj de pared.

### Changed

- `$$NOW` y `$currentDate` resuelven la hora en el borde de cada comando real
  o lote lógico; la hora no se guarda en planes de update cacheados.

## [4.0.0] - 2026-07-26

### Breaking changes (4.0)

- Una referencia a variable `$$...` que no exista deja de convertirse
  silenciosamente en `None`: ahora falla con `OperationFailure` código `17276`.
  Consulte la [guía de migración de la versión 4.0](docs/release-4.0.0.md)
  antes de actualizar.
- Las fechas generadas por `$currentDate` se truncan a milisegundos, que es la
  precisión BSON observable.
- `$$REMOVE.path` se comporta como un valor ausente al calcular campos en
  `$project`, `$addFields` y `$set`, en lugar de escribir `null`.

### Added

- `$$NOW` es efectivo en los dialectos MongoDB 7.0 y 8.0 mediante un contexto
  de ejecución inmutable, UTC naïve y truncado a milisegundos, compartido por
  cada comando real o lote clásico de `bulk_write`.
- `find` y `count_documents` declaran soporte efectivo para `let`; las
  variables inexistentes informan `OperationFailure` código 17276.

### Changed

- Los planes de consulta y de updates por pipeline ya no almacenan bindings;
  éstos viajan exclusivamente por la ejecución. Los cursores, agregaciones,
  subpipelines, comandos de base de datos, wire y evaluadores directos del
  núcleo preservan el mismo contexto.
- `$currentDate` conserva su evaluación por documento, pero sus fechas usan la
  precisión BSON de milisegundos.
- `bulk_write` usa lotes lógicos clásicos de hasta 100.000 modelos y captura
  un `$$NOW` por lote, sin pretender emular `bulkWrite` ni límites BSON.

### Fixed

- `$$REMOVE.path` conserva *missing* sólo al calcular campos de
  `$project`/`$addFields`/`$set`; en expresiones de valor se normaliza a `null`.
  Además, `$$REMOVE` ya no puede filtrarse a `$ifNull` ni a acumuladores.
- SQLite preserva `let` y el contexto temporal al borrar, y el *pushdown* de
  `$search` ya no descarta `$match` con `$expr` dependientes de variables.

## [3.6.0] - 2026-07-05

### Changed

- Los facades publicos (`mongoeco`, `_types`, `mongoeco.api`,
  `mongoeco.compat`, `mongoeco.cxp`, `core.aggregation`, `driver`, `engines`
  y `wire`) resuelven exports bajo demanda, conservando `__all__` y reduciendo
  coste/ciclos de importacion. `mongoeco.types` conserva su contrato de
  agregador solo-imports.
- Los tests de imports y exports publicos ejecutan los subprocess en paralelo
  con salida determinista por nombre de modulo o paquete.
- Los exports raiz de `mongoeco.__all__` se validan tambien con un interprete
  frio por simbolo, cubriendo el caso canonico `from mongoeco import X`.
- Se añaden benchmarks locales de contencion sync y hot paths sync para
  comparar cliente compartido/por worker y medir `find_one`, `update_one` y la
  compilacion de updates sobre workloads fijos.
- `CompiledQuery` y `compile_update_operation` reutilizan plantillas compiladas
  con LRU acotado y valores parametrizados por instancia, evitando recompilar el
  mismo shape sin mezclar literales entre filtros o updates.
- Los tests de change streams usan ventanas `max_await_time_ms` mas cortas en
  esperas negativas, reduciendo cola sin cambiar el contrato publico.

### Fixed

- El profiler evita planificar y construir payloads pesados cuando el profiling
  esta desactivado o la operacion queda por debajo de `slow_ms`; los comandos de
  insert con `deepcopy()` se construyen ahora solo si pueden registrarse.
- Los change streams locales saltan la materializacion de eventos cuando no hay
  watchers ni journal persistente. Las escrituras omitidas dejan un gap
  explicito, y reanudar con un token anterior al gap falla con el error publico
  de token no disponible en vez de saltarse eventos en silencio.
- Los snapshots de rollback del `MemoryEngine` copian contenedores de estado sin
  duplicar todos los documentos, reutilizando la invariante MVCC de documentos
  tratados como inmutables por reemplazo.
- La validacion de unicidad de `_id` evita scans completos en Memory y SQLite,
  incluido `insert_many`, manteniendo los unique secundarios sin cambios.
- `MemoryEngine` usa point lookup por `_id` en `update_one` y `delete_one`
  cuando el filtro lo permite, resolviendo contra la vista MVCC activa dentro de
  transacciones.
- `AggregationCursor` sync agrupa la iteracion por chunks para reducir cruces
  sync/async por documento.
- `MemoryEngine` cachea por instancia la inspeccion de la firma del codec de
  decode e invalida si cambia el codec o su callable `decode`.
- `count_documents` en Memory cuenta sobre documentos internos filtrados sin
  materializar documentos publicos cuando no es necesario.
- El cliente sync reutiliza un helper persistente y lazy solo cuando una llamada
  sync se hace desde un loop activo, permite completar inline operaciones
  cortas de `MemoryEngine` sin sesiones bajo el mismo lock del runner, y evita
  deadlocks si el cierre se solicita desde el propio helper.

### Known Debt

- SQLite mantiene fallback Python O(N) para algunos unique secundarios
  compuestos; queda fuera de este ciclo porque requiere otra pasada sobre los
  indices auxiliares.
- SQLite `find` sin indice puede caer a full-scan con `json_extract` por fila;
  la correccion real pertenece al planner y queda fuera de esta release.

## [3.5.1] - 2026-06-28

### Added

- Se añade un caso diferencial contra MongoDB real para una matriz de filtros
  booleanos que cubre `$or` con campos hermanos, `$and` anidado, `$nor`,
  `$expr` y dotted paths sobre arrays en `find()` y `aggregate()`, junto con
  replay golden local para Memory y SQLite.

### Changed

- Se centraliza la clasificacion de operadores de query y la validacion de
  listas de clausulas booleanas en `mongoeco.core.query_operators`, compartida
  por `$match`, prefilters de search/vector, `$pull`, `$elemMatch`, upserts y
  traduccion SQL.

### Fixed

- Las operaciones basadas en documentos previamente seleccionados validan ahora
  identidad estable por `_id`/storage key en vez de comparar el documento
  completo, evitando falsos `selected document storage mismatch` en rutas como
  `delete_many()`, `find_one_and_delete()`, `update_many()`,
  `find_one_and_update()` y `find_one_and_replace()`.
- `aggregate()` acepta el alias PyMongo `allowDiskUse` ademas de
  `allow_disk_use`, y el mock async incorpora soporte de `bulk_write()` para
  los casos `UpdateOne`/upsert usados por migraciones idempotentes.
- `$match` y los prefilters locales preservan correctamente combinaciones de
  `$or` con campos hermanos, `$expr`, `$and`/`$nor` anidados y dotted paths
  sobre arrays, evitando falsos negativos en aggregation y filtros downstream
  de `$search`/`$vectorSearch`.
- `$pull` soporta condiciones con operadores top-level como `$or`, `$and`,
  `$nor` y `$expr` sobre documentos embebidos, y las rutas posicionales de
  update/projection manejan selectores de array con `$or` y campos hermanos.
- Los filtros con claves de campo no-string fallan o degradan de forma
  controlada en `compile_filter()`, prefilters y upsert seeding, evitando
  `AttributeError` en rutas indirectas de optimizacion.
- La traduccion SQL de nodos booleanos vacios queda alineada con la semantica
  canonica local: `AndCondition(())` es verdadero y `OrCondition(())` es falso.

## [3.5.0] - 2026-06-27

### Changed

- Se añade soporte oficial para el perfil de compatibilidad PyMongo 4.17,
  incluyendo resolucion explicita, deteccion `auto-installed`, snapshots de
  catalogo y matriz de superficie publica contrastada con PyMongo real.
- La matriz de perfiles PyMongo puede generar directamente el resumen estable
  usado como fixture, evitando edicion manual al incorporar nuevas versiones.
- Se centraliza la validacion de documentos seleccionados contra storage key en
  `mongoeco.core.identity`, reduciendo duplicacion entre API, `$merge`, Memory
  y SQLite.
- Se centraliza la atomicidad de escrituras SQLite en un scope interno comun y
  la restauracion de snapshots Memory en scopes de coleccion/runtime, reduciendo
  duplicacion de begin/commit/rollback y snapshot/restore.

### Fixed

- Se alinea la semantica de `_id` en updates/upserts con MongoDB:
  los operadores clasicos y pipeline updates preservan el `_id` final,
  rechazan cambios reales y evitan corrupcion entre documento y storage key.
- Se rechaza `_id` raiz de tipo array en inserts, bulk inserts y upserts,
  manteniendo validos los documentos `_id` con arrays descendientes.
- Las escrituras y borrados seleccionados sobre documentos legacy con `_id`
  raiz de tipo array fallan con `code=53` antes de mutar o eliminar datos.
- Los `writeErrors` de `bulk_write` y comandos admin conservan indice y codigo
  MongoDB al propagar violaciones de escritura sobre `_id`.
- Los replacement updates (`replace_one` y `find_one_and_replace`) reportan
  `code=66` cuando intentan cambiar el `_id`.
- Las escrituras sobre documentos legacy sin `_id` o con `_id` desalineado
  respecto al storage key ya no producen `KeyError`, no inventan `_id: None`
  en replacements, bloquean retargets corruptos y evitan que los lookups por
  la storage key antigua devuelvan o borren un documento con otro `_id`.
- Los borrados directos del engine y las inserciones nuevas ya no pueden operar
  sobre la storage key antigua ni duplicar un `_id` que ya existe en el payload
  de un documento corrupto.
- Las operaciones seleccionadas (`update_many`, `find_one_and_update`,
  `find_one_and_replace`, `delete_many` y `find_one_and_delete`) vuelven a poder
  operar sobre documentos legacy sin `_id` cuando viven bajo la storage key
  estable, mientras `delete_one` y los deletes internos rechazan documentos cuyo
  `_id` del payload no corresponde a su storage key.
- Los updates no-op sobre documentos cuyo `_id` del payload no corresponde a su
  storage key ya no reportan `matched_count=1`; ahora fallan con `code=66` antes
  de aplicar el operador.
- `$merge` valida que el documento target encontrado por `_id` tenga una storage
  key coherente antes de resolver `whenMatched`, incluyendo `keepExisting`, para
  evitar éxitos silenciosos sobre targets corruptos.
- El purgado TTL oportunista valida `_id` antes de borrar, evitando que
  documentos legacy con `_id` raiz array o storage key desalineada desaparezcan
  durante lecturas o escrituras no relacionadas.
- La creacion de indices TTL prevalida los documentos que quedarian expirados:
  si detecta identidad corrupta falla sin registrar parcialmente el indice.
- Las actualizaciones clasicas aplicadas con `UpdateEngine.apply_update` son
  atomicas ante errores runtime: si una instruccion posterior falla, el
  documento de entrada queda intacto.
- La creacion de indices SQLite hace rollback tambien ante errores runtime no
  `IntegrityError` (por ejemplo deadlines durante el backfill), evitando que
  queden metadatos o indices fisicos parcialmente creados en una transaccion
  abierta.
- Las rutas SQLite de update/upsert/delete hacen rollback ante errores runtime
  durante la reconstruccion o limpieza de indices/search, no solo ante
  `IntegrityError`, evitando que un commit posterior persista documentos ya
  modificados o borrados parcialmente.
- Las escrituras SQLite dentro de una transaccion de sesion usan savepoints por
  operacion, evitando que un fallo intermedio deje cambios parciales que luego
  puedan confirmarse con el commit externo de la sesion.
- Las rutas SQLite invalidan caches y metadatos antes de liberar el savepoint o
  confirmar la escritura, de modo que un fallo en callbacks post-escritura no
  deje cambios parciales confirmables dentro de transacciones de sesion.
- El purgado TTL oportunista que se dispara despues de crear un indice SQLite es
  best-effort: si falla tras haberse confirmado el indice, ya no convierte la
  creacion exitosa del indice en una excepcion con cambios persistidos.
- Los rollbacks SQLite limpian caches runtime sensibles (`collection_id`,
  indices fisicos multikey y backends search/vector), evitando que una cache
  adelantada sobreviva a un savepoint o transaccion abortada.
- Los rollbacks y cambios de backend search SQLite limpian tambien las caches
  auxiliares de ranking compound, evitando reusar buckets antiguos si una
  version search vuelve a `0` tras abortar una transaccion.
- Los change streams locales ya no publican eventos de escrituras dentro de
  transacciones abortadas; los eventos se encolan en la sesion y solo se
  publican despues de que el commit del engine haya tenido exito.
- El fast path SQLite de seleccion del primer documento respeta la sesion de
  lectura, evitando fallos al preleer documentos para eventos dentro de una
  transaccion activa.
- Los fallos del hub local de change streams ya no hacen fallar escrituras o
  commits que el engine ya habia completado, evitando resultados ambiguos con
  datos persistidos y excepciones de publicacion.
- Los fallos de profiling y metadata operacional son best-effort y ya no
  enmascaran operaciones completadas ni errores originales del engine.
- Las sesiones cerradas se validan antes de entrar en los flujos publicos de
  cliente, base de datos, coleccion y comandos, evitando que inserts, bulk
  writes, indices o cambios de namespace se apliquen antes de fallar al
  actualizar metadata causal.
- `create_search_indexes()` deshace ahora los search indexes que haya creado en
  la misma llamada si un modelo posterior falla, igualando el comportamiento
  atomico que ya tenia `create_indexes()`.
- `renameCollection` con `dropTarget: true` valida que el origen exista y que
  origen y destino sean distintos antes de borrar el destino, evitando perder la
  coleccion destino cuando el comando acaba fallando.
- La limpieza de profiler tras `drop_database()` es best-effort en Memory y
  SQLite, evitando que un fallo de observabilidad convierta un drop ya aplicado
  en excepcion publica.
- SQLite invalida caches de `drop_database()` antes del commit, de modo que un
  fallo en esa fase revierte la escritura en vez de dejar la base borrada con
  una excepcion publica.
- `insert_many()` mantiene semantica de insert en Memory cuando existe un `_id`
  duplicado en el lote, conserva el documento previo y no ejecuta documentos
  posteriores al fallo; los inserts previos al error publican sus eventos de
  change stream.
- `insert_many()` en Memory trata tambien los duplicados de indices unicos
  secundarios como fallo ordenado del documento actual, alineandose con SQLite
  y publicando los eventos de los inserts previos al error.
- Memory prevalida los documentos de `insert_many()` contra el validador de
  coleccion antes de escribir, evitando batches parcialmente insertados cuando
  un documento posterior falla validacion.
- `MemoryEngine.create_index()` revierte ahora el indice, sus datos auxiliares
  y el registro de coleccion creado implicitamente si falla un paso posterior
  del flujo, evitando que una excepcion deje metadatos de indice parciales.
- Las mutaciones de search indexes en Memory (`create`, `update` y `drop`)
  ejecutan los pasos que pueden fallar antes de alterar el catalogo, evitando
  que un fallo de invalidacion de cache deje cambios aplicados pese a la
  excepcion publica.
- Las mutaciones de search indexes en Memory (`create` y `update`) restauran
  tambien catalogo y caches runtime si falla la marca de indice pendiente tras
  iniciar la mutacion.
- `MemoryEngine.drop_collection()` invalida caches antes de borrar el namespace,
  de modo que un fallo en esa fase no elimina la coleccion mientras la llamada
  publica acaba fallando.
- Las escrituras Memory de documentos (`insert`, `update`, `upsert` y `delete`)
  invalidan caches antes de tocar storage e indices, evitando mutaciones
  persistidas cuando la llamada termina fallando por esa invalidacion.
- Memory restaura ahora el estado de coleccion si falla la codificacion o la
  actualizacion de indices durante inserts, updates, upserts, deletes o purgado
  TTL, evitando documentos persistidos sin indices coherentes.
- Memory protege tambien `drop_index`, `drop_indexes`, `drop_search_index` y
  `drop_collection` con snapshots de coleccion/runtime, evitando catalogos o
  caches parciales si falla una poda o invalidacion intermedia.
- `MemoryEngine.rename_collection()` restaura origen, destino y caches runtime
  si falla un paso intermedio del rename, evitando namespaces parcialmente
  movidos.
- `MemoryEngine.create_collection()` copia las opciones antes de registrar el
  nombre, evitando namespaces fantasma si falla la normalizacion de opciones.
- SQLite marca como asegurados los indices fisicos multikey y backends FTS solo
  despues de que su commit interno haya terminado, evitando caches adelantadas
  si ese commit falla.
- `MemoryEngine` conserva el snapshot MVCC cuando un commit detecta conflicto
  de escritura, evitando que la sesion siga activa sin aislamiento y que
  escrituras posteriores se filtren al storage global.
- SQLite conserva el owner transaccional si fallan `commit()` o `rollback()`,
  y no pierde el savepoint si falla `ROLLBACK TO SAVEPOINT`, evitando sesiones
  activas con estado engine ya limpiado.
- Las sesiones transaccionales de otro cliente/engine se rechazan en Memory y
  SQLite, en vez de ejecutar lecturas o escrituras fuera de la transaccion que
  luego no serian afectadas por `abort_transaction()`.
- Las sesiones de otro cliente/engine se rechazan tambien fuera de transaccion
  en Memory y SQLite, incluyendo lecturas, escrituras, planning y comandos de
  profiling, evitando mezclar ownership y metadata causal entre clientes.
- Los comandos admin estaticos como `ping` y `hello` tambien validan la
  pertenencia de la sesion al engine antes de responder, cerrando rutas que no
  pasaban por el storage engine.
- Los atajos internos de `system.profile` validan ahora la pertenencia de la
  sesion antes de leer, listar, borrar o limpiar entradas del profiler, evitando
  que una sesion de otro cliente acceda o borre observabilidad local.
- Los fallbacks de lectura SQLite, incluido `count_documents()` con filtros que
  requieren evaluacion Python, validan la pertenencia de la sesion antes de
  cargar documentos locales.
- La ejecucion directa de comandos admin ya parseados valida ahora la
  pertenencia de la sesion antes de resolver comandos estaticos, `currentOp`,
  `killOp` o `configureFailPoint`.
- Los stages informativos de aggregation, como `$currentOp`, validan ahora la
  pertenencia de la sesion antes de tomar snapshots runtime del engine.
- `ClientSession.close()` conserva la sesion activa si falla el abort de una
  transaccion pendiente, permitiendo reintentar el rollback en vez de dejar el
  engine con owner transaccional inaccesible.
- `endSessions` en el wire proxy solo elimina una sesion del store despues de
  cerrarla correctamente, evitando perder la referencia si el abort implicito
  falla.

## [3.4.0] - 2026-04-09

### Added

- Controlador local de failpoints de driver para pruebas de runtime con
  `configureFailPoint`, incluyendo fallos retryable, errores transitorios de
  transaccion, timeouts de write concern, timeouts de seleccion de servidor y
  scoping por namespace.
- Reintentos de `with_transaction()` a nivel de cliente ante errores
  transitorios del callback o del commit, con eventos de monitorizacion
  coherentes para los intentos del driver.
- Proyeccion estricta `mongoeco.compat.export_mock_safe_profile_catalog()` para
  tooling de mocks/tests sobre el capability model canonico CXP.
- Baseline diferencial local con fixture golden y tests de replay para comparar
  Memory y SQLite contra casos derivados de MongoDB real.
- Stages locales de introspeccion de aggregation, incluyendo `$currentOp`,
  `$indexStats`, `$planCacheStats` y `$listSessions`.
- Soporte ampliado de aggregation con `$redact` y operadores/acumuladores de
  ventana avanzados.
- Nuevas capacidades locales de `$search` y `$searchMeta`: count/facet,
  metadatos `includeMeta`, facets tipados para boolean, ObjectId y UUID,
  highlight `maxNumPassages`, regex flags/options, wildcard/autocomplete
  enriquecidos, fuzzy autocomplete y semantica clasica `$text` con frases,
  exclusiones e indices textuales hinted.
- Mejoras de `$vectorSearch` y explain de retrieval: orden determinista,
  fuentes de prefiltrado hibrido, pruning summary, retention ratios y paridad
  estable entre Memory y SQLite.
- Compatibilidad administrativa de indices para `background`,
  `wildcardProjection`, `min`, `max` y `bucketSize` como metadata local
  validada.

### Changed

- Se regeneran snapshots de compatibilidad y catalogos CXP para reflejar la
  surface real de search, vector search, `searchMeta`, indices y runtime local.
- Se endurece la ordenacion determinista de resultados empatados en search y
  vector search para que Memory y SQLite tengan resultados reproducibles.
- Los eventos de monitorizacion del driver incluyen metadata mas rica en
  refresh de topologia, seleccion fallida, failpoints y planes de ejecucion.
- La documentacion de search/vector aclara ratios de retencion, fuentes de
  prefiltrado, semantica de `searchMeta` count/facet y fronteras de negociacion
  CXP.

### Fixed

- Se corrigen ramas de cobertura y paridad en search/vector, incluyendo regex
  analizado, fallback SQLite, facets tipados, orden estable y explain
  determinista.
- Se valida la pertenencia de documentos a indices virtuales durante expiracion
  TTL para evitar que el purgado use metadata incompatible.
- Se estabiliza la exposicion de failpoints y writeConcernTimeout en comandos
  de insert y rutas de driver, preservando monitor events esperados.

### Quality

- Se anade smoke de contrato publicado contra PyPI para `3.3.0` y se deja la
  release `3.4.0` preparada con version bump y validacion de snapshots.
- La suite de tests y coverage de la release se mantiene cerrada en `100%`
  segun la validacion local de `3.4.0`.

## [3.3.0] - 2026-04-08
### Added

- Integracion con `cxp>=3.0.0` como base canonica de contrato para
  `database/mongodb`, manteniendo `mongoeco.cxp` como fachada controlada y
  `mongoeco.compat` como proyeccion.
- Exportes publicos de CXP por operacion y capability con metadata de
  telemetria canonica (`spans`, `metrics`, `events`) consumible por tooling
  sin heuristicas de nombres.
- Proyeccion de telemetria del driver con atributos canonicos de recurso
  (`cxp.resource.name`, `cxp.resource.kind`) para snapshots comparables entre
  providers.
- Cierre de metadata publica `mongodb-platform` para
  `collation`/`persistence`/`topology_discovery` en capas de
  capabilities/operations/profiles con cobertura de tests.

### Changed

- `mongoeco.compat` delega la exportacion de `profiles`,
  `profileSupport` y `operations` a los exports canonicos de `mongoeco.cxp`,
  evitando drift entre superficies.
- Se limpia la surface publica 3.x retirando aliases legacy de root y
  reforzando una forma CXP-first mas explicita.
- Se amplia el subset local de busqueda textual (`$text`) y las validaciones
  de metadata de indices en `memory` y `sqlite`, con explicaciones mas ricas.

### Quality

- Suite de tests y coverage cerradas a `100%` en `src/mongoeco`.

## [3.2.0] - 2026-04-06
### Added

- CXP pasa a actuar como fuente canonica del capability model publico para
  `database/mongodb`, y `compat`, `explain()` y la narrativa publica se
  proyectan ya desde ese modelo en vez de mantener fuentes paralelas.
- `mongoeco` expone ahora su contrato publico en vocabulario canonico de CXP
  para `database/mongodb`, sin asumir responsabilidades de provider ni de
  resolucion de instancias.
- El catalogo local `database/mongodb` canoniza ahora tambien operaciones
  publicas de primer nivel (`find`, `insert_one`, `update_one`,
  `aggregate`, `watch`, `with_transaction`, etc.) para que los snapshots CXP
  puedan declarar bindings interoperables y no solo capabilities gruesas.
- El export publico de compatibilidad incluye ya un bloque top-level `cxp`, y
  `local_runtime_subsets` queda como proyeccion legacy derivada del modelo
  canonico, no como fuente primaria de verdad.
- `find(...).explain()` y `aggregate(...).explain()` exponen ya un bloque
  top-level `cxp` para dejar visible que capability publica de
  `database/mongodb` se esta ejerciendo.
- Los snapshots CXP publican ya bindings de operaciones publicas por
  capability, y `aggregation` expone ademas metadata estructurada del subset
  real soportado (`supportedStages`, expresiones y acumuladores) en vez de
  fingir soporte total del lenguaje de agregacion.
- El subset local de `$search` soporta ahora tambien `equals`, `range`, `in`,
  `regex` y `phrase.slop`, manteniendo `MemoryEngine` como baseline semantico
  y `SQLiteEngine` como pushdown/fallback honesto segun el shape real.
- `$vectorSearch` deja mas visibles sus knobs y metadata publicas de producto:
  `similarity`, `numCandidates`, `minScore`, `vectorSearchScore`, residual
  filter y exact fallback diagnostics.
- La documentacion publica gana ahora ejemplos ejecutables bajo `examples/`,
  guias cortas de casos de uso, comparativas frente a MongoDB real y
  `mongomock`, y una narrativa de release ya ordenada por capabilities
  canonicas.

## [3.1.0] - 2026-04-04

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
- El runtime local de `$search` amplía ya su subset documentado con
  `autocomplete`, `wildcard` y `compound`, manteniendo `autocomplete`
  empujable a FTS5 en `SQLiteEngine` y dejando `wildcard` y `compound` como
  fallback Python explícito cuando no hay una traducción honesta a backend.
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
  `dotProduct` y `euclidean`, y `SQLiteEngine` materializa indices ANN locales
  con `usearch` manteniendo `MemoryEngine` como baseline exacta. Esa surface
  queda reflejada ya en explain, compatibilidad declarada y tests.
- El subset geoespacial local deja ya de ser `point-only`: soporta geometrías
  GeoJSON amplias con semántica planar honesta en queries y `$geoNear`, sin
  prometer geodesia real.
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

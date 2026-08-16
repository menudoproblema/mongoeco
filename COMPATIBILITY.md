# Compatibility Guide

Arquitectura relacionada:

* [docs/architecture/index.md](docs/architecture/index.md)
* [docs/architecture/testing-and-compatibility.md](docs/architecture/testing-and-compatibility.md)

Esta guía resume cómo configurar `mongoeco` cuando quieres controlar:

* la semántica objetivo de MongoDB (`mongodb_dialect`)
* la superficie pública objetivo de PyMongo (`pymongo_profile`)

Tambien expone ya un tercer eje publico de descripcion interoperable:

* el capability model CXP canónico para `database/mongodb`

Regla importante:

* el export publico de compatibilidad y los bloques `cxp` de `explain()` se
  proyectan desde ese modelo canónico;
* el bloque `cxp` publica capabilities canónicas, operaciones públicas de
  primer nivel y metadata estructurada del subset real cuando el lenguaje de
  una operación no está soportado completo;
* el export `cxp` incluye también perfiles reutilizables recomendados, como
  `mongodb-text-search` para tests de `$search` textual sin exigir
  `vector_search`, y `mongodb-platform` para consumers que quieran apoyarse en
  metadata canónica de `collation`, `persistence` y `topology_discovery`;
* si solo quieres el catálogo de perfiles reutilizables, la API pública
  expone `export_cxp_profile_catalog()`, y si quieres ese mismo catálogo con
  soporte evaluado contra el runtime público actual, expone también
  `export_cxp_profile_support_catalog()`;
* si quieres consumir la surface pública desde el punto de vista de las
  operaciones (`find`, `update_one`, `aggregate`), también expone
  `export_cxp_operation_catalog()`;
* eso permite hacer gating simple de tests o recursos desde tooling externo,
  sin que `mongoeco` tenga que negociar características ni resolver instancias;
* esos perfiles se exportan con requisitos estructurados
  (`capabilityName`, `requiredOperations`, `requiredMetadataKeys`) y
  `explain()` deja visible tanto el perfil mínimo aplicable como sus
  requisitos estructurados y los perfiles compatibles más amplios cuando puede
  inferirse honestamente;
* `local_runtime_subsets` sigue existiendo por compatibilidad documental, pero
  ya no es la fuente primaria de verdad.

## 1. Dos ejes distintos

`mongoeco` separa dos conceptos:

* `mongodb_dialect`
  * controla semántica observable del servidor
  * ejemplos: comparación con `null`, tratamiento de `undefined`, validaciones y deltas de MQL
* `pymongo_profile`
  * controla compatibilidad de la API Python
  * ejemplos: parámetros aceptados por métodos públicos o diferencias pequeñas de superficie

La versión instalada de `pymongo` **no** decide la semántica del servidor MongoDB.

### Variables de ejecución

Los dialectos MongoDB `7.0` y `8.0` declaran `$$NOW` como variable de sistema
efectiva. Cada comando real captura una fecha UTC naïve, truncada a milisegundos,
y reutiliza ese valor en sus filtros `$expr`, actualizaciones por pipeline,
agregaciones y subpipelines. Esta semántica pertenece al dialecto MongoDB y no
varía con el perfil PyMongo.

Las variables de usuario `let` son efectivas en `find` y `count_documents`,
además de las operaciones de escritura y `aggregate` que ya las declaraban. No
se declara `let` en `distinct`; aun así, `distinct` captura su propio `$$NOW`.
Una variable no definida falla con `OperationFailure` código `17276`.

`$$REMOVE.path` conserva el estado interno *missing* únicamente durante el
cálculo de campos de `$project`, `$addFields` y `$set`, donde omite el campo.
En expresiones de valor se normaliza a `null`, por lo que mantiene la
veracidad, comparación y persistencia de un valor ausente. El centinela
interno de `$$REMOVE` tampoco puede escapar a `$ifNull` ni a acumuladores.

`$currentDate` conserva su evaluación por documento y su contador de
`timestamp`; las fechas de tipo `date` se truncan a milisegundos para respetar
la precisión BSON.

`bulk_write` acepta los seis modelos oficiales de PyMongo (`InsertOne`,
`UpdateOne`, `UpdateMany`, `ReplaceOne`, `DeleteOne` y `DeleteMany`) ademas de
los modelos equivalentes de `mongoeco`. Preserva `collation`, `array_filters`,
`hint` y `sort`; esta ultima opcion sigue su gate por perfil PyMongo.

Los `array_filters` de `update_one`, `update_many`, `find_one_and_update` y de
los modelos equivalentes en `bulk_write` atraviesan exactamente una vez la
frontera `DocumentCodec.to_internal()`. La normalizacion es recursiva, no muta
la entrada y aplica a los `datetime` la misma conversion UTC y precision BSON
que a los documentos persistidos, tanto en la API sync como en la async.

La ejecucion agrupa los modelos en lotes lógicos clásicos
`insert`/`update`/`delete`
de hasta 100.000 modelos: runs contiguos en modo ordenado y grupos por familia
en modo no ordenado. Cada lote comparte un `$$NOW`. No emula todavía límites de
tamaño BSON/mensaje ni el comando `bulkWrite` introducido para clientes modernos
de MongoDB 8.0.

### Frontera BSON publica

Las entradas de coleccion atraviesan una frontera BSON comun antes de llegar al
motor. Esto convierte tipos oficiales de `bson`, normaliza recursivamente los
`datetime` a UTC naive y trunca microsegundos a milisegundos. La misma regla se
aplica a documentos, filtros, replacements, updates, `let`, pipelines y modelos
bulk. Las proyecciones de lectura y los `partialFilterExpression` de indices
usan la misma normalizacion; SQLite guarda estos ultimos con el codec
reversible, no como JSON BSON-incompleto.

Las lecturas y los IDs de resultados de escritura se materializan con tipos
oficiales de PyMongo (`bson.ObjectId`, `bson.Binary`, `bson.Decimal128`,
`bson.Regex`, `bson.Timestamp` y `bson.DBRef`). Las selecciones internas de una
escritura no cruzan esa frontera publica, para conservar estable la identidad
de almacenamiento. La misma materializacion recursiva se aplica a metadata de
indices, eventos de change streams y detalles parciales de errores bulk.

Los cursores de `find()` y `aggregate()` capturan su contexto inmutable,
incluido `$$NOW`, al crearse; `clone()` captura un contexto temporal nuevo. El
batching consume una unica fuente estable hasta agotarla, de modo que una
escritura concurrente no reordena ni duplica las paginas restantes. Los
cursores async son de consumo unico, admiten `to_list(length=None)`, longitudes
parciales y `close()`; las fachadas async de agregacion, metadata y change
streams tambien pueden esperarse directamente.

Los change events se publican en el orden de confirmacion de las mutaciones,
antes de cualquier espera de profiling. Si el backend local del stream no
puede registrar un evento despues de confirmar la escritura, la escritura
conserva su resultado y el hub pasa a estado degradado: los consumidores fallan
con `OperationFailure` y `change_stream_state()` expone `degraded` y
`lastPublishError`. Esto evita tanto reintentos de escrituras ya aplicadas como
una continuidad falsa del stream.

En los engines integrados, ese orden se apoya en una secuencia de commit
explicita. Memory la asigna al instalar el estado MVCC; SQLite escribe una fila
de outbox en la misma transaccion que el documento y sus indices. El journal
del change stream actua como checkpoint durable para reanudar filas confirmadas
pero aun no publicadas. Memory y SQLite aplican retencion acotada por
checkpoint, con 10.000 entradas por defecto. SQLite persiste los checkpoints
durables; los consumidores sin journal se retiran al desconectar. Si un
consumidor queda por detras del suelo compactado, falla explicitamente en vez
de continuar con una secuencia incompleta. Tambien se rechaza un checkpoint
por delante de la ultima secuencia confirmada; tras compactar todas las filas,
el suelo persistido conserva esa secuencia y evita reiniciar la historia.
La identidad idempotente de cada fila es `(operation_id, event_index)` y se
conserva tambien despues de podar el payload. Las operaciones multi-documento
comparten identidad de operacion, pero asignan un ordinal distinto a cada
mutacion aplicada. Las identidades se retienen durante la ventana
`maxEntries`; replays mas antiguos quedan fuera del contrato idempotente para
evitar un ledger sin limite.
La identidad conserva el tipo de fila y un SHA-256 del efecto semantico
serializado, incluido namespace, operacion, clave e imagen. El payload puede
ser nulo para un hueco sin perder esa identidad. Un replay solo recupera su
secuencia cuando ambos coinciden; reutilizar
`(operation_id, event_index)` para un evento o hueco distinto falla de forma
explicita, tambien tras compactar el payload.
Un mismo consumidor nunca se entrega en paralelo. Memory aplica exclusion
local por consumidor; SQLite persiste un lease renovable con owner y
generacion, de forma que dos instancias no publican simultaneamente ni pueden
confirmar un checkpoint con ownership obsoleto. Una instancia que encuentra el
lease ocupado espera y vuelve a drenar desde el checkpoint confirmado, por lo
que una solicitud concurrente no deja eventos pendientes sin disparador. El
gate process-local se comparte por ruta canonica y rechaza reentrada por otra
instancia antes de esperar el lease. El
esquema de outbox avanza mediante migraciones atomicas y rechaza versiones
futuras sin intentar un downgrade.
La entrega es at-least-once: el callback precede al checkpoint, por lo que un
fallo de heartbeat o de proceso despues del callback puede repetir esa fila.
En bases de fichero, el control plane usa una conexion independiente y nunca
confirma una transaccion de datos del caller. Los consumidores locales usan
owner de proceso y una registration TTL renovable; los journals durables no
caducan.

Los cursores de coleccion consumen snapshots `STABLE` con ownership y cierre
explicitos. `MATERIALIZED` y `LIVE` existen como politicas declarables del SPI,
pero los engines integrados no las usan para scans ordinarios. Un engine
externo v1 se adapta mediante `LegacyEngineAdapter`; el SPI v2 exige
`EngineCapabilities`, outcomes tipados y `OperationContext`. Un engine v2
puede declarar `explicit_read_snapshots=True` e implementar
`open_read_snapshot`, o conservar el fallback compatible de 4.3.0 mediante
`scan_find_semantics`; el adaptador envuelve este ultimo en un snapshot estable
con la identidad de la operacion. Los flags heredados del SPI v1 no
sobrescriben capabilities v2 declaradas. El adaptador v1 queda deprecado en
4.3.0, emite
`DeprecationWarning` y se retirara en 5.0.0.
Los engines v2 pueden declarar `batch_inserts=False`; en ese caso MongoEco
degrada a inserciones individuales sin exigir una primitiva batch inexistente.
Las lecturas ordinarias rechazan snapshots que no sean `STABLE` o cuyo
`operation_id` no coincida con el contexto de apertura.
El cierre de un snapshot tiene un plazo finito configurable; si una fuente
externa no termina su cleanup, la cancelacion o el shutdown recuperan el
control y la tarea restante queda supervisada en un registro acotado.
`SnapshotLifecycle` distingue
`OPEN`, `CLOSING`, `CLOSED` y `FAILED`; `cleanup_pending` y `close_error`
permiten observar el resultado tardio sin perder excepciones de background.
`closed` solo es verdadero en `CLOSED` o `FAILED`.

### Escrituras seleccionadas y atomicidad local

La seleccion para resolver `sort` o una imagen de retorno, la revalidacion del
filtro completo, la modificacion y la captura de imagenes `before`/`after`
pertenecen a una unica primitiva del engine. Esa primitiva usa el mismo
dialecto, collation, variables `let` y contexto temporal de la operacion;
`hint` se valida dentro de la misma frontera.

Si el filtro original deja de coincidir, la operacion devuelve no-match, no
modifica ni borra el documento y no publica un change event. Este contrato se
aplica a `update_one`, `update_many`, `replace_one`, `find_one_and_update`,
`find_one_and_replace`, `delete_one`, `delete_many` y
`find_one_and_delete`. Memory garantiza atomicidad por instancia y coleccion;
SQLite la extiende a instancias que comparten el mismo fichero mediante su
transaccion de escritura. Instancias Memory independientes no comparten estado.

Los upserts de `find_one_and_update` y `find_one_and_replace` publican
`insert` incluso cuando solicitan `ReturnDocument.BEFORE`. Una coincidencia que
no modifica el documento no publica eventos de update o replacement.

### Reloj inyectable para runtimes locales

`AsyncMongoClient` y `MongoClient` aceptan `now_factory: Callable[[], datetime]
| None`. Si se proporciona, el cliente lo consulta una vez por comando real o
lote lógico y normaliza el valor a UTC naïve con precisión BSON de milisegundos.
El mismo instante alimenta `$$NOW`, `$currentDate` de tipo fecha y la caducidad
TTL. No afecta a telemetría, handshakes, perfiles ni `ObjectId`.

El contrato solo esta disponible en engines SPI v2 cuya
`EngineCapabilities.injected_clock` sea `True`; Memory y SQLite lo soportan.
Los flags privados de SPI v1 se interpretan exclusivamente dentro del adapter
de compatibilidad 4.x. Un backend externo o real sin la capability lo rechaza
al construir el cliente, para no ofrecer una falsa sensacion de determinismo.

## 1.1 Baseline soportado

`mongoeco` no persigue compatibilidad hacia atrás por debajo de estos mínimos:

* MongoDB `7.0`
* PyMongo `4.9`

Consecuencias prácticas:

* no se aceptan como objetivo de diseño semánticas específicas de MongoDB `6.x` o anteriores
* no se aceptan como objetivo de diseño firmas o comportamientos específicos de PyMongo anteriores a `4.9`
* cuando se amplía superficie pública o semántica, la referencia es siempre PyMongo `4.9+` sobre dialectos MongoDB `7.0+`

## 1.2 Subset embebido honesto

Dentro de esos ejes, `mongoeco` sigue modelando un runtime embebido/local, no
un servidor MongoDB completo.

Esto implica:

* `currentOp` y `killOp` existen solo con semántica local y best-effort;
* `vectorSearch` usa ya ANN local con `usearch` en `SQLiteEngine` y baseline
  exacta en `MemoryEngine`;
* `$merge`, `$densify` y `$fill` existen como subset explícito;
* los pipeline-style updates ya están soportados end-to-end para su subset;
* geoespacial entra ya como subset local amplio y planar;
* `$text` clásico existe ya como subset local explícito, con `textScore`
  observable pero sin pretender semántica full-text de servidor MongoDB.
* `$search` local soporta ya `text`, `phrase`, `autocomplete`, `wildcard`,
  `regex`, `exists`, `in`, `equals`, `range`, `near` y `compound`
  como subset explícito y documentado, sin pretender semántica Atlas Search
  completa.
* ese subset textual local se publica bajo el contrato estable `search-v1`;
  incluye collectors, highlight y explain locales sin prometer paridad Atlas.
* el runtime local añade ya un siguiente subset avanzado y explícito:
  * `autocomplete.tokenOrder` con `any` y `sequential`;
  * `regex.flags` con `i`, `m` y `s`;
  * `wildcard.allowAnalyzedField`;
  * `$searchMeta` con count total/lower-bound y facets simples o nombradas;
  * `$search.highlight` como metadata runtime tipada proyectable mediante
    `$meta: "searchHighlights"`; el alias legacy se resuelve como campo virtual,
    sin contaminar el payload persistible ni sobrescribir un campo real;
  * `explain("queryPlanner"|"executionStats")`, conservando las previews
    antiguas solo como aliases deprecated durante 4.x; `executionStats` requiere
    un indice listo y separa matches, hits retornados, candidatos y scans. La
    traza canonica declara estado, fases, dominio, exactitud, origen y
    disponibilidad; `collectorDocumentCount` y `pipelineOutputCount` no se
    confunden con `collectorCount`. Todas las metricas estan representadas,
    aunque su estado sea `unavailable`; los contadores planos son aliases 4.x.
* La provenance de Search vive fuera del documento BSON en
  `RuntimeDocumentState`. Proyecciones, stages estructurales y writeback aplican
  reglas explicitas; el namespace privado con NUL queda limitado al adapter SPI
  v1 deprecado y nunca se persiste.
* El planner Search compila efectos semanticos, dominio de stage, reglas,
  rechazos y ownership una vez. Ejecucion y `explain()` consumen ese mismo
  plan, y la suite compara resultados, collectors, errores, writeback y eventos
  con un oraculo interno reproducible sin optimizaciones en Memory/SQLite y
  sync/async.
* Los informes de `mongoeco.conformance` usan el schema estable
  `mongoeco-conformance-report/v1` y estados `passed`, `failed`, `error` y
  `not-applicable`. El JSON Schema Draft 2020-12 se obtiene mediante
  `conformance_report_schema()` y evoluciona de forma aditiva dentro de v1; un
  cambio incompatible requiere otro `schemaVersion`. El constructor 4.5 con
  `passed=` sigue aceptado.
* Wheel y sdist incluyen `py.typed`. La garantia PEP 561 cubre las superficies
  publicas documentadas de clientes, SPI v2, Search y conformidad; no convierte
  modulos internos o privados en API estable.
* `mongoeco.compat.deprecation_entries()` y `deprecation_catalog()` son la
  fuente versionada de retiradas previstas. `decision-pending` significa que no
  existe aun una deprecacion contractual.
* `public_api_manifest()` separa compatibilidad Python de compatibilidad de
  datos. `scripts/update_public_api_manifest.py --check` detecta deltas de
  exports, firmas, defaults, tipos, async, schemas y recursos. La fachada
  `ObjectId` mantiene el mismo contrato con o sin PyMongo mediante el protocolo
  estructural `ObjectIdLike`; una dependencia opcional no crea otro perfil de
  API publica.
* La fixture `tests/fixtures/sqlite/mongoeco-4.5.0-bridge.sqlite` fue generada
  con el wheel oficial 4.5.0 de SHA-256
  `f168ab9f4172abbf1a7e35f8996c3e01463a26557b213028c83ef64d102a2fd3`.
  Abrirla con versiones posteriores prueba BSON, indices, Search, outbox y
  checkpoints; no implica que una ruptura de API Python sea compatible. Si la
  apertura rechaza un schema futuro o falla durante la inicializacion, todas
  las conexiones parciales se cierran y el engine permanece desconectado.
* La CLI `python -m mongoeco.conformance` usa el runner publico, valida el mismo
  schema y reserva stdout para el informe y stderr para diagnosticos.
* los mappings locales de `$search` cubren ya una familia más rica de campos:
  `string`, `autocomplete`, `token`, `number`, `date`, `boolean`,
  `objectId`, `uuid`, `document` y `embeddedDocuments`; los tipos textuales
  siguen siendo `string`, `autocomplete` y `token`, el subset escalar se usa
  para filtros y matching escalar honesto, `document` deja declarar objetos
  anidados explícitos como `metadata.topic`, y tanto `document` como
  `embeddedDocuments` admiten ya búsquedas por path padre estructurado
  (`metadata`, `contributors`) que se resuelven contra sus leafs textuales
  mapeados en `text`, `phrase`, `autocomplete`, `wildcard` y `regex`, y
  contra leafs mapeados en `exists`; `explain()` publica ya los
  `resolvedLeafPaths` de esa resolución; además `embeddedDocuments` deja declarar arrays de documentos
  anidados con paths explícitos como `contributors.name`,
  `contributors.verified` o `contributors.impact`.
* cuando una pipeline deja un `skip/limit` seguro tras `$search`, el runtime
  local puede usar ese `top-k` para limitar candidatos y materialización sin
  cambiar el contrato observable.
* cuando despues de `$search` hay una pipeline `prefix-monotonic` con filtros
  por documento y una ventana finita, el runtime puede expandir `top-k` de
  forma iterativa sin perder exactitud; `explain()` lo expone mediante
  `searchTopKStrategy`.
* esa expansion iterativa usa crecimiento adaptativo por tasa de retencion
  observada (`searchTopKGrowthStrategy`), no una heuristica fija opaca.
* cuando el tramo posterior a `$search` empieza por `$match`, el runtime puede
  usar ese filtro como `downstreamFilterPrefilter` exacto antes del ranking
  final; no se promete lo mismo para filtros colocados despues de stages que
  transformen documentos.
* en SQLite, un `$match` simple sobre paths textuales realmente indexados puede
  volverse candidateable dentro de `compoundPrefilter.downstreamFilter`, no solo
  un filtro documental posterior.
* si ese `$match` simple implica exactamente una clausula textual del
  `compound`, `explain()` lo deja visible como `downstreamRefinement` sobre esa
  clausula y el runtime usa ese refinamiento para estrechar candidatos.
* cuando todas las clausulas `should` candidateables de un `compound` permiten
  calcular score exacto desde FTS, SQLite puede podar por tiers exactos de
  `matchedShould` + `shouldScore` antes de cargar documentos completos; esa
  poda aparece en `topKPrefilter.strategy`.
* antes de ese score exacto, SQLite puede recortar primero por tiers de
  `matchedShould`; `topKPrefilter` deja visible ese paso con
  `candidateCountBeforePartialRanking`, `candidateCountAfterPartialRanking` y
  `partialRanking.strategy`.
* `compoundPrefilter` deja visible la clase de cada clausula
  (`candidateable-exact`, `candidateable-ranking`, `post-match-only`) y
  `topKPrefilter.cutoffTier` expone el tier usado para el corte.
* cuando el ranking final de un `compound` puede reconstruirse exactamente
  desde las entradas materializadas de FTS, SQLite lo deja visible como
  `rankingSource="fts-materialized-entries"` y evita cargar todos los
  documentos candidatos antes del corte final.
* tanto `compoundPrefilter.downstreamFilter` como `vectorFilterPrefilter`
  admiten ya booleanos locales conservadores:
  - `$and` puede aprovechar la parte soportada aunque siga quedando resto no
    candidateable;
  - `$or` solo se vuelve candidateable si todas sus ramas lo son.
* en `vectorSearch` con `filter`, SQLite declara ya
  `candidateExpansionStrategy="adaptive-retention"` en `explain()` para dejar
  visible que la expansion ANN posterior al filtro ya no usa una heuristica
  fija.
* `vectorSearch` deja visible en `explain()` la `similarity` efectiva del
  indice, los candidatos realmente pedidos/evaluados (`candidatesRequested` /
  `candidatesEvaluated`) y `exactFallbackReason` cuando la ruta ANN degradada
  tiene que caer al baseline exacto.
* para filtros simples (`eq`, `$in`, `$exists`, `range`) sobre paths escalares
  ya vistos por el backend vectorial materializado, `vectorSearch` puede
  aplicar tambien `vectorFilterPrefilter` antes del ranking ANN/documental; si
  el subconjunto es exacto, `filterMode` pasa a `candidate-prefilter`.
  Esta optimizacion se limita al `filter` interno del stage: un `$match`
  posterior permanece despues del top-k y `explain()` no lo anuncia como
  prefilter.
* `MemoryEngine` tambien materializa ya un subset local para `vectorSearch` y
  deja visible en `explain()` tanto `vectorFilterPrefilter` como
  `documentsScannedAfterPrefilter`; cuando queda filtro documental residual,
  `vectorFilterResidual` deja visible si ese resto viene de clausulas no
  candidateables o de un prefilter no exacto, sin cambiar la shape publica del
  stage.
* la proyeccion avanzada de `find` cubre ya el subconjunto diario mas util
  (`$slice`, `$elemMatch`, proyeccion posicional y `$meta: "textScore"`);
* `$collStats` existe tanto como comando administrativo como stage de
  agregacion local de introspeccion;
* los indices `hidden` existen como opcion local honesta: se listan y se
  preservan en metadata, pero el planner no los usa ni acepta `hint` contra
  ellos.
* la elegibilidad de indices parciales se decide de forma conservadora con
  igualdad y orden BSON; valores Python parecidos pero BSON-distintos, como
  `True` y `1`, no permiten inferir una implicacion insegura. Una collation en
  el indice parcial exige la misma collation efectiva en la operacion, incluida
  la seleccion explicita mediante `hint`;
* los indices de igualdad de Memory canonizan la familia numerica con semantica
  BSON, sin confundir booleanos con numeros. SQLite evita el pushdown cuando un
  operando o valor decimal no puede conservar esa semantica en SQLite y evalua
  el predicado con el comparador comun de Python.

## 2. Configuración explícita recomendada

La forma más estable y reproducible es fijar ambos ejes explícitamente:

```python
from mongoeco import AsyncMongoClient

client = AsyncMongoClient(
    mongodb_dialect="7.0",
    pymongo_profile="4.9",
)
```

También puedes usar los objetos oficiales:

```python
from mongoeco import AsyncMongoClient, MongoDialect70, PyMongoProfile411

client = AsyncMongoClient(
    mongodb_dialect=MongoDialect70(),
    pymongo_profile=PyMongoProfile411(),
)
```

La misma idea se aplica a ambos ejes: `mongoeco` resuelve y conserva metadata
de la decisión tomada.

## 3. Dialectos MongoDB disponibles

Hoy el catálogo oficial incluye:

* `7.0`
* `8.0`

Regla práctica:

* `7.0` es la baseline de desarrollo
* `8.0` se trata como compatibilidad adicional con deltas explícitos
* la selección del dialecto es explícita; `mongoeco` no autodetecta servidor en el flujo normal
* no existe catálogo oficial para versiones anteriores a `7.0`

## 3.1 Resolución del dialecto MongoDB

La API pública ya expone una resolución estructurada equivalente a la de
`pymongo_profile`:

```python
from mongoeco import resolve_mongodb_dialect_resolution

resolution = resolve_mongodb_dialect_resolution("8.0")

print(resolution.resolved_dialect.key)
print(resolution.resolution_mode)
```

Campos disponibles:

* `requested`
* `detected_server_version`
* `resolved_dialect`
* `resolution_mode`

Modos posibles hoy:

* `default`
* `explicit-alias`
* `explicit-instance`

## 4. Perfiles PyMongo disponibles

Hoy el catálogo oficial incluye:

* `4.9`
* `4.11`
* `4.13`
* `4.17`

Regla práctica:

* `4.9` es la baseline de API pública
* `4.11` activa el primer delta real: `update_one(sort=...)`
* `4.13` queda disponible como perfil posterior compatible
* `4.17` queda disponible como perfil posterior compatible
* no existe catálogo oficial para perfiles anteriores a `4.9`

## 5. Autodetección de PyMongo instalada

Puedes pedir a `mongoeco` que resuelva el perfil según la versión instalada del
paquete `pymongo`.

### Modo flexible

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="auto-installed")
```

Política:

* si la versión instalada coincide con un perfil conocido, usa ese perfil
* si aparece una minor nueva dentro de la misma major conocida, cae al último
  perfil compatible de esa major
* si aparece una major nueva no registrada, falla

Ejemplos actuales:

* `4.8.x` -> error explícito
* `4.10.x` -> `4.9`
* `4.12.x` -> `4.11`
* `4.14.x` -> `4.13`
* `5.x` -> error explícito

### Modo estricto

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="strict-auto-installed")
```

Política:

* solo acepta versiones instaladas que encajen exactamente en un perfil
  registrado
* si aparece una minor nueva todavía no modelada, falla

Este modo es el recomendable para CI o validación contractual estricta.

## 6. Inspeccionar la resolución aplicada

Si quieres conocer exactamente qué política se ha aplicado, usa la API pública
de resolución:

```python
from mongoeco import resolve_pymongo_profile_resolution

resolution = resolve_pymongo_profile_resolution("auto-installed")

print(resolution.installed_version)
print(resolution.resolved_profile.key)
print(resolution.resolution_mode)
```

Campos disponibles:

* `requested`
* `installed_version`
* `resolved_profile`
* `resolution_mode`

Modos posibles hoy:

* `default`
* `explicit-alias`
* `explicit-instance`
* `auto-exact`
* `auto-compatible-minor-fallback`

También puedes inspeccionar la resolución ya aplicada en el cliente:

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="auto-installed")

print(client.pymongo_profile.key)
print(client.pymongo_profile_resolution.installed_version)
print(client.pymongo_profile_resolution.resolution_mode)
```

Y de forma simétrica para el dialecto:

```python
from mongoeco import MongoClient

client = MongoClient(mongodb_dialect="8.0")

print(client.mongodb_dialect.key)
print(client.mongodb_dialect_resolution.resolution_mode)
```

## 7. Recomendación operativa

Para trabajo diario:

* `mongodb_dialect="7.0"`
* `pymongo_profile="auto-installed"`

Para CI y suites de compatibilidad:

* `mongodb_dialect` fijado explícitamente
* `pymongo_profile` fijado explícitamente, o `strict-auto-installed`

## 7.1 Surface administrativa local actual

Para un runtime embebido/local, la surface administrativa ya cubre:

* introspección y estado local: `buildInfo`, `hello`, `serverStatus`,
  `connectionStatus`, `hostInfo`, `getCmdLineOpts`, `whatsmyuri`,
  `listCommands`, `currentOp`, `killOp`, `profile`;
* namespace e índices: `listCollections`, `listDatabases`, `create`, `drop`,
  `renameCollection`, `dropDatabase`, `listIndexes`, `createIndexes`,
  `dropIndexes`;
* lectura/escritura administrativa: `find`, `aggregate`, `count`, `distinct`,
  `insert`, `update`, `delete`, `findAndModify`;
* validación y observabilidad: `collStats`, `dbStats`, `dbHash`, `validate`,
  `explain`.

Límites conscientes:

* no hay administración distribuida de cluster, réplica o sharding;
* no hay `usersInfo`/`createUser`/`dropUser` completos;
* `killOp` solo cancela operaciones locales registradas como cancelables;
* el wire passthrough replica esta misma surface local, no una surface de
  servidor completa.

## 7.2 Subset geoespacial local actual

El runtime local soporta ya un subset geoespacial explícito y limitado:

* datos geoespaciales:
  * `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`,
    `MultiPolygon` y `GeometryCollection`;
  * pares legacy `[x, y]` para puntos;
* queries:
  * `$geoWithin` con `Polygon`, `MultiPolygon` y legacy `$box`;
  * `$geoIntersects` entre cualquier geometría soportada del subset local;
  * `$near` y `$nearSphere` con query point-only y distancia mínima planar
    contra la geometría almacenada;
* agregación:
  * `$geoNear` con `near`, `distanceField`, `key`, `query`, `minDistance`,
    `maxDistance` e `includeLocs` dentro del subset local.

Límites conscientes:

* `SQLiteEngine` ejecuta este subset con fallback Python explícito, no con
  pushdown SQL;
* `$nearSphere` conserva semántica local de distancia plana, no geodesia
  completa;
* la presencia de índices `2d`/`2dsphere` no implica todavía un planner
  geoespacial especializado.

## 7.3 Subset local actual de `$text` clasico

El runtime local soporta ya un subset explícito y limitado de `$text`:

* queries:
  * filtro top-level `{ "$text": { "$search": "..." } }`;
  * tokenizacion local por minusculas, separacion por espacios o puntuacion y
    plegado diacritico basico;
* indices:
  * un unico indice `text` de un solo campo por coleccion para el camino
    clasico local;
* score:
  * materializacion local de `textScore`;
  * proyeccion `{campo: {"$meta": "textScore"}}`;
  * ordenacion por `textScore`.

Límites conscientes:

* `caseSensitive=true` y `diacriticSensitive=true` quedan fuera del subset
  soportado;
* no hay stemming, idioma, weights ni planner full-text especializado;
* `SQLiteEngine` ejecuta el subset clasico como fallback Python explicito y lo
  deja visible en `explain()`.

## 7.4 Subset local actual de `vectorSearch`

`vectorSearch` forma ya parte del runtime embebido como búsqueda vectorial local
con baseline exacta y backend ANN:

* similitudes:
  * `cosine`
  * `dotProduct`
  * `euclidean`
* surface:
  * `filter` opcional reutilizando `QueryEngine`;
  * `minScore` opcional como corte local explícito por score;
  * proyeccion `{campo: {"$meta": "vectorSearchScore"}}` sobre resultados de
    `$vectorSearch`;
  * `explain` con backend, modo, similitud, escaneo, candidatos evaluados,
    corte `minScore`, vectores válidos/inválidos y razones de degradación.
* backend:
  * `MemoryEngine` mantiene baseline exacta para semántica y contraste;
  * `SQLiteEngine` usa `usearch` como backend ANN local cuando el índice
    vectorial está materializado.

Límites conscientes:

* no hay servicio remoto Atlas-like, ANN distribuido ni embeddings
  automáticos;
* `filter` sigue siendo post-candidate, con ampliación adaptativa de
  candidatos antes de degradar a exacto;
* si el filtro degrada demasiado el resultado, `explain()` deja visible la
  degradación a exacto.

## 7.5 Subset local actual de `$search`

El runtime local soporta ya un subset explícito de `$search`:

* operadores:
  * `text`
  * `phrase` (con `slop` local opcional)
  * `autocomplete`
  * `wildcard`
  * `regex`
  * `exists`
  * `in`
  * `equals`
  * `range`
  * `near`
  * `compound`
* surface observable:
  * `explain()` con `queryOperator`, paths y backend real;
  * `SearchIndexDocument.capabilities` alineado con el runtime real;
  * `SQLiteEngine` usando FTS5 cuando la traducción es defendible y fallback
    Python cuando no lo es.

Límites conscientes:

* `$searchMeta`, count, facets y highlight siguen el contrato local
  `search-v1`, no el contrato completo de Atlas Search;
* SQLite solo baja collectors a SQL cuando demuestra exactitud sobre mappings
  textuales explicitos; en el resto usa el acumulador semantico compartido;
* `wildcard` sigue siendo matching local `fnmatch` y no sintaxis Atlas Search
  completa;
* `regex` entra como matching Python local sobre entradas materializadas, ahora
  con flags `i` / `m` / `s`, pero sin semántica Atlas Search avanzada completa;
* `autocomplete` es local y basado en prefijos de tokens, ahora con
  `tokenOrder`, pero no en analyzer avanzado;
* `in`, `equals` y `range` entran como operadores locales sobre paths escalares,
  con matching exacto/por rango y backend Python explícito cuando no hay una
  traducción materializada defendible;
* `near` entra como subset local para valores numericos y fecha/datetime,
  con `path`, `origin` y `pivot`, y ordena por cercania local sin pretender
  scoring Atlas Search completo;
* `compound` se limita a combinar el subset local soportado
  (`text`/`phrase`/`autocomplete`/`wildcard`/`regex`/`exists`/`in`/`equals`/`range`/`near`)
  con `must`, `should`, `filter`, `mustNot` y `minimumShouldMatch`;
* `explain()` publica ya `pathSummary` consistente tambien para `in`,
  `equals`, `range` y `near`, incluyendo `resolvedLeafPaths` cuando la query
  se apoya sobre mappings escalares conocidos;
* `phrase` acepta `slop` entero no negativo como subset local explícito; con
  `slop=0` conserva la frase exacta, y con `slop>0` permite tokens
  intermedios extra entre términos manteniendo orden;
* `SQLiteEngine` usa FTS5 directo para `text`, `phrase` con `slop=0` y
  `autocomplete`, y
  usa el backend materializado como prefilter de candidatos para `wildcard`,
  `exists` y parte de `compound` antes del matching Python exacto; `in`,
  `equals` y `range` siguen entrando como operadores locales honestos sobre el runtime
  Python cuando no hay una traducción candidateable defendible; cuando
  `phrase.slop > 0`, SQLite puede usar FTS5 como prefilter candidato, pero la
  validación exacta final sigue siendo local y visible en `explain()`;
* `$vectorSearch` debe seguir siendo el primer stage;
* la semantica sigue siendo local, no de cluster o servicio remoto.

## 8. Modo de planning

La compatibilidad semántica y la compatibilidad de API no sustituyen al modo de
planning.

`mongoeco` expone dos políticas:

* `PlanningMode.STRICT`
  * es la baseline recomendada
  * falla en compilación cuando el shape recibido no es ejecutable de forma
    coherente
* `PlanningMode.RELAXED`
  * conserva metadata de la operación y deja visibles `planning_issues`
  * no convierte documentos inválidos o no soportados en no-ops silenciosos
  * es útil para explain, tooling y superficies que prefieren degradación
    explícita frente a error inmediato

## 8.1 Regla de endurecimiento para nueva superficie pública

Cuando se amplía compatibilidad o se añade una feature nueva, el criterio de
aceptación no es solo que el caso feliz funcione en una ruta concreta.

La regla operativa del proyecto pasa a ser:

* si la feature existe en API async y sync, ambas rutas deben quedar cubiertas
  por tests de parity o por regresiones equivalentes
* si la semántica se promete igual para `MemoryEngine` y `SQLiteEngine`, debe
  añadirse cobertura cruzada entre engines
* si una feature depende de reconstruir fachadas (`with_options()`,
  `database`, `get_collection()`, `rename()`), los tests deben fijar también
  la preservación de opciones heredadas y metadata runtime
* cuando la degradación sea parte del contrato (`planning_issues`, errores
  públicos, gaps explícitos de implementación), el shape observable debe quedar
  fijado en tests en lugar de dejarlo implícito

## 9. Alcance actual de collation

La implementación actual no intenta exponer toda la superficie de collation de
MongoDB.

Hoy el contrato soportado y testeado es:

* locales `simple` y `en`
* `strength` `1`, `2` y `3`
* `numericOrdering` y `caseLevel` para `locale=en`
* `simple` se mantiene como comparador BSON/Python base sin tailoring extra

Para collation Unicode:

* `PyICU` se mantiene como dependencia opcional por contrato
* `mongoeco` prefiere `PyICU` cuando está disponible
* si `PyICU` no está instalado, usa `pyuca` como backend runtime de base
* ambas rutas quedan cubiertas por tests, pero pueden existir diferencias
  menores en tailoring avanzado fuera de este subconjunto soportado

Matriz práctica de capacidades:

* backend `icu`
  * soporta el subconjunto básico anterior
  * soporta también `backwards`, `alternate`, `maxVariable` y
    `normalization`
* backend `pyuca`
  * soporta Unicode collation básica
  * no soporta tailoring avanzado compatible con ICU
  * si el usuario pide `backwards`, `alternate`, `maxVariable` o
    `normalization`, `mongoeco` falla explícitamente
* sin backend Unicode
  * solo `simple`

Los change streams locales mantienen además un historial en memoria acotado.
El tamaño de esa retención es configurable desde cliente y determina hasta qué
token o `startAtOperationTime` se puede reanudar sin error.

Ese historial retenido puede persistirse opcionalmente a un journal local
mediante `change_stream_journal_path`. Cuando se configura, los cursores
pueden reanudar desde `resume_after` o `start_after` incluso tras recrear el
cliente o la colección dentro del mismo entorno local, siempre dentro de la
ventana retenida.

La persistencia local usa además un journal incremental con compactación sobre
snapshot retenido, para no reescribir el historial completo en cada evento.
Cada entrada incremental incluye checksum de integridad, el reload tolera una
cola truncada si la última escritura quedó a medias y el usuario puede endurecer
la persistencia con:

* `change_stream_journal_fsync=True`
* `change_stream_journal_max_bytes=<limite>`

Si una escritura ya confirmada no puede publicarse en ese journal, el hub
persiste un marcador de degradacion junto al journal. El estado sobrevive a la
recreacion del cliente y bloquea consumo y reanudacion hasta que el operador
resuelva la causa y reconstruya una continuidad verificable; no se avanza el
resume token en memoria ni se presenta una historia incompleta como reanudable.

Cuando no existe ningun watcher activo y no hay journal persistente, las
escrituras locales pueden omitir la materializacion del evento de change stream
para no pagar el coste de preseleccion, relectura y `deepcopy()` de documentos.
Esa omision no es silenciosa para la reanudacion: el hub registra un gap de
historial y cualquier `resume_after`, `start_after` o `startAtOperationTime`
anterior al gap falla con el error publico de token no disponible. Si hay al
menos un watcher activo, o si el journal esta habilitado, los eventos siguen
materializandose y reteniendose como antes.

Además, cliente, base de datos y colección exponen `change_stream_state()`
para inspeccionar en runtime:

* offsets retenidos
* estado del snapshot y del log incremental
* bytes/entradas pendientes desde la última compactación
* número de compactaciones realizadas

Cliente, base de datos y colección exponen además
`change_stream_backend_info()`, que deja explícito si el backend actual es:

* local o distribuido
* persistente o solo en memoria
* reanudable entre recreaciones de cliente/proceso
* acotado por ventana de retención

La API runtime expone también la política de collation en
`mongoeco.collation_backend_info()`, que devuelve:

* `selected_backend`
* `available_backends`
* `unicode_available`
* `advanced_options_available`

Y `mongoeco.collation_capabilities_info()`, que devuelve:

* `supported_locales`
* `supported_strengths`
* `supports_case_level`
* `supports_numeric_ordering`
* `optional_icu_backend`
* `fallback_backend`
* `advanced_options_require_icu`

## 10. Topología local y discovery

La capa driver no implementa SDAM completo, pero ya no trata un seed único
normal como topología `single` definitiva salvo que el usuario pida
`directConnection=true`.

Contrato actual:

* `directConnection=true`
  * arranca como `single`
* `replicaSet=...`
  * arranca como `replicaSet` provisional
* seed único sin `directConnection`
  * arranca como `unknown`
  * la selección usa el seed como candidato provisional
  * `refresh_topology()` usa `hello` para converger a `standalone`,
    `replicaSet` o `sharded`
* en `replicaSet`, `refresh_topology()` descubre ya miembros adicionales desde
  `hosts`, `passives` y `arbiters`, y marca la topología como incompatible si
  aparecen familias mezcladas o `setName` conflictivos
* el monitor usa también `primary` y `me` para discovery adicional, clasifica
  `arbiterOnly` como miembro explícito del replica set y evita degradar el
  estado local cuando llega un `hello` con `topologyVersion` más viejo
* cada `ServerDescription` mantiene además un estado de salud local
  (`unknown`, `healthy`, `recovering`, `degraded`, `unreachable`) y contadores
  de fallos consecutivos para observabilidad y ordenación de candidatos
* los fallos reales de red en transporte wire (`connect`, `drain`, `read`) se
  normalizan a `ConnectionFailure`, de modo que los retryable reads/writes ya
  no dependen solo de labels devueltos por el servidor

La API runtime expone este contrato en `mongoeco.sdam_capabilities_info()` y
en `client.sdam_capabilities()`, para que el proceso pueda distinguir entre:

* soporte de discovery por `hello`
* awareness de `topologyVersion`
* tracking de salud por server
* awareness de metadatos de elección
* ausencia deliberada de SDAM completo y `hello` long-polling

## 11. Verificación contractual contra PyMongo real

La ampliación de superficie pública no debe decidirse por memoria ni por lectura
aislada de firmas.

El repositorio incluye un arnés repetible:

* [scripts/run_pymongo_profile_matrix.py](scripts/run_pymongo_profile_matrix.py)
* [tests/fixtures/pymongo_profile_matrix.json](tests/fixtures/pymongo_profile_matrix.json)

Uso recomendado:

```bash
python3 scripts/run_pymongo_profile_matrix.py
```

El script crea entornos aislados para `PyMongo 4.9`, `4.11` y `4.13`, ejecuta
una sonda de aceptación de parámetros reales y devuelve un JSON con los
resultados.

El JSON versionado en `tests/fixtures/` actúa como snapshot contractual del
último contraste validado y debe actualizarse cuando cambie la matriz real.

Regla de mantenimiento:

* cualquier parámetro nuevo en la API pública debe contrastarse primero con este
  arnés
* solo se añade un hook nuevo a `PyMongoProfile` cuando la matriz real detecta
  un delta observable entre perfiles

Matriz ya verificada:

* baseline común en `4.9/4.11/4.13`:
  * `hint`, `comment` y `let` en `update_*`, `replace_one`, `delete_*`
  * `comment` y `let` en `bulk_write`

## 12. Superficie aceptada frente a semántica efectiva

No toda opción aceptada por la API pública tiene ya un efecto real en los
engines locales.

El proyecto distingue ahora entre:

* `effective`
  * la opción ya participa en la semántica observable
* `accepted-noop`
  * la opción se acepta y valida por compatibilidad, pero todavía no cambia el
    comportamiento real del motor

API pública disponible:

```python
from mongoeco import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
    get_operation_option_support,
    is_operation_option_effective,
)

support = get_operation_option_support("aggregate", "let")
assert support is not None
assert support.status is OptionSupportStatus.EFFECTIVE

assert is_operation_option_effective("find", "hint")
```

Casos relevantes hoy:

* `aggregate(let=...)` -> `effective`
* `find(hint=...)` -> `effective`
* `find(comment=...)` -> `effective`
* `find(max_time_ms=...)` -> `effective`
* `find(batch_size=...)` -> `effective` con batching local del cursor
* `aggregate(batch_size=...)` -> `effective` en pipelines streamables; stages globales siguen materializando completo
* `update_one(let=...)` -> `effective` cuando el filtro usa `$expr`
* `replace_one(let=...)` -> `effective` cuando el filtro usa `$expr`
* `bulk_write(comment=...)` -> `effective`
* `bulk_write(let=...)` -> `effective` cuando las operaciones usan filtros con `$expr`

## 12.1 Superficie de comandos de base de datos

Ademas de la matriz de opciones de la API publica estilo coleccion,
`mongoeco` declara ya una matriz separada para comandos crudos de
`database.command(...)` y para la misma surface expuesta via proxy `wire`.

La diferencia importante es esta:

* `database_commands`
  * declara el inventario de comandos soportados, su familia administrativa y
    si forman parte tambien de la surface wire local, y si tienen
    superficie `explain` declarada
  * en `listCommands`, esa metadata se expone tambien en runtime como
    `adminFamily`, `supportsWire`, `supportsExplain` y `note`
* `operation_options`
  * usa nombres de opciones de la API Python publica (`max_time_ms`,
    `batch_size`, `allow_disk_use`, ...)
* `database_command_options`
  * usa nombres crudos del documento de comando (`maxTimeMS`, `batchSize`,
    `allowDiskUse`, `authorizedCollections`, ...)

Casos relevantes ya declarados:

* `database_commands.find` -> familia `admin_read`, `supports_wire=True`
* `database_commands.dbHash` -> familia `admin_introspection`, `supports_wire=True`
* `database_commands.findAndModify` -> familia `admin_find_and_modify`, `supports_wire=True`
* `database_commands.profile` -> familia `admin_control`, `supports_wire=True`
* `listCommands` expone `adminFamily`, `supportsWire` y `supportsExplain` por comando
* `find(maxTimeMS, batchSize, hint, comment, let)` -> `effective`
* `find(filter, projection, sort, skip, limit)` -> `effective`
* `aggregate(maxTimeMS, batchSize, hint, comment, allowDiskUse, let)` -> `effective`
* `findAndModify(arrayFilters, hint, maxTimeMS, let, comment, sort, bypassDocumentValidation)` -> `effective`
* `listCollections(filter, nameOnly, authorizedCollections)` -> `effective`
* `listDatabases(filter, nameOnly)` -> `effective`
* `count(query, skip, limit, hint, comment, maxTimeMS)` -> `effective`
* `distinct(query, hint, comment, maxTimeMS)` -> `effective`
* `connectionStatus(showPrivileges)` -> `effective`
* `dbHash(collections, comment)` -> `effective`
* `profile(slowms)` -> `effective`
* `createIndexes(comment, maxTimeMS)` -> `effective`
* `validate(scandata, full, background, comment)` -> `effective`
* `explain(find, aggregate, update, delete, count, distinct, findAndModify)` -> `effective`

Notas observables adicionales de runtime:

* `serverStatus.mongoeco` expone tambien `collation` y `sdam`, para hacer
  visible el backend de collation seleccionado y el subconjunto SDAM local
  soportado.
* `serverStatus.mongoeco.changeStreams` expone tambien el backend local y un
  resumen de estado del hub (`persistent`, `boundedHistory`, `retainedEvents`,
  `currentOffset`, `nextToken`), sin necesidad de consultar APIs auxiliares.
* `serverStatus.mongoeco` expone ademas `adminFamilies` y
  `explainableCommandCount`, para resumir la surface administrativa local desde
  la misma fuente de verdad que usa `listCommands` y el catálogo de compat.
* `serverStatus.mongoeco.engineRuntime` expone tambien diagnostico estructurado
  del engine activo (`planner`, `search`, `caches`), incluyendo en SQLite el
  resumen de modos de pushdown (`sql` / `hybrid` / `python`), disponibilidad
  de FTS5, numero de search indexes declarados/pendientes y tamano de caches
  de indices/colecciones.
* `serverStatus.opcounters` refleja ya actividad local real del runtime
  embebido (`insert`, `query`, `update`, `delete`, `getmore`, `command`) en
  lugar de quedar fijado a ceros.
* `validate` mantiene `warnings=[]` en el camino base, pero cuando se usan
  flags aceptados solo por compatibilidad (`scandata`, `full`, `background`)
  devuelve avisos explicitos en vez de silenciarlos.
* `validate` anade tambien warnings reales de TTL cuando detecta indices con
  `expireAfterSeconds` cuyos documentos actuales no contienen ningun valor
  fecha usable; esos documentos no expiraran bajo la semantica TTL local.
* `collStats.totalIndexSize` y `dbStats.indexSize` reflejan ya una medida local
  real del peso de metadata de indices, en lugar de quedar fijados a `0`.
* `listIndexes` expone ya `ns` por documento en la surface administrativa, y
  `explain` devuelve `collection` y `namespace` de forma uniforme para todas
  las rutas soportadas.
* `createIndexes` y `create_index()` aceptan ya `hidden` como metadata local
  explicita del indice, y los `hint` contra indices ocultos fallan de forma
  estable en lugar de ignorarse silenciosamente.
* Los indices `unique` se validan por namespace tambien cuando una ruta
  indexada atraviesa arrays. Entradas multikey repetidas dentro de un mismo
  documento se permiten, pero las que colisionan con otro documento fallan;
  un patron compuesto no puede aportar arrays paralelos desde mas de una ruta
  indexada.
* `explain` en SQLite materializa tambien un bloque `pushdown` para hacer
  visible si la ruta ejecuta SQL puro, plan hibrido o fallback Python, junto
  con fragmento SQL, numero de parametros, residual, collectors, ownership,
  `usesSqlRuntime`, `pythonSort` y `fallbackReason` cuando aplica. El value
  object rechaza SQL sin fragmento, residual sin plan o sort/window SQL antes
  de un residual Python.
  Cuando existe fallback del engine, `planning_issues` incorpora ya tambien un
  issue estructurado con `scope=\"engine\"`, para que tooling no dependa solo de
  interpretar `fallback_reason` como texto libre.
* `aggregate(...).explain()` expone ya tambien un bloque top-level `pushdown`
  con `mode`, `totalStages`, `pushedDownStages`, `remainingStages` y
  `streamingEligible`, para hacer visible cuanto de la pipeline se resolvio en
  la ruta de pushdown y cuanto queda en core.
* SQLite traduce ya tambien `$size` simple, `$mod` entero sobre campos
  escalares y un subconjunto seguro de `$regex` literal
  (`literal`, `^literal`, `literal$`, `^literal$`, `^literal.*`) a SQL en
  explain/ejecucion cuando la ruta no requiere fallback estructural. Si el
  campo contiene arrays o reales, o si el regex usa opciones/semantica mas
  amplia, la ruta sigue degradando a Python para preservar semantica BSON en
  vez de forzar un pushdown incorrecto.
* SQLite traduce ya tambien `$all` sobre arrays escalares simples y
  `$elemMatch` muy acotado sobre arrays escalares top-level cuando el predicado
  interno puede compilarse a una condicion SQL segura.
* Dentro de ese subconjunto, SQLite acepta tambien `$options: "i"` solo para
  patrones literales ASCII y fields que no contienen texto no ASCII, evitando
  prometer un `ignoreCase` Unicode que el backend SQL no pueda reproducir con
  fidelidad.
* Las comparaciones de rango (`$gt`, `$gte`, `$lt`, `$lte`) admiten ya tambien
  pushdown SQL en paths top-level que mezclan escalares y arrays, siempre que
  todos los escalares y elementos del array pertenezcan al mismo tipo
  comparable (`number`, `string` o `bool`).
* `find(...).explain()` en SQLite expone ya tambien `pushdown_hints` cuando una
  query cae a fallback por limites del engine, para señalar de forma
  estructurada que operador esta bloqueando el pushdown y cual seria la siguiente
  extension natural de esa familia. Esos hints ya clasifican no solo familias de
  operador como `$regex`, `$mod`, `$all` o `$elemMatch`, sino tambien bloqueos
  estructurales como `sort`, `collation`, `array-comparison` o
  `array-traversal`.
* `profile` expone ya tambien `namespaceVisible`, `trackedDatabases` y
  `visibleNamespaces`, ademas de `level` y `entryCount`.
* `listCommands` expone ya tambien `supportsComment` y `supportedOptions`, para
  que tooling local pueda descubrir desde runtime que opciones raw estan
  declaradas como soportadas por cada comando.
* Los explains de search exponen tambien detalles de lifecycle/backend
  (`backendAvailable`, `backendMaterialized`, `physicalName`, `readyAtEpoch`,
  `fts5Available`) para hacer visible el estado real del indice de busqueda en
  tiempo de ejecucion.

La surface wire local queda verificada tambien contra cliente PyMongo real para
familias administrativas ya soportadas como:

* `listCollections`
* `listDatabases`
* `collStats`
* `dbStats`
* `listIndexes`
* `createIndexes`
* `dropIndexes`
* `findAndModify`
* `count`
* `distinct`
* `dbHash`
* `validate`
* `explain`

La exportacion publica queda disponible en:

```python
from mongoeco.compat import (
    export_database_command_catalog,
    export_database_command_option_catalog,
)

command_catalog = export_database_command_catalog()
option_catalog = export_database_command_option_catalog()

assert command_catalog["find"]["family"] == "admin_read"
assert command_catalog["find"]["supports_comment"] is True
assert "comment" in command_catalog["find"]["supported_options"]
assert option_catalog["find"]["batchSize"]["status"] == "effective"
```

Regla de mantenimiento:

* no se debe promocionar una opción a `effective` sin test observable
* no se debe aceptar una opción nueva sin registrarla en esta matriz
  * `max_time_ms` en `find_one_and_*`
  * `hint`, `comment`, `let`, `batchSize/maxTimeMS` en `aggregate`
* delta real desde `4.11+`:
  * `sort` en `update_one`
  * `sort` en `replace_one`
  * `sort` en `UpdateOne(...)` y `ReplaceOne(...)` para `bulk_write`
* explícitamente no soportado en `4.9+`:
  * `max_time_ms` en `update_one`, `update_many`, `replace_one`,
    `delete_one` y `delete_many`

## 13. Surface wire declarada dentro de 8.0 / 4.x

Dentro del alcance soportado de `MongoDB 8.0` y `PyMongo 4.x`, el proxy wire
declara ya cobertura contractual para familias administrativas y de control
que antes quedaban menos fijadas por tests reales:

* introspeccion/control:
  * `buildInfo`
  * `listCommands`
  * `connectionStatus`
  * `serverStatus`
  * `hostInfo`
  * `getCmdLineOpts`
  * `whatsmyuri`
  * `profile`
  * `dbHash`
* admin read/stats:
* `count`
* `distinct`
* `collStats`
* `dbStats`
* `validate`
* `explain`

En agregacion local se considera ya tambien parte del subset estable:

* `$collStats` como stage inicial de introspeccion local
  * `count`
  * `storageStats`

Ademas, el proxy endurece la validacion temprana de payloads malformed para:

* `auth`: `authenticate`, `saslContinue`
* `sessions`: `endSessions`, `commitTransaction`, `abortTransaction`
* `cursor`: `getMore`, `killCursors`

El objetivo es que el wire falle antes y con mensajes publicos estables,
evitando que errores de shape atraviesen varias capas antes de materializarse.

## 14. Qué no hace `mongoeco`

`mongoeco` no:

* infiere la semántica del servidor MongoDB a partir de la versión instalada de
  `pymongo`
* acepta silenciosamente majors nuevas de `pymongo`
* mezcla dialecto de servidor y perfil de driver en una sola opción

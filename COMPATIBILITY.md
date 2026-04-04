# Compatibility Guide

Arquitectura relacionada:

* [docs/architecture/index.md](docs/architecture/index.md)
* [docs/architecture/testing-and-compatibility.md](docs/architecture/testing-and-compatibility.md)

Esta guía resume cómo configurar `mongoeco` cuando quieres controlar:

* la semántica objetivo de MongoDB (`mongodb_dialect`)
* la superficie pública objetivo de PyMongo (`pymongo_profile`)

## 1. Dos ejes distintos

`mongoeco` separa dos conceptos:

* `mongodb_dialect`
  * controla semántica observable del servidor
  * ejemplos: comparación con `null`, tratamiento de `undefined`, validaciones y deltas de MQL
* `pymongo_profile`
  * controla compatibilidad de la API Python
  * ejemplos: parámetros aceptados por métodos públicos o diferencias pequeñas de superficie

La versión instalada de `pymongo` **no** decide la semántica del servidor MongoDB.

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
  `exists`, `near` y `compound`
  como subset explícito y documentado, sin pretender semántica Atlas Search
  completa.
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
* la proyeccion avanzada de `find` cubre ya el subconjunto diario mas util
  (`$slice`, `$elemMatch`, proyeccion posicional y `$meta: "textScore"`);
* `$collStats` existe tanto como comando administrativo como stage de
  agregacion local de introspeccion;
* los indices `hidden` existen como opcion local honesta: se listan y se
  preservan en metadata, pero el planner no los usa ni acepta `hint` contra
  ellos.

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

Regla práctica:

* `4.9` es la baseline de API pública
* `4.11` activa el primer delta real: `update_one(sort=...)`
* `4.13` queda disponible como perfil posterior compatible
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
  * `explain` con backend, modo, similitud, escaneo, candidatos evaluados,
    vectores válidos/inválidos y razones de degradación.
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
  * `phrase`
  * `autocomplete`
  * `wildcard`
  * `exists`
  * `near`
  * `compound`
* surface observable:
  * `explain()` con `queryOperator`, paths y backend real;
  * `SearchIndexDocument.capabilities` alineado con el runtime real;
  * `SQLiteEngine` usando FTS5 cuando la traducción es defendible y fallback
    Python cuando no lo es.

Límites conscientes:

* no hay `facet`, `range` ni scoring Atlas-like;
* `wildcard` sigue siendo matching local simple, no sintaxis Atlas Search;
* `autocomplete` es local y basado en prefijos de tokens, no en analyzer
  avanzado;
* `near` entra como subset local para valores numericos y fecha/datetime,
  con `path`, `origin` y `pivot`, y ordena por cercania local sin pretender
  scoring Atlas Search completo;
* `compound` se limita a combinar el subset local soportado
  (`text`/`phrase`/`autocomplete`/`wildcard`/`exists`/`near`) con `must`,
  `should`, `filter`, `mustNot` y `minimumShouldMatch`;
* `SQLiteEngine` usa FTS5 directo para `text`, `phrase` y `autocomplete`, y
  usa el backend materializado como prefilter de candidatos para `wildcard`,
  `exists` y parte de `compound` antes del matching Python exacto;
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
* `explain` en SQLite materializa tambien un bloque `pushdown` para hacer
  visible si la ruta ejecuta SQL puro, plan hibrido o fallback Python, junto
  con `usesSqlRuntime`, `pythonSort` y `fallbackReason` cuando aplica.
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

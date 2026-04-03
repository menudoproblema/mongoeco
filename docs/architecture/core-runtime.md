# Runtime semantico

## Que vive en `core`

`core` concentra la semantica ejecutable de `mongoeco`. Su funcion es responder
a la pregunta: "que significa esta operacion?" antes de decidir "como la
ejecuta este engine?".

Los bloques principales son:

- query planning y filtering;
- sorting y projections;
- updates, paths y operadores;
- aggregation;
- schema validation;
- search query modeling;
- semantica BSON;
- collation y comparacion.

## Patron `compile-then-execute`

La arquitectura semantica sigue un patron consistente:

1. normalizar shape publico;
2. compilar a una forma semantica interna;
3. ejecutar esa semantica;
4. exponer el plan, la degradacion o el error de forma explicita.

Esto aparece en:

- `FindOperation` -> `EngineFindSemantics`;
- `UpdateOperation` -> `EngineUpdateSemantics`;
- `AggregateOperation` -> pipeline y planning de aggregation;
- validadores de coleccion -> `CompiledCollectionValidator`.

## Query planning y filtering

`query_plan.py` compila filtros a un arbol de nodos semanticos. `filtering.py`
los ejecuta y `compiled_query.py` ofrece una ruta compilada cuando el shape lo
permite.

Dentro de `filtering`, la fachada estable sigue siendo `QueryEngine`, pero la
semantica ya no vive en un unico bloque:

- `_filtering_support.py` concentra paths, regex y mappings auxiliares;
- `_filtering_matching.py` concentra igualdad, comparacion y membership;
- `_filtering_specials.py` concentra `type`, `bitwise`, `mod`, `regex`,
  `size`, `all` y `elemMatch`;
- `filtering.py` conserva `QueryEngine`, `BSONComparator` y el dispatch desde
  `QueryNode`.

`_filtering_support.py` concentra:

- division y cacheo de rutas;
- acceso a campos y expansion de candidatos;
- compilacion de regex y validacion de opciones;
- normalizacion de mappings especiales como `DBRef`.

La idea no es solo acelerar consultas. Tambien:

- separar parsing y matching;
- separar matching escalar de operadores especiales;
- fijar reglas semanticas en un solo punto;
- permitir que los engines decidan pushdown o fallback sin reinterpretar la
  query publica desde cero.

En esta misma capa entra ya un subset geoespacial local y explícito:

- `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`,
  `MultiPolygon` y `GeometryCollection`, ademas de pares `[x, y]` para puntos;
- `$geoWithin` con `Polygon`, `MultiPolygon` y legacy `$box`;
- `$geoIntersects` sobre cualquier geometria soportada del subset;
- `$near` y `$nearSphere` con distancia minima planar desde el punto consultado
  hasta la geometria almacenada.

La arquitectura vuelve a ser la misma: el planner compila ese subset a nodos
propios y el engine decide si puede empujarlo o si debe degradar a Python. En
SQLite, hoy queda documentado como fallback explícito y observable vía
`fallback_reason`, `planning_issues` y `pushdown_hints`.

Tambien entra ya un subset local de `$text` clasico:

- filtro top-level `$text`;
- tokenizacion local explicita;
- `textScore` materializado para proyeccion y ordenacion;
- `MemoryEngine` como baseline semantico y `SQLiteEngine` como fallback Python
  observable en `explain()`.

Y el runtime local de `search` queda ya ampliado a un subset explicito de
`$search`:

- `text`;
- `phrase`;
- `autocomplete`;
- `wildcard`.
- `compound`.

La regla arquitectonica es la misma:

- `core/_search_contract.py` fija el inventario declarativo de operadores
  textuales soportados y evita que runtime, tipos y snapshots mantengan listas
  distintas;
- `core/search.py` define el contrato semantico y el matching baseline;
- `core/search.py` usa ya un registro explicito de operadores para separar
  compilacion de clause, matching baseline y shape de explain, en vez de seguir
  creciendo por cadenas de `if/elif`;
- `MemoryEngine` actua como baseline observable;
- `SQLiteEngine` usa FTS5 cuando la traduccion sigue siendo defendible
  (`text`, `phrase`, `autocomplete`) y cae a Python cuando no (`wildcard`).
  `compound` se apoya hoy en ese baseline Python local y no intenta fingir un
  planner FTS compuesto mas ambicioso de lo que existe realmente.

## Sorting, projection y updates

El runtime comparte reglas para:

- ordenacion BSON y collation-aware;
- projection de documentos;
- rutas de actualizacion;
- operadores escalares y de arrays;
- materializacion de upserts.

Esto reduce deriva entre engines y evita que cada backend implemente su propia
semantica ad hoc.

Dentro del objetivo embebido/local, esta capa ya considera baseline:

- projection posicional en `find`;
- operadores de proyeccion como `$slice` y `$elemMatch`;
- `$meta: "textScore"` cuando existe un filtro `$text`.

## Aggregation

`core/aggregation` es un subsistema propio con varios niveles:

- planning de pipeline;
- evaluacion de expresiones;
- stages de transformacion, join y grouping;
- runtime compilado y no compilado;
- control de costes y spill guardrails.

El objetivo no es imitar cada detalle de un servidor MongoDB completo, sino dar
una semantica local consistente y suficientemente honesta sobre:

- que stages se soportan;
- que gaps existen;
- cuando un pipeline puede o no ejecutarse.

En la superficie publica, `aggregate().explain()` ya deja visible ademas un
resumen estructurado de pushdown (`mode`, stages empujados, stages restantes y
si la pipeline puede ejecutarse en streaming por batches). Eso evita depender
solo de `remaining_pipeline` para inferir como se repartio la ejecucion entre
engine y core.

La pipeline materializada soporta tambien ya stages analiticos locales como
`$densify` y `$fill`, y stages con side effects locales como `$merge`. En este
ultimo caso la decision de arquitectura es explicita: el runtime mantiene
`apply_pipeline()` como transformacion pura y deja la escritura final de
`$merge` en la capa de cursor/ejecucion, para no mezclar stages puros con
efectos persistentes.

Tambien soporta ya `$collStats` como stage inicial de introspeccion local. La
frontera importante es que el stage no reconstruye snapshots administrativos
por su cuenta: consume un `collection_stats_resolver` inyectado desde la capa
de cursor, para mantener separadas la semantica del stage y la obtencion de
stats de coleccion.

Ese mismo subsistema soporta ya tambien `$geoNear` como stage materializante
local con semantica planar explicita. La restriccion consciente ya no esta en
las geometrías soportadas, sino en el modelo espacial: requiere `key`
explicito y no pretende simular geodesia real ni indices espaciales de
servidor.

En el caso de SQLite, `find(...).explain()` deja ya tambien issues
estructurados del engine cuando la ruta cae a hibrido o Python (`scope="engine"`),
y el conjunto de operadores simples empujables a SQL sigue creciendo de forma
incremental; por ejemplo, `$size` simple ya no obliga a fallback cuando la ruta
puede resolverse con `json_array_length(...)`, y `$mod` entero sobre campos
escalares puede empujarse a SQL cuando la coleccion no mezcla arrays ni reales
en ese path. Del mismo modo, SQLite puede empujar un subconjunto seguro de
`$regex` literal (`contains`, `prefix`, `suffix`, `exact`) cuando el field es
escalar string y no requiere semantica de arrays, opciones o evaluacion regex
mas amplia. A ese conjunto se suman ya `$all` sobre arrays escalares simples,
`$elemMatch` muy acotado sobre arrays escalares top-level y comparaciones de
rango sobre paths que mezclan escalares y arrays cuando todos los valores
siguen siendo homogeneos en el mismo tipo comparable. Cuando el pushdown no aplica, `find(...).explain()` deja tambien
`pushdown_hints` estructurados para priorizar que familia de operador seria la
siguiente candidata natural a ampliar. En el caso de `$options: "i"`, esa ruta
SQL queda limitada de forma consciente a patrones ASCII literales sobre texto
ASCII, para no fingir una semantica Unicode-insensitive que SQLite no garantiza
igual que el runtime Python. Esos `pushdown_hints` tambien recogen bloqueos
estructurales del planner (`sort`, `collation`, `array-comparison`,
`array-traversal`, `dbref-subfield`, etc.), de modo que el explain funciona
como backlog tecnico observable y no solo como diagnostico de una query
concreta.

## Schema validation

La validacion de coleccion se compila desde las opciones de la coleccion y se
aplica como semantica reutilizable. Eso permite:

- crear colecciones con validadores;
- validar inserts, replacements y updates;
- degradar o fallar segun accion configurada.

Es importante documentar que esto forma parte del runtime semantico, no del
engine fisico.

## Semantica BSON y tipos

`core/bson_scalars.py`, `core/bson_ordering.py` y `mongoeco.types` modelan gran parte
de la semantica de tipos BSON:

- ordenacion;
- wrappers numericos;
- `ObjectId`, `Decimal128`, `Regex`, `DBRef`, `UNDEFINED`, etc.;
- coerciones y operaciones compatibles con BSON.

Esto es clave para entender por que la semantica no depende sin mas de los tipos
nativos de Python.

La superficie publica sigue siendo `mongoeco.types`, pero su implementacion se
apoya ya en modulos internos agrupados por dominio (`_types`). Eso separa:

- `ObjectId` y helpers de deteccion/normalizacion;
- wrappers BSON y fallback nativo frente a `bson` opcional;
- concerns y transacciones;
- documentos y snapshots;
- indices y search indexes;
- write models y resultados.

## Collation

`core/collation.py` normaliza el contrato de collation y decide como comparar
valores segun el backend disponible. La arquitectura actual explicita:

- `simple` como comparador BSON/Python base;
- `en` como locale Unicode soportado;
- `PyICU` como backend preferido y opcional;
- `pyuca` como fallback del subset soportado;
- capacidad introspectable via `collation_backend_info()` y
  `collation_capabilities_info()`.

## `planning_mode`

`planning_mode` afecta a la frontera entre `api` y `core`:

- `STRICT` exige que el shape compile a algo ejecutable de forma coherente;
- `RELAXED` conserva metadata y hace visibles `planning_issues`.

No es un detalle cosmetico. Es una decision de arquitectura para hacer visibles
los limites del runtime y permitir tooling, `explain` y degradacion contractual
sin no-ops silenciosos.

## Search y vector search locales

El runtime local de search distingue ya dos familias:

- `$search`, con subset local de operadores textuales;
- `$vectorSearch`, con baseline exacta y backend ANN local.

En esta fase, `vectorSearch` soporta:

- similitud `cosine`, `dotProduct` y `euclidean`;
- `filter` opcional reutilizando `QueryEngine`;
- backend `usearch` en `SQLiteEngine` con baseline exacta en `MemoryEngine`;
- explain con backend real, modo ANN/exacto, paths vectoriales, similitud,
  shape del filtro y metadata de materializacion.

La decision sigue siendo consciente: no hay Atlas Search remoto, ANN
distribuido ni embeddings automaticos. La compatibilidad se modela como subset
local defendible, no como equivalencia con Atlas Search.

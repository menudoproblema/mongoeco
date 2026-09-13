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

El cliente selecciona y valida una vez su adapter SPI v2. Las bases,
colecciones y clones de opciones creados desde ese cliente reutilizan el mismo
adapter, por lo que no vuelven a validar el contrato del engine ni a registrar
el mismo consumidor de cambios por cada wrapper. El ownership sigue siendo del
cliente: construir directamente una `AsyncCollection` mantiene el fallback de
adaptacion propio y dos clientes no comparten estado por recibir la misma
instancia de engine. La propuesta SPI v3 formaliza esta separacion como
`EngineContract`, `EngineRuntime` y `BoundEngineNamespace`; 4.x no expone esos
tipos ni cambia SPI v2.

### Preparacion de lecturas y proyecciones

La coleccion entrega al cursor la misma `FindOperation` normalizada y ligada
que ha preparado. Los mutadores permitidos invalidan su preparacion; no se
reconstruye la operacion al cruzar cada capa. El scan normal compila su
`EngineFindSemantics` en el coordinador de coleccion; la ruta de fallback del
cursor conserva su propia preparacion para engines adaptados.

Cada stream de resultados proyectados conserva un programa privado de
proyeccion, incluidos operandos propios y el arbol de rutas inclusivas. Se
prepara en la primera fila efectivamente proyectada, no por documento. Esto
conserva el momento de validacion: una consulta vacia o cuyas filas se omiten
con `skip` no empieza a rechazar una proyeccion que nunca llega a ejecutar.
Las aplicaciones sucesivas producen resultados mutables independientes. La
proyeccion de un unico documento no necesita retener/copiar sus operandos.

El programa no es una cache global de sesiones, reloj o resultados. El contexto
ligado conserva identidad, variables y reloj durante la ejecucion; `clone`
crea un contexto nuevo y `rewind` conserva el contexto del cursor original.
Los matchers dependientes del documento siguen evaluandose por documento.
Esta preparacion interna no modifica SPI v2 ni elimina las fronteras de copia
defensiva, conversion BSON y codecs de la salida publica.

### Materializacion publica y orden de codecs

La salida de coleccion comparte un materializador privado para lecturas,
agregacion, informacion de indices y change streams. Con `document_class=dict`
y sin decoders personalizados, construye los contenedores dict/list comunes
una sola vez. Conserva una lista de ubicaciones que necesitan conversion de
codec posterior, como fechas, UUID y subarboles tuple o de clases especiales.
Esas conversiones se ejecutan solo despues de completar toda la conversion
BSON; un fallo BSON posterior no dispara prematuramente efectos de timezone
o decoders de un campo anterior.

Las subclases de opciones, las clases de documento y registros de tipos
personalizados, la ausencia de BSON opcional y la ausencia de opciones
mantienen la ruta de dos fases previa.
No se cambia el orden de callbacks ni se aplican codecs recursivamente dentro
de un BSON `Code` o `DBRef` que antes se trataba como escalar. La lista diferida
crece con los valores que requieren trabajo de codec, no constituye una cota
global de memoria. Los contenedores publicos siguen siendo propios; esta fusion
no elimina las fronteras defensivas de snapshots ni autoriza a los engines
externos a entregar buffers prestados.

Memory aplica el programa de proyeccion sobre su documento interno antes de
construir los contenedores publicos que sobreviven a ella. La copia publica se
hace por demanda al consumir cada lote; no se construyen N documentos publicos
para entregar la primera fila. Esta regla no convierte en streaming operadores
bloqueantes: una ordenacion sin acceso ya ordenado debe reunir y ordenar sus
candidatos antes de producir el primero, aunque difiere su proyeccion y copia.

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
- indices `text` locales de uno o varios campos `text`, incluidos compound
  mixtos con claves ordenadas;
- tokenizacion local explicita;
- `textScore` materializado para proyeccion y ordenacion;
- `hint` por nombre de indice para desambiguar varios text indexes locales;
- `MemoryEngine` como baseline semantico y `SQLiteEngine` como fallback Python
  observable en `explain()`.

Y el runtime local de `search` queda ya ampliado a un subset explicito de
`$search`:

- `text`;
- `phrase`;
- `autocomplete`;
- `wildcard`;
- `exists`;
- `in`;
- `equals`;
- `range`;
- `near`;
- `compound`.

La regla arquitectonica es la misma:

- `core/_search_contract.py` fija el inventario declarativo de operadores
  textuales soportados y evita que runtime, tipos y snapshots mantengan listas
  distintas;
- `core/search.py` define el contrato semantico y el matching baseline;
- `core/search.py` usa ya un registro explicito de operadores para separar
  compilacion de clause, matching baseline y shape de explain, en vez de seguir
  creciendo por cadenas de `if/elif`;
- `MemoryEngine` actua como baseline observable y reutiliza una materializacion
  textual por indice para no repetir tokenizacion por documento en cada search;
- `SQLiteEngine` usa FTS5 cuando la traduccion sigue siendo defendible
  (`text`, `phrase`, `autocomplete`) y, en `wildcard`, `exists` y parte de
  `compound`, usa la tabla materializada como prefilter de candidatos antes
  del matching Python exacto;
- `in`, `equals` y `range` entran ya como operadores locales explicitos sobre paths
  escalares, con matching baseline compartido y explain coherente aunque no
  intenten fingir una traduccion Atlas-like al backend;
- `compound` deja visible en `explain()` tanto el inventario de operadores por
  clausula como el `compoundPrefilter` que esta reduciendo candidatos en SQLite,
  y usa ya ranking local por `should` con sensibilidad a `near`;
- cuando los `should` candidateables son amplios, `compoundPrefilter` deja
  visible tambien cuantas clausulas `should` son realmente candidateables para
  distinguir ranking util de falso pruning.
- cuando despues de `$search` solo hay stages que preservan orden/cardinalidad
  y una ventana final `skip/limit`, el runtime propaga ese `top-k` seguro al
  engine; en SQLite eso permite recortar candidatos antes de materializar todo
  el conjunto para ranking final.
- cuando la pipeline posterior a `$search` sigue siendo `prefix-monotonic`
  pero ya incluye filtros por documento como `$match`, el cursor usa expansion
  iterativa sobre prefijos ordenados y `explain()` distingue esa ruta con
  `searchTopKStrategy="prefix-iterative"`.
- esa expansion ya no crece doblando a ciegas: usa `adaptive-retention`,
  estimando el siguiente prefijo a partir de la tasa real de descarte observada
  en iteraciones previas.
- cuando esa pipeline `prefix-monotonic` empieza con uno o varios `$match`,
  el runtime puede pasarlos al engine como `downstreamFilterPrefilter` exacto
  para reducir candidatos antes del ranking final. Ese prefilter no se intenta
  si el `match` aparece despues de stages transformadores.
- `near` entra como operador local para numericos y fecha/datetime con
  `path`, `origin` y `pivot`, y mantiene explain/backends explicitos en vez de
  fingir scoring Atlas Search completo.

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

Cuando el limite existente `max_materialized_documents` aplica a una pipeline
bloqueante y no hay spill disponible, el cursor lee como maximo `limite + 1`
documentos de la fuente. Ese ultimo documento basta para conservar el mismo
rechazo y el mismo error sin materializar el resto ni cargar colecciones
referenciadas; el cursor fuente se cierra tambien si falla esa admision. Una
fuente con exactamente el limite se consume completa y se acepta.

Esta optimizacion solo acredita la admision inicial de la fuente. El limite
vigente cuenta documentos, no bytes, y todavia no suma foreign collections,
expansion de stages, acumuladores ni buffers de sort/spill. Tampoco convierte
el round-trip generico a disco en memoria acotada. Esa contabilidad compuesta y
los algoritmos externos por operador permanecen dentro de P7/P8.

El sort con spill divide la entrada en runs de como maximo el umbral y los
fusiona en varias pasadas con un fan-in maximo de 32 runs de lectura. Cada
pasada mantiene un documento decodificado por run y elimina sus temporales al
terminar; error, deadline o cancelacion limpian tambien runs originales e
intermedios. Esto acota descriptores y heap de fusion, no la memoria total: la
interfaz vigente recibe la entrada ya materializada y devuelve otra lista
completa. Convertir esas dos fronteras en streams y sumar sus bytes al budget
compuesto sigue pendiente.

El `$lookup` simple por `localField`/`foreignField` puede construir un indice
hash efimero sobre la coleccion foreign. Su admision reutiliza
`max_materialized_documents` como maximo de asociaciones y, cuando existe una
politica de spill, toma tambien su umbral como techo. Si una ruta multikey
supera esa capacidad, o si intervienen collation, pipeline correlacionada,
tipos no cubiertos o un dialecto personalizado, se conserva el nested loop
canonico. El indice solo selecciona candidatos: cada par se revalida con la
igualdad BSON existente, conserva el orden foreign y elimina duplicados por
documento antes de copiar el resultado.

Este limite hace acotada la estructura auxiliar, pero no acredita todavia un
budget global: la lista foreign y el resultado pueden seguir creciendo. El
acceso foreign mediante consulta estable y la contabilidad compuesta permanecen
en P7/P8.

El deadline de la operacion se propaga por los runtimes compilado e interpretado
y se comprueba dentro de los bucles Python de transformacion, unwind, sort y
top-k, spill, group/bucket/window, joins, facet/union, densify/fill/geo y stages
informativos. Entre dos comprobaciones de una misma iteracion o comparador se
ejecutan como maximo 256 pasos; los caminos que expanden resultados reutilizan
la misma primitiva de control. Esto es cancelacion cooperativa, no una garantia
hard real-time: una evaluacion de expresion, comparacion BSON, operacion del
codec, llamada a una extension o primitiva de I/O puede ser indivisible. El
budget compuesto de bytes, el streaming de entrada/salida del sort externo y
los algoritmos externos de group siguen pendientes.

Cuando el primer operador bloqueante que queda despues del pushdown es
`$group`, el cursor alimenta un estado acumulador incremental desde trabajos de
lectura finitos. Respeta el `batchSize` solicitado o usa bloques internos de
256 documentos, aplica antes el prefijo streamable y conserva globalmente sus
`$skip`/`$limit`. Sin spill, la admision existente sigue deteniendose
exactamente en `limite + 1`; con spill disponible no se retiene la entrada
completa. La memoria del acumulador depende del numero de grupos y de
acumuladores cuyo resultado crece (`$push`, `$addToSet`, etc.), que es salida
inevitable y no se presenta como O(1).

El resultado de grupos y los stages bloqueantes posteriores aun usan la
frontera materializada vigente. Particionar estado de acumuladores a disco y
convertir la salida del sort externo en stream requieren el budget compuesto;
siguen pendientes y no se ocultan bajo la mejora de entrada incremental.

En la superficie publica, `aggregate().explain()` ya deja visible ademas un
resumen estructurado de pushdown (`mode`, stages empujados, stages restantes y
si la pipeline puede ejecutarse en streaming por batches). Eso evita depender
solo de `remaining_pipeline` para inferir como se repartio la ejecucion entre
engine y core. `pushdown.lookupPlans` informa si cada join es candidato al hash
acotado o requiere nested loop, junto con el motivo y la capacidad. Es una
decision de planning: la saturacion observada al construir el indice puede
degradar a nested loop sin cambiar resultados ni errores publicos.
`incrementalGroupInput` distingue el acumulador alimentado por lotes de una
pipeline completamente materializada; `sourceBatchExecution` agrupa esa ruta y
el streaming completo sin afirmar que la salida de `$group` sea incremental.

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
- `filter` opcional reutilizando `QueryEngine`, con ampliacion adaptativa de
  candidatos antes de degradar a exacto;
- backend `usearch` en `SQLiteEngine` con baseline exacta en `MemoryEngine`;
- explain con backend real, modo ANN/exacto, paths vectoriales, similitud,
  shape del filtro y metadata de materializacion.

La decision sigue siendo consciente: no hay Atlas Search remoto, ANN
distribuido ni embeddings automaticos. La compatibilidad se modela como subset
local defendible, no como equivalencia con Atlas Search.

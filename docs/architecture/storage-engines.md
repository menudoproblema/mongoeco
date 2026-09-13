# Engines de almacenamiento

## Contrato base

La capa `engines` expone un contrato de almacenamiento basado en protocolos
delgados en vez de una jerarquia abstracta grande. `AsyncStorageEngine`
compone varios protocolos parciales:

- lifecycle;
- sesiones;
- CRUD;
- indices;
- search index admin;
- planning de lectura;
- explain;
- administracion de bases y namespaces;
- profiling.

Esto hace que el contrato se describa por capacidades y no por herencia forzada.

### SPI versionado y frontera de operacion

La API no inspecciona ya cada engine con flags privados. `EngineCapabilities`
declara la version del SPI, snapshots y estrategia de entrega de cambios. Los
engines v2 retornan siempre `MutationOutcome`, `DeleteOutcome`, `InsertOutcome`
o `MergeOutcome`; `LegacyEngineAdapter` concentra la compatibilidad con
retornos union, callbacks y `put_document` del SPI v1.

Toda operacion CRUD o lectura compilada del SPI v2 recibe un
`OperationContext` inmutable creado en el borde publico. Ese objeto captura una
sola vez dialecto, collation normalizada, bindings y `$$NOW`, codec, sesion y
politica de publicacion. Filtros, proyecciones, updates, pipelines y `let`
atraviesan la misma conversion BSON recursiva antes de compilarse, incluidas
las rutas de comandos administrativos. Los engines no deben volver a resolver
estos valores ni consultar el reloj para completar una operacion que ya lleva
contexto. Las operaciones administrativas conservan sus protocolos delgados
especificos, no una semantica BSON paralela.
El binding forma parte de la compilacion: un cambio efectivo de dialecto,
collation o bindings recompila el plan; un contexto semanticamente identico
reutiliza el artefacto ya normalizado.

### Snapshots de lectura

Los scans v2 se consumen como `ReadSnapshot`, no como iterables sin ownership.
Un engine puede abrirlo de forma nativa o declarar el fallback compatible
`scan_find_semantics`, que el adaptador envuelve en el mismo contrato. Su
metadata declara una politica `STABLE`, `MATERIALIZED` o `LIVE`, y su lifecycle
garantiza cierre tanto al agotar como al cancelar o fallar. Los cursores de
coleccion consumen hoy snapshots `STABLE`; una nueva implementacion debe
declarar explicitamente cualquier politica distinta.
El ownership incluye un plazo finito de cleanup. Una fuente externa que no
responde no puede bloquear indefinidamente la cancelacion del consumidor.
El estado observable diferencia cierre pendiente, completado y fallido.
El supervisor conserva todas las obligaciones async que siguen activas y las
retira al terminar; superar su umbral diagnostico registra presion, pero no
cancela el unico cleanup responsable de un recurso. Ese conjunto puede crecer
con fuentes externas atascadas: SPI v2 no declara admision de snapshots y no se
presenta el umbral como cota real. La propuesta SPI v3 liga esa cota a un lease
adquirido antes del recurso.

## `MemoryEngine`

`MemoryEngine` es el backend mas directo y actua como baseline semantico local.
Su valor arquitectonico no es solo la sencillez:

- facilita parity tests;
- sirve como referencia para semantica compartida;
- mantiene MVCC local con snapshots;
- implementa metadata, profiling, indices y search indexes sin depender de un
  backend externo.
- En `$search` textual reutiliza una materializacion ligera por indice para no
  reconstruir entradas y tokens en cada consulta repetida.

Se usa como baseline semantico, no como "mock" desechable.

### Publicacion de estado y transacciones Memory

Las escrituras ordinarias, cambios de catalogo y purgas TTL comparten una
generacion de estado con el commit transaccional. Esta generacion no es la
secuencia del outbox: una mutacion con publicacion de eventos deshabilitada
tambien invalida una transaccion escritora que parta de una version anterior.
Las transacciones de solo lectura se cierran sin reinstalar sus contenedores.
El indicador de escritura pertenece al estado transaccional, no se infiere
de que existan eventos pendientes.

La publicacion valida que almacenamiento, indices y catalogo pertenezcan a la
misma vista vigente. Los journals de namespace y la instalacion de la nueva
generacion comparten el guard de metadatos. La preparacion del outbox ocurre
antes de instalar su secuencia e historial; un fallo mantiene o restaura el
estado anterior. Los scopes anidados de un rename comparten ownership. Los
hooks de transaccion rechazan reentrada mientras se publica estado parcial;
los callbacks de outcomes y de entrega no se ejecutan dentro de ese scope.

Esta contencion conserva conflictos globales conservadores. Al confirmar, la
transaccion prepara roots nuevos que fusionan solo las bases o namespaces
registrados por sus scopes de escritura; los namespaces ajenos conservan sus
roots vivos y nunca se reinstala indiscriminadamente la captura completa. La
preparacion ocurre antes de sustituir cualquiera de los seis roots
autoritativos, por lo que un fallo conserva todos los roots anteriores.
`rename` registra ambos nombres y `drop_database` declara alcance de base
completo.

Las mutaciones de documentos usan un journal de undo: registran solo las
entradas de storage, las
pertenencias a buckets de indices y la metadata de alta que realmente cambian.
El rollback no reinvoca el helper que fallo y restaura tambien los ordinales de
insercion. DDL conserva su snapshot de coleccion porque cambia catalogos enteros.
Iniciar una transaccion todavia captura los contenedores superiores de bases,
namespaces y catalogos para fijar un instante coherente. Esta copia depende del
numero de namespaces e indices, no de las filas; no se presenta como admision
por bytes ni como un nuevo nivel de aislamiento.

### Seleccion de candidatos y orden Memory

Scan, count, update y delete seleccionan un acceso secundario elegible antes
de enumerar el almacenamiento. El catalogo, los buckets y la collation usados
pertenecen a la vista de la operacion. La seleccion no sustituye el filtro
residual ni amplía la elegibilidad de indices parciales, multikey o collation.
Un conjunto vacio es un resultado del indice, no una peticion de scan completo.

Cada coleccion conserva ordinales de insercion. Reemplazar un documento mantiene
su ordinal; eliminarlo retira sus metadatos y reinsertarlo recibe uno posterior.
Cada bucket secundario conserva tambien una raiz persistente ordenada por esos
ordinales. Recuperar los primeros K candidatos cuesta O(log C + K), sin ordenar
el bucket completo ni recorrer filas ajenas; recuperar todos cuesta O(C). Los
ordinales pertenecen a la misma vista que documentos y buckets y se conservan
al capturar, restaurar o renombrar la coleccion; no son identidades globales ni
metadata publica BSON.

Documentos, ordinales y enlaces de orden viven en raices HAMT inmutables. Una
copia superficial comparte esas raices; reemplazar, insertar o borrar sustituye
solo los caminos afectados. El orden persistente por bloques conserva scans
O(N); sus huecos se acotan y compactan dentro de cada bloque, nunca mediante
una copia de la coleccion completa. Los buckets secundarios usan la misma
regla: las raices de pertenencia y orden se comparten y una mutacion reemplaza
solo los caminos del bucket afectado. Un `limit` exacto sobre igualdad puede
pedir solo su prefijo natural; los filtros residuales no recortan candidatos
antes de evaluarlos.

Los indices TTL Memory mantienen, junto al bucket secundario, una agenda
persistente ordenada por `(vencimiento, token)`. Cada documento aporta como
maximo una entrada por indice, calculada con la fecha minima cuando el campo es
un array. Consultar el siguiente vencimiento cuesta O(log N) y enumerar K
candidatos vencidos cuesta O(log N + K); si ninguno ha vencido no se decodifica
ningun documento. Aplazar o eliminar una fecha retira la entrada anterior antes
de insertar la nueva, de modo que no existe basura de tokens obsoletos que
compactar.

La agenda se calcula independientemente de sparse/partial y la elegibilidad se
revalida sobre el documento al vencer. Esto es necesario porque `$expr` puede
usar `$$NOW`: un candidato ya vencido pero todavia excluido permanece en la
agenda y puede revisitarse en operaciones posteriores hasta que sea elegible o
cambie. Las raices de buckets, agenda y lookup por storage key se comparten en
capturas MVCC y se restauran juntas en rollback; no hay un cache lateral fuera
de la vista transaccional.

Retirar un documento elimina idempotentemente sus claves anteriores aunque el
filtro parcial ya no sea verdadero con el reloj actual; solo la insercion decide
la pertenencia nueva. Las claves datetime se normalizan a UTC naive y precision
BSON antes de indexar, por lo que el input aware y su version decodificada no
pueden crear membresias distintas para el mismo instante.

El inicio MVCC copia los contenedores de namespaces y catalogos, pero no las N
entradas de documentos ni las pertenencias de indices. Por tanto su coste es
proporcional al numero de bases, colecciones e indices visibles, no al numero
de filas. Los documentos almacenados continúan siendo versiones reemplazables;
las raices antiguas se reclaman cuando dejan de estar referenciadas por
transacciones, cursores o scopes de rollback. `rpds-py` es un detalle interno
del engine y no forma parte de SPI v2.

Los diagnosticos MVCC exponen por separado transacciones y snapshots de lectura
activos, referencias y versiones documentales retenidas, versiones ya
sustituidas en el root vivo y raices de coleccion/indice. `retainedBytes` es una
estimacion deduplicada de los owners Python alcanzables por esas vistas;
`retainedBytesEstimateKind="python-lower-bound"` hace explicito que no incluye
los nodos HAMT ni buffers nativos u opacos. La metrica vuelve a cero cuando la
ultima vista cierra, pero no constituye un limite de admision ni una medida de
RSS. El commit publica el write-set de
namespaces sobre roots preparados, pero conserva el conflicto global de P1:
una transaccion incompatible falla antes de instalar su vista. Los scans no
selectivos y las
capturas de candidatos mantienen sus costes propios; la agenda TTL añade coste
O(log N) y retencion por documento indexado a las escrituras Memory.

### Snapshots Memory por demanda

Un scan captura bajo el lock referencias a versiones `_StoredDocument` y la
informacion de catalogo necesaria para interpretar su consulta. Las escrituras
reemplazan esas versiones en vez de mutarlas, por lo que el cursor conserva su
vista tras update/delete, drop, rename, TTL o fin de su transaccion. El lock se
libera antes de filtrar, proyectar, convertir o ceder una fila al consumidor.

La decodificacion, proyeccion y copia de contenedores publicos se ejecutan por
demanda. Un decoder de almacenamiento personalizado puede reutilizar su buffer;
su primera salida se copia al cache de la version antes de otra decodificacion,
sin obligar a decodificar el resto del snapshot. Para `$text`, la definicion de
indice necesaria se captura con la vista para conservar fields y weights tras
DDL concurrente.

El cierre, agotamiento, error o cancelacion cierran el generador y liberan su
frame y las referencias candidatas. Un scan completo sin indice captura O(N)
referencias candidatas al abrirse aunque la raiz transaccional sea compartida.
Una ordenacion bloqueante conserva O(N) documentos
internos hasta ordenar; solo la proyeccion y materializacion publica posteriores
son perezosas. Reducir esa captura de candidatos exige otra evolucion del plan
de acceso y no queda resuelto por las raices persistentes.
Como el finalizador Memory no ejecuta I/O ni cede control, su snapshot usa una
transicion terminal inmediata sin crear una tarea de event loop. Una fuente
externa o SQLite conserva cierre async supervisado; el fast path no se decide
por `hasattr` ni se aplica a recursos cuyo owner no controla Memory.

## `SQLiteEngine`

`SQLiteEngine` es el backend fisico mas complejo y esta organizado como
coordinador de subsistemas internos. No debe leerse como un modulo unico, sino
como una fachada sobre piezas internas:

- runtime y caches;
- runtime de sesion/transaccion;
- admin runtime y namespace admin;
- catalogo;
- index admin;
- search admin;
- read ops;
- write ops;
- modify ops;
- read execution;
- fast paths;
- planner heuristics.
- outbox transaccional y migraciones versionadas de su esquema.

La intencion de esta modularizacion es bajar el coste de cambio y evitar que el
engine siga siendo una fuente unica de verdad dispersa.

### Preparacion bulk SQLite por bloques

La preparacion conserva un unico trabajo en vuelo por operacion. Cada trabajo
valida y codifica hasta 128 documentos o alcanza un objetivo de 1 MiB estimado
de payloads y filas multikey preparados. Un documento indivisible puede superar
ese objetivo: no se trunca ni se rechaza por una politica interna de batching.
Los metadatos se capturan una vez antes de preparar; la publicacion existente
revalida opciones e indices si cambiaron antes de escribir.

Los bloques no publican datos. La escritura conserva su frontera transaccional
y los resultados parciales contractuales; dividir preparacion no introduce
commits por bloque. Las validaciones conservan precedencia sobre errores de
codificacion y cada documento/fase usa un contexto independiente del llamador.
Cancelar durante preparacion senaliza parada entre documentos; la llamada ya
iniciada puede terminar, pero no puede publicar por si misma.

La admision FIFO pertenece al executor fisico y admite como maximo tantos
trabajos como workers. Por ello varios engines que comparten el mismo pool
comparten tambien la capacidad: las coroutines esperan fuera del executor y no
llenan su cola interna. Cancelar al consumidor no devuelve el permiso hasta que
termina la llamada fisica ya enviada. Los jobs de cierre de un reader pueden
saltar esta admision para no perder la unica obligacion de cleanup; siguen
serializados por el owner del recurso.

Esta capacidad acota trabajos enviados, no la memoria de todas las coroutines
que esperan. La entrada, los registros preparados y los outcomes pueden seguir
ocupando memoria proporcional al bulk. La escritura final y un documento
individual no se convierten en trabajo interrumpible por esta division; no se
declara una cota global de memoria ni de latencia del engine. Los diagnosticos
de lectura exponen capacidad, trabajos fisicos y waiters del executor.

### Catalogo SQLite y vista de lectura

Los catalogos reutilizables son internos y de solo lectura, incluidos sus
contenedores anidados y la vista de indices TTL. Las respuestas publicas de
`list_indexes` e `index_information` siguen produciendo valores mutables propios.
Los valores BSON opacos que aun no tienen representacion interna inmutable
mantienen el camino de copia defensiva; esta optimizacion no autoriza compartir
hojas mutables ni declara eliminado todo coste de copia para esos catalogos.

Cada conexion posee su cache de catalogos y la libera al cerrar, incluso si el
objeto conexion sigue referenciado. La generacion local de metadatos cubre DDL
propio; `PRAGMA data_version`, comparado solo en esa misma conexion, detecta
commits externos. Este ultimo invalida conservadoramente los catalogos de la
conexion aunque el cambio externo no sea DDL: no es invalidacion fina por indice
ni un contador global comparable entre conexiones.

Al cargar metadatos desde otra conexion se comparan todos los registros
persistidos de su vista con la ultima representacion compartida del namespace.
Si coinciden, puede reutilizarse el catalogo inmutable sin deserializarlo otra
vez. No se infiere equivalencia a partir de contadores de conexiones diferentes.
El pool conserva una sola representacion por namespace, no un historial de
generaciones, y se vacia al desconectar el engine. Leer y comparar esos registros
sigue siendo proporcional al numero de indices en una conexion fria; un hit
valido de la cache local no los vuelve a consultar.

Una transaccion establece su snapshot antes de validar el catalogo. El plan y
la adquisicion de filas de scan, lookup puntual y count comparten la vista:
un lector dedicado mantiene una transaccion de lectura hasta su cierre;
una conexion compartida ociosa usa un scope corto. Un scope de lectura no
confirma ni aborta la transaccion de usuario en la que participa. Esto evita
usar un indice eliminado por otro cliente entre la planificacion y la consulta,
sin cambiar a metadatos recientes dentro de una vista antigua ni reintentar
escrituras automaticamente.

El catalogo reutilizado no acredita todos los caches derivados, presupuestos
globales o politicas de retencion. La seleccion TTL proporcional se mantiene
en una estructura persistente separada y conserva su propia invalidacion.

### Purga TTL y ownership de escritura SQLite

SQLite mantiene `ttl_index_entries`, una agenda persistente con una entrada por
documento e indice TTL que tenga al menos una fecha util. Su clave primaria
acota el estado a `(collection_id, index_name, storage_key)` y el indice
`(collection_id, expires_at_epoch_ms, index_name, storage_key)` permite
enumerar solo K candidatos vencidos, sin deserializar N documentos cuando no
hay trabajo. Inserts, updates, deletes y DDL actualizan documentos, indices y
agenda dentro del mismo scope de escritura. Al abrir una base anterior, una
migracion versionada reconstruye la agenda antes de publicar la conexion; una
version futura se rechaza sin modificar el fichero.

La agenda conserva fechas independientemente del filtro parcial. Esto es
necesario porque una expresion parcial puede cambiar de resultado solo por el
reloj o los bindings, sin que cambie el documento. Por ello un candidato de la
agenda no autoriza el borrado: dentro del scope de escritura se recargan
documento e indices TTL y se comprueba de nuevo la elegibilidad con el reloj de
la operacion. Se conserva asi un documento que otro cliente haya actualizado
para aplazar su vencimiento, excluido del filtro parcial o dejado sin indice TTL
antes de adquirir la escritura. La purga de `count_documents` tambien se
ejecuta sin transaccion explicita y utiliza el reloj ligado a su semantica,
igual que la lectura de documentos.

Los scopes de escritura pertenecen a la conexion, no al hilo. Una escritura
anidada usa un savepoint y no confirma la transaccion exterior. Cada scope
retira exclusivamente su frame al deshacer; un fallo interior no cambia el
destino del rollback exterior. Documentos y entradas de indices se restauran
juntos cuando falla la purga. Si falla el propio rollback o la liberacion del
savepoint, la conexion exige rollback completo antes de admitir lecturas,
nuevas escrituras o commit. Una sesion conserva ownership hasta que su abort
termine; no se reconoce un commit de estado parcialmente restaurado.

El error de la operacion o del commit se conserva si tambien falla cleanup;
el error secundario se adjunta como nota. El cierre del engine no confirma una
escritura inconclusa para ejecutar mantenimiento administrativo sobre la misma
conexion. Los frames terminados no conservan referencias a conexiones.

La agenda solo cachea el instante minimo derivado de los valores fecha y del
`expireAfterSeconds`; no cachea la elegibilidad parcial ni sustituye la
revalidacion. Fechas en arrays, `$unset`, cambios de indice, rollback y escrituras
desde otras conexiones conservan ese reparto de autoridad. El coste persistente
adicional es proporcional al numero de pares documento/indice TTL con fecha, no
al producto de consultas ni al numero de purgas.

### Lectores SQLite por demanda

Un scan posee un lector fisico independiente del worker que ejecuta cada lote.
La apertura y el primer fetch comparten trabajo; los siguientes fetch y el
cierre son trabajos finitos. Un consumidor pausado retiene su vista y, cuando
corresponde, su conexion, pero no un worker esperando que avance. La entrega
espera el resultado del trabajo, sin polling ni un segundo worker de cola.

La conexion y el cursor tienen acceso serializado por lector. Ningun binding
thread-local permanece abierto entre trabajos: cada fetch y close enlaza y
restaura su conexion, y los trabajos conservan el contexto capturado al iniciar
el scan. Las conexiones de una sesion transaccional siguen perteneciendo a su
owner; el lector no las cierra. Disconnect detiene y cierra lectores retenidos
sin esperar una nueva peticion del consumidor, e intenta cerrar los demas
recursos aunque una limpieza falle. Cancelar la espera no cancela ni abandona
la responsabilidad de un fetch fisico ya iniciado.
El control de refcount y la senalizacion de disconnect se ejecutan en el
worker; su fachada async no espera el lock de conexion desde el event loop.

El transporte prepara como maximo 64 documentos por lote, examina como maximo
256 filas fuente y usa un objetivo interno de 1 MiB estimado de contenedores
Python. La cuota de filas se aplica tambien cuando un filtro residual no produce
ningun resultado: un scan selectivamente vacio devuelve capacidad al executor
sin tener que agotar primero la fuente. El residual tipado pertenece a Python;
el prefiltro SQL no se vuelve a ejecutar en esa fase. El objetivo de bytes puede
excederse en un documento indivisible; estas cuotas no introducen un nuevo
rechazo de documentos ni un limite publico de memoria. Una operacion limit=1
agota y cierra su recurso en el mismo trabajo cuando produce su resultado.

Los lectores dedicados de fichero conservan el snapshot de SQLite. Una
conexion compartida (`:memory:` o transaccion de usuario) no aisla un SELECT
de writes posteriores en esa misma conexion: el lector captura las filas
seleccionadas bajo el guard antes de ceder control. En el camino SQL conserva
payloads serializados; un fallback Python puede conservar documentos ya
decodificados. Esta captura y los operadores bloqueantes pueden seguir
teniendo coste proporcional a la entrada; los lotes no acreditan por si solos
memoria total acotada ni preparacion inicial constante.

Los diagnosticos `readResources` exponen lectores retenidos (incluidos los que
estan cerrando), conexiones dedicadas, bytes estimados de las capturas de
conexion compartida, cuotas de documentos/filas y objetivo de bytes del lote,
estado de admision del executor y fallos de cierre. No son RSS, tamano WAL ni
toda la memoria de buffers de la API. La admision de snapshots, la retencion
de snapshots bajo presion y los presupuestos de operadores requieren sus
propias garantias; un limite por lote no las sustituye.

## Estado runtime y caches en SQLite

La extraccion reciente a `SQLiteRuntimeState` y `SQLiteCacheState` concentra
invariantes que antes estaban repartidas:

- conexion y recuento de conexiones;
- executor y ownership;
- owner transaccional por sesion;
- cache de indices;
- cache de ids de coleccion;
- capacidades fisicas aseguradas, como search backends y multikey indexes.

El criterio arquitectonico es que `sqlite.py` sea coordinador de lifecycle y
wiring, no almacen de estado desestructurado.

La coordinacion de sesion y transaccion local ya no depende solo del engine
principal. `_sqlite_session_runtime.py` encapsula:

- binding de conexion por hilo;
- ownership transaccional por `ClientSession`;
- inicio, commit y abort;
- politicas de begin/commit/rollback para writes locales.

La capa administrativa restante tampoco vive ya solo en `sqlite.py`.
`_sqlite_admin_runtime.py` encapsula:

- listados de bases y colecciones;
- opciones de coleccion;
- visibilidad de `system.profile`;
- grabacion y lectura puntual de entradas de profiling;
- ajuste de nivel de profiling;
- `drop_database` y las invalidaciones transversales ligadas a ese ciclo;
- la lectura documental del namespace de profiling sin volver a mezclarla en el
  engine principal.

Con esto, `sqlite.py` queda mas cerca del rol que se persigue desde la
arquitectura: coordinador de lifecycle, wiring y wrappers publicos, no
contenedor principal de metadata administrativa.

La politica de mantenimiento en esta zona es explicita:

- si un cambio afecta a profiling, stats, `system.profile`, namespaces o
  invalidaciones administrativas, debe evaluarse primero si vive ya en
  `_sqlite_admin_runtime.py`;
- `sqlite.py` no debe reabsorber esa logica salvo que exista una razon de
  contrato publico muy concreta.

La misma regla aplica ya a explain y fallback. `_sqlite_explain_contract.py`
concentra:

- la traduccion de fallback a `planning_issues`;
- el bloque `pushdown` visible en explain;
- los `pushdown_hints` y su taxonomia por operador/familia.

Eso reduce el acoplamiento entre planner, runtime y contrato publico y deja a
`sqlite.py` en un rol mas claro de coordinador.

En lectura ocurre ya otra separacion util:

- `_sqlite_read_execution.py` concentra helpers de planning y fast paths
  defendibles por familia;
- `_sqlite_read_fast_path_runtime.py` concentra la coordinacion de seleccion
  temprana y fast paths de lectura;
- `_sqlite_read_runtime.py` concentra la coordinacion de `compile/plan/explain`
  para lecturas SQLite, incluido el caso especial de `$text` clasico.

Con eso, `sqlite.py` deja de mezclar en el mismo bloque:

- semantica especial de planning;
- wiring del planner;
- shape de `EXPLAIN QUERY PLAN`;
- wrappers async/sync.

Tambien en el mantenimiento fisico de indices existe ya una frontera propia:

- `_sqlite_index_runtime.py` concentra asegurado de indices fisicos, rebuild de
  filas derivadas y backfills de `scalar_index_entries` / `multikey_entries`.

Los indices fisicos de SQLite son aceleradores sobre una tabla de documentos
compartida. La semantica logica de cada indice (en especial `unique`, sparse,
partial, collation y multikey) se valida por base y coleccion en el engine, no
mediante una restriccion fisica que pueda cruzar namespaces. `MemoryEngine`
aplica la misma separacion: sus mapas de indice solo se usan para seleccionar
candidatos cuando son seguros y el filtrado documental final conserva el
contrato observable.

Con eso, `sqlite.py` conserva el lifecycle general y el wiring de operaciones,
pero deja de ser el contenedor principal de:

- mantenimiento fisico de tablas derivadas de indices;
- reposicion de filas auxiliares por documento;
- backfills de metadata fisica al conectar.

En search existe ya otra frontera explicita por capas:

- `_sqlite_search_runtime.py` concentra el lifecycle local de search y
  vectorSearch en SQLite: carga de definiciones, materializacion de backends,
  rebuild/invalidate, ejecucion y shape de `explain()`;
- `_sqlite_search_backend.py` concentra la politica de capacidad real del
  backend local para `$search`:

- que operadores pueden ir por FTS5;
- cuales usan el backend materializado solo como prefilter de candidatos;
- cuales degradan por completo a Python;
- que shape de explain corresponde a cada backend (`backend`,
  `backendAvailable`, `backendMaterialized`, `fts5_match`).
- el runtime evita ya volver a cargar la coleccion completa cuando FTS5 o
  `usearch` devuelven `storage_key` candidatos; en esas rutas recupera solo
  los documentos necesarios para materializar resultados o aplicar filtros.
- en `compound`, esos candidatos se usan tambien para ordenar mejor las
  clausulas `should`, y `near` participa ya en el ranking local cuando aparece
  dentro de esa familia.
- `in`, `equals` y `range` forman parte del mismo contrato local de `$search`, pero
  se mantienen como operadores honestos de matching/ranking sobre paths
  escalares, sin forzar una traduccion FTS que no preserve semantica.
- ese mismo contrato deja visible cuando un `should` candidateable sigue siendo
  demasiado amplio y solo aporta ranking, no una reduccion material de
  candidatos.
- cuando `aggregate()` deja un `top-k` seguro tras `$search`, SQLite lo usa ya
  como `limit hint` interno para recortar candidatos exactos antes de cargar
  todos los documentos necesarios para el ranking final.
- cuando ese `top-k` solo puede derivarse despues de filtros por documento
  (`$match`) pero la pipeline sigue siendo `prefix-monotonic`, SQLite participa
  en la expansion incremental por prefijos y evita igualmente materializar el
  conjunto completo si la ventana posterior ya esta satisfecha.
- esa expansion usa ya crecimiento adaptativo guiado por la tasa de retencion
  observada, en vez de un doblado fijo por iteracion.
- SQLite puede ademas aplicar un `downstreamFilterPrefilter` exacto cuando la
  pipeline posterior a `$search` empieza por `$match`; con eso reduce el coste
  del ranking final sin alterar el orden observable de los documentos que
  sobreviven a ese filtro.
- si ese `$match` simple restringe paths textuales realmente indexados
  (`token`, `string`, `autocomplete`), SQLite puede convertir parte de ese
  filtro en interseccion candidateable de `storage_key` antes del ranking.
- cuando ese filtro simple ademas implica exactamente una clausula textual de
  `compound.must`, `compound.filter` o `compound.should`, SQLite marca esa
  clausula como refinada por `downstreamFilter` dentro de `compoundPrefilter`
  y estrecha su conjunto de candidatos antes del ranking final.
- si todas las clausulas `should` relevantes son candidateables y su score se
  puede reconstruir exactamente desde las filas FTS, SQLite puede hacer poda
  top-k por tiers exactos de `matchedShould` + `shouldScore` antes del ranking
  documental final.
- antes de calcular ese `shouldScore` exacto, SQLite puede recortar primero por
  tiers de `matchedShould`; como ese campo ya es la primera clave del ranking,
  esa poda reduce empates y materializacion sin cambiar el orden observable.
- `compoundPrefilter` deja ya visible la clase de cada clausula
  (`candidateable-exact`, `candidateable-ranking`, `post-match-only`) para que
  esa poda no quede implicita.
- cuando la ordenacion final de un `compound` puede reconstruirse solo desde
  las entradas materializadas de FTS, SQLite expone
  `rankingSource="fts-materialized-entries"` y evita cargar todos los
  documentos candidatos antes de aplicar la ventana final.
- cuando esa poda aplica una ventana finita, `topKPrefilter.cutoffTier`
  identifica el tier exacto o aproximado que ha servido de corte antes de la
  materializacion documental final.
- cuando entra tambien la poda previa por `matchedShould`, `topKPrefilter`
  expone `candidateCountBeforePartialRanking`,
  `candidateCountAfterPartialRanking` y `partialRanking.strategy` para dejar
  claro cuanto trabajo se evitó antes del score exacto.
- los prefiltros candidateables de search/vector aceptan ya una booleana local
  conservadora:
  - `$and` puede intersectar la parte soportada aunque queden ramas no
    candidateables;
  - `$or` solo se usa como prefilter cuando todas sus ramas son soportadas.
- en `vectorSearch`, cuando el `post-filter` documental descarta demasiados
  candidatos ANN, SQLite ya no expande `numCandidates` doblando a ciegas: usa
  `candidateExpansionStrategy="adaptive-retention"` para estimar la siguiente
  expansion segun la retencion observada.
- ademas, para filtros simples sobre paths escalares ya observados al
  materializar el backend ANN, `vectorSearch` puede generar un
  `vectorFilterPrefilter` exacto o parcial antes de cargar documentos; el
  `filterMode` de `explain()` distingue si ese filtro se resuelve solo como
  prefilter o si aun necesita validacion documental posterior. Ese prefilter ya
  cubre igualdad, `$in`, `$exists`, rangos simples y booleanos conservadores
  (`$and` parcial y `$or` totalmente soportado).
- cuando sigue quedando validacion documental posterior, `vectorFilterResidual`
  deja visible si el resto viene de clausulas no candidateables o de un
  prefilter no exacto.

`MemoryEngine` mantiene el contrato semantico local pero ya no trata
`vectorSearch` ni `$search.compound` como puro full-scan documental:

- materializa por indice los vectores, paths escalares filtrables y presencia
  de campos para no reextraerlos en cada consulta;
- `vectorSearch` deja visible en `explain()` cuanta lectura documental se evita
  con `vectorFilterPrefilter`, `filterMode`, `documentsScanned` y
  `documentsScannedAfterPrefilter`; si hay resto documental, tambien expone
  `vectorFilterResidual`;
- `$search.compound` reutiliza ese mismo enfoque para aplicar antes filtros
  simples aguas abajo cuando son candidateables localmente.

Las consultas vectoriales de Memory comparten un presupuesto interno de
cache por engine: 8 MiB estimados en total y 1 MiB por entrada. Incluye scores
en orden de fila, filtros y rankings; una entrada demasiado grande no se
admite y la consulta sigue calculando el resultado exacto. El presupuesto no
es un limite de RSS: excluye almacenamiento, documentos/matrices del indice y
asignaciones temporales. Los diagnosticos `caches.vectorQueries` distinguen
capacidad, bytes estimados, hits, misses, evicciones y rechazos de admision.
Las vistas antiguas todavia retenidas por una operacion comparten el mismo
presupuesto; sus entradas se liberan al desaparecer el ultimo owner.

Los indices vectoriales materializados tienen otro owner y otro presupuesto:
una LRU interna admite hasta 64 MiB estimados por engine y 32 MiB por indice.
La estimacion recorre contenedores propios y buffers nativos `ndarray`; si
encuentra una vista prestada o un grafo opaco, no conserva la entrada. Rechazo
o eviccion solo fuerzan reconstruccion en una consulta posterior y nunca
rechazan ni aproximan la operacion. `caches.vectorIndexes` expone capacidad,
retencion estimada, hits, misses, evicciones y rechazos por separado. Una
operacion que ya tomo la referencia puede terminar aunque la LRU la retire.

Un hit del indice materializado se comprueba antes de decodificar la
coleccion. La matriz y las normas de coseno son de solo lectura y se
reutilizan entre consultas. Con elegibilidad exacta, la seleccion top-k
particiona los scores y ordena los ganadores conservando el desempate publico
por identidad BSON. Los empates en el corte no se resuelven por la posicion
arbitraria de la particion. Cuando el conjunto supera 8.192 filas y existe un
limite finito sin `minScore`, la puntuacion se calcula por bloques de ese
tamano y un heap conserva solo top-k; NaN degrada al orden completo para
preservar su semantica excepcional. Solo los resultados seleccionados reciben
el payload de salida y su metadata de score.

Un prefiltro parcial sigue necesitando comprobacion documental, tanto en
ejecucion como en explain. En esa ruta se conserva la evaluacion residual
antes de top-k; tampoco se materializan los payloads descartados. Los scores
NaN conservan una ruta de ordenacion completa compatible con el orden anterior.
La puntuacion exacta sigue recorriendo la matriz completa y el filtro residual
puede recorrer todos los candidatos; los bloques acotan su buffer de scores,
no el trabajo CPU ni el indice retenido. No se presenta como ANN ni como
memoria constante. La invalidacion por escritura sigue siendo por coleccion:
normas, filtro y payload se reconstruyen juntos. Los commits transaccionales
usan su write-set P2 para invalidar solo los namespaces publicados; un
`drop_database` invalida esa base, no las demas. La actualizacion incremental
dentro de un indice no queda implementada por este cambio.

Eso evita que `sqlite.py` siga replicando en paralelo la misma decision en la
ruta de ejecucion, en la de `explain()` y en el lifecycle documental de los
indices de search/vector.

El hotspot estructural que sigue pendiente en SQLite no es ya explain, sino la
cadena completa `semantic_core -> sqlite_planner -> sqlite_query -> sqlite
runtime`. La politica de pushdown real sigue repartida entre esas capas y la
proxima inversion aqui deberia hacerse solo cuando una familia concreta de
operador, explain o fallback justifique una frontera nueva y estable.

## MVCC y sesiones

La semantica de sesiones y transacciones locales se apoya en:

- `ClientSession` en la superficie publica;
- snapshots/ownership local en los engines;
- estructuras MVCC de memoria y ownership transaccional en SQLite.

No se pretende reproducir una infraestructura distribuida de transacciones. Se
modela una semantica local suficientemente consistente para testing y uso
embebido.

## Commit sequence y outbox

`MemoryEngine` asigna una secuencia monotona bajo su lock de metadata. Las
mutaciones de una transaccion MVCC se mantienen pendientes y reciben secuencia
solo cuando el snapshot se instala con exito; un abort no consume tokens.

`SQLiteEngine` usa una tabla append-only `change_outbox`. La fila de evento o
hueco se inserta dentro del mismo `sqlite_write_scope` que documentos e
indices, por lo que commit y rollback son atomicos. El dispatcher consume en
orden y solo avanza su checkpoint despues de que el hub acepte la fila.
Lease, heartbeat, checkpoint y compactacion usan una conexion de control
dedicada en bases de fichero; nunca confirman la conexion de datos. En
`:memory:`, donde SQLite no permite compartir ese estado entre conexiones, el
alta inicial puede participar en la transaccion sin confirmarla; dispatch,
lease y checkpoint rechazan una transaccion de datos activa.
La entrega se serializa por consumidor. SQLite coordina tambien instancias
distintas del mismo proceso mediante un gate por ruta, con deteccion de
reentrada, y procesos distintos mediante un lease durable con generacion y
heartbeat.
La garantia de entrega es at-least-once, no exactly-once: si el callback
termina pero se pierde el lease antes del checkpoint, la fila se vuelve a
entregar. Los consumidores deben ser idempotentes respecto a la secuencia.

Memory y SQLite conservan por defecto hasta 10.000 entradas, configurable con
`change_log_max_entries` y `change_outbox_max_entries`. La compactacion usa el
checkpoint minimo registrado. SQLite persiste checkpoints y distingue
consumidores efimeros de durables; el alta durable se deriva de un hub con
`journal_path`. Si el limite obliga a podar por delante de un consumidor
rezagado, su siguiente lectura falla explicitamente en vez de ocultar perdida
de eventos. `changeDelivery` expone limites, secuencia y suelo podado en los
diagnosticos del engine.
Los consumidores efimeros incluyen owner de proceso y caducidad renovada por
un heartbeat durante toda la conexion, para que ni una ejecucion larga ni un
crash dejen metadata incorrecta. La expiracion respeta los leases de dispatch
activos. Los consumidores durables no tienen TTL y solo se retiran de forma
explicita.
El esquema del outbox usa pasos versionados dentro de una transaccion o
savepoint. Versiones futuras se rechazan antes de mutar la base.

Preparar la entrega antes de cada operacion no renueva el registro si ya
existe y sigue siendo valido. SQLite comprueba mediante lecturas su checkpoint,
durabilidad, owner y caducidad, incluido el suelo de historia retenida; no usa
un checkpoint local como autoridad. Evita asi una escritura administrativa
redundante por la conexion de control que podria invalidar un snapshot WAL
abierto en la conexion de datos. Las altas, cambios de checkpoint o durabilidad,
registros caducados y cambios de owner conservan la ruta de registro; el
heartbeat sigue siendo responsable de la renovacion periodica. Esta
optimizacion no elimina conflictos SQLite producidos por otras escrituras,
incluido mantenimiento real concurrente, ni cambia el aislamiento transaccional.

## Helpers compartidos entre engines

Se han extraido solo helpers con semantica claramente compartida:

- TTL;
- namespace admin documental;
- search-index admin documental.

Esto evita dos extremos malos:

- duplicar semantica identica en `MemoryEngine` y `SQLiteEngine`;
- forzar una "super base engine" con demasiada herencia artificial.

## Profiling y metadata administrativa

Ambos engines exponen una semantica comun de:

- `system.profile`;
- listados de bases y colecciones;
- opciones de coleccion;
- stats y shape de metadatos.

La documentacion debe dejar claro que la visibilidad de `system.profile` forma
parte del contrato de namespace admin, no de un detalle casual del engine.

## Search index admin

La administracion de search indexes comparte:

- validacion de definicion;
- shape documental de `list_search_indexes`;
- estados de readiness;
- errores publicos estables para indice inexistente o tipo incorrecto.

Lo que no se comparte a la fuerza es la ejecucion fisica de `$search` o
`$vectorSearch`, porque ahi el backend importa de verdad: SQLite usa FTS5 para
parte de `$search` y `usearch` para `vectorSearch`, mientras `MemoryEngine`
mantiene el baseline semantico Python/exacto.

## Tradeoff principal de la capa de engines

La arquitectura busca un equilibrio:

- compartir semantica donde es realmente la misma;
- dejar que cada engine ejecute de forma distinta;
- fijar parity tests cuando el contrato observable se promete igual.

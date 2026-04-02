# Driver local y surface wire

## Objetivo del subsistema

`driver` y `wire` existen para modelar un runtime de cliente y protocolo local
sin exigir un servidor MongoDB real para cada flujo de desarrollo o testing.

No se trata de una implementacion completa de un cluster MongoDB. El contrato
actual es explicito y parcialmente introspectable.

## Arquitectura del driver

El driver se organiza en varios bloques:

- `uri.py`: parsing y materializacion de URI;
- `policies.py`: timeout, retry, selection y concerns;
- `topology.py`: modelos de topologia, server description y capacidades SDAM;
- `topology_monitor.py`: refresh de topologia;
- `runtime.py`: coordinador de planificacion, checkout, ejecucion y monitor;
- `_runtime_planning.py`: construccion declarativa del `RequestExecutionPlan`
  y del request ya materializado, sin mezclar esa logica con intentos ni
  resolucion dinamica;
- `_runtime_plan_resolution.py`: resolucion de planes dinamicos contra la
  topologia vigente sin mezclar esa logica con retries ni con probe plans;
- `_runtime_attempts.py`: ciclo de vida de intentos concretos
  (`prepare/complete/discard/execute`) sin mezclarlo con construccion del plan;
- `execution.py` y `connections.py`: ejecucion de requests y pool/registry;
- `monitoring.py`: eventos de runtime.

## Patron `Strategy`

El driver usa objetos de politica para encapsular decisiones variables:

- `TimeoutPolicy`
- `RetryPolicy`
- `SelectionPolicy`
- `ConcernPolicy`
- `AuthPolicy`
- `TlsPolicy`

Esto permite separar:

- parsing/configuracion;
- seleccion de servidores;
- ejecucion de requests;
- concerns y autenticacion.

## Runtime de ejecucion

`DriverRuntime` coordina:

- resolucion de URI y SRV;
- construccion de topologia inicial;
- planificacion de requests;
- resolucion de candidatos;
- retries y monitorizacion.

Una decision importante reciente es que los planes dinamicos re-resuelven sus
`candidate_servers` contra la topologia vigente antes de ejecutar, para no usar
seleccion obsoleta tras cambios de topologia. Esa frontera ya no vive mezclada
con `DriverRuntime`: la resolucion de planes dinamicos y la preservacion de
probe plans quedaron separadas en un helper interno especifico.

Ademas, `DriverRuntime` ya no concentra directamente el ciclo de vida completo
de cada intento. `_runtime_attempts.py` encapsula:

- seleccion efectiva del servidor dentro del plan ya resuelto;
- checkout y checkin de conexiones;
- emision de eventos de seleccion y checkout;
- ejecucion del pipeline de intentos sin remezclar esa logica con el builder
  del plan.

La frontera buscada es simple:

- `runtime.py` decide y coordina;
- `_runtime_planning.py` construye el plan inicial;
- `_runtime_plan_resolution.py` actualiza candidatos;
- `_runtime_attempts.py` materializa intentos;
- `execution.py` ejecuta el pipeline y clasifica outcomes.

## Topologia y SDAM

`topology.py` modela:

- `ServerDescription`
- `TopologyDescription`
- `ServerType`
- `ServerState`
- `TopologyType`
- `SdamCapabilitiesInfo`

La API publica deja claro que el soporte SDAM es parcial:

- hay awareness de `topologyVersion`;
- hay discovery por `hello`;
- hay health tracking por servidor;
- no hay full SDAM ni monitorizacion distribuida.

## Surface wire

La capa `wire` adapta comandos estilo MongoDB a la superficie del cliente local:

- handshake;
- autenticacion SCRAM;
- sesiones;
- cursores wire;
- serializacion/deserializacion de mensajes;
- proxy local de conexiones.

`WireCommandExecutor` es el coordinador principal:

- construye contexto de request wire;
- resuelve capacidad del comando;
- clasifica el comando por familia estable (`handshake`, `auth`, `cursor`,
  `admin_read`, `admin_write`, `admin_index`, `admin_stats`,
  `admin_namespace`, `admin_explain`, `admin_find_and_modify`,
  `admin_validate`, `admin_introspection`, `admin_control`);
- enruta a handlers especiales o passthrough;
- materializa resultados y errores en shape wire.

La implementacion ya no concentra esas responsabilidades en un unico modulo:

- `_executor_support.py` contiene parseo de contexto, normalizacion de
  `OP_QUERY` legacy, validacion temprana de payloads wire y materializacion de
  errores wire;
- `_executor_handlers.py` concentra los handlers especiales y el mapa de
  dispatch estable por familia (`auth`, `handshake`, `cursor`, `sessions`,
  passthrough);
- `_executor_passthrough.py` concentra la ejecucion de comandos que delegan en
  la administracion local, con separacion entre `connectionStatus`,
  passthrough autenticado y materializacion final de resultados;
- `executor.py` queda como coordinador entre parseo, routing y respuesta.

Con esa separacion, `wire/admin` se apoya ya en una frontera reutilizable con
`database_admin`:

- el executor wire valida y clasifica temprano;
- el passthrough delega por familias de comando administrativas;
- el admin local compila y ejecuta con la misma semantica que
  `database.command(...)`.

Sobre esa base, la surface wire ya se valida tambien con PyMongo real para
familias administrativas como `listCollections`, `listDatabases`, `collStats`,
`dbStats`, `listIndexes`, `createIndexes`, `dropIndexes`,
`findAndModify`, `count`, `distinct`, `dbHash`, `validate`, `explain`,
`buildInfo`, `listCommands`, `connectionStatus`, `serverStatus`,
`hostInfo`, `getCmdLineOpts`, `whatsmyuri`, `profile`, `currentOp` y `killOp`, no solo con
tests unitarios de routing interno.

La compatibilidad declarada tambien refleja ya esta superficie de forma
ejecutable:

- `compat` exporta un inventario `database_commands` con familia
  administrativa, presencia en wire, disponibilidad declarada de explain y
  opciones raw soportadas por comando;
- `database_command_options` declara el subset de opciones raw soportadas por
  cada comando;
- `_database_command_contract.py` actua como frontera interna compartida entre
  `compat`, `database.command(...)` y `listCommands` para que esa metadata no
  se vuelva a recomponer por varias rutas;
- `listCommands` materializa parte de esa informacion en runtime
  (`adminFamily`, `supportsWire`, `supportsExplain`, `supportsComment`,
  `supportedOptions`, `note`) para tooling e introspeccion local;
- los snapshots del catalogo congelan ambas vistas para evitar que la
  documentacion contractual vaya por detras del soporte real.

La validacion temprana del executor tambien cubre ya familias especiales
fuera del passthrough puro:

- `auth`: `authenticate`, `saslContinue`;
- `sessions`: `endSessions`, `commitTransaction`, `abortTransaction`;
- `cursor`: `getMore`, `killCursors`.

En `admin_control`, el wire local soporta tambien `currentOp` y `killOp`, pero
sin fingir semantica de servidor distribuido:

- `currentOp` solo refleja operaciones locales registradas por el runtime;
- `killOp` solo intenta cancelar operaciones locales marcadas como killable;
- no existe routing remoto ni cancelacion cross-node.

Y en la surface administrativa passthrough se endurecen tambien mas shapes de
payload antes de entrar en `database_admin`, por ejemplo en:

- `find`, `count` y `distinct` (`filter`, `projection`, `sort`, `skip/limit`,
  `batchSize`, `hint`, `maxTimeMS`);
- `dbHash` (`collections`, `comment`);
- `aggregate` (`allowDiskUse`, `let`, `cursor.batchSize`);
- `createIndexes`, `dropIndexes`, `listIndexes`;
- `findAndModify`;
- `listCollections` y `listDatabases`.

Eso evita que errores de shape atraviesen varias capas antes de materializarse
y deja mensajes publicos mas estables para requests malformed.

La surface de `explain` tambien cubre ya, ademas de `find` y `aggregate`,
los comandos `update`, `delete`, `count`, `distinct` y `findAndModify`,
reutilizando el mismo routing administrativo y devolviendo shapes
serializables por wire. Ademas, `find` y `aggregate` dejan ya `command` y
`explained_command` explicitos para hacer mas estable la observabilidad del
resultado administrativo.

En observabilidad administrativa local, `serverStatus.mongoeco` expone ya
tambien informacion estructurada de `collation` y `sdam`, para que el proceso
pueda distinguir de forma directa el backend de collation activo y el alcance
SDAM local soportado sin consultar APIs auxiliares separadas. Del mismo modo,
`validate` devuelve `warnings` explicitos cuando se usan flags aceptados solo
por compatibilidad (`scandata`, `full`, `background`) que no alteran el
comportamiento real del runtime embebido, y ahora tambien anade warnings
especificos de TTL cuando detecta indices `expireAfterSeconds` cuyos
documentos actuales no contienen ningun valor fecha usable.

Ese mismo bloque de observabilidad local incluye ya tambien
`serverStatus.mongoeco.changeStreams`, con resumen del backend y del estado del
hub local (`persistent`, `boundedHistory`, `retainedEvents`, `currentOffset`,
`nextToken`), junto con un resumen de la propia surface administrativa
(`adminFamilies`, `explainableCommandCount`). `profile` devuelve tambien
visibilidad local de namespace y resumen global del profiler
(`trackedDatabases`, `visibleNamespaces`).

`serverStatus.mongoeco.engineRuntime` deja ya visible, ademas, diagnostico
estructurado por engine. En `MemoryEngine` resume planner Python, lifecycle de
search simulado y colecciones/caches rastreadas. En `SQLiteEngine` resume
modos de pushdown (`sql`, `hybrid`, `python`), disponibilidad de FTS5 y
numero de search indexes declarados/pendientes, junto con caches de indices,
collection ids y features, para que tooling y debugging administrativo puedan
inspeccionar el runtime sin acoplarse a APIs internas.
Ademas, `serverStatus.opcounters` refleja ya actividad local real del runtime
embebido (`insert`, `query`, `update`, `delete`, `getmore`, `command`) en vez
de valores estaticos.

La observabilidad de stats e introspeccion administrativa tambien se ha ido
acercando a un contrato local mas util:

- `collStats.totalIndexSize` y `dbStats.indexSize` dejan ya de ser constantes
  triviales y materializan una medida local del peso de metadata de indices;
- `listIndexes` expone `ns` por documento para facilitar debugging y tooling;
- `explain` materializa ahora `collection` y `namespace` de forma uniforme en
  todas las rutas administrativas soportadas.

En SQLite, la observabilidad de `explain` deja tambien visible un bloque
`pushdown` comun, con modo (`sql`, `hybrid`, `python`), uso real de runtime SQL
y necesidad de sort Python. En explain de search se expone ademas lifecycle del
backend (`backendAvailable`, `backendMaterialized`, `physicalName`,
`readyAtEpoch`, `fts5Available`) para distinguir entre indice declarado,
materializado o aun pendiente.

Las pruebas estructurales del proyecto fijan ya este reparto para que no se
reempaquete accidentalmente:

- `WireCommandExecutor` no debe reabsorber parsing, handlers especiales y
  passthrough en un unico archivo;
- `DriverRuntime` no debe volver a mezclar planning, resolucion dinamica e
  intentos concretos.

## Relacion entre driver y wire

Aunque estan relacionados, no son lo mismo:

- `driver` modela un runtime de cliente local y su topologia;
- `wire` modela el surface del protocolo;
- ambos se apoyan en el runtime local y en la `api`, pero no deben
  confundirse con un servidor MongoDB completo.

## Limites conscientes

- el driver no pretende emular todo SDAM;
- el wire runtime esta pensado para compatibilidad local;
- las capacidades soportadas se hacen visibles via APIs de introspeccion y
  docs, en vez de dejarse implicitas.

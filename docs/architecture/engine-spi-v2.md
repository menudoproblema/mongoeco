# Engine SPI v2

## Alcance

El SPI v2 es la frontera estable para engines de almacenamiento externos desde
MongoEco 4.3.0. Sustituye la deteccion distribuida mediante `hasattr`, flags
privados, retornos union y callbacks de documentos por cuatro contratos
explicitos:

- `EngineCapabilities` declara version, snapshots, batch insert y entrega de
  cambios;
- `OperationContext` captura una sola vez semantica y recursos de la operacion;
- los outcomes tipados describen resultado, imagenes y secuencia de commit;
- `ReadSnapshot` declara consistencia, ownership y cierre.

Los tipos publicos se importan desde `mongoeco.engines`. Los protocolos
completos de lifecycle, CRUD, planning y administracion viven en
`mongoeco.engines.base`.

La [decisión de evolución del SPI](engine-spi-v3-proposal.md), posterior a
T1–T6 y S1/S2, mantiene v2 y aparca v3. No hay deprecación de v2 ni una
migración a un sucesor prevista para la próxima entrega.

## Declaracion minima

Un engine v2 debe exponer `capabilities` y cumplir de forma nativa los metodos
que declara. MongoEco valida el contrato al adaptar el engine y falla pronto si
falta una primitiva.

```python
from mongoeco.engines import EngineCapabilities


class CustomEngine:
    capabilities = EngineCapabilities(
        spi_version=2,
        batch_inserts=True,
        explicit_read_snapshots=True,
        change_delivery='none',
    )
```

La superficie CRUD minima incluye `insert_document`, `get_document`,
`update_with_operation`, `delete_with_operation` y `merge_document`.
`insert_documents` solo es obligatorio con `batch_inserts=True`; si se declara
`False`, el adaptador ejecuta `insert_document` por elemento, conserva la
identidad de operacion y deriva el ordinal de evento. Un engine v2 que declara
`explicit_read_snapshots=True` debe implementar `open_read_snapshot`. Si lo
declara `False`, debe implementar el fallback `scan_find_semantics` conservado
desde 4.3.0; el adaptador lo envuelve en un `ReadSnapshot` estable. Una
declaracion incoherente falla al adaptar el engine. Una estrategia
`commit-sequence` o
`transactional-outbox` exige registrar, despachar y retirar consumidores.
Solo se acepta `spi_version=2`; una declaracion ausente o distinta no se
interpreta por compatibilidad optimista. `EngineCapabilities` es la unica
fuente de verdad: cualquier subclase que cambie una capacidad debe declarar un
nuevo valor.

## Contexto de operacion

Las primitivas v2 reciben `operation_context: OperationContext`. El contexto es
inmutable y contiene:

- dialecto MongoDB;
- `ExpressionExecutionContext`, incluido `$$NOW` y bindings `let`;
- `CodecOptions`;
- sesion;
- collation normalizada;
- politica y tipo de publicacion de cambios;
- identificador unico de operacion.
- ordinal del change event dentro de la operacion.

El engine no debe consultar de nuevo el reloj, renormalizar collation o codec,
ni reconstruir bindings. Los bindings son inmutables tambien en profundidad y
los valores BSON internos llevan una marca no publica para no repetir la
frontera. Una specification compilada se liga mediante `operation.bind(context)`:
si dialecto, collation o bindings cambian una entrada de compilacion, MongoEco
recompila el plan antes de ejecutarlo; si coinciden, conserva el plan y no
repite la normalizacion BSON. El adaptador rechaza una operacion o semantica
compilada con un contexto distinto del argumento SPI. `derive()` crea un nuevo
valor para suboperaciones sin mutar el contexto original.
Las operaciones ligadas validan dialecto y los duplicados explícitos de collation
y variables; un valor ausente puede heredar del contexto durante la conversión.
La semántica ligada exige coherencia canónica con ese contexto. Estas guardas
no acreditan que un plan suministrado mediante override corresponda a su filtro.
La [propuesta de preparación interna](engine-preparation-proposal.md) consolida
los productores conservando esos tipos y el contrato v2; no aprueba su retirada.

La frase prospectiva anterior «En 5.0 esos duplicados deben desaparecer de los
comandos ejecutables» pertenecía al plan histórico de ADR-016. La revisión de
ADR-018 registra expresamente su corrección documental; no cambia firmas ni
concede una transición a un SPI sucesor.

## Outcomes y snapshots

Las escrituras retornan siempre `InsertOutcome`, `MutationOutcome`,
`DeleteOutcome` o `MergeOutcome`. Un no-match se expresa en el resultado
tipado, no cambiando el tipo de retorno. Las imagenes `before` y `after` se
capturan dentro de la misma frontera atomica que aplica la escritura.
MongoEco rechaza en la frontera del adaptador outcomes v2 aplicados que no
incluyan las imagenes requeridas, conteos imposibles o secuencias de commit en
operaciones no aplicadas.

`UpdateResult.did_upsert` distingue la existencia de un upsert del valor de
`upserted_id`, que puede ser null. El nuevo argumento es opcional y keyword-only:
los constructores anteriores con ID no null siguen infiriendo `did_upsert=True`.
Los engines que inserten un ID null deben proporcionar `did_upsert=True` y
mantener `matched_count=0` en el outcome del SPI. La fachada individual adapta
ese conteo a la representación de PyMongo; bulk, raw y publicación usan el
indicador explícito. No cambia la versión del SPI ni los parámetros de sus
operaciones.

Los value objects de outcome toman una copia profunda de imagenes y payloads
al construirse. Un engine puede reutilizar o mutar sus buffers internos despues
de retornar sin alterar el resultado ya publicado; la garantia vive en los
tipos centrales y no depende de que cada implementacion recuerde copiar.
`BulkOutcome` valida tambien el resultado agregado y la tupla de efectos que
expone; todos los DTO publicos aplican la misma politica de ownership.

`open_read_snapshot()` retorna `ReadSnapshot`. Para lecturas de coleccion, el
adaptador exige politica `STABLE` y el mismo `operation_id` del contexto que lo
abre. El consumidor es propietario de su cierre y puede usar `async with`;
agotamiento, cancelaciones repetidas y error cierran la fuente exactamente una
vez sin forzar una suspension en el cierre inmediato de la fachada sync.
El cleanup tiene un plazo finito y cualquier tarea que lo exceda queda
supervisada, evitando bloquear indefinidamente cancelaciones o shutdown. El
lifecycle publico distingue `OPEN`, `CLOSING`, `CLOSED` y `FAILED`.
`MATERIALIZED` y `LIVE` quedan disponibles para contratos que los declaren
fuera del scan ordinario.
Cada documento entregado es propiedad del consumidor y puede mutarse sin
alterar buffers del engine ni otras lecturas. Si el adapter rechaza un snapshot
por identidad o politica, llama `discard()` y supervisa su cierre antes de
propagar el error de contrato.

## Entrega de cambios

`change_delivery='none'` permite publicar el outcome tras retornar.
`commit-sequence` obliga a asignar secuencias monotonas en el commit efectivo.
`transactional-outbox` obliga a persistir la mutacion y su evento o hueco en la
misma transaccion. Mientras una transaccion de usuario sigue abierta, el
outcome puede no incluir aun secuencia: esta se hace observable al commit, no
antes.

El checkpoint se confirma despues de que el consumer retorna con exito. Si el
callback falla, el error se propaga y el evento permanece pendiente para el
siguiente dispatch; adelantar el checkpoint violaria el contrato at-least-once.
Cada callback recibe una copia independiente del payload confirmado. Si un
lote falla despues de confirmar un prefijo, el siguiente dispatch entrega solo
el sufijo no confirmado; mutaciones hechas por otro consumer nunca son
observables.

Search es una capability compuesta. `SearchEngineCapabilities.operators`
declara `$search` y, opcionalmente, `$vectorSearch`; un engine vectorial debe
declarar ademas las similitudes soportadas. El adapter rechaza requests que no
esten cubiertos por esa matriz.

Los outcomes Search del SPI v2 transportan documentos BSON y `RuntimeMetadata`
separados mediante `SearchHit`. Un engine no debe insertar campos privados para
scores o highlights. La representacion canonica es `RuntimeDocumentState` y
`persistence_document()` siempre descarta metadata no materializada. El
`SearchRequest` transporta tambien el plan semantico
inmutable compilado por el cursor; engine y `explain()` no pueden reconstruir
reglas distintas a partir de hints sueltos.

Una operacion que produce varios eventos conserva un `operation_id` comun y
deriva un ordinal distinto por efecto. SQLite persiste esa pareja como
identidad idempotente independiente de la fila viva de outbox, de modo que un
replay posterior a la compactacion recupera la secuencia original sin crear un
evento nuevo. Cada identidad retiene ademas tipo y hash del efecto completo,
incluso si la politica solo registra un hueco sin payload; reutilizar
la misma identidad para otro efecto falla explicitamente, incluso despues de
compactar la fila viva. El ledger conserva como maximo la misma ventana
`maxEntries` que la outbox, evitando crecimiento no acotado y haciendo
explicita la ventana de idempotencia. `$merge` atraviesa la misma publicacion
por outcome cuando el engine declara `change_delivery='none'`.

Los consumidores mantienen checkpoint propio. Un consumidor con journal es
durable; uno local es efimero y se retira al desconectar. La compactacion nunca
finge continuidad: si el limite de retencion poda eventos que un consumidor no
ha confirmado, su siguiente lectura falla explicitamente. Tambien se rechaza
un journal cuyo checkpoint este por delante de la secuencia confirmada, porque
continuar podria saltar eventos futuros.
SQLite lee bajo lock, entrega fuera de la seccion critica y confirma cada
secuencia despues del callback. Un dispatch drena por lotes todo el horizonte
confirmado al iniciarse, sin depender de que una escritura futura reactive la
entrega. Solo puede existir un dispatcher activo por `consumer_id`: Memory lo
serializa mediante un gate local. SQLite usa primero un gate process-local
segmentado por ruta canonica, que tambien rechaza reentrada por otra instancia,
y despues un lease persistente con owner, generacion, expiracion y heartbeat
para coordinar procesos distintos.
La garantia es at-least-once. Perder el heartbeat o el proceso despues de
ejecutar el callback y antes de confirmar el checkpoint provoca replay. El
dispatcher debe propagar ese fallo y nunca informar exito ni avanzar un
checkpoint con una generacion obsoleta. En SQLite de fichero, todo el control
plane usa una conexion separada de la transaccion de datos.

Los consumidores sin journal son efimeros: su identidad incluye una instancia
de proceso y SQLite persiste owner y TTL renovable mientras el engine sigue
conectado para recuperar registros de procesos abortados. La expiracion no
retira una registracion que conserve un lease de dispatch vivo. Los
consumidores con journal son durables y no caducan.

El esquema de outbox evoluciona mediante migraciones consecutivas y atomicas.
Abrir una base creada por una version futura falla explicitamente y nunca
rebaja el numero registrado.

## Migracion a 4.7

MongoEco 4.7 elimina deliberadamente el engine SPI v1. La
[guia de migracion](../migrating-to-4.7.md) enumera las sustituciones y el gate
de conformidad. SPI v2 es el unico SPI estable publicado por esta version.

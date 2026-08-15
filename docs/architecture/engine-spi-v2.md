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
Solo se aceptan las versiones SPI publicadas `1` y `2`; una version futura no
se interpreta por compatibilidad optimista.

Los flags heredados de v1 no alteran una declaracion v2. En particular,
`supports_injected_clock` deja de ser una segunda fuente de verdad: cualquier
subclase que cambie esa capacidad debe declarar un nuevo
`EngineCapabilities`.

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
Mientras los campos duplicados sigan presentes por compatibilidad 4.x, una
operacion o semantica ligada valida que dialecto, collation y variables
coincidan exactamente con el contexto. En 5.0 esos duplicados deben desaparecer
de los comandos ejecutables.

## Outcomes y snapshots

Las escrituras retornan siempre `InsertOutcome`, `MutationOutcome`,
`DeleteOutcome` o `MergeOutcome`. Un no-match se expresa en el resultado
tipado, no cambiando el tipo de retorno. Las imagenes `before` y `after` se
capturan dentro de la misma frontera atomica que aplica la escritura.
MongoEco rechaza en la frontera del adaptador outcomes v2 aplicados que no
incluyan las imagenes requeridas, conteos imposibles o secuencias de commit en
operaciones no aplicadas.

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

## Entrega de cambios

`change_delivery='none'` permite publicar el outcome tras retornar.
`commit-sequence` obliga a asignar secuencias monotonas en el commit efectivo.
`transactional-outbox` obliga a persistir la mutacion y su evento o hueco en la
misma transaccion. Mientras una transaccion de usuario sigue abierta, el
outcome puede no incluir aun secuencia: esta se hace observable al commit, no
antes.

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

## Migracion desde SPI v1

SPI v1 sigue operativo durante 4.x mediante `LegacyEngineAdapter`, pero desde
4.3.0 emite un `DeprecationWarning` por clase de engine y se retirara en 5.0.0.
La migracion recomendada es:

1. declarar `EngineCapabilities(spi_version=2, ...)`;
2. reemplazar `put_document` y `put_documents_bulk` por outcomes de insert;
3. aceptar un unico `OperationContext` en las primitivas de mutacion;
4. retornar outcomes tipados, sin booleanos ni unions contextuales;
5. implementar `ReadSnapshot` estable e identificado o conservar
   `scan_find_semantics` declarando `explicit_read_snapshots=False`;
6. declarar una estrategia de cambios y sus primitivas de consumidor;
7. ejecutar `tests/contracts/engines/test_storage_engine_v2_contract.py`
   contra el engine.

No se debe silenciar la advertencia como sustituto de la migracion. Un engine
puede mantener wrappers v1 propios mientras sus primitivas v2 sean la unica
implementacion semantica.

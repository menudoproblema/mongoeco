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

La superficie CRUD minima incluye `insert_document`, `insert_documents`,
`get_document`, `update_with_operation`, `delete_with_operation` y
`merge_document`. Si se declaran snapshots explicitos, tambien se exige
`open_read_snapshot`. Una estrategia `commit-sequence` o
`transactional-outbox` exige registrar, despachar y retirar consumidores.

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

El engine no debe consultar de nuevo el reloj, renormalizar collation o codec,
ni reconstruir bindings. `derive()` crea un nuevo valor para suboperaciones sin
mutar el contexto original.

## Outcomes y snapshots

Las escrituras retornan siempre `InsertOutcome`, `MutationOutcome`,
`DeleteOutcome` o `MergeOutcome`. Un no-match se expresa en el resultado
tipado, no cambiando el tipo de retorno. Las imagenes `before` y `after` se
capturan dentro de la misma frontera atomica que aplica la escritura.

`open_read_snapshot()` retorna `ReadSnapshot`. El consumidor es propietario de
su cierre y puede usar `async with`; agotamiento, cancelacion y error cierran la
fuente exactamente una vez. `SnapshotPolicy.STABLE` conserva una vista estable,
`MATERIALIZED` declara materializacion y `LIVE` permite observacion concurrente
explicita.

## Entrega de cambios

`change_delivery='none'` permite publicar el outcome tras retornar.
`commit-sequence` obliga a asignar secuencias monotonas en el commit efectivo.
`transactional-outbox` obliga a persistir la mutacion y su evento o hueco en la
misma transaccion.

Los consumidores mantienen checkpoint propio. Un consumidor con journal es
durable; uno local es efimero y se retira al desconectar. La compactacion nunca
finge continuidad: si el limite de retencion poda eventos que un consumidor no
ha confirmado, su siguiente lectura falla explicitamente. Tambien se rechaza
un journal cuyo checkpoint este por delante de la secuencia confirmada, porque
continuar podria saltar eventos futuros.

## Migracion desde SPI v1

SPI v1 sigue operativo durante 4.x mediante `LegacyEngineAdapter`, pero desde
4.3.0 emite un `DeprecationWarning` por clase de engine y se retirara en 5.0.0.
La migracion recomendada es:

1. declarar `EngineCapabilities(spi_version=2, ...)`;
2. reemplazar `put_document` y `put_documents_bulk` por outcomes de insert;
3. aceptar un unico `OperationContext` en las primitivas de mutacion;
4. retornar outcomes tipados, sin booleanos ni unions contextuales;
5. implementar `ReadSnapshot` si se declara `explicit_read_snapshots=True`;
6. declarar una estrategia de cambios y sus primitivas de consumidor;
7. ejecutar `tests/contracts/engines/test_storage_engine_v2_contract.py`
   contra el engine.

No se debe silenciar la advertencia como sustituto de la migracion. Un engine
puede mantener wrappers v1 propios mientras sus primitivas v2 sean la unica
implementacion semantica.

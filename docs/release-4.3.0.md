# Release 4.3.0

Status: release cerrada y publicada en PyPI.

Distribucion: <https://pypi.org/project/mongoeco/4.3.0/>

## Resumen

MongoEco 4.3.0 estabiliza el SPI v2 de engines y cierra las fronteras de
consistencia que antes dependian de convenciones privadas. Memory y SQLite
comparten outcomes tipados, un `OperationContext` inmutable, snapshots
explicitos y entrega ordenada por commit. SQLite persiste mutacion y change
event en una outbox transaccional.

La version tambien consolida las correcciones de atomicidad, BSON, indices,
cursores y `$merge` auditadas desde 4.2.1. No cambia el baseline: Python 3.13 o
3.14, MongoDB 7.0/8.0 como dialectos y PyMongo 4.9 o superior.

## Superficie nueva

`mongoeco.engines` exporta `EngineCapabilities`, `OperationContext`,
`ChangePublicationPolicy`, `MutationOutcome`, `DeleteOutcome`, `InsertOutcome`,
`BulkOutcome`, `MergeOutcome`, `CommittedChange`, `ReadSnapshot`,
`SnapshotMetadata` y `SnapshotPolicy`.

Los engines v2 se validan al cruzar la frontera del cliente. Memory y SQLite
implementan el contrato de forma nativa. La guia canonica y los pasos de
migracion estan en [architecture/engine-spi-v2.md](architecture/engine-spi-v2.md).

## Deprecaciones

SPI v1 no se elimina en 4.3.0. `LegacyEngineAdapter` conserva compatibilidad y
emite `DeprecationWarning` una vez por clase. La retirada esta reservada para
5.0.0. Los autores de engines externos deben migrar antes de esa major.

## Persistencia y change streams

- SQLite guarda la mutacion y el evento o hueco en la misma transaccion.
- Memory asigna la secuencia monotona en el commit MVCC efectivo.
- Los checkpoints de SQLite son persistentes para hubs con journal y efimeros
  para consumidores locales.
- La retencion por defecto es de 10.000 entradas y puede configurarse por
  engine.
- Un consumidor que cae por detras del suelo compactado recibe un error
  explicito; no se fabrica continuidad.

## Compatibilidad

No hay cambios incompatibles en la API de coleccion. El unico cambio visible
para engines externos v1 es la advertencia de deprecacion. Los bytes publicados
de 4.2.1 no se modifican.

## Gates de publicacion

Antes del commit y la etiqueta quedaron verdes:

- suite pytest y unittest;
- cobertura de `src/mongoeco` igual o superior al 99%;
- contrato SPI v2 y paridad Memory/SQLite, sync/async;
- matriz de perfiles PyMongo y snapshots de compatibilidad;
- build de wheel/sdist, `twine check` y smoke de ambos artefactos;
- ratchet Ruff sobre lineas nuevas;
- benchmarks cortos sin regresion bloqueante;
- diferenciales MongoDB 7.0/8.0 cuando existan servicios configurados.

La etiqueta anotada `v4.3.0` apunta al commit de preparacion `c95ff74`. PyPI
expone los dos artefactos construidos desde ese estado y el smoke desde el
indice publico confirma la version y el contrato CXP esperado.

## Evidencia de cierre

Validacion ejecutada el 12 de agosto de 2026:

- Python 3.13.13: `3115 passed`, `15 skipped` y `2229 subtests passed`;
- Python 3.14.5: `3115 passed`, `15 skipped` y `2229 subtests passed`;
- `unittest`: `3244 tests`, `2 skipped`;
- cobertura: 34.651 statements, 345 sin cubrir, `>=99%`;
- ratchet Ruff: verde sobre 73 ficheros Python modificados;
- perfiles PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0: resumen identico
  al fixture canonico;
- snapshots de compatibilidad: sin delta;
- `twine check`, instalacion limpia y smoke funcional: verdes para wheel y
  sdist;
- benchmarks generales y diagnosticos `search`/`vectorSearch`: completados
  en Memory y SQLite, sync y async, sin errores de ejecucion.

Artefactos publicados y verificados contra la API JSON de PyPI:

- wheel `mongoeco-4.3.0-py3-none-any.whl`:
  `62da619bd63fadb2a516f8f7c12ae02ea4ed3fc0409d152e1e50aaa982fea575`;
- sdist `mongoeco-4.3.0.tar.gz`:
  `257abace7481ef08c5647f4ae5d9590fc6078fd782ae58f544acee15de893d0f`.

No existe un baseline historico directamente comparable para convertir la
matriz de benchmark en un gate porcentual; se conserva como smoke y evidencia
diagnostica. Las suites diferenciales contra MongoDB 7.0/8.0 reales no se han
ejecutado porque `MONGOECO_REAL_MONGODB_URI` no esta configurada. No se ha
simulado ese resultado.

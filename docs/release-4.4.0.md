# Release 4.4.0

Status: release cerrada y publicada en PyPI.

Distribucion: <https://pypi.org/project/mongoeco/4.4.0/>

## Resumen

MongoEco 4.4.0 estabiliza las fronteras introducidas por el SPI v2 sin retirar
la compatibilidad de 4.x. Consolida un unico `OperationContext`, ownership BSON
profundo, snapshots con lifecycle supervisado y entrega de cambios ordenada.
SQLite refuerza su outbox transaccional con leases persistentes y registros
efimeros renovables entre procesos.

No cambia el baseline: Python 3.13 o 3.14, dialectos MongoDB 7.0/8.0 y PyMongo
4.9 o superior.

## Compatibilidad SPI

SPI v1 sigue deprecado pero operativo mediante `LegacyEngineAdapter`; su
retirada continua reservada para 5.0.0. SPI v2 conserva las dos estrategias de
lectura publicadas en 4.3.0:

- `explicit_read_snapshots=True` exige `open_read_snapshot`;
- `explicit_read_snapshots=False` exige `scan_find_semantics`, que el adapter
  envuelve en un `ReadSnapshot` estable.

Toda operacion v2 recibe un `OperationContext` y los outcomes aplicados deben
incluir las imagenes atomicas exigidas por su tipo.

## Consistencia

- La frontera BSON posee documentos y arrays anidados, incluidos `DBRef`,
  scopes de `bson.Code` y subtipos `Binary`, sin exponer contenedores internos.
- Los cursores sync y async capturan una sola operacion y un solo contexto;
  `$$NOW`, `let`, collation y codec no se recompilan al consumir.
- Los finalizadores sync nunca esperan mientras el runner esta ocupado: el
  cleanup abandonado se delega al helper y el cierre explicito conserva su
  semantica determinista y observable.
- `ReadSnapshot` aplica el mismo deadline de cleanup al cierre explicito,
  agotamiento, fallo y cancelacion, conservando la ruta sync no suspendente.
- Los gates de consumidores no se retiran mientras existan owners o waiters.

## SQLite Outbox

- Mutacion y evento o hueco permanecen en la misma transaccion.
- Lease, heartbeat, checkpoint y compactacion usan el control plane dedicado.
- Un lease con generacion evita checkpoints de owners obsoletos y serializa
  instancias y procesos que comparten el mismo fichero.
- Los registros efimeros renuevan su TTL mientras el engine esta conectado;
  un lease activo impide su expiracion prematura.
- La entrega sigue siendo at-least-once: un crash despues del callback y antes
  del checkpoint repite la fila cuando expira el lease.

## Gates

Antes de etiquetar deben quedar verdes la suite completa en Python 3.13 y
3.14, `unittest`, cobertura `>=99%`, ratchet Ruff, matriz PyMongo, snapshots de
compatibilidad, benchmarks cortos y smokes de wheel y sdist. Los hashes de los
artefactos se registraran tras construirlos desde el commit definitivo.

Las diferenciales contra deployments MongoDB reales solo se declaran
ejecutadas cuando `MONGOECO_REAL_MONGODB_URI` esta disponible; no se simula esa
evidencia.

## Evidencia local

Validacion ejecutada el 14 de agosto de 2026 sobre el estado candidato:

- Python 3.13 y 3.14: `3200 passed`, `15 skipped` y `2250 subtests passed`;
- `unittest`: `3325 tests`, `2 skipped`;
- cobertura: `99.00%` real (`35641` statements, `355` sin cubrir);
- ratchet Ruff y snapshots de compatibilidad: verdes;
- matriz PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0: verde;
- benchmark principal Memory/SQLite sync/async y mongomock: completo sin
  errores; diagnósticos `search` y `vectorSearch` Memory/SQLite: completos;
- wheel y sdist: `twine check` y smokes desde entornos limpios verdes,
  importando 4.4.0 desde `site-packages`.

`MONGOECO_REAL_MONGODB_URI` no estaba disponible, por lo que no se atribuye
evidencia diferencial contra MongoDB real.

## Publicacion

La etiqueta anotada `v4.4.0` apunta al commit de preparacion `fbb2c52`. El
workflow de etiqueta supero build, imports minimos, Python 3.13/3.14, cobertura,
ratchet, snapshots, benchmarks y smokes. El intercambio OIDC de Trusted
Publishing fue rechazado con `invalid-publisher` porque PyPI no tenia registrado
el publisher correspondiente a `menudoproblema/mongoeco`, `ci.yml` y el
environment `pypi`.

Para completar este corte, wheel y sdist se reconstruyeron desde la misma
etiqueta con `SOURCE_DATE_EPOCH` fijado, pasaron `twine check` y ambos smokes, y
se publicaron con la credencial local existente. El smoke posterior instalo
`mongoeco==4.4.0` desde PyPI y lo importo desde `site-packages`.

Artefactos publicados:

- wheel `mongoeco-4.4.0-py3-none-any.whl`:
  `f467a1abb69fea41eb06f5b61a8c158209f97c3291b8c4600067c4df239785ae`;
- sdist `mongoeco-4.4.0.tar.gz`:
  `7af9bc005650181b213c04f1fcec56a725367a5053bdac1bd4fe625cd6d022ea`.

Antes de la proxima etiqueta debe configurarse el Trusted Publisher en PyPI
con esos claims; no debe asumirse que declarar el environment de GitHub crea
automaticamente la vinculacion en PyPI.

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

## `MemoryEngine`

`MemoryEngine` es el backend mas directo y actua como baseline semantico local.
Su valor arquitectonico no es solo la sencillez:

- facilita parity tests;
- sirve como referencia para semantica compartida;
- mantiene MVCC local con snapshots;
- implementa metadata, profiling, indices y search indexes sin depender de un
  backend externo.

Se usa como baseline semantico, no como "mock" desechable.

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

La intencion de esta modularizacion es bajar el coste de cambio y evitar que el
engine siga siendo una fuente unica de verdad dispersa.

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
`$vectorSearch`, porque ahi el backend importa de verdad.

## Tradeoff principal de la capa de engines

La arquitectura busca un equilibrio:

- compartir semantica donde es realmente la misma;
- dejar que cada engine ejecute de forma distinta;
- fijar parity tests cuando el contrato observable se promete igual.

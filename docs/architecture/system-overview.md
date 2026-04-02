# Vision general del sistema

## Estructura por capas

La arquitectura de `mongoeco` se apoya en una separacion estable entre capas
semanticas, de persistencia y de adaptacion:

1. `api`
   - define la superficie publica async/sync;
   - normaliza argumentos;
   - compila operaciones publicas;
   - aplica concerns, sessions y metadata de perfil.
2. `core`
   - concentra la semantica ejecutable;
   - resuelve query plans, filtering, sorting, projection, updates y
     aggregation;
   - modela semantica BSON, validation y collation.
3. `engines`
   - ejecuta la persistencia y el acceso fisico;
   - implementa contratos de lectura, escritura, indices, admin y profiling;
   - hoy ofrece dos backends locales: memoria y SQLite.
4. `driver`
   - modela un runtime de cliente local con URI, policies, topologia,
     ejecucion y eventos de monitorizacion.
5. `wire`
   - adapta comandos del protocolo wire a la superficie del cliente local.
6. `compat`
   - separa dos ejes de compatibilidad: dialecto MongoDB y perfil PyMongo.
7. `_change_streams`
   - encapsula el runtime interno de change streams.

## Limites entre subsistemas

La frontera principal del sistema es:

- la `api` conoce concerns, sessions, perfiles y operaciones publicas;
- el `core` no conoce la forma externa del cliente, pero si la semantica de las
  operaciones;
- los `engines` consumen semantica compilada y exponen capacidades fisicas;
- el `driver` y `wire` viven encima del runtime local, no debajo del engine;
- `compat` no ejecuta operaciones: define catalogos, resolucion y deltas
  soportados;
- `_change_streams` es un detalle interno con una fachada publica minima en
  `change_streams.py`.

## Flujo end-to-end de una operacion

### Lectura (`Collection.find`)

1. La coleccion publica recibe argumentos PyMongo-shaped.
2. `public_api.py` y `argument_validation.py` normalizan aliases y restricciones.
3. `operations.py` compila un `FindOperation`.
4. `semantic_core.py` lo convierte en `EngineFindSemantics`.
5. El engine decide planificacion fisica o fallback.
6. El cursor publica resultados y `explain()` desde esa semantica.

### Aggregation (`Collection.aggregate`)

1. La `api` compila un `AggregateOperation`.
2. `core/aggregation` valida stages y expresiones.
3. El pipeline se ejecuta parcialmente en runtime y parcialmente con pushdown
   cuando el engine puede expresarlo.
4. Si el shape no es ejecutable bajo el modo actual, se falla o se degrada via
   `planning_mode`.

### Escritura (`insert/update/findAndModify`)

1. La `api` valida shape publico y replacement/update docs.
2. `operations.py` compila la operacion.
3. El engine ejecuta la semantica de seleccion y modificacion.
4. Si hay profiling, metadata de operacion o change streams, la `api` publica
   esos efectos laterales alrededor de la llamada al engine.

## Criterios globales de arquitectura

### Async-first

La semantica primaria vive en la ruta async. La ruta sync es una fachada que
adapta al runtime async mediante un runner dedicado y wrappers de cursores,
bases de datos y colecciones.

### Compatibilidad explicita

La compatibilidad no se deja inferida por dependencias instaladas. Se modela
con dos ejes explicitamente resueltos:

- `mongodb_dialect`;
- `pymongo_profile`.

### Planificacion honesta

El sistema prefiere exponer gaps de ejecucion con `planning_issues`, errores
explicitos o capacidades inspectables antes que degradar a no-ops silenciosos.

### Local-first

El runtime esta orientado a desarrollo local, pruebas, entornos embebidos y
modelado de compatibilidad. Muchas capacidades avanzadas existen, pero se
declaran con su limite real de producto.

## Invariantes transversales

- La superficie publica intenta ser simetrica entre async y sync cuando ambas
  existen.
- Lo que se promete igual entre `MemoryEngine` y `SQLiteEngine` debe fijarse
  con parity tests.
- Los contratos parciales deben exponerse de forma explicita cuando el sistema
  no implementa la capacidad completa.
- Las decisiones de compatibilidad y degradacion deben quedar visibles en
  tests, docs y APIs de introspeccion.

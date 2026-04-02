# Follow-ups

Este fichero recoge trabajo de profundizacion, hardening y cobertura
sobre funcionalidades ya implementadas.

No sustituye a:

- [MISSING_FEATURES.md](/Users/uve/Proyectos/mongoeco2/MISSING_FEATURES.md), que lista capacidades ausentes o incompletas a nivel producto.
- [TODO.md](/Users/uve/Proyectos/mongoeco2/TODO.md), que sigue siendo el backlog mas amplio de evolucion del proyecto.

## 1. Hardening inmediato

### Query top-level `$jsonSchema`

Estado: cubierto en `query_plan`, `QueryEngine` e integracion
`async`/`sync`, incluyendo mezcla con filtros de campo y schemas mas
ricos (`additionalProperties`, `items`, `minLength` / `maxLength`,
`minimum` / `maximum`).

Pendiente razonable:

- si aparece otro bug real, anadir la regresion sobre el helper comun
  de casos `async`/`sync`.

### Smoke del artefacto empaquetado

Motivacion: ya hubo un bug que solo aparecia en `site-packages`.

Estado actual:

- ya existe una validacion manual reproducible que:
  - construye `dist/*.whl`
  - crea un entorno limpio
  - instala el wheel
  - ejecuta una operacion minima real con `AsyncMongoClient` y
    `MemoryEngine`
- este smoke ya detecto una regresion real de imports ansiosos a
  `bson`, que quedo corregida antes de preparar `2.1.0`
- el smoke del artefacto ya corre en CI sobre el `wheel` y el `sdist`
  construidos, no solo sobre `src/`.

### Snapshots del catalogo de compatibilidad

Motivacion: al tocar el catalogo de operadores soportados se rompen los
snapshots y es facil olvidarlo.

Estado actual:

- ya existe el script:
  - `scripts/update_compat_snapshots.py`
- el flujo esperado queda asi:
  1. tocar catalogo/export;
  2. regenerar snapshots;
  3. incluir snapshots en el mismo commit.

## 2. Cobertura con mejor retorno

### Alta prioridad

- [engines/sqlite.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/engines/sqlite.py)
  Motivo: modulo grande con retorno alto, aunque mas caro de atacar.

### Prioridad media

- zonas internas extraidas de SQLite con menos cobertura que el resto
  del hardening reciente, siempre que el test nuevo tenga frontera
  clara y no se convierta en reproduccion del engine entero.

### Ya bastante cubierto

- [api/_async/cursor.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/api/_async/cursor.py)
- [compat/base.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/compat/base.py)
- [search.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/core/search.py)
- [types.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/types.py)

Motivo: el retorno inmediato por mas tests ya no es especialmente alto
frente a otras zonas.

### No perseguir por ahora

- apurar del `98%` al `100%` en:
  - `core/filtering.py`
  - `core/bson_scalars.py`
  - `core/aggregation/runtime.py`
  - `core/aggregation/scalar_expressions.py`

Motivo: el retorno marginal ya es bajo salvo que aparezca un bug real.

## 3. Mantenibilidad de tests

### Ya dividido y estable

- `aggregation`
- `architecture`
- `filtering`

### Vigilar antes de que crezca mas

- [test_search.py](/Users/uve/Proyectos/mongoeco2/tests/unit/core/test_search.py)
  Aun no necesita split, pero si sigue creciendo conviene partirlo por:
  - index definitions
  - stage compilation
  - vector search
  - runtime matching

### Refactor de integracion a considerar mas adelante

- [test_async_api.py](/Users/uve/Proyectos/mongoeco2/tests/integration/api/test_async_api.py)
- [test_sync_api.py](/Users/uve/Proyectos/mongoeco2/tests/integration/api/test_sync_api.py)

Pendiente:

- seguir extrayendo utilidades comunes cuando un caso nuevo obligue a
  tocar ambos ficheros a la vez.
- ya existe un primer helper comun para casos de integracion de
  top-level `$jsonSchema`.

No merece la pena una gran reescritura preventiva ahora.

## 4. Criterio operativo

Regla recomendada para cambios proximos:

1. si falta una capacidad observable, va a `MISSING_FEATURES.md`;
2. si la capacidad ya existe pero necesita mas solidez, va aqui;
3. si es una linea amplia de producto o roadmap, va a `TODO.md`.

# Follow-ups

Este fichero recoge trabajo de profundizacion, hardening y cobertura
sobre funcionalidades ya implementadas.

No sustituye a:

- [MISSING_FEATURES.md](/Users/uve/Proyectos/mongoeco2/MISSING_FEATURES.md), que lista capacidades ausentes o incompletas a nivel producto.
- [TODO.md](/Users/uve/Proyectos/mongoeco2/TODO.md), que sigue siendo el backlog mas amplio de evolucion del proyecto.

## 1. Hardening inmediato

### Query top-level `$jsonSchema`

Estado: implementado en `query_plan` y `QueryEngine`, con cobertura
unitaria e integracion `async`/`sync`.

Pendientes recomendados:

- anadir pruebas explicitas combinando `$jsonSchema` con:
  - `$and`
  - `$or`
  - `$nor`
- anadir pruebas de mezcla de `$jsonSchema` con filtros de campo
  sobre documentos validos e invalidos en la misma consulta;
- anadir un test explicito de fallback SQLite para dejar fijado que
  `JsonSchemaCondition` no se traduce a SQL y se resuelve en Python
  sin romper resultados;
- anadir algun caso de schema mas rico en filtro top-level:
  - `additionalProperties`
  - `items`
  - `minLength` / `maxLength`
  - `minimum` / `maximum`

### Smoke del artefacto empaquetado

Motivacion: ya hubo un bug que solo aparecia en `site-packages`.

Pendientes recomendados:

- crear un smoke reproducible que:
  - construya `dist/*.whl`
  - cree un entorno limpio
  - instale el wheel
  - verifique imports criticos desde el paquete instalado
- si se usa CI en el futuro, ejecutar ese smoke sobre el artefacto
  construido, no solo sobre `src/`.

### Snapshots del catalogo de compatibilidad

Motivacion: al tocar el catalogo de operadores soportados se rompen los
snapshots y es facil olvidarlo.

Pendientes recomendados:

- dejar un script pequeño para regenerar:
  - `tests/fixtures/compat_catalog_snapshot.json`
  - `tests/fixtures/compat_catalog_snapshot.md`
- documentar en el flujo de cambios del catalogo que los snapshots
  deben actualizarse en el mismo commit.

## 2. Cobertura con mejor retorno

### Alta prioridad

- [search.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/core/search.py)
  Motivo: sigue siendo una zona de producto visible y con retorno real
  por test nuevo.
- [sorting.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/core/sorting.py)
  Motivo: modulo acotado y barato de subir.

### Prioridad media

- [compat/base.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/compat/base.py)
  Motivo: rutas de error y resolucion de estrategias.
- [driver/uri.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/driver/uri.py)
  Motivo: parser con muchas ramas y bajo coste relativo.
- [wire/protocol.py](/Users/uve/Proyectos/mongoeco2/src/mongoeco/wire/protocol.py)
  Motivo: superficies de framing con buena señal por caso nuevo.

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

No merece la pena una gran reescritura preventiva ahora.

## 4. Criterio operativo

Regla recomendada para cambios proximos:

1. si falta una capacidad observable, va a `MISSING_FEATURES.md`;
2. si la capacidad ya existe pero necesita mas solidez, va aqui;
3. si es una linea amplia de producto o roadmap, va a `TODO.md`.

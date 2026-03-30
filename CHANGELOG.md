# Changelog

Todos los cambios relevantes de este proyecto se documentan en este
archivo.

El formato sigue las recomendaciones de Keep a Changelog y el proyecto
usa Semantic Versioning.

## [Unreleased]

### Fixed

- Se corrige la resolucion de anotaciones en
  `mongoeco.api._async.database_commands` para evitar errores en
  Python 3.13+ al combinar forward refs internas con el operador `|`.
- Se corrige un `NameError` en `mongoeco.core.aggregation.runtime`:
  `_subtract_values` dependia de `_require_numeric` sin importarlo,
  rompiendo la ruta interna de resta numero-numero.

### Added

- Se anade soporte para la forma generada por joins correlacionados que
  fijan condiciones de campo con `$and` y `$or` dentro de `$expr`,
  junto con pruebas de unidad e integracion para pipelines `$lookup`.
- Se anade soporte para reutilizar operadores de `query_filter`
  (`$exists`, `$all`, `$nin` y `$elemMatch`) dentro de `$expr` en
  pipelines `$lookup`, reutilizando la semantica de `QueryEngine` y
  actualizando los snapshots de compatibilidad.
- Se blindan con pruebas los joins correlacionados de lista que usan
  `$in` dentro de `$lookup`, incluyendo la variante con rutas
  variables punteadas sobre listas de subdocumentos.
- Se anade una prueba en interprete limpio para validar que las
  anotaciones de `AsyncDatabaseCommandService` se resuelven
  correctamente.
- Se amplian las pruebas de `admin_parsing` y `core.search` para cubrir
  validaciones, normalizacion de entradas y edge cases de busqueda
  textual y vectorial.
- Se anaden pruebas especificas para `driver.transports`,
  `engines.virtual_indexes` y los adaptadores `raw_batch_cursor`,
  elevando la cobertura de esos modulos y reforzando caminos de error,
  roundtrips wire y helpers internos de implicacion.
- Se amplia la cobertura de `change_streams` con pruebas de offsets,
  reanudacion, espera bloqueante, iteracion async y validacion de
  pipelines.
- Se refuerzan `engines.virtual_indexes`, `core.filtering` y
  `api._async.database_admin` con pruebas adicionales sobre helpers de
  implicacion, claves hashables especiales, compilacion de comandos y
  ramas de error en comandos administrativos.
- Se reorganiza la suite para facilitar mantenimiento y nuevas tandas
  de cobertura: la infraestructura sync compartida de integracion se
  mueve a `tests/support.py`, `test_aggregation.py` se divide por
  familias funcionales y `test_architecture.py` se separa por
  responsabilidades.
- Se amplian las pruebas de agregacion sobre `stages`, `runtime` y
  `scalar_expressions`, cubriendo el camino interpretado, optimizaciones
  de ventana para `sort`, helpers BSON y conversiones escalares
  internas.
- Se reorganizan las pruebas de `filtering` separando consultas y
  helpers internos en ficheros distintos, y se amplian los casos de
  cobertura sobre comparacion, tipos BSON, bitwise, membership y
  resolucion de rutas anidadas.
- Se amplian las pruebas de `bson_scalars` sobre overflows, division y
  modulo, rewrap interno, helpers de metadata numerica y rutas bitwise
  con wrappers BSON.
- Se amplia la cobertura de `json_compat`, `driver.topology_monitor`,
  `core.sorting`, `wire.protocol`, `driver.uri` y `core.operators`,
  reforzando ramas de error, roundtrips wire, parsing de URI y helpers
  internos de actualizacion.

## [2.0.1] - 2026-03-30

### Fixed

- Se corrige un `NameError` al importar `mongoeco.engines.base` en
  Python 3.13+ porque `AsyncIndexAdminEngine` usaba `IndexKeySpec` sin
  importarlo en el modulo.

### Added

- Se anade un smoke test que importa todos los modulos bajo
  `src/mongoeco` en interpretes limpios para detectar errores de
  importacion antes de publicar una nueva version.
- Se anaden pruebas que fuerzan la resolucion de anotaciones en los
  protocolos `Async*Engine` y validan que los paquetes publicos exportan
  simbolos resolubles desde `__all__`.

### Changed

- Se prepara la matriz de CI para Python `3.13` y `3.14`.

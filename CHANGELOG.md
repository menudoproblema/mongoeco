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

### Added

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

# Release 4.8.1

Fecha: 1 de octubre de 2026. Release de correccion sobre Mongoeco 4.8.0.

Estado: publicada en [PyPI](https://pypi.org/project/mongoeco/4.8.1/).
La etiqueta `v4.8.1` apunta a
`ad90d8462d1d49a8161717271b073049ceb896d8`.

## Correcciones

Un `$lookup` o `$unionWith` posterior a `$group` recibia un pipeline sin el
resolver de colecciones. La preparacion comun descubre las dependencias antes
de elegir streaming, materializacion, spill o Search, y conserva los documentos
y snapshots de cada coleccion al entrar en pipelines anidados.

- Se soportan joins despues de agrupacion con `localField`/`foreignField` y
  `let`/`pipeline`, varios joins, resultados paginados y grupos con spill.
- Las extensiones que delegan en stages builtin conservan sus recursos sin
  ejecutar handlers ni imponer parsers builtin durante la preparacion.
- Las restricciones de `$facet` alcanzan todos sus descendientes; los joins
  validos dentro de facets siguen funcionando.
- Reubicar un pipeline preparado recalcula coleccion, ambitos y direcciones.
  Los fragmentos fisicos conservan sus posiciones logicas; cambiar dialecto
  o registrar o retirar extensiones invalida la preparacion anterior.
- Los paths de `$lookup.as` con puntos generan campos anidados. Los pipelines
  que empiezan con `$documents` admiten fuentes independientes de colecciones
  y respetan la validacion de namespaces de MongoDB 8.
- Comparaciones, pertenencia, grupos, ventanas y spill comparten collation e
  identidad BSON, incluida equivalencia numerica, precision Decimal128,
  cero con signo y distincion de subtipos binarios.

## Compatibilidad y migracion

La version mantiene los import roots de 4.8, SPI v2, los formatos persistidos
y `cxp[exchange]>=5.0.0,<6`. No requiere una migracion de datos.

`$collStats.count` devuelve ahora el numero compatible con MongoDB, en lugar
de un documento anidado. Los pipelines que proyectaban `$count.count` deben
proyectar `$count`. La guia de [migracion CXP](cxp-c4-migration.md) conserva
el contrato de 4.8 y documenta esta correccion.

Los consumidores que fijan versiones, incluido `gdynamics-testing`, pueden
actualizar `mongoeco==4.8.0` a `mongoeco==4.8.1` despues de la publicacion.
El pin de `mochuelo-testkit` y PI-0009 se actualizan en su repositorio owner.

## Verificacion

El candidato se verifica con el [checklist de release](release-checklist.md):
wheel y sdist reproducibles, suites desde el wheel instalado en Python 3.13 y
3.14, cobertura minima del 99 %, typing publico, SPI independiente, imports
minimos, manifiesto publico, snapshots, fixture SQLite 4.5, property tests
profundos, matriz PyMongo, benchmarks y diferenciales completos MongoDB 7/8.

La publicacion utiliza los artefactos del commit etiquetado `v4.8.1`, despues
de verificar los gates del workflow. El smoke de contrato desde PyPI y los
SHA-256 del indice publico verifican la entrega final.

## Evidencia de entrega

- El [workflow del tag](https://github.com/menudoproblema/mongoeco/actions/runs/36851239057)
  termino correctamente, incluida la publicacion mediante Trusted Publishing.
- Las suites completas del wheel instalado en Python 3.13 y 3.14 pasaron:
  4.672 tests y 3.982 subtests de pytest, y 3.587 casos de unittest.
  La cobertura medida en Python 3.14 fue del 99,04 %.
- Los diferenciales completos pasaron 29 casos por version contra MongoDB
  7.0 y 8.0, sin fallos ni skips. Los property tests profundos, la matriz
  PyMongo, los contratos publicos y la matriz de benchmarks tambien pasaron.
- La instalacion limpia desde PyPI verifico la version 4.8.1, imports,
  contrato CXP y `pip check`. Las regresiones de referencias posteriores a
  `$group` pasaron desde el paquete publicado, en sync y async, Memory y
  SQLite, con paginacion y spill, para ambas formas de `$lookup` y `$unionWith`.
- Los SHA-256 publicados coinciden con los dos builds locales reproducibles:

| Archivo | SHA-256 |
| --- | --- |
| `mongoeco-4.8.1-py3-none-any.whl` | `0d2e6a81cf8ba1fd98bf5bcf68b3256c10836739f92429ef474d3b2987e18c86` |
| `mongoeco-4.8.1.tar.gz` | `b3ce72db5f9e36d4f5e5acf151e74890466de32b3a9f4468383d90717b116ff6` |

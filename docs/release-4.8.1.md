# Release 4.8.1

Fecha: 1 de octubre de 2026. Release de correccion sobre Mongoeco 4.8.0.

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

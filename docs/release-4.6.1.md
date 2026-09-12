# Release 4.6.1

Status: candidata preparada para validacion y publicacion.

## Resumen

MongoEco 4.6.1 corrige bloqueos durante la finalizacion concurrente de
cursores, clientes y recursos internos. El caso original podia aparecer cuando
el recolector liberaba un cursor sincronico mientras el mismo hilo mantenia el
bloqueo no reentrante del runner auxiliar. La release sustituye esa dependencia
del momento de recoleccion por transiciones de cierre explicitas, acotadas e
idempotentes.

La version tambien adopta `cxp>=4.1.0` como minimo de runtime y fija CXP 4.1.0
en la resolucion controlada de CI. La integracion sigue usando la API Python de
catalogo preservada y no requiere el extra `cxp[exchange]`.

## Correcciones de lifecycle

- Los finalizadores sync difieren el cleanup sin intentar reentrar en el runner
  activo; el cierre solicitado desde el owner o el hilo auxiliar termina sin
  autobloqueo.
- El cierre async concurrente comparte una sola transicion terminal, sobrevive
  a la cancelacion de un caller y agrupa errores de recursos sin abandonar el
  resto del cleanup.
- Change streams, monitores de topologia y esperas de pools reciben una senal
  explicita de parada. Un recurso cerrado no acepta nuevos watchers ni
  conexiones.
- Los recursos de red se descartan ante cancelacion, timeout o una respuesta
  wire que no corresponda a la peticion activa.
- SQLite detiene los productores de scans vivos antes de apagar el executor y
  no ocupa un segundo worker esperando una cola, por lo que el cierre funciona
  tambien con un unico worker y un cursor todavia vivo.

## Compatibilidad y migracion

No hay cambios incompatibles en la API publica, el SPI v2, los formatos
persistidos ni las fixtures historicas. No se requiere migracion de datos. Los
consumidores que fijen dependencias deben permitir `cxp>=4.1.0`.

La fixture SQLite de MongoEco 4.5.0 conserva sus dependencias y hashes
originales para seguir siendo una evidencia historica reproducible.

## Evidencia requerida antes de etiquetar

- Suites completas en Python 3.13 y 3.14, cobertura global minima del 99 %,
  typing publico, canario SPI v2, lint ratchet y snapshots de compatibilidad.
- Repeticion intensiva de los escenarios concurrentes de cierre sin bloqueos
  ni flakiness.
- Property tests profundos con las semillas versionadas del workflow.
- Matriz de perfiles PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0.
- Diferenciales contra MongoDB 7.0 y 8.0.
- Wheel y sdist reproducibles byte a byte, `twine check`, smokes desde
  `site-packages` y conformidad de Memory, SQLite y el canario externo.

## Publicacion

La etiqueta prevista es `v4.6.1`. Los hashes finales y el resultado de PyPI se
registraran aqui despues de publicar los artefactos construidos desde el commit
etiquetado.

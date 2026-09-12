# Release 4.6.1

Status: publicada el 12 de septiembre de 2026.

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

## Evidencia de cierre

- Las suites completas pasan en Python 3.13 y 3.14. `unittest` ejecuta 3.479
  tests con 2 skips y pytest ejecuta 3.484 tests, 26 skips y 2.509 subtests.
  La cobertura global es 99,04 %, por encima del minimo de 99 %.
- Typing publico, canario SPI v2, lint ratchet, manifiesto de API y snapshots
  de compatibilidad pasan sin deltas no explicados.
- Los 9 escenarios concurrentes criticos de cierre pasan 20 veces: 180
  ejecuciones sin bloqueos ni flakiness.
- Los property tests profundos pasan con las semillas 4600, 4601 y 4602. La
  matriz de perfiles PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0 coincide con la
  fixture versionada.
- Los diferenciales requeridos pasan contra MongoDB 7.0 y 8.0 en el
  [workflow del tag](https://github.com/menudoproblema/mongoeco/actions/runs/34692146789).
- El benchmark completo pasa para Memory y SQLite sync/async y para
  mongomock. La fixture SQLite 4.5 conserva su SHA-256 y pasa lectura de
  indices, Search y replay de outbox.
- Wheel y sdist son reproducibles byte a byte y pasan `twine check`, smokes
  desde `site-packages` sin constraints internas y conformidad de Memory,
  SQLite y el canario externo.

## Publicacion

La etiqueta anotada `v4.6.1` apunta al commit
`11c14b868777e71b74ba280086ee63efe55c3327`. El build, los diferenciales y las
suites del workflow del tag pasaron. El paso de Trusted Publishing no publico
los artefactos, por lo que se uso la credencial local de fallback despues de
confirmar que la version todavia no existia en PyPI.

La version publicada esta disponible en
[PyPI](https://pypi.org/project/mongoeco/4.6.1/) y pasa el smoke de contrato
instalado desde el indice publico. PyPI registra los mismos SHA-256 que los
artefactos reproducibles construidos desde el commit etiquetado:

- `mongoeco-4.6.1-py3-none-any.whl`:
  `721cc4753def8c58f183d7841d0705bcbbcc8eff0bd1ee989497e8678ebcd843`.
- `mongoeco-4.6.1.tar.gz`:
  `be0265ff6f5c74a62f84c096b3d785f57118b8f275edc05dbbbbd1af2f961d06`.

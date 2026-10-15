# Release 4.9.1

Fecha: 15 de octubre de 2026. Corrección de fidelidad de actualizaciones sobre
Mongoeco 4.9.0. Estado: candidato en validación; todavía no publicado.

## Comportamiento corregido

- Las rutas `$[]` y `$[id]` rechazan arrays ausentes o de tipo incorrecto con
  código 2. El documento que falla conserva todos sus valores originales,
  incluso al combinar varios operadores, usar upsert o ejecutar bulk.
- Las rutas posicionales que alcanzan el mismo campo, o un campo y su ancestro,
  fallan con código 40 antes de confirmar cambios. Se mantienen válidos los
  filtros disjuntos, los arrays vacíos y los filtros sin coincidencias. Las
  coincidencias de un documento se conservan durante toda su actualización.
- SQLite ejecuta las rutas posicionales mediante su implementación Python,
  incluidas las hojas `$[]` y `$`, conservando el mismo resultado que Memory.
- Los segmentos numéricos respetan el tipo del contenedor existente. Los nuevos
  contenedores intermedios son objetos; los elementos null existentes no se
  convierten en contenedores y su recorrido inválido falla con código 28.
- Las semillas de upsert usan contenedores mutables separados de los valores
  BSON inmutables del comando, preservando tipos escalares y entradas del caller.

Las correcciones se aplican a las fachadas sync/async y a ambos motores. La
atomicidad es por documento: update_many y bulk pueden conservar escrituras
anteriores que terminaron correctamente. Bulk conserva el código 65 y los códigos
individuales en writeErrors.

## Compatibilidad y migración

No hay migración de datos, cambios del Engine SPI v2, del formato SQLite, del
contrato CXP, de los import roots ni de los defaults de dialecto/perfil. Las pruebas
que dependían de ignorar rutas inválidas o de crear arrays implícitos donde MongoDB
crea objetos deben corregir esas expectativas.

El perfil explícito más reciente sigue siendo PyMongo 4.18; la matriz de entrega
conserva PyMongo 4.18.2. El soporte explícito de PyMongo 4.19 queda para la próxima
entrega.

## Evidencia

La [matriz posicional](positional-update-validation.md) describe los 114 casos y
sus códigos, estado persistido y controles válidos. La evidencia de este candidato
se conserva en [evidence/mongoeco-4.9.1](evidence/mongoeco-4.9.1/). El cierre de
entrega y la publicación se registrarán después de terminar sus gates.

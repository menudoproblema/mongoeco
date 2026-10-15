# Release 4.9.1

Fecha: 15 de octubre de 2026. Corrección de fidelidad de actualizaciones sobre
Mongoeco 4.9.0. Estado: cierre local verificado; pendiente de CI alojada y publicación.

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

## Cierre local del candidato

El código validado corresponde a `b8db9238db511f4173a19a4d845798b5859a9466`.
Los metadatos y hashes del candidato están en
[validation.json](evidence/mongoeco-4.9.1/validation.json) y los dos builds
reproducibles en [build.json](evidence/mongoeco-4.9.1/build.json).

| Comprobación | Resultado local |
| --- | --- |
| Wheel instalado, Python 3.13 y 3.14 | 8.840 passed, 31 skips esperados y 4.456 subtests en cada versión. |
| Cobertura Python 3.14 | 99,01 %, con el floor original de 99,00 %. |
| Unittest / propiedades deep | 3.606 tests, 3 skips; nueve propiedades correctas. |
| Paridad posicional | 114 escenarios por motor y versión: 684 comparaciones, con códigos 2, 28, 40 y 65. |
| Servidores reales locales | MongoDB 7.0.39 / 8.0.32 / 9.0.2, FCV estable exacta. |
| Paridad y capturas anteriores | 29 casos y 284 capturas por versión, sin omisiones. |
| Contratos | Typing, manifiesto, snapshots, conformance completo de Memory/SQLite y canario externo correctos. |
| Persistencia e instalación | Fixture SQLite 4.5 y smokes limpios de wheel/sdist correctos. |
| Perfiles PyMongo | Matriz 4.9.2 / 4.11.3 / 4.13.2 / 4.17.0 / 4.18.2 idéntica a la fixture. |
| Rendimiento | 95 escenarios comparados con 4.9.0, máximo delta de mediana +5,63 %; diagnóstico de spill con 100.000 documentos correcto. |

La publicación utilizará los artefactos inmutables de la CI de la etiqueta. Los
hashes locales corresponden al commit de código indicado; la CI vuelve a construir
con el timestamp del commit etiquetado y registra sus propios hashes. Los gates
alojados exigen MongoDB 7.0.43 / 8.0.32 / 9.0.2 y las dos versiones de Python antes
de que Trusted Publishing pueda publicar.

# B07: admisión BSON y respuesta de comandos públicos

Implementación `3bfe5fb`, base `fd37c14`. Aceptación acotada de
Database.command; el objetivo completo y B07/D06/D05 siguen abiertos.

## Cambio y revisión

La fachada pública compartía el ejecutor servidor: podía normalizar/compilar
antes de admitir BSON y ejecutar un prefijo de un comando inválido. Ahora
reutiliza el encoder cliente una vez para la solicitud completa; el ejecutor
preparado conserva su entrada interna. Wire y las delegaciones internas usan
esa entrada sin imponerles otra codificación cliente ni codecs del usuario.
No se añade otro codec, contexto ni evaluador. La revisión del replay detectó
que RawBSONDocument se recorría como mapping y perdía undefined: el adaptador
SDK ahora conserva sus bytes. Los errores de comando desconocido comparten
command_not_found entre directa y wire; la exclusión configurada de un comando
conocido conserva su rechazo propio.

Database.command posee opciones propias: el default es UNSPECIFIED y no hereda
los codecs de la base/colección. Las opciones explícitas controlan UUID, fechas
y la fábrica documental del sobre completo; los TypeDecoders no intervienen,
como en el SDK. El default global de colección sigue siendo STANDARD hasta
completar las demás fronteras. La respuesta usa el materializador existente,
con cursor firstBatch/id/ns e id int64; el resultado write conserva n,
nModified y ok en orden nativo. Distinct selecciona valores internos y ya no
aplica prematuramente los codecs de colección dentro del ejecutor servidor.

La mezcla de kwargs conserva la mutación del mapping del llamador antes del
error; sesión, BSON y parser mantienen su precedencia observada. check y
allowable_errors son controles del cliente. Comando desconocido usa 59;
check=False devuelve el error completo materializado. Los nombres inválidos
y el comando vacío tienen controles pareados nativos sync/async: este último
lanza StopIteration en sync y RuntimeError con esa causa en async.

Doce expectativas antiguas de ordered se actualizan a OperationFailure/14,
conservando estado intacto; su origen se acredita en el corte bulk anterior.
El fake administrativo incorpora las entradas internas reales y su test de
distinct aporta un valor exacto por cada fila, sin debilitar la deduplicación.

## Evidencia y límites

- [Receipt](receipt.json), [fuentes](runtime-sources.json) y guards de imports:
  wheel instalado de 285 módulos, SHA `52e3a3538fa886c58c47b760656d226b24568c351346c2d85747b7c9f4d93e17`.
- Siete acciones y 236 observaciones por MongoDB 7.0.43/8.0.32/9.0.2,
  standalone y SDK 4.19.0. Capturas/checks finales coinciden byte a byte y
  contienen el hash del corpus final. Seis controles pareados adicionales
  acreditan las excepciones sync/async antes del dispatch.
- [Base](baseline.txt.gz): 120 fallos/51 correctos; candidato: 171 checks nuevos
  correctos, directa/wire sync/async × Memory/SQLite × 7/8/9.
  [Replay amplio](replay.txt.gz): 9441 correctos y 72
  fallos; el conjunto restante es subconjunto exacto del conjunto previo,
  sin nodos nuevos. No equivale a un gate global verde.
- [Cohorte de unitarias/contratos/propiedades/SPI](unit.txt.gz): 2085 correctas,
  4677 subtests y dos fallos de batching reproducidos en la base. No se
  ocultan ni se atribuyen al nuevo sender. Las [25 pruebas finales](boundary.txt.gz)
  incluyen doce regresiones de forma de comando añadidas después de esa
  cohorte inicial y también forman parte de la cohorte final. El módulo final
  se verifica contra el mismo runtime instalado.
- [Errores/anotaciones](errors.txt.gz): 40 correctas. Once fallos de la base
  eran stand-ins sin contexto de colección; se aporta CodecOptions conservando
  todas las aserciones, sin cambiar el adaptador productivo para los tests.
- Esas pruebas comprueban admisión única, ausencia de deepcopy previo,
  estado/índices/catálogo intactos, barrera de eventos y reapertura SQLite.
  Conservar el error cliente del segundo elemento sin escribir el prefijo.
- [SDK-free](sdk-free.json): 1.362 observaciones de comandos en Python 3.13.15
  con bson ausente. Omite el constructor Code exclusivo del SDK; Decimal128
  usa texto decimal público e Int64 la clase int portable declarada.
  Se conservan además 2.880 observaciones bulk, 3.918 de mutación,
  942 de inserción y 748 controles de lectura sobre el mismo wheel.
- [API](api-check.txt): 18 pruebas/11 subtests, manifiesto sin cambios.
  [Typing](typing.txt): dos positivos y 20/25 negativos; [lint](lint.txt)
  correcto. [Históricos](protected.json): 281
  fixtures/observadores intactos. [Recursos](resources.json): 20 contenedores
  y 40 volúmenes propios verificados ausentes con Docker operativo.
- [Coste](cost.json), ABBA×2/100 solicitudes, CPU diagnóstico con otros gates
  concurrentes: MemoryEngine/ping: 1.64, MemoryEngine/find: 1.31, MemoryEngine/distinct: 1.10, SQLiteEngine/ping: 1.48, SQLiteEngine/find: 1.15, SQLiteEngine/distinct: 1.06.
  Estos incrementos quedan en D05; no se acepta su presupuesto integrado.

## Trabajo que conserva su owner

Wrappers create_collection/create_index/create_indexes aún necesitan su
admisión cliente y separación de ejecutores: raw create/createIndexes verde
no los acepta. Después coordinar UUID global y completar proyecciones/sobres
positivos, batching y los demás efectos de respuesta D06/B08. Siguen los nodos
bson_admission pendientes, H53, decisión D07 y D05 (matrices, cobertura,
persistencia, costes y gates integrados). No se acredita soporte nuevo de
transacciones, cifrado ni Atlas por este corte.

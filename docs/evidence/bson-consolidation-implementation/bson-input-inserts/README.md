# B07: inserciones en la frontera BSON correcta

Implementación `b713e52`, base `29504ca`. Aceptación acotada de insert_one e
insert_many; B07/D06/D05 y el objetivo completo siguen abiertos.

## Cambio y revisión

La entrada pública codifica documento(s) y comentario una sola vez antes de
entrar en la ejecución compartida. El comentario precede a la secuencia de
documentos, como en el mensaje del driver: dos errores de codificación pueden
competir y su orden es observable. No se endurece DocumentCodec.to_internal.
La preparación de forma y asignación de IDs tiene un owner común, usado también
por bulk. La guarda servidor del ID se aplica después de la admisión cliente.

El recorrido del iterable asigna IDs secuencialmente: un error de forma o del
generador posterior conserva las asignaciones anteriores, sin escribir datos.
Los IDs devueltos conservan UUID, Binary 0, Int64 y los microsegundos suministrados;
no se derivan del BSON almacenado. Las copias de documentos y comentarios nunca
preceden al rechazo BSON de un objeto desconocido. Cuatro controles adicionales
comparan esta precedencia con BSON.encode real de PyMongo, sin enviar el objeto
inválido al servidor ni normalizarlo para hacerlo válido.

El ejecutor privado de insert recibe las entradas internas de comandos/bulk
sin imponer otra frontera cliente. Mantiene el contexto, profiling y entrega
existentes. Un paquete inválido no escribe un prefijo; un error servidor sí
conserva sus éxitos ordered/unordered. Las pruebas comprueban estado, contadores,
índice único reutilizable, reapertura SQLite y eventos con una barrera posterior.

Los errores públicos usan el materializador existente con opciones builtin
UUID/fecha y dict, sin fábricas o decoders del usuario. La operación original
adjunta a BulkWriteError se excluye de la decodificación y se añade después,
como hace el driver. El DTO común conserva el orden nInserted/nUpserted/nMatched/
nModified/nRemoved. Se mantienen las familias compatibles de errores del
proyecto: el corpus de errores reutiliza public_error del observador independiente,
sin cambiarlo, y conserva por separado el contenido BSON y su orden completo.

La admisión convierte las entradas DBRef del SDK a documentos ordenados. La
prueba de lookup ya no presupone wrappers después de una inserción pública.
Su nueva variante siembra representaciones antiguas mediante el SPI real de
ambos motores: conserva los residuales, el fallback local y los mismos resultados
nativos, incluyendo collation. No se debilita el oráculo ni se optimiza por tag.
El adaptador async incorpora drop_collection delegando al método real; su
omisión anterior era un defecto del testkit, separado de los fallos productivos.

## Evidencia

- [Receipt](receipt.json), [fuentes runtime](runtime-sources.json) y
  [fuentes del cambio](changed-sources.json): wheel de 285 módulos, verificado
  contra src y cada módulo importado en master/workers. Sin contaminación de src.
- Cinco familias / 169 observaciones por versión en MongoDB
  7.0.43/8.0.32/9.0.2 standalone, imágenes fijadas y buildInfo/FCV en los manifests
  native-7. Las tres nuevas referencias coinciden byte a byte con sus checks.
  Las rondas anteriores son feedback; no se recapturan referencias históricas.
- Baseline: [60 fallos / 63 checks correctos](baseline.txt.gz). Candidato:
  123 checks del corpus nuevo correctos, incluidos directa/wire, sync/async,
  Memory/SQLite y 7/8/9. El [gate amplio](replay.txt.gz) tiene 8.856 correctos y
  96 fallos, todos bson_admission preexistentes; no es un gate global verde.
- [Unitarias, contratos y propiedades](unit.txt.gz): 1.403 correctas y 4.535
  subtests, sin los warnings tratados como error. Incluye contratos de bulk,
  inserts, comandos, errores, SPI y paridad de escrituras por perfil.
- Python 3.13.15 realmente sin SDK: [942 observaciones de inserción](sdk-free-inserts.json)
  y [748 controles de lectura](sdk-free-reads.json). El subset omite constructores
  Code/Int64 exclusivos del SDK; Decimal128 portable se observa por su texto
  decimal público, mientras el perfil SDK conserva BID. El adaptador de familias
  portable exige las excepciones reales WriteError/BulkWriteError de ese perfil.
- [Typing](typing.txt): dos positivos y 20/25 negativos; [API pública](public-api.json)
  idéntica a la base. Lint ratchet y enlaces documentales correctos.
- [272 fixtures/observadores anteriores](protected.json) sin cambios.
  [63 recursos propios](resources.json), 21 contenedores y 42 volúmenes de las
  siete rondas, comprobados ausentes con Docker operativo.
- [Coste](cost.json), ABBA×2/200 solicitudes y replay concurrente: MemoryEngine lote 1: 1.05; MemoryEngine lote 4: 1.02; SQLiteEngine lote 1: 1.01; SQLiteEngine lote 4: 0.98.
  Diagnóstico, sin aceptación de presupuesto D05.

## Pendientes que conserva el corte

La familia bulk aún debe codificar cada grupo antes de sus escrituras, conservando
los grupos previos; reutilizar su agrupador y la preparación de IDs, sin endurecer
anticipadamente todos los modelos. Siguen update/delete/findAndModify, comandos,
índices/validators y las otras entradas B07. Insert soporta las opciones UUID,
pero STANDARD sigue siendo el default hasta completar las demás escrituras.
Proyecciones calculadas, sobres raw/update y resultado público bulk conservan
sus testigos positivos D06/B08. H53, decisión D07, matrices/cobertura/coste e
integración D05 siguen por sus owners. No se declara completa la equivalencia
de todos los efectos de mappings ni el objetivo global.

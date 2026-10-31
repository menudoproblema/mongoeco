# D01b: contenido de efectos y persistencia BSON

Base de runtime archivada: `375382a`; HEAD documental previo: `369c407`.
El [registro](../../../bson-consolidation-implementation.md) conserva el estado
del bloque y el [plan](../../../bson-semantics-consolidation-plan.md) sus gates.
Este incremento no cierra D04, D05, D06 ni B07 general. D01b conserva la
regresión permanente de commands/findAndModify y su revisión final pendientes.

## Implementación y oráculos

- `bson_size` comparte framing físico con `$bsonSize` y el constructor de
  contenido. La propiedad de 300 ejemplos compara bytes codificados por el SDK,
  incluyendo Code scope, Binary 2, Int64, regex, UUID estándar y representaciones
  raw/owned/codec/públicas. Los errores de entradas imposibles del helper neutral
  son TypeError/ValueError; no se inventa para ellos un error de servidor. El
  error público de `$bsonSize` no documental conserva 31393 y captura nativa.
- `change_events` construye contenido compartido; journal/outbox/scopes conservan
  la entrega. Los operadores clásicos registran rutas realmente aplicadas en su
  contexto efímero. No se infiere toda modificación desde igualdad de imágenes:
  unset de slot null, aritmética NaN, push con sort y append+truncate pueden
  escribir una imagen idéntica. Slice sin efecto sigue siendo no-op. Rename
  actualiza el destino en su posición original.
- Pipeline conserva imagen calculada y guardada. La selección update/replace
  y los subdiffs usan tamaño BSON, con límites estrictos y testigos de tamaños;
  no se deduce un umbral universal a partir de dos documentos con padding.
  El documentKey de actualización/reemplazo procede de la preimagen.
- Merge emparejado reutiliza planes clásicos o la política de replacement.
  La selección de storage key conserva la clave persistida si el ID físico no
  cambia; un rekey permitido conserva orden natural y rehace auxiliares. Se
  retira el materializador manual de merge y la duplicación de esa selección.
  Errores de escritura en merge se exponen como fallos de comando, preservando
  la familia especial DuplicateKeyError para 11000.
- El journal incremental V2 protege orden mediante checksum; lee V1 sin migrar
  tokens. Snapshot V1 conserva orden, y decode mantiene widths hasta la
  materialización pública. El corte no reconstruye información perdida antes.

El corpus `bson_size` tiene cuatro acciones y `update_event` nueve, registrados
en el capturador/replay existentes. Las referencias 7.0.43/8.0.32/9.0.2, FCV de
su major, fueron verificadas con `--check`. Eventos usan replica sets propios
49973/49974/49975 con directConnection y pipeline watch vacío para aislar al
productor. La diferencia addToSet (array completo en 7, índices nuevos en 8/9)
se conserva. No hay otro runner de aceptación ni normalización productiva en
el esperado; se observan BSON físico, errores/contadores y scope relacional.
Tokens/cluster times opacos se excluyen de este oráculo de contenido.

Watch wire sigue rechazado explícitamente. Tamaños sí ejercen sync/async y wire;
eventos ejercen sync/async directos. No se cuentan omisiones wire como paridad.
Commit/abort ejercen el contrato transaccional compartido con referencia nativa:
muestreo precommit y barrera posterior acompañan estado y eventos completos.
No se promete concurrencia/aislamiento de cluster por esos controles.

## Gates y límites

- La misma base archivada falla 192 checks y pasa 18 en el corpus final; el
  candidato pasa 210 focales y 5.905 integrados. Las capturas antiguas y observers
  protegidos no se modifican.
- Core/propiedades/entrega: 2.660 pasan, 942 subtests, un skip local. Consumidores
  de escritura: 12.155 y 1.478 subtests. Wire: 2.746 y 92 subtests. Orjson: 253.
- Fault después de registrar el cambio revierte documento, índices y evento;
  reintento publica una vez. Reapertura incremental/compactada conserva evento,
  tipo de ID anterior y widths int64/double. Un archivo SQLite creado por la
  base anterior conserva keys/esquema y supera integrity_check. Un journal
  realmente creado por esa base admite una entrada V2 y reabre con tokens y
  contenido BSON conservados.
- Typing público/boundary, SDK-free 3.13 y lint sin deuda nueva pasan. La sonda
  pequeña de rendimiento es secuencial, con cinco muestras y checksums iguales;
  no sustituye cargas/matrices de D05.
- La sonda adicional de 36 combinaciones command/findAndModify/API por versión
  y par local coincide (108 nativas, 216 locales). Es diagnóstico sync, no
  regresión permanente ni aceptación async de esas entradas. Ese pendiente se
  conserva explícitamente en D01b.

Las cinco fuentes C++ r8.0.0 están fijadas por URL/hash; sus copias locales
coinciden. Explican mecanismos, no sustituyen los diferenciales de cada major.
No se copia el código del servidor ni se implementa un oplog general.

Los logs de feedback distinguen errores de adaptación del test (cursor/session,
shorthand merge no soportado, metadata de corpus aún sin recapturar), permisos
de socket, un gate integrado interrumpido tras detectar fallos focales y los
gates finales. La revisión amplia encontró errores reales: familia duplicate
key y pérdida de orden/widths del journal, corregidos antes de los gates finales.
No se actualiza la allowlist histórica de emisores sin código para aceptar el
helper nuevo. Los servicios propios permanecen disponibles para D04 y el resto
del objetivo; las bases desechables se eliminan y la instancia habitual no se toca.

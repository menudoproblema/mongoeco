# D04: admisión compartida de tokens

Base: `c7fc668`. Esta entrega corrige una dependencia del corte de
protocolo/checkpoint; no cierra ese corte ni D04. El código sigue en
`_change_streams/cursor.py::parse_resume_token`, con el alias anterior y las
entradas sync/async de colección, base y cliente. Reutiliza clasificación BSON
y diagnósticos; no añade otro parser ni modifica el productor del token.

El [receipt](receipt.json) posee comandos, resultados, límites y hashes. El
[manifest de runtime](runtime-sources.json) identifica el candidato; los
[metadatos nativos](native-captures.json) identifican las tres referencias.

## Garantías verificadas

- Documentos y `_data` BSON string: códigos 14 y 40647 por tipo/ausencia,
  40649 por cadena vacía, 9 por cuerpo malformado. Code no es string.
- Validación de Base64/JSON y dominio v1 antes de usar campos: sin fugas
  AttributeError/ValueError, bool como versión/posición ni posición negativa.
- Corpus permanente `semantic_watch_token_cases`: 66 rechazos y seis controles
  de continuidad por major, replay en seis pares motor/dialecto y ambas APIs.
  Las capturas y su segunda ejecución con --check son físicas en replica sets
  propios 7.0.43/8.0.32/9.0.2, con FCV correspondiente; cleanup por base UUID.
- Propiedades de roundtrip y cadenas arbitrarias; testigos deterministas de
  payload inválido sin registro de watcher, mutación de estado ni del input.
- [Productor anterior](create-legacy.py.txt) y [lector actual](read-legacy.py.txt):
  SQLite/journal reales, evento escrito tras cerrar el stream, token v1 con
  bytes idénticos y decimal heredado, reanudación después de reabrir. No se
  acredita downgrade ni portabilidad de tokens nativos al backend local.
- [Smoke sin SDK](no-sdk.py.txt) en Python 3.13; typing y lint ratchet sin deuda
  nueva; core/propiedades, orjson y replay integrado con fuentes congeladas.

## Revisión y límites

La primera versión del control positivo escribía sin watcher ni journal y
tropezaba con un hueco deliberado del subset local. Los logs de feedback se
conservan, pero no son aceptación. El testigo definitivo mantiene disponible
el historial durante la escritura, tanto nativa como localmente. La prueba
de reapertura usa journal y acredita la garantía persistente por separado.

El formato opaco del servidor no es el JSON/base64 local. Los controles usan
tokens obtenidos de su propio backend; solo comparan continuidad, errores,
clase y fase, nunca los bytes entre backends. Los bytes locales anteriores
sí se comparan con el productor local actual para detectar una migración.

La revisión comprueba todos los callers del parser, el alias, clasificación
String/Code, positivos/negativos, metadatos de referencia, observadores
protegidos y compatibilidad persistida. No queda un finding nuevo dentro de
esta dependencia. Permanecen con D04 el bucle común, protocolos, cancelación,
invalidate filtrado, checkpoint/posición atómicos, opciones y sus precedencias,
retención/gaps, imágenes/DDL y fixture RS del job. Wire watch positivo no se
introduce aquí; la matriz amplia, wheel y cierre integrado siguen en D05.

El [benchmark](benchmark.py.txt) mide abrir/cerrar streams con token válido;
ABBA secuencial, diez muestras por runtime/motor, 3.000 aperturas por muestra
y checksums iguales. No mide throughput de eventos ni el resto de D05.

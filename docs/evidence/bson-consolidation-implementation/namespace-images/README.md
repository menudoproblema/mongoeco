# Integración de imágenes, namespaces y respuestas BSON

Base de runtime `a981610`; refinamiento contractual `9eea45c`. Código:
`eb0074e` y `7dd54ab`. El [receipt](receipt.json) conserva comandos y resultados.
Se acepta este corte funcional; D04 sigue abierto para replica set de CI.

- [Capturas](native-captures.json) y [topología](native-topology.json): 11 familias
  por major; capturas/--check con MongoDB 7.0.43, 8.0.32 y 9.0.2 y FCV propio.
  Los nueve fixtures nuevos están en tests/fixtures; sus hashes y fuentes constan
  aquí. No se modifican goldens históricos ni observadores protegidos.
- [Runtime](runtime-sources.json), [workset](workset.json), [baseline real](baseline-source-verification.json)
  y [wheel](wheel-source-verification.json): identidad de cada candidato.
- [Replay completo](acceptance-integrated.txt.gz): 6.616 pruebas. El gate
  [core/APIs/contratos](acceptance-core.txt.gz) conserva sus skips explícitos.
  [Focal](acceptance-focus.txt.gz), [wheel](acceptance-wheel.txt.gz) y
  [orjson](acceptance-orjson.txt.gz): 237 por entorno. [Baseline](before-regression.txt.gz):
  160 fallos/77 controles. [Sort anterior](before-binary-sort.txt.gz): ocho/cuatro.
- [Censo de salidas](response-consumers.json) y [efectos](impact.json): separar
  valores de respuesta, IDs asignados y documentos originales de errores bulk.
  Las sondas de [errores](error-values-before.json) y [comandos](command-values-before.json)
  conservan los fallos previos; las regresiones permanentes están en los corpus.
- [Persistencia anterior](acceptance-legacy-reader.txt.gz) y
  [perfil sin SDK](acceptance-sdk-free.txt.gz): scripts reproducibles *.py.txt.
- [ABBA](benchmark-paired-summary.json): 21 casos, diez muestras por candidato/caso,
  checksums iguales, ratios de medianas 0,963–1,036 respecto de a981610. Ejecuciones
  antes/después secuenciales, una vez terminales los demás gates propios.
- Los archivos feedback/superseded y el baseline de tests API son diagnóstico,
  nunca aceptación. El primer benchmark falló por comparar /tmp y /private/tmp
  como strings; la secuencia ABBA completa se repitió con rutas resueltas.

La aceptación de watch es sync/async local; las otras familias incluyen wire.
No se acreditan watch wire positivo, distribución/concurrencia nativa ni matrices
completas Python/SDK/pytest. D04 CI, D06, H53, conversión, D07 opcional, D05 y B07
conservan sus obligaciones. No se cambia formato persistido ni se acredita downgrade.
El fixture RS pendiente tiene un [censo estático](remaining-ci-inventory.json),
que no acredita su implementación ni ejecución en CI.

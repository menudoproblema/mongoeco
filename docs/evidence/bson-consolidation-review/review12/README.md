# Revisión 12: contraste del mismo informe sobre el candidato actual

Base `a981610`, con producción/tests ya modificados al iniciar la revisión.
El [receipt](receipt.json) identifica ese candidato y el input sin copiar el
adjunto privado. Esta revisión solo cambia documentación/evidencia; no acepta
el candidato ni corrige el defecto observado.

- [Focal](focal.txt): 172 pruebas; [replay](replay.txt): 486 pruebas con capturas
  existentes, incluidas las del candidato. No es recaptura ni aceptación global.
- [Representaciones](representations.json): 18 trazas en seis pares locales.
- [Fronteras públicas](public-boundaries.json): sonda nueva contra MongoDB
  7.0.43/8.0.32/9.0.2 y seis pares locales. options conserva Binary 0 local donde
  los servidores devuelven bytes. Lectura, ID asignado y respuesta parcial bulk
  funcionan como controles; no deducir un defecto del caller textual de bulk.
- [Precedencia](preparation-order.json): ocho escenarios por servidor/por par
  local, coincidencia de fase/clase/código/nombre y destino sin documentos.
  Caracteriza únicamente los errores competidores registrados.
- [Fuentes](sources-check.json): 16 hashes oficiales r8.0.0 coincidentes.
- [Lint](candidate-lint.txt): pasa para 26 Python cambiados; reemplaza la
  observación pendiente de revisión 11, sin reescribirla.

Los scripts `*.py.txt` documentan reproducción. Las sondas usan bases UUID y
limpieza en finally; no modifican servicios ni documentos previos. No son un
segundo runner permanente. Las nuevas sondas físicas son sync; el replay sí
incluye otras superficies, con el alcance de cada corpus. La sonda de options
no es regresión permanente ni acredita codecs, async o wire. Esas garantías
quedan pendientes en la revisión 12 del plan.

No se repitieron TTL/H53, suite global, wheel, matrices SDK/Python, rendimiento
ni CI. No se cierra D04/D05/B07, no se reabren cortes aceptados ni se modifican
receipts/goldens históricos. Los hashes del runtime y workset permiten reutilizar
esta evidencia solo cuando sus inputs permanezcan iguales.

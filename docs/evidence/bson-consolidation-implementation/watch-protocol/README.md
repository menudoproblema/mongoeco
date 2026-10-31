# D04: bucle común, protocolos y cierre

Base: `1b8705a`. La entrega concentra next/try_next y la limpieza de lectura,
añade protocolos sync/async y conserva el contexto/materialización existentes.
No cierra D04 ni la reanudación/checkpoint. El [receipt](receipt.json) posee
comandos, gates, hashes y límites; [runtime-sources.json](runtime-sources.json)
identifica los tres módulos productivos modificados.

## Oráculos y regresiones

- Corpus permanente `semantic_watch_protocol_cases`, registrado en el
  capturador/replay existente: tres casos y once escenarios por major.
  [Capturas](native-captures.json) y segunda ejecución física con --check en
  replica sets propios 7.0.43/8.0.32/9.0.2, FCV correspondiente. Iteración y
  contexto conservan identidad; cerrado termina; fallos cierran; invalidate
  visible y filtrado termina los streams de colección/base.
- [Escenario async nativo](native-async.json), ejecutado por
  [native-async.py.txt](native-async.py.txt): usa sin cambios `async_lifecycle`
  del contrato permanente, nueve pares major/ámbito. La API local ejecuta ese
  mismo escenario en seis pares motor/dialecto, sin el puente sync.
- [Try vacío por defecto](native-empty.json) mediante
  [native-empty.py.txt](native-empty.py.txt): las tres versiones retornan None
  alrededor de un segundo y conservan alive. El test local acota la espera;
  no exige igualdad de tiempos de pared ni tiempo real estricto.
- La [sonda amplia](protocol-diagnostic.json) y su
  [fuente](protocol-diagnostic.py.txt) conservan otros diagnósticos D04 pendientes.
  No acepta todas las opciones/tokens por haber verificado protocolos. El cierre
  concurrente con getMore puede producir una carrera nativa CursorKilled/fin de
  iteración; no se usa su código como golden determinista.
- El puente `_AsyncChangeStreamView` comprueba __aiter__/__aenter__ reales y
  traduce solamente StopAsyncIteration al protocolo del testigo sync. No aporta
  métodos faltantes al producto ni modifica observadores protegidos.

## Recursos y compatibilidad

Los tests locales deterministas cubren cancelación en espera/lookup, cierre
durante lookup, callbacks fallidos y ausencia de watchers/waiters residuales.
No se entrega un evento cuando el cierre sucede mientras se relee su documento.
La cancelación se propaga; el fallo original tampoco se sustituye por un error
de cursor cerrado. Las tres expectativas antiguas sobre cerrado se actualizan
solo con las referencias nuevas, sin cambiar goldens históricos.

Close async conserva su efecto inmediato. Un Awaitable completado e independiente
del loop permite await/re-await sin crear una coroutine olvidada para callers
previos. No añade otro estado de cierre. La anotación Self y la reexportación
estática del TypedDict ya público permiten comprobar los protocolos con typing.

El [productor anterior](create-legacy.py.txt) usa realmente 1b8705a para crear
SQLite y journal con drop/invalidate/recreate. El [lector](read-legacy.py.txt)
comprueba token ordinario idéntico, ambas formas de invalidación y datos de la
colección recreada. No hay cambio de formato/journal/esquema/SPI ni downgrade.

El [smoke sin SDK](no-sdk.py.txt) usa Python 3.13 realmente sin bson y ambos
motores/dialectos. El [benchmark](benchmark.py.txt), coordinado
[secuencialmente](paired-benchmark.py.txt), usa ABBA, diez muestras por
runtime/caso, default/updateLookup y checksums iguales. No es el coste global
de D05 ni una prueba de majority/distribución o watch wire positivo.

Pendientes: resume_token/checkpoint, bootstrap y avance atómicos, retención/gaps,
opciones y precedencias, interacciones adicionales con imágenes y fixture RS de
CI. El éxito del bucle común no acepta esas garantías ni todo el programa.

# D04: checkpoint y continuidad local

Base: `ad2f030`. El [receipt](receipt.json) identifica el candidato, comandos,
gates y límites; el [manifest](runtime-sources.json) fija su runtime. No se cierra
D04: faltan primera tanda/backlog, opciones restantes, imágenes y fixture RS.

## Referencia y estado público

El corpus permanente `semantic_watch_checkpoint_cases` reutiliza registro,
capturador y replay: cuatro casos, veinte escenarios por major. Las
[capturas](native-captures.json) y --check son físicas en los tres replica sets
propios 7.0.43/8.0.32/9.0.2, FCV correspondiente, con bases UUID desechables.
El replay cubre colección/base/cliente por sync/async y ambos motores/dialectos.
El puente async solo lee la propiedad real, sin fabricar un checkpoint.

Se observan propiedad owned, continuidad al reanudar, filtro sin documento,
invalidation visible/filtrada, rechazo 260/InvalidResumeToken, startAfter y
selección de argumentos PyMongo. No se comparan bytes opacos entre backends.
El primer intento del testigo vacío exigía superar también la última barrera
recién escrita y falló en los tres nativos. Un checkpoint vacío puede ir detrás
de esa escritura: el testigo definitivo usa una barrera posterior para demostrar
que el cambio anterior fue superado, sin exigir igualdad con el último token.

La [fuente fijada de códigos](error-code-source.json) acredita nombres; asignar
286 a una pérdida de historial local y 9 a coordenadas de journal malformadas
no se presenta como una captura de pérdida de oplog en MongoDB.

## Mecanismos y compatibilidad

- El hub resuelve offset, coordenada y registro de watcher bajo el mismo guard.
  El cursor publica una copia de su checkpoint y conserva el anterior ante error.
- Un timeout no avanza sobre una escritura intercalada. Los lectores activos no
  cruzan huecos, aunque hubiese un evento anterior todavía entregable.
- El encoder/decoder v1 sigue siendo único. `i=true` marca invalidate, `i=false`
  un checkpoint explícito; ausente conserva evento/legado. Los bytes de eventos
  ordinarios siguen iguales. Esta extensión opcional no exige schema/journal
  nuevo; no se acredita downgrade semántico.
- Un evento perdido no se inventa a partir de su coordenada. Un checkpoint
  explícito justo antes del primer evento retenido puede continuar si toda su
  continuación está disponible. Fuera de esa frontera falla.
- Los huecos se reconstruyen desde coordenadas ya persistidas; su cola sin
  evento también persiste next_token. Una reproducción de reinicio detectó
  que esa cola antes desaparecía y obligó a corregir/repetir gates.
- Se retira metadata de huecos fuera de retención y se usa búsqueda de
  intervalos ordenados para el camino activo. No hay otra secuencia/outbox.

El [productor anterior](create-legacy.py.txt) usa realmente ad2f030. El
[lector](read-legacy.py.txt) comprueba bytes ordinarios, invalidate antiguo,
SQLite conservado, hueco de journal antiguo y nuevo checkpoint tras reapertura.
El [smoke sin SDK](no-sdk.py.txt) comprueba seis pares por ambas APIs en Python
3.13 realmente sin bson. No se modifica observador, golden histórico ni baseline
de errores para aceptar el candidato.

## Revisión y costes

Las pruebas deterministas cubren bootstrap con escritor bloqueado por el guard,
timeout/escritura, error sin avance, hueco antes de evento/EOF, frontera retenida,
checkpoint posterior a invalidate de otro ámbito, tiempo futuro y tres formas
de omisión persistente. La intersección de intervalos se contrasta con un cálculo
entero independiente y se verifica la retención acotada de metadata.

Los logs de feedback distinguen fallos del candidato de expectativas anteriores:
la frontera confundía un evento perdido con checkpoint; el gate rechazó cinco
emisores sin código; un test de retención usaba un token futuro inexistente.
Los replays detenidos por esos findings se conservan y no son aceptación.
Los consumidores API antiguos se actualizan con las referencias ya aceptadas
de imágenes y namespaces: lookup después de delete es null; drop precede a
invalidate. No se cambia producción para conservar aquellas expectativas.

El [benchmark](benchmark.py.txt), [ABBA secuencial](paired-benchmark.py.txt), mide
default/updateLookup y escrituras sin watchers, diez muestras por runtime/caso,
400 updates por muestra, checksums y lectura final iguales. No acredita costes
globales D05 ni mayoría/distribución o watch wire positivo.

Pendientes explícitos: evaluación de backlog al abrir y fase de sus errores,
full_document/maxAwaitTime y precedencias restantes, imágenes/DDL y CI replica
set. El reloj entero local conserva su dominio; no se convierte en Timestamp.

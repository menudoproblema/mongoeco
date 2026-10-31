# D06: diagnóstico nativo de bytes y callbacks

El [receipt](receipt.json) identifica las sondas y sus límites. Este diagnóstico
fija el siguiente trabajo; no acepta una corrección ni sustituye el corpus
permanente. Los tres scripts matrix arrancan fixtures propias con los digests
fijados por el workflow y registran salida/cleanup.

- El tamaño de la tanda no es la suma simple de tamaños de documentos.
  Para documentos de 4.194.288 bytes, find raw devuelve primero cuatro en
  MongoDB 7 y tres en 8/9; aggregate raw devuelve tres en los tres servidores.
  Local devuelve los cinco en una sola tanda.
- Con error en el documento 5, los servidores pueden entregar tres documentos
  antes del error de getMore; local falla sin entregarlos.
- Dos documentos `v=[1]` y `v=[0]`, seguidos de unwind/divide, demuestran otra
  pérdida de frontera: aggregate local invoca el decoder de `_id=1` aunque
  falle la apertura. Ningún servidor lo invoca. El control batchSize=1 entrega
  el primer documento y ejecuta un callback antes de fallar en getMore.
- El caso de un solo documento `v=[1,0]` coincide en ambos: conservarlo como
  control evita atribuir el fallo a toda expansión.

`native.tar.gz` conserva los tres cohorts 7/8/9 y sus fixtures; los scripts
`local` cambian solo la construcción del cliente y opciones de registro de
codecs. El framing y observación son comunes y no usan helpers productivos.
La [auditoría de recursos](../raw-response/native-cleanup.json) comprueba ausencia
por nombres propios. Las fuentes r8.0.0 consultadas constan en el receipt;
explican mecanismos, sin sustituir los resultados físicos por versión.

Siguiente aceptación: incorporar estos testigos al registro/capturador/replay
existentes antes de cambiar producción. Medir fronteras exactas por ruta y
dialecto, conservar el documento pendiente, evaluar la respuesta antes de
aplicar codecs y distinguir error del servidor de error del decoder cliente.
No inferir rollback de efectos externos de callbacks anteriores.

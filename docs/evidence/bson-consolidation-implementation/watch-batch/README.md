# D04: respuestas iniciales, batches y fases de opciones

Base de runtime: `f0a14fb`; revisión del contrato: `9b40c97`. El
[receipt](receipt.json) fija el candidato, comandos, resultados y límites.
El [manifest de runtime](runtime-sources.json) y el [workset](workset.json)
identifican los inputs. Este corte no cierra todo D04 ni D05.

## Comportamiento y owners

El cursor existente inicializa una vez la respuesta de apertura. Las entradas
sync esperan esa inicialización; las async la ejecutan al esperar watch o leer.
La respuesta evalúa el backlog antes de devolver el cursor y materializa solo
las filas admitidas. Un error posterior dentro de esa respuesta descarta sus
filas; un documento evaluado para decidir overflow queda preparado para la
siguiente tanda, sin decodificación pública anticipada.

La lectura sigue compartiendo el bucle next/try_next. Los batches vacíos
actualizan el checkpoint; los no vacíos no lo adelantan sobre filas pendientes.
El bootstrap captura también límite inicial y anchor bajo el guard del hub.
El anchor retenido pasa por el mismo pipeline al reanudar: un filtro puede
excluirlo y un error puede fallar antes de producir un cambio posterior.
Checkpoints explícitos e invalidates conservan sus reglas propias.

La validación común distingue entrada, aggregate inicial y getMore. None en
full_document usa default. Opciones negativas/bool pueden dejar consumir una
respuesta ya admitida y fallar al pedir getMore; cero no se convierte en el
timeout predeterminado. No hay validadores divergentes en colección/base/cliente.
Los errores de división/módulo por cero usan el emisor común del evaluador
aritmético, conservando código y añadiendo el codeName canónico.

La preparación, contexto, imágenes, identidad del token, codec y publicación
conservan sus owners anteriores. No se introduce intérprete, secuencia, journal,
esquema SQL, SPI ni formato de tokens nuevos.

## Referencia diferencial y errores encontrados en la revisión

Siete familias permanentes reúnen 91 escenarios por major; prefetch es un
escenario con dos campos observados, no dos escenarios. Las
[capturas](native-captures.json) usan los tres replica sets propios
7.0.43/8.0.32/9.0.2, FCV correspondiente, UUIDs y cleanup. Registro, capturador
y replay son los existentes; watch positivo sigue limitado a sync/async.

La frontera inicial no es solo suma de documentos. El monitor público del
driver mide prefijos 53/52 bytes para firstBatch/nextBatch; cada fila añade
tipo y clave del array. La revisión exacta r8.0.32 reserva además el siguiente
PBRT en getMore, diferencia ausente en la fuente anterior r8.0.0. Las
[fuentes versionadas](batch-size-source.json) explican el mecanismo; las
capturas 7/8/9 son el oráculo. No se copia implementación externa.

El testigo fija tamaños con el token real de cada backend y los verifica con
el encoder BSON del SDK. Usa payload ASCII almacenado y substrCP para evitar
que una prueba de bytes construya millones de límites UTF-8 o retenga un literal
de 8 MiB en la caché de compilación. No modifica el operador de strings.

La revisión corrigió además:

- el límite de 101 filas pertenece a la apertura, no a cada getMore; aplicarlo
  siempre entregaba un prefijo que MongoDB descarta ante un error en la fila 102;
- un evento filtrado tratado como candidato de tamaño cero podía cortar una
  respuesta en la frontera y ocultar un error posterior; la referencia falla
  al abrir, mientras el candidato anterior devolvía dos documentos;
- un gap al final del historial descartaba el prefijo válido; ahora se entrega
  sin cruzar el gap y la siguiente lectura rechaza el hueco sin avanzar token;
- close antes de inicializar retenía el anchor; cancelación/cierre durante
  lookup libera watcher y filas preparadas sin decodificar ni entregar;
- el helper suplementario async resolvía una colección llamada client al usar
  collection.database.client; recibe el cliente real explícitamente. Sus dos
  escenarios se repiten en nativos y local, en los tres ámbitos. No se altera
  la API para satisfacer el helper ni se reescriben receipts anteriores.

El invalidate filtrado en primera tanda vacía conserva el caso PyMongo observado
260/alive=True; no se le aplica la regla de cierre del invalidate leído después.
Los logs de feedback, gates interrumpidos o invalidados por cambios del corpus
son diagnóstico, nunca aceptación final.

## Compatibilidad y límites

El archivo anterior se produce con un archive Git verificado de f0a14fb,
no con el candidato. Se conservan SQLite, índice, journal y tokens ordinarios;
la imagen prefetched sigue en v2 aunque se escriba v3 antes de leerla, y el nuevo
checkpoint continúa tras otra reapertura. El smoke sin SDK cubre seis pares
motor/dialecto, tres ámbitos y ambas APIs en Python 3.13 realmente sin bson.

El benchmark ABBA mide default/updateLookup/unwatched y apertura más consumo de
101 updates en backlog, diez muestras por runtime/caso, con checksums y lectura
final iguales. Sus resultados se registran después de terminar los otros gates.
Lectura continua tiene ratios de medianas 1,03–1,14; sin watchers 0,99–1,01.
Backlog default pasa de unos 1,1 ms a 2,8 ms por 101 eventos (ratio 2,49–2,54),
y updateLookup tiene ratios 1,05–1,25. Es coste adicional medido de este corte,
no una afirmación de rendimiento equivalente. No se sustituye la contabilidad
exacta por un fast path sin garantía. D05 conserva el análisis global; esta
muestra no acredita throughput distribuido ni su matriz completa.

Restan imágenes con DDL/recreación y fixture replica set de CI; D06, H53,
conversión escalar y D05 conservan sus obligaciones. No se acepta watch wire
positivo, sesiones, transacciones o semántica distribuida mediante este corte.

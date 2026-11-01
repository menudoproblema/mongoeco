# B07 — Solicitudes de namespace y ejecución DDL preparada

Base `32f8d7f5`; código `b938556d`. Se aceptan los frames de Collection.drop/rename, sus reglas
cliente y el enlace con la ejecución DDL compartida. No cierra B07, UUID global,
respuestas positivas completas, batching ni D05.

## Causa y cambio

Las dos fachadas llegaban al motor sin codificar la solicitud. Rename también
mezclaba nombre cliente y namespace servidor: rechazaba el vacío con otra familia
de error, comprobaba la sesión antes que el nombre y entregaba al motor un Code
que el SDK convierte a String al construir el namespace. El wrapper sync volvía
a construir el resultado con el argumento original en vez del nombre confirmado.

El preparador de namespace usa el encoder existente con opciones UNSPECIFIED.
No materializa con codecs de la colección, como el SDK. La validación lexical de
rename produce InvalidName compatible con el SDK y disponible sin dependencia SDK.
Se conserva el retorno Collection de la API local, con nombre String canónico,
opciones, dialecto, reloj y hub existentes. No se cambia a la respuesta dict de
PyMongo por este corte.

Drop conserva el tipo original hasta codificar: Code con scope inválido falla en
BSON antes del parser. Rename construye namespaces String antes de codificar,
como el SDK; scope Decimal/UUID en Code se descarta en ese contexto concreto.
No se introduce una conversión global de Code a String ni una regla universal
para todos los nombres.

El normalizador de drop recibe obligatoriamente el dialecto. Los controles
nativos muestran códigos 2/14/73 para argumentos no String en 7/8/9; vacío/NUL
son 73. Son reglas diferentes de validate. Las fachadas y comandos reutilizan
el mismo ejecutor DDL y su scope de entrega/rollback; el comando ya decodificado
no reingresa en una fachada que codifique otra vez. No cambia SPI, almacenamiento,
engine de mutación ni protocolo de journal.

## Evidencia y límites

- Cuatro acciones, 55 observaciones por MongoDB 7.0.43/8.0.32/9.0.2 standalone,
  PyMongo 4.19.0. La captura final native-frozen y su --check coinciden.
  Incluye coerción contextual Code, scope UUID en cinco representaciones,
  nombres/namespace, precedencias de sesión, error cliente/servidor y estado.
- Base: 72 fallos/27 correctos; candidato: 99 checks nuevos correctos en
  Memory/SQLite × sync/async/wire × tres dialectos. El adaptador async delega
  get_collection real; no sustituye los resultados del testigo ni el observador.
- 16 controles de frontera: rechazo antes de DDL, índice único y estado intactos,
  reapertura SQLite, barrera de eventos, codificación pública una vez/cero en
  ejecución preparada, y Collection devuelta con opciones y nombre confirmado.
- La cohorte añade rollback/journal/checkpoint DDL. Se encontraron seis fallos
  previos al corte porque un test esperaba que el comentario de create quedara
  almacenado. La base los reproduce. Se usa ahora un validator persistido como
  testigo de rollback y se conservan datos, índice, estado y publicación/reintento.
- SDK-free real Python 3.13.15: 240 observaciones sobre todos los argumentos
  portables de las cuatro acciones. Omite los constructores Code y sus scopes;
  no normaliza el observador. Se repiten ocho familias BSON anteriores.
- El manifiesto de firmas públicas permanece idéntico. Se añade expresamente
  mongoeco.errors.InvalidName con puente SDK. Typing, lint y pruebas de import
  instalado están ligados a la wheel y sus 286 módulos en el receipt.
- Los 296 fixtures/observadores previos permanecen intactos; nueve contenedores
  y 18 volúmenes propios se confirman ausentes con Docker operativo.
- Las capturas nativas son standalone: no constituyen nueva evidencia física de
  change streams. Se conservan los oráculos nativos watch anteriores; el enlace
  de las fachadas al ejecutor compartido se prueba localmente con barrera,
  rollback, journal/reapertura y checkpoint. No se amplía semántica distribuida.
- La comparación observa solicitud/estado y descarta el retorno de rename solo
  para conservar el contrato local Collection. No acepta por asociación todos los
  campos de respuesta del comando drop: siguen en la unidad de positivos/replies.
  Tampoco añade kwargs públicos de comment/dropTarget ni valida constructores
  generales fuera de las entradas censadas.
- Las rondas iniciales son diagnóstico. Solo la última wheel, capturas congeladas
  y gates finales acreditan el corte. El coste ABBA×2 bajo carga concurrente es
  diagnóstico, sin aceptar el presupuesto integrado D05.

Siguen cinco entradas Search del censo antes del default UUID coordinado, más
positivos/respuestas/batching, contextos D06 restantes, H53, decisión D07 y D05.
Los 72 nodos rojos previos del replay y los dos tests antiguos de batching siguen
visibles y obligatorios; no se sustituyen por xfail ni se declara cierre global.

Resultado final instalado: 10008 correctos/72 fallos previos; cohorte 2250
correctos/4686 subtests y dos fallos antiguos de batching. API 18 pruebas/13
subtests, typing dos positivos y 20/25 negativos, lint sin nuevos diagnósticos.
Los nodos fallidos son exactamente los del corte anterior.

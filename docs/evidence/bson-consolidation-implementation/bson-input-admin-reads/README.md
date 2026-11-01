# B07 — Lecturas administrativas y reglas de flags BSON

Base `bd09bcc`; código `d9c9c80`. Alcance: list_collection_names/list_collections/validate_collection,
sus delegaciones, lectores fuente de collStats/dbStats/dbHash y el error espejo de
Decimal128 cero en flags numéricos. No cierra B07, UUID global, las respuestas
positivas completas, sus tandas ni D05.

La raíz era decidir sobre valores originales en una capa semántica antes de
la admisión cliente, y reingresar en APIs públicas desde comandos internos.
Las lecturas auxiliares heredaban codecs que el driver no usa para estos
comandos. La refactorización comparte ejecución preparada y lectura sobre el
snapshot SPI existente; no introduce otro contexto, planificador, codec interno,
formato de almacenamiento o dependencia obligatoria PyMongo.

## Decisiones verificadas

- Los dos listados codifican una vez con opciones propias UNSPECIFIED; primera
  respuesta dict sin fábricas/TypeDecoders heredados. names exige Mapping en el
  cliente; collections conserva la precedencia BSON→error servidor 14 de filtro.
  nameOnly se deriva como en el SDK. El comentario se conserva en todas las
  fachadas hasta los metadatos de sesión, mediante specs/catálogo actuales.
- validate reutiliza Database.command para admisión y respuesta. El nombre
  cliente se resuelve sin perder el original antes del frame; namespace vacío
  y ausente producen OperationFailure 73/26. No se devuelve CollectionInvalid
  para namespace inexistente. Una respuesta moderna valid:false sí produce
  CollectionInvalid, comparada con el método real PyMongo bajo respuesta controlada.
- El parser común posee namespace/filtros/flags; se retiran sus rechazos wire
  duplicados. Los listados usan el mismo ejecutor preparado desde comando y
  fachada; validate no vuelve a codificar al leer documentos o índices.
- validate usa verdad BSON legacy: cero/null/false falsos, estructuras y String
  incluso vacías verdaderas. full y background verdaderos son incompatibles
  (72). Los flags tipados mantienen su política distinta. Ambos consumidores
  toman el valor de wrap_bson_numeric: reconocer Decimal128 sin extraerlo hacía
  que cero contara como verdadero. Referencias upsert/ordered/new comprueban
  valor, respuesta y efectos, no solo que ambos evaluadores coincidan.
- Validación, stats y hash comparten el lector de documentos sobre ReadSnapshot;
  sus índices usan el lector preparado existente. Una fábrica del usuario no
  interviene en selección, conteo, tamaño o hash. Esto no acredita los algoritmos
  completos de stats/hash contra el servidor por asociación.
- Se actualiza la proyección CSV del catálogo. El test exige SHA/revisión exactos
  del owner real, sin mantener otro hash literal en código. Cambiar el owner sin
  actualizar la proyección sigue haciendo fallar el gate. No cambia el formato CSV.

## Evidencia y límites

- Seis acciones: 269 observaciones administrativas y 18 de flags por MongoDB
  7.0.43/8.0.32/9.0.2 standalone, PyMongo 4.19.0. La captura y su segunda ejecución
  --check coinciden. Solo native-consumers y la última wheel acreditan el corte;
  las rondas anteriores y los intentos intermedios son feedback, no aceptación.
- Base: 120 fallos/30 aciertos; candidato: 150 checks nuevos correctos en
  Memory/SQLite × sync/async/wire × tres dialectos. El adaptador async delega los
  métodos reales que le faltaban; no inventa resultados ni altera el observador.
- 27 controles de frontera: rechazo antes de lecturas, estado y reapertura,
  barrera de eventos, una codificación pública/cero preparada, conservación de
  comentarios por fachada/sesión y respuesta inválida del wrapper frente al SDK.
- La respuesta inválida usa un comando sustituido en una instancia fijada del
  driver sin conexión. El primer intento de ese test tomó otra instancia de
  Database y terminó en timeout; no constituye un fallo del producto ni una
  reproducción de corrupción física del servidor.
- SDK-free real Python 3.13.15: 1566 + 108 observaciones nuevas; siete familias
  previas repetidas. Omite el constructor Code del SDK; usa las vistas portables
  declaradas de Decimal128/Int64. No cambia observadores ni fixtures históricos.
- API pública idéntica; se declaran dos opciones comment de listado en el catálogo
  y se precisa la conversión legacy de validate. Typing, lint y gates instalados
  están ligados al material exacto en receipt/runtime-sources/import proofs.
- El replay amplio conserva exactamente los 72 nodos rojos anteriores; la cohorte
  conserva dos tests de batching rojos. No hay xfail ni gate global verde.
- 290 históricos protegidos intactos; 15 contenedores y 30 volúmenes propios
  confirmados ausentes con Docker operativo. El feedback sin permisos TCP no se
  usa para aceptar el candidato. El diagnóstico ABBA×2 bajo carga concurrente
  no acepta el presupuesto integrado D05.

**Background y checkpoints:** la primera sonda de background solo encontró 115
por ausencia de checkpoint en 8/9. El contrato local sigue accepted-noop y
síncrono; no se implementan ni simulan checkpoints. Los testigos finales de ese
campo fijan full=True y aíslan la conversión/incompatibilidad 72 antes de esa
condición del servidor. No afirman paridad del trabajo físico en background.

Los nombres de los códigos 26/73 se contrastan con el
[catálogo oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/base/error_codes.yml).
Las referencias físicas observan clase/código/fase; esta lectura fija los nombres,
sin atribuirse una nueva captura nativa de todos los mensajes.

Siguen siete entradas del censo sin disposición (drop, rename y cinco Search),
UUID coordinado, positivos/respuestas/batching, otros contextos D06, H53, decisión
D07 y cierre integrado D05. Los listados solo acreditan los contextos capturados;
no se infiere semántica completa de getMore ni metadata por este corte.

Resultado instalado final: 9909 correctos/72 fallos previos; cohorte 2192
correctos/4686 subtests y dos fallos previos de batching. API 18 pruebas/13
subtests, typing dos positivos y 20/25 negativos, lint sin infracciones nuevas
en 20 archivos Python. Es aceptación acotada con gates globales pendientes.

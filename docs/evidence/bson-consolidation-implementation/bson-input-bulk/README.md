# B07: preparación y admisión de frames bulk clásicos

Implementación `5a89dcc`, base `954ba15`. Aceptación acotada del recorrido
público bulk y de sus comandos wire alcanzados. B07/D06/D05 y el objetivo
completo permanecen abiertos.

## Causa, cambio y revisión

La preparación anterior normalizaba y copiaba modelos antes de admitir BSON,
y podía ejecutar un elemento antes de descubrir un error cliente del siguiente.
La preparación común ahora recorre los modelos secuencialmente, adapta sin
copiar valores arbitrarios, valida forma y asigna IDs. Un error cliente de
preparación conserva las asignaciones previas y no escribe datos.

El agrupador existente conserva runs contiguos ordered y familias agrupadas
unordered, con límite de 100.000 elementos. El encoder común conserva orden,
tipos y opciones UUID; comparte su primitiva con las lecturas y mutaciones.
La nueva secuencia cuenta BSON real y forma frames de 48.000.000 bytes con el
presupuesto de 36 bytes de la sesión UUID nativa del perfil clásico admitido.
El SDK aporta los tamaños de bytes codificados; el perfil portable reutiliza
bson_size sobre su dominio cerrado. No se añade otro codec ni comparador.

Se codifica un lookahead antes de decidir el desbordamiento: si no es codificable,
no se envía el prefijo aún pendiente; si es válido y demuestra desbordamiento,
el prefijo puede ejecutarse. El elemento pendiente se conserva entre frames.
Un error posterior no revierte frames ni familias ya confirmados. El encoder
solo vuelve a codificar la cabecera al comenzar otro frame. La validación
servidor ocurre después, reutilizando las reglas estructurales de comandos
movidas a su owner compartido; los ejecutores internos no recodifican.

El caso ordered=None distingue dos decisiones: el driver agrupa como unordered,
pero el servidor usa el default true dentro de cada comando null. Un fallo de
item detiene ese frame sin imponer una parada global del driver. Se conserva
el normalizador de ordered del comando, con errores servidor de código 14;
no se sustituye por bool() ni por validación cliente TypeError. La clasificación
de update usa el primer campo: el diccionario vacío es reemplazo. Estos dos
fallos compartidos aparecieron al ejecutar wire y quedaron corregidos sin
alterar los observadores ni omitir esa superficie.

El mismo diferencial expuso el orden raíz de _id. Inserciones y upserts de
reemplazo reutilizan document_with_id_first, sin reordenar documentos embebidos,
cambiar los IDs devueltos o migrar datos antiguos. Dos tests de preservación de
orden antiguo se siembran por el SPI real en vez de crear ese estado mediante
la API pública corregida; conservan sus aserciones de orden y contadores.

Las respuestas bulk usan el materializador builtin UUID/fecha existente y
conservan operaciones originales de error sin ejecutar decoders/fábricas del
usuario. BulkWriteResult.bulk_api_result reutiliza el DTO de contadores y
upserts existente. Es la única adición del manifiesto API, prevista en B07
unidad 5; no se regeneran snapshots históricos ni se declara equivalencia de
mutaciones arbitrarias de wrappers de resultados.

## Evidencia

- [Receipt](receipt.json), [runtime](runtime-sources.json), [fuentes del cambio](changed-sources.json)
  y guards de imports: wheel de 285 módulos, comprobado contra src y los módulos
  realmente cargados en master/workers, con fuente final del corpus congelada.
- Nueve acciones / 518 observaciones por MongoDB 7.0.43/8.0.32/9.0.2 standalone,
  imágenes fijadas, buildInfo/FCV y SDK en manifests native-5. Capturas y checks
  finales coinciden byte a byte; los checks se conservan comprimidos. Las rondas
  anteriores son feedback, sin regeneración de goldens históricos.
- [Baseline](baseline.txt.gz): 144 fallos y 75 checks correctos. Candidato:
  219 checks nuevos correctos, directa/wire sync/async × Memory/SQLite × 7/8/9.
  El [replay amplio](replay.txt.gz) tiene 9246 correctos y
  96 fallos, todos bson_admission preexistentes, visibles y obligatorios.
  No es un gate integrado verde. El feedback inicial conserva sus 24 fallos
  wire nuevos en dos familias; la aceptación posterior los resuelve.
- [Unitarias/contratos/propiedades](unit.txt.gz): 1562 pruebas y
  4577 subtests correctos, con Runtime/unraisable/thread warnings
  tratados como error. Incluye los dos tests legacy de orden, el parser
  administrativo y contratos SPI/comandos/errores. El feedback inicial de
  unitarias conserva las dos siembras públicas inválidas como caracterización.
- Las pruebas nuevas comprueban efectos de familias previas, ausencia de prefijo
  de frame inválido, índice único reutilizable, barrera de eventos, reapertura
  SQLite y objetos que prohíben deepcopy. Los tamaños de frame se contrastan con
  el builder independiente de PyMongo; los testigos nativos grandes observan IDs
  y longitudes de payload sin incorporar decenas de MB al modelo de resultados.
- Las respuestas de upsert y errores conservan tipos/orden BSON en cinco modos
  UUID y valores Binary 0/5, fecha y DBRef. El corpus usa canonical-json existente
  para no convertir el modelo EJSON de error en valores SDK antes de comparar;
  public_error/BSONObservation protegidos permanecen intactos.
- Python 3.13.15 realmente sin SDK: [2.880 observaciones bulk](sdk-free.json),
  [3.918 de mutación](sdk-free-mutations.json), [942 de inserción](sdk-free-inserts.json)
  y [748 controles de lectura](sdk-free-reads.json). Bulk portable omite Code
  exclusivo del SDK, su acción de formas de constructores SDK y el contrato
  completo de errores tipados; usa los modelos propios. Decimal128 se observa
  como texto decimal público; el perfil SDK conserva BID y las acciones completas.
- [Typing](typing.txt): dos positivos y 20/25 negativos. [API](api-check.txt):
  18 tests y 11 subtests; [delta](public-api-delta.json) limitado a la propiedad
  prevista. [Lint](lint.txt) correcto. [Históricos](protected.json): 278
  fixtures/observadores intactos. [Recursos](resources.json): 15 contenedores y
  30 volúmenes propios verificados ausentes con Docker operativo.
- [Coste diagnóstico](cost.json), ABBA×2/100 solicitudes y replay concurrente:
  MemoryEngine insert lote 1: 1.14; MemoryEngine insert lote 4: 1.03; MemoryEngine update lote 1: 1.28; MemoryEngine update lote 4: 1.15; SQLiteEngine insert lote 1: 1.07; SQLiteEngine insert lote 4: 1.02; SQLiteEngine update lote 1: 1.08; SQLiteEngine update lote 4: 1.08. D05 conserva estos incrementos; no es aceptación de presupuesto.

## Pendientes conservados

Comandos públicos e índices/validators todavía necesitan su admisión cliente
completa y la ruta interna preparada correspondiente. Después, coordinar UUID
por defecto: STANDARD permanece hasta completar esas entradas. Unidad 5
conserva proyecciones calculadas, sobres raw/update y otros efectos de respuesta;
la propiedad bulk aceptada no cierra toda la unidad. Continúan D06, H53,
decisión opcional D07 y D05 (matrices, cobertura, persistencia, costes y gates
integrados). No se amplía el soporte a cifrado, compresión ni transacciones
por el cálculo de frames clásicos.

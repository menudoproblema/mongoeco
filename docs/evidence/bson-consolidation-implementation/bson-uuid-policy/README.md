# B07 — UUID coordinado y ausencia en distinct

Base `30365046`; UUID `d5bdadb8`, distinct `b29e9bf1`. Se acepta la unidad 4
de B07 sobre los cortes de admisión ya entregados. No cierra positivos/respuestas,
batching, todos los contextos D06, H53/D07 ni D05.

## Causa, cambio y uso

El encoder de solicitudes ya aplicaba las opciones del dueño y el materializador
separaba lectura/escritura; la divergencia restante era el default STANDARD en
el constructor y el campo dataclass de CodecOptions. Ambos usan UNSPECIFIED.
No se añade otro normalizador ni comparador, no cambia DocumentCodec interno,
SPI, serialización o esquema SQLite. Las constantes STANDARD del encoder de
respuestas internas conservan su función: no son el default del cliente.

UUID de Python se rechaza con ValueError por defecto, en la fase nativa de cada
API. Para escribirlo, elegir CodecOptions(uuid_representation=
UuidRepresentation.STANDARD) o un legacy explícito en el dueño de la solicitud.
La lectura default conserva Binary/subtipo/bytes. Database.command conserva
su default propio; no hereda el codec de la base/colección. Sus opciones se
pasan explícitamente cuando corresponde. No reescribir Binary 4 a 3 al leer.

El diferencial halló otra autoridad compartida: resolve_distinct_candidates
convertía ausencia en [None]. Un solo cambio omite esos candidatos y conserva
null explícito, arrays y comparación previa a materializar, tanto en fachada
como en comando. Tres tests antiguos exigían el error y se corrigen contra
los testigos nativos, no contra una coincidencia Memory/SQLite. Solo el orden
exterior no garantizado de distinct se estabiliza en el corpus; los valores,
tipos BSON y orden de campos se observan completos, sin deduplicación artificial.

La fixture de ordenación binaria que escribe UUID ahora elige STANDARD de forma
explícita: conserva UUID, SQL ejecutado, fallback antes de ventana, índice,
invalidación y reapertura. El test de catálogo de opciones tenía dos fallos
previos por buscar métodos de base en AsyncCollection. Se reproducen en la
base archivada y se comprueban ahora contra AsyncDatabase y su spec de kwargs.
No se añaden métodos falsos ni se retiran campos del catálogo.

## Evidencia

- Cuatro acciones/216 observaciones por MongoDB 7.0.43/8.0.32/9.0.2 standalone,
  PyMongo 4.19.0; capture y --check finales idénticos. Se cruzan seis opciones
  de escritor y lector, Binary 3/4, bytes/orden legacy, IDs, filtros/in y
  comandos con opciones implícitas/expresas; ausencia/null/arrays por API/comando.
- Base del nuevo corpus: 72 fallos/27 correctos; candidato instalado: 99 checks
  correctos. La selección UUID ampliada anterior conserva su log separado.
- Replay completo: 10.278 correctos/24 fallos previos de admission_options.
  Resuelve exactamente 48 de los 72 nodos anteriores sin modificar su oráculo.
  Los 24 restantes pertenecen a positivos/proyecciones; no se ocultan.
- Cohorte instalada: 2.330 correctos/4.709 subtests y dos fallos previos de batching.
  Los 24 controles nuevos incluyen rechazo sin prefijo de datos/eventos usando
  barrera de commit, estado/índice/reapertura, herencia y override de opciones,
  lectura independiente y una fixture SQLite creada por el runtime 30365046
  con default STANDARD. Ese archivo anterior de 122.880 bytes no se regenera
  con el candidato: conserva UUID, índice único y actualización por su clave.
- Python 3.13.15 realmente sin SDK: 1.296 observaciones, cuatro acciones completas,
  sin omitir casos ni normalizar el observador. Diez familias anteriores pasan.
- Wheel con 287 módulos verificados y guards de import en master/cuatro workers.
  API: 18 tests/13 subtests; typing instalado: dos positivos y 20/25 negativos.
  El manifiesto público cambia exactamente dos valores default de CodecOptions,
  constructor y dataclass; no es una firma accidental ni una snapshot actualizada.
- Los 302 fixtures/observadores históricos permanecen idénticos. Nueve
  contenedores y 18 volúmenes propios se comprueban ausentes con Docker operativo.
- ABBA×2 sobre lectura default, escrituras explícitas STANDARD/Java y comando,
  en ambos motores, es diagnóstico bajo gates concurrentes; no acepta D05.

Los logs de feedback distinguen el defecto real, las expectativas espejo y el
uso de UUID que dependía del default anterior. La aceptación usa la wheel final
y los tests finales. No hay captura nueva de change streams distribuido:
rechazo cliente nativo y barrera local tienen alcances separados. Sigue la
unidad 5 de positivos/respuestas, con los 24 nodos y dos tests de batching como
testigos obligatorios; permanecen D06, H53, D07 y D05.

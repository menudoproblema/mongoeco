# D06a/b: fronteras de respuesta, capacidad y codecs

El [receipt](receipt.json) acepta el corte acotado `381aa10` y fija su fuente y
resultados terminales. Los gates anteriores a la última corrección son feedback,
no aceptación. Este corte no cierra D06 ni D05 completos.
Los logs de fallos con whitespace de pytest se conservan en gzip, sin editar
su salida original; el receipt registra hashes comprimidos y descomprimidos.

Las nueve acciones `response-frontier` y dos `response-command` amplían el
registro/capturador/replay existentes. Las referencias nuevas se capturan y
comprueban físicamente con MongoDB 7.0.43, 8.0.32 y 9.0.2, FCV correspondiente,
en replica sets propios fijados por digest. El [archivo nativo](native.tar.gz)
incluye sondas iniciales, capturas intermedias y las finales:
`mongoeco-frontier-native-v4` y `mongoeco-response-command-native-v2`.

Los testigos separan estas garantías:

- La frontera por bytes no es una suma simple de tamaños documentales. El
  framing y la reserva de resume token cambian entre primera tanda y getMore,
  entre find y aggregate y, para el primer find, entre 7 y 8/9. Se prueban cuatro
  tamaños consecutivos alrededor de esa diferencia, además del límite por cantidad.
- Un documento devuelto puede superar 16 MiB; en estos servidores el límite
  interno observado es 16 MiB + 16 KiB, inclusive. Dos Strings válidas permiten
  probarlo sin confundirlo con capacidad String. Los errores de primer campo
  String/Binary/documento comprueban el renderer existente y su truncación.
- Una String calculada de 16 MiB ya falla: 16493 en 7/8 y 10334 en 9. `$concat`
  y `$replaceAll`, ASCII/UTF-8 y cola null distinguen productores, bytes y
  cortocircuitos. La comprobación compartida conserva la clasificación String
  frente a Code y reutiliza el tamaño BSON existente.
- Un error del servidor invalida la respuesta completa antes de decodificarla.
  Un error del decoder del cliente conserva los callbacks anteriores que
  observa el SDK. Raw conserva framing/hash y no ejecuta callbacks del usuario.
  Se comparan fase, clase, código, codeName y errmsg; solo se normaliza el
  namespace efímero exacto, no las diferencias del diagnóstico.
- Entrada pequeña/salida grande con error de filtro tardío también falla al
  abrir en los tres servidores. Este control retira la hipótesis de que toda
  prelectura local sea incorrecta; no se introduce una fuente unitaria distinta.
  Otros controles distinguen orden del índice primario de sort bloqueante con
  hint, manteniendo residual antes de ventana.
- getMore con batchSize cero elimina la cota por cantidad; no produce tandas
  vacías indefinidamente. Los arrays de killCursors usan int64, igual que cursor.id;
  Unknown queda vacío cuando el estado es determinado. Se preservan las guardas
  de namespace/sesión/principal y el contenido de arrays del usuario.

`ResponseBatchReader` comparte la formación de respuesta y retiene un único
documento evaluado que desborda la tanda. Los adaptadores conservan apertura y
promoción de batchSize propias del SDK. Evaluación, respuesta y materialización
pública mantienen fronteras separadas, sin otro evaluador o almacén de cursores.

La revisión encuentra además dos pérdidas de contexto en rutas compartidas:

- El materializador público no convertía todos los wrappers sin SDK; las fuentes
  antiguas lo ocultaban convirtiendo antes. La conversión se concentra en el
  materializador, después de formar la respuesta. El wheel real sin SDK y la
  regresión numérica anidada observan valores públicos y preservación de entrada.
- El separador streamable convertía `PreparedPipeline` en list y repetía la
  preparación. Conservar su porción mantiene direcciones/contexto e invalidación
  por dialecto/bindings/registro. Las 24 regresiones existentes fallaban también
  en la base anterior: no se añade una caché ni se relaja su validación.

EOF/error/rewind/cancelación tienen testigos locales de documento pendiente,
evaluación una sola vez, cierre reintentable y precedencia del error original.
El find con tandas cerraba dos veces y perdía el dueño al fallar cleanup; se
retira ese cierre duplicado y se conserva el recurso hasta cerrar correctamente.
Los tres constructores internos y sus fallbacks comparten configuración sin
codecs públicos; la regresión legacy reproducía cuatro callbacks antes de un
error que invalida la apertura y ahora exige cero, en ambos motores.

Las [fuentes fijadas](sources.json) explican el framing: se verifican los hashes
de 15 archivos descargados; otras 12 URLs intentadas devolvieron 404 y no se
cuentan como fuentes verificadas. La [auditoría independiente](native-cleanup.json)
comprueba ausencia de 30 contenedores y 60 volúmenes por sus nombres propios.
No se ejecuta prune ni se modifica la instancia habitual.

El replay completo pasa 7.447 checks; el corpus final falla 228 veces en la base
aislada y conserva 18 positivos. Pasan 3.216 pruebas amplias, 110 de integración,
246 desde el wheel instalado y 180 escenarios sin SDK. La evidencia conserva
wheel/hash de sus 284 módulos, typing/SPI/lint y medición de coste. El ABBA
diagnóstico (32 muestras, resultados BSON idénticos) observa ratios CPU
sort/group/unwind/Strings grandes de 1,22/1,07/0,96/1,36 en Memory y
1,16/1,12/0,95/1,23 en SQLite; no equivale a aceptación del presupuesto D05.
Los observadores,
goldens históricos y catálogos protegidos no se reescriben. Permanecen los demás
contextos D06a/b, consumidores D06c, H53, sNaN, decisión D07, D05 y admisión B07,
según el [registro](../../../bson-consolidation-implementation.md).

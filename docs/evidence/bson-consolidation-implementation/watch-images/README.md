# D04: imágenes y recursos de lectura

[Plan owner](../../../bson-semantics-consolidation-plan.md),
[registro de implementación](../../../bson-consolidation-implementation.md).
Corte CRUD de imágenes implementado; D04 conserva DDL/cierre por ámbito y el
fixture replica set de CI. Receipt/workset identifican base y candidato exactos.

## Reutilización y review

- `updateLookup` usa un recurso de lectura nuevo al consumir, con el coordinator,
  OperationContext y ReadSnapshot existentes. El mismo helper sirve colección,
  DB y cliente. No se materializa por API pública antes del pipeline: se conservan
  Int64 pequeño y Binary/subtipo, y codec_options se aplican al final. Un cursor
  standalone sin recurso rechaza updateLookup antes de registrar el watcher;
  no se confunde un fallo de wiring con un documento inexistente.
- `whenAvailable`/`required` usan disponibilidad capturada en la escritura, no
  opciones consultadas después. Un metadato interno opcional viaja por el payload
  común, snapshot y journal; no aparece en el evento público. Missing produce
  null o OperationFailure 47 y cierre, según el modo. Insert/replace conservan
  siempre su imagen; default omite la de update.
- Create/collMod reutilizan una validación común de la opción de imágenes; sus
  formas válidas/erróneas, precedencia y códigos están contrastados físicamente.
  Null ignora la opción, enabled null cuenta como requerido ausente, y un campo
  desconocido precede a enabled ausente. No se habilitan otros collMod options.
  SQLite reutiliza la misma vista de opciones dentro de cada escritura para
  validación/disponibilidad; una traza puntual muestra dos lecturas antes y una
  después, con el mismo resultado. No es un cache entre operaciones.
- El reloj del lookup es actual para recursos/TTL; el de expresiones del stream
  sigue congelado. La prueba de TTL verifica además la entrega de su delete.
- El matcher canónico evalúa una guarda necesaria antes de requerir imagen.
  El pipeline original sigue siendo la autoridad del resultado. Se siguen aliases
  de campos demostrables por set/project/unset con flags del dialecto; reemplazo
  de raíz es barrera. AND/OR parciales y negación conservan sus reglas; no se
  adelantan expresiones aleatorias, dinámicas o registradas. La clasificación
  común distingue String de Code para no crear otra fuga de representación.
  Una propiedad de 250 ejemplos cambia la imagen capturada frente a la resuelta
  y comprueba conservación de candidatos, incluida
  composición de aliases/proyecciones, and/or/nor y un literal Code.
- La reapertura pública encontró un Memory vacío incapaz de adjuntar su journal.
  `register_change_consumer` reutiliza durable para adoptar la secuencia solo
  si el motor está completamente vacío, sin consumidores. Marca el log anterior
  como no disponible para otros consumidores; no recupera documentos volátiles.
  Se prueban continuidad de tokens y rechazos con datos/consumidor existentes.

No nuevo AST, contexto, runner ni cambio de firmas SPI v2. No se retiran
allowlists ni se cambian observadores para acomodar producción.

## Evidencia y aceptación

Ocho acciones del registro/capturador/replay existentes, 118 escenarios por
major, tienen referencias nuevas 7.0.43/8.0.32/9.0.2 / FCV respectivo, cada una
verificada con otra captura --check. Son replica sets de un miembro propios,
puertos 49973/49974/49975; bases UUID borradas en finally, instancia habitual
intacta. Se mantienen fixtures históricos; watch wire no se declara soportado.

Cubren modos × configuración × borrado posterior en los tres ámbitos, cambios
de opciones entre escritura y consumo, filtros puros/mixtos/expr/lógicos y tras
transformaciones, errores de opciones y controles insert/replace. El observador
retiene presencia frente a null, contenido/tipo BSON, fase de consumo y cierre;
no usa comparación, guardas o identidad productivas para calcular el esperado.

- Gate integrado: 6.118 checks, 403,94 s.
- Core/propiedades/contexto/entrega: 2.690, con 936 subtests y un skip local.
- Replay de imágenes y contratos con orjson: 110.
- Base archivada 429deec: 84 fallos y 15 controles/metadata verdes sobre el corpus.
- Typing público/boundary, lint sin deuda nueva y SDK-free real 3.13 pasan.
  El smoke sin SDK ejercita ocho escenarios por cada uno de seis pares, con alias.
- Reapertura/journal sync/async en ambos motores, publicación local commit/abort,
  reloj separado/TTL y guardas de adopción de checkpoint tienen tests permanentes.
- Create/collMod de imágenes se contrastan por wire sync/async con las mismas
  referencias; esto no es aceptación positiva de watch wire.

El SQLite/journal legacy lo creó realmente 429deec. El candidato lee/reproduce
los eventos anteriores, conserva widths en lookup, escribe una imagen disponible
y conserva schema/storage_keys. No altera esquema SQL, tokens ni claves.
Sin la bandera histórica no se inventa disponibilidad: default y lookup funcionan,
whenAvailable da null y required falla 47. Los payloads nuevos omiten la bandera
false, preservando fingerprints legacy de esa forma. Esto acredita upgrade de
lectura/escritura, no downgrade de un outbox nuevo a un runtime anterior.

El benchmark pequeño secuencial usa muestras de 200 updates/eventos sobre
100 documentos; checksums idénticos y ningún gate concurrente. La repetición
ABBA de diez muestras por runtime queda aproximadamente 1,05/1,03 en default
y 1,33/1,57 en updateLookup para Memory/SQLite: ahora realiza la lectura omitida
por la base. D05 conserva la
matriz completa y sus costes; no se confunde esa omisión con una optimización.

## Pendientes y límites

La sonda lifecycle-probe concreta los pendientes D04 en los tres servidores y
seis pares: protocolos ausentes, cierre/iteración y error de argumentos, invalidate
filtrado, resumeAfter rechazado nativamente con 260 y startAfter que pierde el
siguiente evento local. El token anidado ya coincide con 280 y cierre. La pérdida
posterior es coherente con publicación DDL directa que adelanta el contador del
hub sin el del engine; el siguiente corte debe usar el registro atómico común.
Es diagnóstico, no aceptación de ese protocolo ni una prueba de latencia.

D04 conserva DDL/invalidate/rename, ámbitos, continuidad tras recrear namespaces,
resumeAfter/startAfter de invalidate, cierre/cancelación y fixtures nativos RS.
Este corte no acepta interacción de imágenes con DDL ni distribución/majority
local, ni retención nativa de pre/postimágenes. Las transacciones de esta ficha
son controles locales; la aceptación física más amplia sigue en D05.

La revisión encontró H55: Code con prefijo $/$$ se interpreta como ruta/variable
en el runtime/preparador, también fuera de watch. `code-path-probe.json` conserva
referencias 7/8/9 y seis pares locales; el control $literal coincide. Es una
caracterización tipada/repr, no otra regresión permanente ni aceptación BSON.
D06a posee su arreglo conjunto de gramática/runtime con primitivas comunes;
la guarda de imágenes ya evita tratarlo como alias. No queda oculto por los
checks verdes ni se declara cerrado el programa.

Los logs de feedback y el global interrumpido no son aceptación. La primera
sonda reutilizaba una salida histórica: se separó de inmediato y se restauró el
archivo histórico byte por byte; sus hashes siguen intactos. El primer test
wire tuvo un import de proxy erróneo, corregido antes de ejecutarlo. Ambos
incidentes de tooling se distinguen de fallos productivos y de capturas finales.

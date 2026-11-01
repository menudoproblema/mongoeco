# Implementación de consolidación BSON

Contrato autorizado por el usuario: implementar el
[plan refinado](bson-semantics-consolidation-plan.md), revisando cada bloque y
commiteando semánticamente. Base: `eef9c09`. Estado: implementación en curso.
No cambia SPI v2 ni declara implementados los pilotos opcionales.

## D00 — Activo

Se reutiliza el runner/capturador/replay existente. El corpus
`tests/differential/semantic_binary_cases.py` añade selección por operadores,
eq/in, deduplicación pública por subtipo/contenido y no-op clásico/pipeline.
Sus observaciones son independientes del comparador y claves de producción.

Primera [caracterización anterior al arreglo](evidence/bson-consolidation-implementation/d00-initial/receipt.json):
MongoDB 9.0.2, FCV 9.0, standalone desechable; los tres actions difieren en
Memory y SQLite con dialecto 9.0. Se comprobaron selección, valores distinct
por contenido/subtipo y contadores no-op. Es evidencia inicial, todavía fuera
del registro/capturador/replay, no un cierre de bloque. El corpus usa 16 bytes
para subtipo 5: la ejecución inicial nativa rechazó su literal de expresión de
dos bytes, un rechazo que los motores locales no aplicaron. No se admite esa
ejecución fallida como referencia de igualdad; su caracterización de gramática
queda pendiente dentro del dominio Binary.

Pendientes antes de aceptar D03a: integrar el corpus en el registro, capturas
nativas 7/8/9, fallo previo del replay, propiedades generadas de coherencia en
fronteras reales y regresión pública. Un corpus preparado no cierra D00.

### Corte Binary: integración D00 y primer arreglo D03a

Se integra `binary` en SemanticCorpus, el capturador y la matriz existentes,
sin runner paralelo. Capturas nativas 7.0.43, 8.0.32 y 9.0.2 con FCV de su
major; las tres referencias se verificaron otra vez con `--check`, sin
reescribirlas. Las tres propiedades generadas atraviesan bytes, SDK, local,
owned, decode y salida pública, y comprueban rango/igualdad/claves/subtipos.

El runtime previo aislado reproduce 60 fallos semánticos del replay público
(15 checks pasan). La primera ejecución sin permiso de sockets mezclaba
fallos reales y PermissionError: queda como diagnóstico, no como gate previo.
Las propiedades previas fallan en sus tres leyes. Después del arreglo, el
replay focal tiene 75 checks verdes y las tres propiedades pasan.

El cambio introduce la vista neutral `bson_values.binary_parts`, empleada por
orden, igualdad/hash lookup, claves semánticas e identidad de asignación. Se
conservan las claves físicas y el formato almacenado. El orden binario compara
longitud, subtipo y contenido. SQLite conserva fallback semántico para eq/in
binario, sin perder Binary 0 por diferencia de tags históricos.

Evidencia del corte en [receipt](evidence/bson-consolidation-implementation/binary-first-cut/receipt.json).
Regresión semántica ampliada: 4.964 verdes sobre el primer candidato; después
del cleanup de imports/dispatch, 145 checks focales verdes y ratchet de lint
sin nueva deuda. El receipt distingue ambas revisiones y la ejecución previa
con permisos de sockets corregidos. No es la suite global ni el gate D05.
Los contenedores/servidor propios se retiraron y sus tres puertos están cerrados.

Este corte no cierra D03a: faltan la extracción de clasificación general fuera
del matcher, los testigos ampliados de longitudes/arrays/gramática y observación
de caminos SQL y storage/cache. Tampoco cierra D01a: Code y la reubicación del
helper físico siguen pendientes. D05 aún exige matrices, coste y paquete final.

### D01a y clasificación común — implementación y validación focal

La clasificación atómica y la vista documental se extraen a `bson_values`;
matcher conserva una delegación y schema/virtual_indexes consumen directamente
el owner neutral. La gramática de aliases sigue en query_plan: no se fusionan
traversal MQL, schema y reglas físicas. No cambian hooks de dialecto.

`bson_identity.same_bson_update_value` es ahora la única implementación de
identidad física, consumida por paths, arrays, pipelines y reemplazos. Code
compara cuerpo y scope recursivo con orden/tipos; DBRef usa su vista documental
ordenada. Se preserva distinción scope ausente/vacío y la identidad numérica.
La guardia del observador independiente se amplía al módulo nuevo, sin cambiar
las reglas de los observers protegidos.

El corpus update_identity registra 17 escenarios por classic/pipeline/replacement:
scope reordenado, cambio numérico/nesting, scope ausente/vacío/no-op, Binary,
ancho/tipo numérico, signed zero, payload NaN, cohortes/signo Decimal128 y DBRef.
Observa contadores y bytes BSON producidos por el SDK, no igualdad de clases
Python ni comparadores de producción. Las tres referencias nativas 7/8/9 se
verificaron nuevamente con `--check`. El runtime archivado b661e7a falla los
72 casos públicos del replay; el candidato pasa sus cuatro superficies.

La ampliación descubrió pérdida de payload NaN en JSON SQLite. El commit
`19ed3fa` la corrige independientemente del helper: conserva el tag double con
payload de bits para no finitos y distingue ownership raw/wrapped, sin cambiar
la representación usada por las claves físicas. Soporta lectura del payload
double anterior y float legacy; no reconstruye bits perdidos anteriormente.
La extensión se verifica con 22 casos stdlib/orjson y 67 controles del codec
sobre b661e7a aislado con solo codec.py sustituido. No cierra B07 general.

La [evidencia del corte](evidence/bson-consolidation-implementation/identity-and-classification/receipt.json)
registra 4.969 checks de regresión semántica verdes, 193 controles de unidad/propiedades,
150 checks Binary/identidad con orjson, typing público/boundary, ejecución sin
SDK y lint sin deuda nueva. Las ejecuciones con seed vacío y la comparación
inicial por clase SDK no son gates: el primero no llegaba al fallo semántico y
la segunda distinguía bytes/Binary 0 con idéntico BSON.

Revisión D01a: no quedan cuerpos duplicados ni llamadas de producción al helper
antiguo; paths importa el owner nuevo porque lo usa. D01b sigue abierto para
eventos/outbox/índices y operaciones múltiples. D03a aún necesita los testigos
ampliados de longitudes/arrays/gramática y de caminos físicos; la extracción ya
está hecha. La aceptación global de paquete/matrices permanece en D05.

## D02 — Coordenadas exactas, consumidores y publicación TTL

El corte reemplaza timestamp/total_seconds flotantes por coordenadas enteras
UTC/BSON e inversa común en bson_scalars. Conversión, resta, dateDiff, ventanas
y TTL consumen esa autoridad; runtime reexporta los helpers aritméticos en vez
de mantener otra implementación. La inversa conserva truncado hacia cero y la
aritmética de fechas distingue los empates double y Decimal128 observados en
MongoDB. Truncado por milisegundos usa el ancla nativa de 2000, también antes
de epoch y con binSize que no divide esa distancia.

Conservar long al salir de toLong/dateDiff es necesario para toDate anidado.
La revisión recorre sus consumidores: dateAdd/parts/trunc, arrays/strings,
range/round/percentile, ventanas, fill y densify usan la vista numérica común
en sus validaciones. Fill deja de ordenar por comparación Python. El testigo
de percentiles detecta otro error previo: extremos escalares devueltos como
int frente a double nativo; se corrige en el owner que comparte con group y
se contrasta el BSON físico de ambos consumidores. No se declara completa la
paridad de todos los operadores de ventanas por arreglar estos testigos.

En `0979fd8`, el corpus datetime incorpora seis acciones al registro/runner existentes.
Capturas 7.0.43, 8.0.32 y 9.0.2, con sus FCV, verificadas mediante --check.
El runtime archivado 93b9b34 reproduce 144 fallos públicos y tres checks de
metadata verdes. El candidato pasa los 147 checks focales, también con orjson;
2.382 checks de core/propiedades/TTL y 936 subtests pasan, con un skip local
que no se cuenta como ejecución física. Las dos propiedades de coordenadas
usan el SDK BSON como oráculo, incluyendo fechas aware. Pasan typing público
y boundary, controles sin SDK y lint sin deuda nueva. La regresión semántica integrada pasa 5.116 checks sobre source/corpus
congelados. La evidencia queda en el
[receipt del corte](evidence/bson-consolidation-implementation/exact-dates/receipt.json).

SQLite reconstruye los auxiliares TTL de versión 1 al abrir con coordenadas
exactas de versión 2. Se conservan storage keys y documentos; se comprueban
expiración−1/exacta/+1 ms en ambos motores, índice antes/después de datos,
reapertura, rollback/reintento ante fallo y rechazo de versión futura sin
reconstrucción. Los controles previos de almacenamiento/publicación transaccional
no se reinterpretan como entrega a watch.

### Publicación TTL y cierre funcional

La sonda de `0979fd8` encontraba eliminación sin evento delete. La nueva
[evidencia](evidence/bson-consolidation-implementation/ttl-change-delivery/receipt.json)
la contrasta con replica sets desechables de MongoDB 7.0.43, 8.0.32 y 9.0.2,
FCV de su major y monitor TTL configurado a un segundo. Los tres entregan
exactamente los borrados de IDs 1/2 y conservan ID 3. Se reutilizan el registro,
capturador y replay; el corpus declara superficies directas sync/async. Wire
rechaza actualmente $changeStream: se documenta y se comprueba en ambos motores
y dialectos 7/8/9; no se cuentan doce omisiones wire como ejecución nativa.
Los jobs nativos que ejecuten este caso necesitan replica set y monitor rápido;
su incorporación general a los fixtures de D04/D05 queda explícita, sin imponer
ese límite de espera al monitor de una instancia arbitraria.

La causa estaba en el registro y en la frontera de entrega. Memory incorpora
los payloads TTL a su scope atómico de publicación. SQLite usa su mismo outbox,
con identidad de mantenimiento independiente y mutuamente excluyente de la
identidad de petición; conserva deduplicación/checkpoints y esquema persistido.
No se fabrica un OperationContext ni otro sistema de contexto para un borrado
interno. El coordinador posee un scope común de entrega para lecturas,
escrituras y DDL, también ante errores, y usa el hook transaccional ya existente.

El snapshot notifica una sola vez tras su primera lectura efectiva, incluyendo
vacío, fallo y cancelación, y libera el callback al cerrar sin leer. La revisión
reprodujo seis omisiones adicionales de eventos en lookup/union sobre colecciones
extranjeras: ahora comparten el mismo owner de apertura que las lecturas normales.
Se comprueban cursores poblados aún abiertos, observadores esperando, rollback
tras registrar el segundo efecto, reintento y commit/abort del contrato local.
No se extrapola ese último control a aislamiento o temporización TTL nativos.

La primera medición detectó leases/heartbeat innecesarios en SQLite sin trabajo
pendiente. El camino vacío consulta checkpoint y secuencia persistidos antes del
lease; no crea una caché que pueda ocultar cambios externos o reintentos. Las
cinco muestras secuenciales del benchmark pequeño conservan checksum y muestran
medianas aproximadamente +3 % Memory/+2 % SQLite frente a la base, frente al
+34 % SQLite inicial. Esto no sustituye cargas ni matrices de D05.

El replay focal pasa 15 checks; la regresión integrada pasa 5.131. Pasan 2.932
controles de core/APIs/TTL, con 1.005 subtests y un skip local ajeno a ejecución
física. También pasan orjson focal, typing público/boundary, uso sin SDK y lint
sin deuda nueva. Las capturas nativas se verificaron con --check; sus timestamps
de cluster/operación en buildInfo varían, sin cambiar resultados ni manifiestos.
El receipt distingue candidatos intermedios y gates finales. D02 queda cerrado
funcionalmente con esta evidencia; D05 conserva el cierre integrado completo.

## Revisión documental 5 del plan

El mismo informe se vuelve a contrastar sobre `f69759a` y el árbol Binary en
curso. La [revisión 5 del plan](bson-semantics-consolidation-plan.md) precisa
el incremento compartido raw/gramática y las guardas de `_id`, sin volver a
extraer helpers ni reabrir D02. La nueva
[evidencia de caracterización](evidence/bson-consolidation-review/review5/receipt.json)
no acepta los cambios sin commitear ni cierra D03a/D06/D05. Se mantienen los
owners y receipts de todos los cortes anteriores.

## D03a — Binary en comparación, almacenamiento y transporte

La [evidencia del corte](evidence/bson-consolidation-implementation/binary-and-raw-boundaries/receipt.json)
completa D03a y materializa su dependencia compartida de D06a/D06b. No cierra
estos últimos cortes ni D06c por asociación. Se amplían el registro, capturador
y replay existentes con cinco acciones Binary y cinco DBRef, sin otro runner.
Los seis archivos nuevos tienen referencias 7.0.43/8.0.32/9.0.2, FCV de su
major, verificadas de nuevo con --check; no cambian fixtures históricos.

Binary compara longitud BSON, subtipo y contenido. El subtipo 2 añade cuatro
bytes de longitud interna: len(payload) no basta. Los atajos de rango y sort
de bytes consumen esa regla o delegan; dejan de comparar lexicográficamente
por Python. La vista común reconoce UUID estándar de almacenamiento como
Binary 4, preservando codec_options, tags y claves físicas existentes. Las
cinco propiedades generadas cubren formas SDK/local/owned/decode/públicas,
subtipos, framing codificado por el SDK y coherencia de UUID estándar. Esto no
amplía la política de entrada UUID ni acredita las representaciones legacy.

SQLite mantiene fallback binario lossless en eq/in/ne/nin, incluyendo tags
bytes/binary/uuid. Se observan SQL ejecutado y matcher con traducción normal y
desactivada, índices escalares/multikey/parciales, arrays y rutas punteadas.
Las trazas reales registran separadamente documento y operando, sin fijar sus
clases como contrato público. Distinct/materialización y mutación de copias
públicas conservan contenido/subtipo y el estado posterior de ambos motores.

La selección/unicidad de `_id` extiende su guarda semántica de números a alias
binarios. Las guardas de escritura siguen exigiendo identidad física; no se
aceptan claves de otro payload/subtipo/tipo. Se reconoce la representación owned
de una clave SQLite previa cuando decode entrega la clase local. No se
redefinen canonical_document_id ni claves persistidas. Los controles abarcan
IDs planos/compuestos, duplicados, subtipo distinto, reapertura e índices, y un
archivo realmente escrito por el runtime anterior que el candidato actualiza
sin cambiar su clave. D01b conserva sus efectos públicos más amplios pendientes.

### Dependencia compartida de gramática y BSON raw

`query_operators` decide literal/operadores por primer campo y presencia de
ref/id según contexto, sin exigir tipos del constructor SDK. El parser, la
preparación de membership/elemMatch, upsert y pull consumen esa decisión.
Comparten gramática; mantienen sus diferencias de recorrido. La revisión
reprodujo upserts que omitían el literal y pulls que no retiraban elementos,
en ambos motores y en las tres referencias nativas. Los campos especiales
$ref/$id/$db tienen igualdad de campo en la gramática de objeto; id conserva
collation y ref/db comparación binaria. No se amplía indiscriminadamente la
allowlist de operadores ni se traslada esa excepción a agregación. ElemMatch
ya no convierte errores de preparación en un plan ausente: falla sin filas.

El owner existente bson_raw conserva orden documental, arrays, scopes y
undefined antes del bridge, también desde buffers readonly. Delega primitivas
al SDK y controla límites/longitudes en los contenedores que debe preservar;
no modifica tablas globales ni añade un segundo camino de decodificación. La validación
de literales Binary 3/5 ocurre en preparación, con código 9 y apertura eager,
incluso vacíos. También alcanza DBRef y Code scope mediante las vistas comunes.
No se aplica esa restricción de expresión a todos los campos almacenados o
filtros. Framing inválido y constante inválida conservan fronteras distintas.

La regresión integrada detectó otro cruce de ownership: deepcopy conserva la
raíz inmutable del reemplazo preparado, pero la política de `_id` necesitaba
un documento de trabajo mutable. materialize_replacement_document crea esa
raíz, conserva orden y no modifica la entrada del comando. La corrección se
verifica con reemplazos públicos, validadores y el control de ownership.

### Aceptación y límites

La base f69759a aislada reproduce 84 fallos/39 checks verdes en Binary edges,
120 fallos/3 checks de metadata verdes en DBRef, y dos fallos en las cinco
propiedades ampliadas. El candidato final pasa 5.377 checks integrados, 2.443
de core/propiedades con 936 subtests y un skip local, y 2.740 de wire con 92
subtests. Orjson pasa 246 checks de replay y 110 de componentes. Pasan typing
público/boundary, smoke sin SDK 3.13 y lint sin deuda nueva. Observers protegidos
y snapshots permanecen idénticos. Los logs distinguen candidatos intermedios,
fallos de preparación del test y gates finales sobre source congelado.

El benchmark pequeño conserva resultados: medianas aproximadas +3 % Memory y
−1 % SQLite en peticiones directas; parseo raw +8 % ping/+5 % find/−27 % batch.
Son cinco muestras con otros gates activos, no aceptación de costes D05. Los
servidores propios se detuvieron, los contenedores se retiraron y los tres
puertos nativos están cerrados.

D03a queda cerrado funcionalmente. D06a/D06b conservan aceptación restante de
subpipelines y transporte físico según el plan; no se extrapola la suite
focal a todos sus consumidores. D06c conserva group/claves/lookup/sets/spill/
Search y retirada de guardas duplicadas. Tampoco se cierran B07 general ni D05.

## D03b — Tipos SQL y gramática común

La [evidencia de aceptación](evidence/bson-consolidation-implementation/sql-type-predicates/receipt.json)
cierra D03b funcionalmente. El corpus type_query añade cuatro acciones al
registro/capturador/replay existentes: selección, escrituras, argumentos y
representaciones numéricas. Las capturas 7.0.43/8.0.32/9.0.2, FCV de su major,
se verificaron con --check. No se reescriben referencias históricas.

El traductor sustituye las ramas duplicadas de código/alias por un predicado
por familia, instanciado para campo y elemento de array. Incluye tags
bytes/binary/uuid y pertenencia de binData/objectId/date, además de preservar
ancho numérico y doubles etiquetados. El elemento se inspecciona solo un nivel;
la extracción del tag se protege por su tipo JSON, sin intentar parsear una
cadena arbitraria como documento. TypeCondition.aliases conserva su autoridad
cuando el plan ya está preparado. Las formas no traducibles siguen delegando.

El contraste encontró otro error compartido por ambos motores: se aceptaban
mayúsculas/espacios no nativos y se rechazaban códigos integrales válidos en
Int64/double/Decimal128. Una tabla inmutable código→nombre sirve ahora a ambos
formatos; number deriva sus cuatro familias. Se usan las primitivas numéricas
exactas existentes. Se conservan rechazos nativos por tipo/valor, con código
14/2 y lista vacía 9. Todos los 21 códigos y nombres canónicos y los rechazos
de los aliases antiguos se incorporan al testigo permanente, vacío/poblado.
La aceptación de un nombre de tipo deprecated no acredita su almacenamiento o
decodificación general; B07 conserva ese alcance pendiente.

Las 90 comprobaciones de ruta observan SQL de conexiones lectoras dedicadas y
el matcher, con aliases/códigos, traducción normal/desactivada y archivos
reabiertos. Cubren índices escalares, multikey y parciales, incluido parcial
por tipo y consultas que no lo implican. En los casos demostrados, 7/8 usan SQL
sin residual; 9 conserva su fallback. Se mantienen las guardas para numéricos
etiquetados y rutas que atraviesan arrays, y el fallback binario eq/in de
D03a. No se presenta el test de un constructor como pushdown numérico efectivo.

La propiedad usa los bytes de tipo que codifica el SDK BSON como oráculo, con
250 ejemplos generados y tres testigos explícitos de H51. No usa la
clasificación del matcher para calcular el esperado; falla sobre 542bce9.
El replay anterior aislado falla 40 checks y pasa 59; el candidato final pasa
5.476 checks integrados, 2.546 de core/propiedades con 936 subtests y un skip
local, y 99 de replay con orjson. Pasan typing público/boundary, smoke sin SDK
3.13 y lint sin deuda nueva. Las capturas históricas, observers protegidos y
snapshots siguen idénticos.

La primera medición concurrente motivó una repetición secuencial sin otros
gates activos. En cinco muestras de 300 peticiones sobre 100 documentos, con
resultados idénticos, las razones candidato/base quedan aproximadamente
1,00/1,03 en Memory y 0,99/1,04 en SQLite para string/binData. Es un diagnóstico
pequeño, no aceptación de costes D05; no exige otra especialización. Los tres
servidores propios se retiraron y sus puertos están cerrados.

D03a/D03b están cerrados funcionalmente; D05 conserva sus matrices y cierre de
paquete/integración. Los pendientes siguientes mantienen sus owners y no se
cierran por este incremento.

## Revisión documental 6 y precisiones pendientes

La [revisión 6 del plan](bson-semantics-consolidation-plan.md) contrasta el mismo
adjunto sobre `ef604c7`, reconoce D03a/D03b entregados y conserva todos los gates
anteriores. La [evidencia nueva](evidence/bson-consolidation-review/review6/receipt.json)
pasa 345 checks focales de replay y 69 de propiedades/fronteras; añade sondas
desechables en MongoDB 7.0.43/8.0.32/9.0.2, FCV correspondiente. No es cierre de
D01b, D04 ni D05, ni aceptación del corpus write_effect preexistente sin commit.

D01b conserva contraejemplos concretos: la guarda clásica de `_id` Code
scope permite un cambio físico en Memory que los tres nativos rechazan con 66;
pipeline/replacement sí lo aceptan nativamente. El cambio +0→−0 clásico falla
por el mismo hueco en Memory; payload NaN produce falsos rechazos en Memory y
falsa aceptación clásica en SQLite. La guarda de storage basada en igualdad
Python necesita controles propios sin cambiar las claves persistidas. Además la publicación local
fija update donde el nativo puede emitir replace y omite updateDescription.
El plan precisa reutilización de identidad física en el caller clásico y
construcción compartida de efectos confirmados, con pruebas de commit/ausencia
separadas de preparación/contexto watch. Esta revisión no modifica runtime.

## D01b — Identidad de `_id`, índices y escrituras múltiples

Este incremento implementa las guardas y efectos de estado/contadores/índices;
D01b sigue abierto para construcción/contenido y entrega de eventos. La
[evidencia](evidence/bson-consolidation-implementation/write-effects/receipt.json)
identifica candidato, base archivada, capturas y ejecuciones finales.

Classic usa `same_bson_update_value` en su guarda de `_id`; desaparecen el
detector numérico parcial y su parámetro dialecto sin función semántica. No se
cambia ninguna firma SPI v2. Pipeline/replacement conservan las conversiones
semánticamente equivalentes que aceptan los servidores. La comparación de
claves existentes admite copias BSON del mismo payload NaN, también con
wrappers double/Decimal128 quiet/signaling; recorre tuplas antes de igualdad
Python para evitar InvalidOperation. No acepta otro payload ni cambia el formato. Memory conserva la clave seleccionada para un
`_id` físicamente idéntico. Se mantienen controles de claves/documentos erróneos.

Los testigos encontraron otras dos especializaciones que evitaban su autoridad:
las claves de índices Memory reconocen ahora la vista documental DBRef común;
SQLite deja de rechazar escrituras válidas cuando no puede formar una firma
auxiliar finita. Retorna ausencia de firma y conserva fallback probado, sin
nuevo formato de claves ni pushdown aproximado. La propiedad DBRef amplía D00;
no cierra group/lookup/sets/spill/Search ni todo D06c.

Nueve acciones nuevas del registro/runner existentes tienen capturas nativas
7.0.43/8.0.32/9.0.2, FCV correspondiente, verificadas con --check. El runtime
archivado `c15dfb5` falla 84 checks y pasa 135 sobre el corpus final. El candidato
pasa 219 focales, 5.695 integrados, 2.564 de core/propiedades (936 subtests,
un skip local) y 12.155 de consumidores de escritura (1.478 subtests). Pasan
233 con orjson (corpus y fronteras), typing público/boundary, SDK-free 3.13 y lint sin deuda nueva.
Los observadores protegidos y fixtures históricos conservan su identidad.
Un archivo SQLite realmente creado por la base anterior se actualiza/reabre
sin cambiar claves ni esquema. Cinco muestras secuenciales del benchmark
pequeño conservan checksum y razones de medianas 1,05 Memory/1,01 SQLite; D05 conserva
costes y matrices completos.

La caracterización adicional con barrera precisa el siguiente trabajo:
classic whole y dotted pueden dejar la misma postimagen y publicar distinta
updateDescription. Los efectos deben conservar rutas/procedencia; deducir todo
solo de before/after perdería información. DocumentKey nativo conserva el tipo
anterior de `_id` aunque pipeline cambie su representación; el local usa el
posterior. Ambos defectos siguen abiertos junto con replace/update y entrega
transaccional. No se reinterpretan los controles TTL como aceptación de estos
contenidos de evento ni se normalizan sus diferencias en el observador.

Una sonda adicional de IDs generados confirma mantenimiento de quiet NaN, pero
expone string sNaN aceptado por `$toDecimal` local y rechazado con 241 por MongoDB
9. El owner de conversión escalar conserva ese diagnóstico, con contraste 7/8 y
testigo permanente pendientes; no se cierra mediante una prohibición global de
NaN almacenado ni se declara completa la paridad de conversiones.

## Revisión documental 7

La [revisión 7 del plan](bson-semantics-consolidation-plan.md) contrasta el mismo
informe sobre `375382a` y el trabajo de eventos preexistente sin commitear. La
[evidencia](evidence/bson-consolidation-review/review7/receipt.json) repite 69
checks de propiedades/fronteras y 345 de replay, y verifica 16 hashes r8.0.0.
No sustituye los gates de aceptación ni acepta el trabajo en curso.

Cinco casos públicos nuevos por referencia nativa 7/8/9 y par local muestran
tres defectos compartidos pendientes D01b: unset de array null modifica/publica
nativamente sin cambiar postimagen, rename local mueve un destino existente y
merge omite el cambio físico int→double por igualdad semántica. Los casos de
pipeline que omite `_id`, pequeño/grande, coinciden con el trabajo local actual;
requieren regresión permanente y aceptación del corte. Se precisa reutilización
del plan/outcome y un constructor de contenido común, conservando políticas de
operador y owners transaccionales. La identidad física sola no decide todos los
contadores/eventos. No se modifica producción en esta revisión documental.

## D01b — Contenido de cambios y entrega persistente

La [evidencia del incremento](evidence/bson-consolidation-implementation/update-events/receipt.json)
implementa contenido compartido y su integración en Memory/SQLite, sin cerrar
todavía D01b: commands/findAndModify conservan la regresión permanente y revisión
final pendientes. La sonda sync actual de esas entradas coincide, pero no se
cuenta como prueba permanente ni como ejecución async.

Los planes clásicos conservan las rutas que aplicaron, separadas de identidad
BSON: unset sobre slot null, NaN aritmético y push con sort/append+truncate
pueden producir efectos con postimagen idéntica. Slice sin efecto no modifica;
rename no mueve un destino existente. Pipeline conserva imagen calculada y
guardada para `_id`; tamaño BSON contrastado permite elegir update/replace y
subdiffs sin estimaciones JSON. El mismo cálculo sirve a `$bsonSize`; regex
conserva sus opciones BSON también al atravesar wire.

Merge emparejado reutiliza los planes y políticas existentes. Se retiran su
materializador manual y las decisiones duplicadas de selección de storage key;
rekey permitido conserva orden natural, índices y formato persistido. La
revisión evita convertir DuplicateKeyError en OperationFailure genérico al
adaptar otros WriteError a fallo de comando.

La prueba de reapertura encontró orden JSON recursivamente normalizado y decode
sin widths. Journal conserva ambos: log V2 con checksum sensible al orden,
lectura V1 y snapshot V1 sin reordenación. No se alteran tokens ni esquema de
outbox. Se verifica un journal realmente creado por el runtime anterior; la
información ya perdida en un journal antiguo no puede recuperarse.

Cuatro acciones de tamaño y nueve de eventos tienen referencias 7.0.43/8.0.32/
9.0.2 verificadas con --check. La base archivada 375382a falla 192 checks y pasa
18 sobre el corpus final; el candidato pasa 210 focales y 5.905 integrados.
Pasan 2.660 de core/propiedades/entrega (942 subtests, un skip local), 12.155 de
consumidores de escritura (1.478 subtests), 2.746 wire (92 subtests) y 253 con
orjson. Pasan typing público/boundary, SDK-free 3.13 y lint sin deuda nueva.
Se mantienen observers, snapshots y fixtures históricos. Los controles nuevos
incluyen fallo posterior al registro, rollback, reintento, commit/abort y replay
incremental/compactado una vez; no aceptan por asociación watch transformado,
fullDocument modes, aislamiento distribuido ni todo D04/D05.

La medición pequeña secuencial conserva checksums: razones aproximadas 1,01/0,99
en Memory/SQLite sin observador y 1,01/1,03 en no-ops con observador. D05 conserva
su evaluación completa de costes y matrices.

## D01b — Aceptación de entradas y cierre funcional

La [aceptación restante](evidence/bson-consolidation-implementation/update-entrypoints/receipt.json)
incorpora al registro/capturador/replay cuatro acciones permanentes de comandos
y findAndModify, con 101 escenarios por major. Las referencias 7.0.43/8.0.32/
9.0.2, FCV correspondiente y replica sets, se verifican con --check. Reemplazan
la sonda diagnóstica sync anterior para las garantías ahora ejercidas también
en async; no modifican los goldens de los incrementos ya aceptados.

El corpus observa respuestas reales, errores por item frente a excepciones de
comando, estado BSON y eventos tras barrera. Cubre classic/pipeline/replacement,
no-op/cambio físico, upsert, ID permitido/rechazado y BEFORE/AFTER con proyección
completa, solo ID y vacía. `{}` se conserva como imagen válida. Las APIs find
no exponen modified_count y el observador no lo deduce de la imagen. Un prefijo
confirmado de update command/update_many se entrega después del error sin otra
petición, y la barrera posterior verifica ausencia de duplicados.

Pasan 51 checks focales sync/async en ambos motores y tres dialectos, también
con orjson. La base archivada 375382a falla 48 y pasa los tres de metadata.
Pasan 74 checks de entrega/atomicidad/SPI/inventario con ocho subtests y el
ratchet de lint. El gate integrado final pasa 5.956 checks. La revisión fija
workset y base f8344e7 y verifica que producción es idéntica al incremento
anterior: se reutilizan sus propiedades, typing, SDK-free, consumidores,
persistencia y medidas cuyos inputs no han cambiado. No se introduce código
productivo ni otro mecanismo para un requisito ya satisfecho.

D01b queda cerrado funcionalmente con la evidencia conjunta de identidad,
estado/índices/múltiples, contenido/entrega y estas entradas. La ficha de
aceptación relaciona cada garantía con su receipt. No quedan findings
demostrados abiertos de ese alcance en la revisión; D04 conserva preparación/
contexto, fullDocument soportado y los fixtures replica set de jobs nativos.
D05 conserva matrices, wheel, global/cobertura/costes finales y el cierre
integrado de todo el programa. No se cierra B07 general ni los demás bloques.

## Revisión documental 8 — garantía restante de watch

La [revisión 8 del plan](bson-semantics-consolidation-plan.md) contrasta el
mismo adjunto sobre `bc8fb1c` y el trabajo de watch preexistente sin commitear.
La [evidencia nueva](evidence/bson-consolidation-review/review8/receipt.json)
identifica ese árbol y conserva el cierre funcional D01b. Repite 69 checks de
propiedades/fronteras, 345 de replay Binary/DBRef/type y las trazas de
representación; verifica 16 hashes r8.0.0. No modifica ni acepta runtime/tests.

D04 conserva tres garantías separadas: preparación/contexto, imágenes y ciclo
de vida por ámbito. Las sondas de imágenes se repiten contra los replica sets
7.0.43/8.0.32/9.0.2 y los seis pares locales: updateLookup local devuelve la
imagen capturada, no la actual; whenAvailable/required no distinguen la
configuración de imágenes. Las sondas DDL observan drop/invalidate/cierre
incorrectos por ámbito y el camino rápido drop_database sin eventos. Son
caracterizaciones públicas, no nuevas regresiones permanentes ni aceptación.
El plan posee dependencias/testigos de corrección; este registro conserva esos
pendientes. Los gates del candidato de preparación en curso no los cierran.
No se acepta distribución, retención nativa ni watch wire por asociación.

## D04 — Preparación y contexto del stream

La [evidencia del corte](evidence/bson-consolidation-implementation/watch-context/receipt.json)
acepta la garantía de preparación/contexto; D04 sigue abierto para imágenes,
DDL/cierre por ámbito y el fixture replica set de CI. Se reutilizan preparador,
compilación y contexto existentes; se retira la validación superficial propia
de watch y se comparten checks estructurales con aggregate/update. El stream
captura dialecto/reloj/codec al abrir y conserva el token al transformar; errores
de evento cierran cuando corresponde. El handle collection.database mantiene
now_factory también para agregación ordinaria. No cambia SPI v2 ni se añaden
allowlists arquitectónicas.

Cinco acciones permanentes tienen capturas nativas 7.0.43/8.0.32/9.0.2 verificadas
con --check. Cubren errores eager sin eventos, evaluación/token, matching tipado,
reloj entre eventos/getMore/reanudación y validación compartida vacía/poblada.
La base bc8fb1c falla 56 checks y pasa siete controles; el candidato pasa 6.019
integrados, 2.676 de core/propiedades/contexto/entrega (936 subtests, un skip
local), 146 de componentes y 82 watch/contexto con orjson. Pasan typing/lint y
smokes sin SDK real 3.13, incluidos seis pares motor/dialecto de watch.

La revisión distingue reconocimiento gramatical de capacidad de ejecución,
conservando Search/extensiones; no convierte una restricción de watch en regla
de aggregate/update. Los observers y goldens históricos quedan idénticos.
No quedan findings demostrados abiertos de preparación/contexto dentro de lo
ejercido; los defectos de imágenes/DDL de revisión 8 mantienen sus testigos y
no quedan aceptados por estos gates. D05 conserva matrices/costes y cierre final.

## D04 — Imágenes y recursos de lectura

La [evidencia del corte](evidence/bson-consolidation-implementation/watch-images/receipt.json)
implementa los modos de imágenes CRUD con los recursos/contextos existentes:
updateLookup consulta al consumir y mantiene tipos internos hasta materializar;
whenAvailable/required usan disponibilidad capturada al escribir. Metadatos
opcionales conservan journal y outbox sin cambiar esquema SQL, claves o tokens.
Create/collMod comparten validación; SQLite reutiliza las opciones de cada
escritura para validar y fijar disponibilidad, evitando la segunda consulta.
Una guarda necesaria delega al matcher y
sigue aliases demostrables sin reemplazar el pipeline original. La propiedad
amplía conservación de candidatos y clasificación String/Code común.

La reapertura pública corrige adopción de checkpoint en Memory completamente
vacío para consumidor durable, sin recuperar documentos ni permitir adelantar
un motor con datos/consumidores. Tests sync/async prueban replay/continuidad;
otros controles verifican reloj actual del recurso frente al del stream,
publicación local commit/abort y mantenimiento TTL con entrega.

Ocho acciones permanentes, 118 escenarios por major, tienen capturas físicas
7.0.43/8.0.32/9.0.2 / FCV correspondiente y --check. El candidato pasa 6.118
integrados, 2.690 de core/propiedades/contexto/entrega (936 subtests, un skip),
y 110 de imágenes/contratos con orjson. La base archivada 429deec falla 84 y pasa
15 controles/metadata. Pasan typing/lint/SDK-free real 3.13. La ficha conserva
además la comprobación wire de opciones, compatibilidad de un archivo realmente
creado por la base y medidas secuenciales; no se presenta como cierre D05.

Default mantiene coste próximo al previo; lookup añade el coste de la lectura
antes omitida. El journal anterior sin bandera se lee sin inventar disponibilidad.
D04 sigue abierto para DDL/cierre por ámbito, interacción con imágenes y los
fixtures replica set de CI. D05 conserva matrices/aceptación física más amplia.

La revisión confirma H55 en el plan: Code BSON $/$$ se interpreta como ruta/
variable en preparación/runtime, también en agregación ordinaria. La sonda
nativa/local es caracterización; D06a conserva arreglo y regresión permanente.
No se cierra mediante la guarda de imágenes ni se pierde del trabajo restante.

## D04 — Inicio de revisión de ciclo de vida

La [caracterización de continuidad](evidence/bson-consolidation-implementation/watch-lifecycle/receipt.json)
sobre f16ffb2 añade 21 observaciones nativas y 42 locales. Lookup tras recrear
el namespace ya coincide (documento nuevo); conservarlo sin un metadato nuevo
de encarnación. Drop inexistente publica/invalida localmente, frente a batch
vacío y cursor vivo nativo. Estos testigos se unen al protocolo y bypass de
secuencia ya identificados. Es diagnóstico sync, sin producción ni cierre D04;
siguen pendientes regresiones permanentes, corrección y aceptación del bloque.

## Revisión 9 del mismo adjunto

El [contraste sobre 163ccb4](evidence/bson-consolidation-review/review9/receipt.json)
repite 88 checks focales y 408 de replay sobre capturas existentes, sin cambios
de runtime/tests ni nuevas capturas físicas. El plan reconoce explícitamente
la aceptación de preparación/contexto e imágenes CRUD y acota la extracción
de entrega compartida para DDL. No cambia el estado de ningún corte ni
sustituye los gates de aceptación anteriores.

## D04 — Efectos de namespace y entrega por secuencia

La [evidencia del corte](evidence/bson-consolidation-implementation/watch-namespace-effects/receipt.json)
acepta drop/rename directos y drop/dropDatabase por comando en los escenarios
del corpus. Memory/SQLite registran efecto e invalidacion en su unidad atomica
existente, usando el mismo orden que CRUD y mantenimiento. La API deja de
publicar invalidate directamente para estos motores; coleccion, base y cliente
comparten preparacion de entrega, hooks de commit y despacho en finally.
El adaptador usa publish_committed del hub para comprobar secuencia/publicar
bajo un solo guard, manteniendo el fallback de sinks anteriores y SPI v2.

Drop inexistente no publica. Drop invalida solo la coleccion afectada;
dropDatabase invalida las colecciones existentes y la base, manteniendo cliente
y colecciones que nunca existieron. Rename entrega evento e invalidacion a
origen y destino, conservando `to` en metadata opcional del journal. Invalidate
omite ns/documentKey y dropDatabase omite ns.coll; no cambia el formato de token,
las claves persistidas ni el esquema SQL. No se introduce identidad de encarnacion.

La revision con fallos inyectados detecto dos rollbacks incompletos de Memory:
el snapshot del origen de rename dejaba de protegerlo antes de preparar el
outbox, y dropDatabase retiraba roots sin restaurarlos si fallaba esa fase.
Rename agrupa los dos namespaces en el mismo scope; drop de base retiene y
restaura referencias a los roots, sin copiar todos los documentos. Las pruebas
verifican catalogo, datos, opciones, indices y ausencia de eventos tras error;
tienen controles de reintento, commit/abort local y reapertura del journal.

Un corpus permanente con 35 escenarios por major tiene capturas fisicas
7.0.43/8.0.32/9.0.2, FCV correspondiente, y --check. El runtime base 1a4e28b
falla 12 checks de comportamiento y pasa tres de metadata. El candidato pasa
6.133 integrados y 2.753 de core/propiedades/contratos (936 subtests, un skip).
Pasan 82 de API/admin, 39 con orjson, typing publico/boundary y lint sin deuda
nueva. Python 3.13 realmente sin SDK reproduce los 35 escenarios en seis pares,
ademas de los ocho escenarios de imagen por par ya existentes.

Un archivo SQLite/journal realmente creado por 1a4e28b conserva lectura de su
update, claves/esquema e indices al renombrar; otra reapertura entrega destino
y escritura posterior. No se acredita downgrade. El benchmark secuencial ABBA
con diez muestras por runtime/caso conserva checksums: ratios de medianas
aproximados 0,99/0,98 Memory y 1,01/0,97 SQLite para default/updateLookup. Mide
el camino CRUD compartido, no throughput DDL ni los costes completos de D05.

La nueva sonda de enrutamiento confirma H56: renameCollection por admin funciona
en los tres nativos y falla localmente; por la base de origen sucede lo inverso,
incluida una mutacion local que el servidor rechaza con 13. Es caracterizacion
sync de entradas, no aceptacion ni regresion permanente todavia. D04 sigue
abierto para esta ruta/dropTarget, protocolo/reanudacion/cierre filtrado,
interacciones adicionales con imagenes y fixture replica set de CI.

## D04/H56 — Comando rename y sobrescritura atomica de destino

La [evidencia de aceptacion](evidence/bson-consolidation-implementation/rename-command/receipt.json)
cierra H56 funcionalmente dentro del subset de una base. RenameCollection se
ejecuta en admin; la entrada por la base de origen falla con 13 sin mutar.
El owner de gramatica BSON de comandos comparte campos admitidos, requeridos,
tipos de namespace y booleanos entre directa/wire, con precedencia contrastada.
Code no se admite como string por heredar de str; se usa la clasificacion BSON
existente. Esto no cierra H55 en expresiones de agregacion.

Los motores comparten el borrado de contenido entre drop y la sobrescritura
de destino en rename. El caller conserva una sola unidad de rollback/outbox;
se retiran datos, indices y recursos Search del destino y se mueve el origen.
Solo se publica rename y su invalidacion, sin drop intermedio. La misma
validacion atomica de existencia/conflicto conserva 26/48/20 en API y comando.

El rename ordinario sigue delegando al SPI publico del proveedor. La revision
reprodujo un candidato que elegia el metodo interno aun sin overwrite y
saltaba un override publico; el test permanente lo impide en ambos motores.
DropTarget exige el recurso atomico adicional antes de mutar; un proveedor
que solo implementa SPI v2 conserva rename ordinario y falla con 20 ante
overwrite. No se cambia el protocolo ni se simula atomicidad con dos commits.

Dos corpora permanentes tienen capturas 7.0.43/8.0.32/9.0.2, FCV de su major,
verificadas con --check: 17 escenarios de comando/estado/opciones/indices y
24 de eventos/scopes por major. La base 6e6d31e falla 36 checks y pasa seis de
metadata. El candidato final pasa 6.175 integrados, 2.760 de core/propiedades/
contratos (950 subtests, un skip), 55 con orjson y 20 de API/wire seleccionados
(25 subtests). Pasan typing publico/boundary, lint y smoke Python 3.13 sin SDK.
Los cambios de expectativas antiguas de error/entrada se justifican con los
nuevos diferenciales; no se modifican goldens u observadores anteriores.

Fallos tras retirar destino y registrar efectos prueban restauracion de ambos
namespaces por API sync/async. Search ya usado en origen/destino conserva
recursos tras rollback y sigue al origen tras reintento. Las escrituras TCP
usando cliente prestado entregan rename/invalidate a streams locales publicos
de origen/destino y mantienen cliente/continuidad tras cerrar el proxy; no es
soporte de watch wire. Un SQLite/journal realmente producido por 6e6d31e
conserva clave del origen, sus indices/opciones, esquema central y replay al
sobrescribir destino. No se acredita downgrade.

El benchmark ABBA secuencial conserva checksums y lecturas indexadas finales:
diez muestras por runtime/caso, 200 renames y 100 documentos; ratios de
medianas 0,99 Memory y 1,00 SQLite. Es coste diagnostico del rename ordinario,
no throughput de overwrite ni cierre de la matriz D05. D04 conserva protocolo,
reanudacion/cierre filtrado, interacciones adicionales con imagenes y RS de CI.

## Trabajo restante

El orden vigente se mantiene en el [plan, contraste actual](bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución).
La [revisión 15](evidence/bson-consolidation-review/review15/README.md) conserva el
contraste histórico del candidato sobre `eb85ea3`. El corte posterior `a53824d`
entrega dispatch, consumo y ownership de respuestas con su
[aceptación acotada](evidence/bson-consolidation-implementation/codec-dispatch/README.md).
No vuelve a abrir los cortes anteriores ni cierra todos los contextos D06.

La [revisión 16](evidence/bson-consolidation-review/review16/README.md), sobre
`5348004`, vuelve a contrastar el mismo adjunto. Precisa en el plan los testigos
de fábricas documentales, topología comparable y consumo/estado público. Sus
85 regresiones y el control local de efectos no aceptan el fixture/corpus en
curso ni equivalen a nuevas capturas físicas. El orden restante conserva un
solo owner en el plan.

- D06a/D06b/D06c: completar los contextos restantes de gramática/String/Code/DBRef/raw, planes no cubiertos y consumidores. `a53824d` entrega dispatch/consumo, `d4caba5` construcción/asignación de documentos de respuesta y `72edfa3` posición, alive, propiedad y cierre del cursor sync. `332d07a` entrega claves de grupo/DBRef/collation, conjuntos y decisión preparada de representación/average, con hash/spill y BSON raw nativos. `fbc7cde` entrega persistencia/reapertura Search, contexto de candidatos, ranking de tandas y selección temprana del oráculo de pruebas; otros contextos siguen por acreditar. Esto no cierra los demás contextos/representaciones ni todos los efectos de métodos de mappings. El corte de bytes/callbacks y String calculada de `381aa10` y los previos de sufijos/raw conservan su aceptación. Reutilizar lector y materializador actuales; no declarar D06 completo por estos cierres.
- H53: disposición de observabilidad conforme a C03/B08; la caracterización previa no constituye aceptación.
- Conversión escalar: `51ff167` entrega y verifica el corte String sNaN/241, tipos públicos, codec y controles numéricos. Su [aceptación](#conversión-escalar--gramática-tipos-y-preservación-bson) no cierra todos los contextos D06 ni admisión B07.
- D07: piloto opcional con criterio de continuidad, sin migración obligatoria.
- D05: revisión y gates integrados, matrices y commits de cierre. La suite amplia de feedback del corte codec identifica 97 nodos que también fallan en `181a922`; conservar su diagnóstico y resolverlos contra sus oráculos, sin presentarlos como un gate global verde.
- B07: lecturas, inserciones, mutaciones, bulk clásico y comandos públicos conservan sus aceptaciones. `7524e39` añade [frames cliente de creación de catálogo](#b07--frames-cliente-de-creación-de-catálogo), opciones/contexto y respuesta metadata. El corte posterior `8e261c7a` acepta [proyecciones y contexto](evidence/bson-consolidation-implementation/find-projections/README.md): el replay instalado queda sin fallos y los dos tests antiguos de batching siguen rojos. `c715ae7` añade las cuatro admisiones de índices y los campos observables de la respuesta de retirada; `d9c9c80` añade las seis entradas de listados/validate, lectores fuente preparados y controles de flags. `b938556d` añade drop/rename y `2723fb28` acepta las cinco entradas Search del censo. `d5bdadb8` entrega el default UUID coordinado y `b29e9bf1` omite ausencia en distinct. Continúan positivos/batching y otros contextos D06/B08. UNSPECIFIED es el default global; STANDARD/legacy siguen disponibles explícitamente y el [plan activo](bson-semantics-consolidation-plan.md#b07--admisión-en-la-fase-y-el-lote-correctos) conserva la secuencia.

La [revisión 17](evidence/bson-consolidation-review/review17/README.md), sobre
`3c4032b`, confirma que el adjunto repite la autocrítica anterior. Actualiza el
orden activo con los cierres acotados de fábricas/cursores y concreta D06c por
consumidor: grupo/DBRef/collation, atajo de conjuntos, serialización de spill y
conservación de candidatos. Sus 97 regresiones y sondas locales son diagnóstico,
no aceptación de D06c ni del corpus preexistente. El plan conserva un solo orden
vigente y los owners restantes; no se reabren D01/D02/D03/D04.

La [revisión 18](evidence/bson-consolidation-review/review18/README.md), sobre
`b49d521`, contrasta otra vez el mismo adjunto: 97 regresiones locales y 18 trazas
Binary en seis pares motor/dialecto. Mantiene los cierres de grupo/conjuntos y
Search. Precisa la aceptación pendiente de conversión en el plan: gramática,
plegado/fase, tipo público BSON y controles numéricos físicos. Los cambios de
conversión preexistentes siguen sin aceptar; no modifica runtime ni capturas.

Los cortes focales no acreditan el cierre integrado del programa ni D05.

## Revisión 10: plan restante tras repetir la autocrítica

El [receipt documental](evidence/bson-consolidation-review/review10/receipt.json)
contrasta el mismo input sobre 6ff34d4, con runtime/tests limpios. Pasan 88
checks focales y 408 de replay existente; se verifican otra vez los 16 hashes
de fuentes y 18 trazas del matcher. No hubo nuevas capturas físicas ni cambios
de producción/tests, y no se reabre un corte aceptado.

El plan retira del orden activo la extracción de entrega y DDL ya realizados.
Concreta el siguiente corte de cursor/reanudación, incluyendo cinco testigos
locales del decoder y el error tras invalidate filtrado. Estas sondas son
diagnóstico pendiente de corrección/aceptación, no un cierre D04. Las garantías
y dependencias permanecen en el plan, sin una segunda lista activa aquí.

## D04 — Dependencia del decoder de tokens

La [evidencia de admisión](evidence/bson-consolidation-implementation/watch-token-admission/receipt.json)
documenta el arreglo del parser compartido por colección/base/cliente y por las
dos APIs. Se conserva el alias privado y el productor ordinario de tokens;
no hay otro decoder, formato nuevo, cambio de journal, esquema SQL o SPI v2.

El parser exige un documento y un `_data` BSON string, distinguiendo Code.
Los tipos incorrectos conservan los códigos nativos observados 14/40647;
cadena vacía usa 40649 y cuerpo inválido 9. Base64/JSON se validan antes de
acceder a campos; versión y posición son enteros, sin bool ni posición negativa.
Desaparecen AttributeError/ValueError en los testigos de revisión 10. No se
interpreta el formato opaco del servidor como si fuera el formato local.

El corpus permanente añade 66 rechazos y seis controles de continuidad por
major, con capturas físicas y --check 7.0.43/8.0.32/9.0.2. Las seis combinaciones
motor/dialecto lo ejecutan por sync/async. El control de continuidad mantiene
el historial disponible: sin journal ni watchers, el subset local conserva
su rechazo documentado ante huecos. Un archivo/journal realmente creado por
c7fc668 acredita además reanudación tras cerrar y reabrir con v1 y decimal
heredados, conservando los bytes del token.

Esta dependencia no cierra D04 ni el corte conjunto de protocolo/checkpoint.
Iteración/context manager, cancelación, invalidate filtrado, resume_token,
intercalados, retención/gaps, precedencia y fixture RS conservan su pendiente.
Pasan 6.202 de replay integrado, 2.730 de core/propiedades/contratos (936
subtests, un skip), 56 con orjson, typing público/boundary, lint y smoke sin SDK.
El benchmark ABBA de apertura con token válido mide ratios de medianas 1,11
Memory y 1,12 SQLite: alrededor de un microsegundo adicional por apertura para
la validación más estricta. No mide throughput de eventos ni cierra D05.
Los comandos, fuentes y límites del candidato quedan en el receipt enlazado.

## D04 — Bucle común, protocolos y cierre del cursor

El [receipt del cursor](evidence/bson-consolidation-implementation/watch-protocol/receipt.json)
registra un único bucle para next/try_next y una única limpieza ante error o
cancelación, incluidos errores de lookup y materialización. El filtro puede
ocultar invalidate sin ocultar su efecto de cierre. Los cursores cerrados
devuelven None en try_next y terminan iteración con StopIteration/StopAsyncIteration.
El timeout vacío por defecto es un segundo, contrastado en los tres nativos.

Sync/async implementan iteración con identidad del cursor y context manager.
Close async sigue teniendo efecto inmediato para callers previos; un resultado
completado sin dependencia de loop permite await sin crear coroutines olvidadas.
No se añade un contexto nuevo ni un segundo estado de cierre. La fachada
tipada conserva ChangeEventDocument y await watch conserva el tipo del cursor.

Tres casos permanentes del corpus watch-protocol tienen capturas y --check
7/8/9. La base 1b8705a falla sus 36 checks públicos y pasa tres de metadata.
El adaptador async verifica los métodos reales; no fabrica iteración/contexto.
El escenario async de cancelación/contexto se ejecuta sin cambios por PyMongo
en los nueve pares major/ámbito y por la API local en seis pares motor/dialecto.
Las pruebas deterministas añaden cierre/cancelación durante lookup y fallos de
callbacks sin watcher/waiter residual. El receipt conserva resultados y límites.

Un SQLite/journal realmente producido por 1b8705a conserva token, datos de la
colección recreada y replay drop/invalidate; los cursores visible/filtrado
terminan sin entregar una inserción posterior a invalidate. No hay cambio
de token, esquema SQL, journal o SPI v2 ni promesa de watch wire positivo.

D04 permanece abierto para resume_token/checkpoint, posición inicial/avance
atómicos, retención/gaps, opciones y precedencia, integración restante con
imágenes y fixture replica set de CI. Este corte no acepta esas garantías ni
el cierre amplio D05. Pasan 6.241 de replay integrado, 2.744 de core/contratos
(936 subtests, un skip), 53 con orjson, typing, lint y smoke sin SDK. El ABBA
secuencial de lectura default/updateLookup conserva checksums, con ratios de
medianas entre 0,99 y 1,01 en ambos motores; no es el coste global de D05.

## D04 — Checkpoint, posición y continuidad local

El [receipt del checkpoint](evidence/bson-consolidation-implementation/watch-checkpoint/receipt.json)
registra el getter owned resume_token y la adquisición atómica de offset,
coordenada y watcher bajo el guard existente del hub. El avance comprueba
historia, conserva el checkpoint tras error y no salta una escritura que
llegue entre un timeout vacío y la publicación de su checkpoint.

El decoder sigue siendo único. La metadata opcional `i` distingue invalidación
(true), checkpoint explícito (false) y token de evento/legado (ausente).
Los bytes de eventos ordinarios siguen idénticos; los invalidates antiguos se
reconocen por el historial retenido. No se fabrica el tipo de un evento perdido.
El checkpoint explícito puede continuar justo en la frontera retenida si toda
su continuación existe. Los tokens de eventos ya perdidos mantienen el rechazo.
ResumeAfter de invalidate falla con 260/InvalidResumeToken; startAfter continúa.

La selección de opciones reproduce los casos PyMongo capturados: startAfter
prima sobre resumeAfter, un startAfter vacío puede tomar su token y el reloj
se ignora si el driver selecciona un token. No se convierte la regla del comando
en un rechazo artificial de argumentos de watch. El reloj entero local conserva
su contrato; el corpus no compara valores de Timestamp/tokens entre backends.

La revisión corrigió dos errores espejo de historial: lectores activos que
cruzaban huecos sin detectarlos, y reaperturas que perdían huecos ya reflejados
en coordenadas persistidas. La metadata se reconstruye desde eventos/next_token;
una omisión sin evento posterior persiste también el high water. No cambia
schema SQL ni formato del journal. Los huecos fuera de retención se retiran y
la búsqueda en el camino de lectura usa intervalos ordenados, sin escanear todo
el pasado en cada evento. Los nuevos diagnósticos tienen identidad sin ampliar
la baseline de emisores sin código.

Cuatro casos permanentes suman veinte escenarios por major, con capturas y
--check 7/8/9; su replay cubre sync/async en los seis pares motor/dialecto.
Las pruebas deterministas cubren bootstrap con escritor concurrente, timeout
con escritura intercalada, filtros, pérdida de historial, cierre sin avance,
fronteras retenidas, tiempo futuro, reapertura y límites de metadata.
Un SQLite/journal realmente producido por ad2f030 conserva tokens ordinarios,
reconoce invalidates antiguos, recupera huecos y reanuda un checkpoint nuevo
tras otra reapertura. Los comandos/gates y costes están en el receipt.

D04 sigue abierto para evaluación de primera tanda/backlog y su fase de error,
full_document/maxAwaitTime y precedencias restantes, integración adicional con
imágenes y fixture RS de CI. Este corte no acredita esas garantías ni todo D05.
Pasan 6.292 de replay integrado, 2.764 de core/contratos (936 subtests, un skip),
100 con orjson, seis de API (doce subtests), typing, lint y smoke sin SDK.
El ABBA secuencial con default/updateLookup y sin watchers conserva checksums
y lectura final, con ratios de medianas entre 0,97 y 1,03 en los seis casos.

## D04 — Primera respuesta, batches y fases de opciones

El [receipt de batches](evidence/bson-consolidation-implementation/watch-batch/receipt.json)
registra el corte sobre f0a14fb y el contrato de revisión 11. El cursor existente
inicializa la respuesta una vez: sync al abrir, async al esperar watch o leer.
Evalúa backlog antes de devolver esa respuesta y materializa solo sus filas
admitidas. Se conserva un único pipeline/contexto, decoder y bucle de lectura.
El hub captura anchor y límite inicial junto con offset/checkpoint/watcher bajo
su guard; no hay otro formato, journal, secuencia, esquema SQL ni SPI.

La validación común reproduce fases y precedencia observadas: entrada,
aggregate inicial y getMore. full_document None equivale a default; cero no se
convierte en un segundo. Opciones negativas/bool dejan consumir filas ya
admitidas antes de fallar al pedir getMore. Un anchor retenido atraviesa el mismo
pipeline al reanudar, incluso sin cambio posterior. La invalidación filtrada
en primera tanda vacía conserva 260/alive=True según PyMongo, separada de la
invalidación recibida después. División/módulo por cero usan los diagnósticos
tipados comunes del evaluador aritmético, sin un traductor especial de watch.

La revisión corrigió errores que el testigo inicial de 6 MiB no detectaba:
prefijos y claves BSON del array, reserva del siguiente PBRT en getMore, evento
filtrado que cortaba la tanda en la frontera, y el límite 101 aplicado también
a getMore. Ese límite pertenece solo a la apertura. Se añade la frontera exacta
con el token propio de cada backend y el encoder BSON independiente; no se
comparan bytes de tokens opacos. Un error posterior dentro de una respuesta
descarta sus filas; overflow evaluado no se decodifica anticipadamente.

Los tests deterministas cubren apertura concurrente, cancelación/cierre durante
lookup, liberación del anchor al cerrar antes de inicializar, limpieza de
filas/overflow y entrega del prefijo válido antes de un gap al final del historial.
El helper suplementario async pasa el cliente real explícitamente: la antigua
resolución por collection.database.client probaba una colección llamada client.
Los escenarios corregidos se repiten localmente y en nativos para los tres ámbitos;
no se cambia la API ni se reescriben receipts anteriores para ocultar el error.

Siete familias permanentes suman 91 escenarios por major, con capturas y --check
7.0.43/8.0.32/9.0.2 y FCV correspondiente. El archive f0a14fb verifica sus 303
blobs y falla los 84 checks públicos nuevos; tres checks de metadata pasan.
El candidato pasa 6.379 checks de replay integrado y 2.783 de core/contratos
(936 subtests, un skip), 106 con orjson, seis de API (doce subtests), typing,
lint y el smoke sin SDK. Un SQLite/journal realmente anterior conserva datos,
índice y bytes ordinarios; la imagen prefetched sigue en v2 pese a escribir v3
antes de leer, y el checkpoint continúa tras otra reapertura.

La evidencia conserva feedback e interrupciones como diagnóstico, no aceptación.
El ABBA añade apertura/consumo de 101 updates de backlog a los caminos
default/updateLookup/unwatched; resultados y límites están en el receipt.
La lectura continua tiene ratios de medianas 1,03–1,14; sin watchers 0,99–1,01.
El backlog default de 101 eventos pasa de unos 1,1 ms a 2,8 ms (2,49–2,54×),
y updateLookup tiene ratios 1,05–1,25. Se registra el coste adicional; no se
declara rendimiento equivalente ni se omite la contabilidad BSON para reducirlo.
D04 permanece abierto para integración de imágenes con DDL/recreación y fixture
replica set de CI. D06, H53, conversión, piloto D07 opcional y D05 mantienen sus
obligaciones; este corte no acepta watch wire positivo ni cierra B07 general.

## D04 — Integración de namespaces/imágenes y respuestas BSON

Corte funcional sobre `a981610`, contrato refinado en `9eea45c`. El código queda
commiteado en `eb0074e` (respuestas BSON/ordenación) y `7dd54ab` (preparación de
merge). El [receipt del corte](evidence/bson-consolidation-implementation/namespace-images/receipt.json)
identifica runtime, corpus, wheel, comandos y límites. D04 conserva pendiente
el fixture replica set de CI; no se declara terminado el programa completo.

El corpus watch_namespace_image integra drop, rename y dropDatabase con modos
full_document, imágenes habilitadas/deshabilitadas, recreación y ámbitos de
colección/base/cliente. Cada acción recorre 48 escenarios. La barrera posterior
observa continuidad y la reanudación conserva eventos de recreación pendientes
tras invalidate. updateLookup lee el documento nuevo del namespace recreado;
las imágenes capturadas conservan el anterior. Se mantiene el hub/contexto y
el recurso de lookup existentes, sin identidad de encarnación ni otro outbox.
El código 47 se materializa como NoMatchingDocument, según los tres servidores.

La integración expuso otras salidas de la misma frontera. El materializador de
documentos ahora sirve también a streams de base/cliente. Un adaptador escalar
`materialize_public_value` normaliza wrappers propiedad del engine y decodifica
respuestas sin callbacks de documento. Lo comparten metadatos de colección,
IDs de upsert de APIs y comandos, y detalles de errores. BinData 0 sale como
bytes con SDK; los demás subtipos conservan Binary. Los IDs asignados de insert
y los documentos originales `writeErrors.op` mantienen su representación.
options, list_collections, índices parciales, duplicados y bulk parcialmente
fallido tienen testigos explícitos. Las clases se observan antes de serializar
el resultado, pues Extended JSON puede ocultar Binary 0 frente a bytes.

SQLite comparte una sola detección de la familia binaria para tags históricos
bytes/binary/uuid. Las guardas de query y sort la usan antes de ordenar y aplicar
skip/limit; no ordenan payloads JSON como si fueran valores BSON. La regresión
incluye rutas, índices y caché negativa antes de cambiar el tipo. No cambian
claves persistidas, índices, esquema, journal ni SPI v2.

El corpus merge_key detectó dos reglas adicionales: claves array fallan con
51185; ciertas combinaciones de modos se rechazan con 51181 aun sin documentos.
Una regla compartida valida los modos en preparación y ejecución. El preparador
analiza estructura/placement antes de optimizar expresiones y conserva dirección,
scopes y registro de extensiones. Se contrastan división constante, expresión
desconocida, aridad y variable indefinida anteriores al modo inválido. Esa
precedencia está caracterizada; no se afirma una ley universal para todos los
errores documentales. El destino permanece intacto tras los rechazos probados.

Los tres corpus aportan 11 familias con captura y --check en 7.0.43/8.0.32/9.0.2,
FCV correspondiente y replica sets propios de un miembro. Se usan el registro,
capturador, replay y observadores existentes. Watch se prueba por APIs locales
sync/async; las familias BSON/merge incluyen wire sync/async. No se añade watch
wire positivo. Las bases UUID se eliminan; los servicios prestados permanecen.

Pasan 6.616 pruebas del replay semántico completo y 3.238 del gate ampliado de
core/APIs/propiedades/contratos, con 1.008 subtests y 315 skips separados. Estos
skips no acreditan ejecución física. Las 237 regresiones del corte pasan también
con orjson y wheel instalado. El wheel coincide byte a byte con los 283 módulos
del manifiesto. Typing público/boundary y lint pasan. La base aislada real,
verificada en sus 303 blobs, reproduce 160 fallos y conserva 77 controles; la
regresión de sort binario reproduce ocho fallos y conserva cuatro controles.

El gate ampliado identificó además cinco fallos de tests ya presentes en la
base: clase/código de merge, fase de opción negativa de watch y un objeto usado
como sesión. Se corrigen las expectativas conforme al nativo y se usa una sesión
real, conservando los controles de corrupción/estado. El receipt separa esos
antecedentes y los intentos interrumpidos de la aceptación final.

Python 3.13 sin SDK comprueba los seis pares motor/dialecto y los tres ámbitos
sync/async, imágenes, recreación/startAfter y liberación de watchers. Un SQLite
y journal producidos por a981610 conservan claves e índices; el candidato escribe
mediante alias bytes, entrega la imagen v2 pendiente, reanuda tras invalidate y
continúa tras una segunda reapertura. No se acredita downgrade.

El ABBA secuencial mide 21 casos y diez muestras por runtime/caso, con checksums
idénticos y lecturas finales verificadas. Frente a a981610, los ratios de medianas
van de 0,963 a 1,036: streams de colección/base/cliente, sin watchers, backlog e
índices numéricos SQLite. El receipt conserva los tiempos absolutos. Es coste de
este incremento; no elimina el coste histórico de batching ni cierra D05 global.
D06, H53, conversión, decisión del piloto D07, D05 y B07 conservan sus pendientes.


## D04 — Fixture replica set de CI y cierre funcional

El código queda en `0b5ac53`, sobre el runtime aceptado `e8abf99` y el plan
refinado en `e4a9644`. El [receipt](evidence/bson-consolidation-implementation/replica-set-ci/receipt.json)
conserva workset, imágenes fijadas, comandos, informes y límites. Este corte
completa la obligación restante de D04; D05 conserva la aceptación integrada.
No se afirma que haya ocurrido una ejecución remota de GitHub Actions.

La selección de casos pertenece a `select_cases`: listado, suite y reporte
comparten filtro/versión/partición. `SemanticCorpus` y `RealParityCase` declaran
si requieren replica set; no se infiere de un prefijo watch, pues TTL y eventos
de updates también lo necesitan. Los manifiestos históricos no cambian. El job
existente conserva su matriz de imágenes y servicio standalone, añade el fixture
RS y acredita la unión disjunta de ambas particiones. Una partición vacía por
filtro queda sin ejecutar; una selección total vacía o un skip físico no pasa.

El wrapper posee exclusivamente ciclo de vida: nombre/label UUID, binding
loopback efímero, primary/buildInfo/FCV, oplog de 128 MiB y monitor TTL de un
segundo para estos escenarios. Startup, solicitudes Docker y payload tienen
límites. El mismo guardado de cancelación protege solicitudes Docker y la
creación del proceso antes de entregar una señal pendiente. Timeout de create
conserva incertidumbre hasta inspección. La limpieza comprueba propiedad y
elimina contenedor, volúmenes anónimos y grupo del payload, incluidos hijos que
ignoran TERM. Un fallo de limpieza no convierte el resultado en verde.

Se ejecutaron localmente la selección y el cuerpo RS del workflow contra el
wheel inmutable del corte anterior. Un harness diagnóstico reproduce el servicio
standalone y su CLI, sin ejecutar startup RS. Los 283 módulos de producto siguen idénticos.
Cada versión 7.0.43/8.0.32/9.0.2, FCV correspondiente, pasa 266 casos standalone
y 48 RS: 314 por versión, sin pérdidas, duplicados ni skips físicos. Las 12
capturas RS por servidor pasan --check contra referencias intactas; el replay
acredita 192 nodos por dialecto, 576 en total. No se ejecutan otros dialectos
por duplicado en cada celda. Se conservan JSON/JUnit, metadata y logs nativos.

Pasan 96 pruebas unitarias y 61 subtests; 314 skips de clases físicas importadas
se registran aparte, sin contarlos como ejecución nativa. Lint y bash/YAML pasan.
Los controles físicos finales cubren salida 7, timeout, versión incorrecta,
cancelación en startup, payload y pipeline shell, y señal durante el retorno
real de Popen. Verifican códigos 7/1/143 y limpieza independiente. La auditoría
final confirma ausencia de los 13 contenedores de aceptación y sus volúmenes.
Dos pruebas de mutación documentan las ventanas de cancelación corregidas;
son mutaciones aisladas, no una baseline histórica del producto.

Los wrappers de éxito 7/8 habían cargado el helper anterior a las dos últimas
protecciones de cancelación. Su fuente se reconstruye y verifica contra el
hash de revisión 13: payload, topología, runner, wheel y camino exitoso no
cambian. El wrapper 9 y los siete negativos físicos usan el helper final.
El receipt explicita esta reutilización causal; no presenta las tres entradas
como ejecuciones de un mismo archivo cuando no lo fueron. La prueba standalone
también precede los últimos cambios del lifecycle: se reutiliza solo su resultado
nativo con selección/CLI/producto idénticos, no como garantía de cancelación final.

Los intentos diagnósticos fallidos conservan sus artefactos y no cuentan como
aceptación: conectividad perdida en la primera ejecución paralela, tratamiento
de ausencia Docker, volúmenes y carrera de startup. Se acredita la propiedad
y limpieza de recursos de prototipos; no se hizo prune global ni se reiniciaron
servicios prestados. La aceptación final es secuencial. El trabajo restante
vigente es D06a/b/c, H53, conversión sNaN, decisión D07, D05 y B07, conforme al plan.

## D06a/b — Checkpoint de gramática y representación, pendiente de aceptación

Base `57b5faf`. La [evidencia del checkpoint](evidence/bson-consolidation-implementation/bson-grammar-context/receipt.json)
identifica fuentes, corpus, capturas, baseline y candidato. D06a/b siguen
abiertos; este registro conserva trabajo en curso y no acredita su cierre.

`is_bson_string` comparte la distinción String/Code entre el evaluador,
preparación, constantes, argumentos de nombre de campo y seguimiento de campos
virtuales. `$type` conserva javascript frente a javascriptWithScope. Las claves
de documentos conservan su contrato de cstring; no se sustituyen sus controles
por reglas de valores de expresión.

La materialización de respuestas convierte formas DBRef anidadas conforme al
SDK, conservando la raíz documental y la raíz del scope de Code como documentos.
Ese contrato es diferente del type bracketing: el hook SDK admite Code en ref/db
y db null. Fechas y callbacks de hijos se aplican una vez antes del callback del
contenedor, sin mutar el input. La deduplicación de distinct pasa a ocurrir antes
de esa conversión; API y comando comparten el colector semántico. Dos BSON
físicamente distintos pueden dar dos DBRef públicos idénticos y ambos deben
permanecer en la respuesta.

El cursor local prepara una primera respuesta al abrir/await y conserva su
buffer para el consumo posterior. Se contrastan batchSize omitido/0/1/2 y errores
en filas 1/3/103. El driver conserva el tamaño inicial solicitado y promueve 1 a 2
en getMore; las capturas fijan el comportamiento de PyMongo 4.19.0 y su
[implementación fijada](https://github.com/mongodb/mongo-python-driver/blob/4.19.0/pymongo/cursor_shared.py#L237)
explica esa promoción. El adaptador
async de pruebas devuelve e itera cursores reales: antes materializaba la lista
al devolver aggregate y podía ocultar diferencias de fase. La nueva sonda
ordena explícitamente por `_id`: su diagnóstico inicial sobre orden natural en
SQLite no acreditaba una divergencia de ordenación del engine.

Diez acciones tienen captura y --check en 7.0.43/8.0.32/9.0.2, FCV correspondiente,
además de --check de los cinco controles DBRef históricos. Los tres fixtures
propios finales y sus volúmenes están ausentes en la auditoría independiente.
La baseline aislada verifica los 283 módulos y su import real: falla 200 checks
y pasa 43. El candidato congelado pasa 235 y conserva ocho fallos wire ya rojos
en baseline; no aparecen nuevos nodos fallidos en ese corpus. Pasan 3.035 de
core/API y 1.017 subtests, con un skip separado. La mutación que deduplica después
de decodificar falla los 24 nodos de distinct. Lint pasa para 19 Python cambiados.

El replay completo anterior, sobre otro candidato y nueve acciones, terminó
con 6.815 correctas y 20 fallos. Es diagnóstico, no gate global verde del
checkpoint actual. Los primeros 168 fallos/27 controles usaban ocho acciones;
sus fuentes y capturas están archivadas sin atribuirles alcance posterior.
Las referencias históricas y observadores protegidos permanecen intactos.

**Trabajo obligatorio antes de aceptar este corte:**

- Wire materializa todavía toda la agregación antes de abrir: ocho nodos de
  primera respuesta/getMore siguen fallando en 8/9. Conectar un cursor vivo al
  almacén existente conservando parser/preparación, contexto, failpoints,
  profiling, sesión, principal y namespace; cerrar recursos en EOF/error/kill
  y teardown. No introducir otro runner ni omitir esos checks.
- Mantener codificación BSON del servidor antes de la decodificación SDK de
  formas DBRef. El límite público directo y el transporte raw tienen destinos
  distintos; convertir antes de codificar puede perder orden/campos. Reutilizar
  la lectura sin codecs que ya ofrece el cursor y materializar al entregar.
- Contrastar límite por bytes, sufijos materializantes, cancelación y atomicidad
  de callbacks por respuesta. La cardinalidad de una extensión no puede inferirse
  del nombre de un built-in que haya sobrescrito. El testigo de número de documentos no acredita
  esas otras fronteras ni un protocolo completo de aggregate.
- Completar consumidores String tipados, contextos DBRef/raw y claves de D06;
  repetir revisión/gates sobre candidato final, wheel, perfil sin SDK, costes y
  las matrices exigidas por sus owners. H53, conversión, D07, D05 y B07 conservan
  sus pendientes. El código de este checkpoint sigue siendo un candidato.

## D06a/b — Checkpoint de cursores vivos y apertura del driver

Continúa el checkpoint anterior sobre `57b5faf`, sin reescribir su receipt.
El [nuevo registro de evidencia](evidence/bson-consolidation-implementation/bson-grammar-live-cursors/README.md)
identifica código, wheel, capturas y diagnósticos. D06 sigue abierto.

Wire conserva ahora fuentes vivas en el almacén existente. Find/aggregate usan
el mismo parser y ejecutor de comandos, con contexto, failpoints, profiling y
sesión; un callback interno entrega la respuesta inicial al almacén. La API
pública de command no adquiere ese callback. GetMore conserva namespace,
principal y sesión, serializa lecturas por cursor y cierra en EOF/error.
Kill/teardown comparten cleanup que termina aunque el caller sea cancelado y
recoge fallos de todas las fuentes. Una lista kill mal formada se valida antes
de retirar ninguna fuente. No se crea otro parser, runner ni store paralelo.

El transporte pide valores sin codecs del usuario para codificar BSON antes
del hook SDK de DBRef; la lectura pública conserva su propia materialización.
Esto no acredita todavía todas las salidas raw. El testigo llamado
`raw_nested_values` observa valores públicos: su nombre no demuestra igualdad
de bytes de raw batches ni de envelopes wire.

Las extensiones registradas impiden el pushdown de la etapa sustituida y la
selección de group/sort incremental que asumía una implementación built-in.
Los nombres `$skip`/`$limit` tampoco permiten extraer ventanas de un handler
sustituido. Los tests distinguen siete formas, incluida una especificación de
group que solo acepta la extensión; las mutaciones de las guardas de pushdown
y de respuesta fallan. El cálculo de cardinalidad de entrada solo usa garantías
de built-ins no sustituidos.

La apertura pública reproduce la política PyMongo de omitir el batchSize
inicial de una escritura `$merge`, también cuando el usuario pide cero. La
entrada raw conserva el cero y difiere la evaluación. El nuevo corpus tiene
once acciones: incluye `$merge` con especificación documental, tamaños
omitido/0/1 y estado del destino antes de consumir el cursor. Los tres nativos
tienen captura, --check de esas once acciones y --check de los cinco controles
DBRef históricos. La baseline aislada conserva 283 hashes verificados y falla
12 de los 24 nodos nuevos; las rutas wire ya pasaban ese testigo.

Las sondas adicionales de getMore fijan 13 para namespace, 50736 para añadir
sesión a un cursor sin ella, 50737 para omitirla y 50738 para cambiarla en 7/8/9.
Los tests verifican además rechazo de otro principal sin consumir filas,
continuidad por otra conexión del mismo principal, getMore concurrentes y
cleanup cancelado. El código 13 del principal también tiene respaldo en el
parser de getMore oficial; las sondas sin autenticación no acreditan un
escenario físico multiusuario. No se modifica el inventario de errores legacy.

Pasan 3.183 pruebas core/API/wire, con 1.110 subtests y un skip separado. El
wheel instalado sin red coincide por hash con los 283 módulos y pasa los 267
checks focales. El smoke sin SDK pasa también desde ese wheel en Python 3.13,
con seis pares motor/dialecto y ámbitos sync/async. Typing público y canario SPI
pasan. El check canónico de manifiesto público conserva 11 diferencias frente
al snapshot antiguo; el manifiesto efectivo del candidato coincide con la
baseline. Es un gate pendiente de D05, no una aceptación ni motivo para cambiar
un snapshot por conveniencia.

El replay completo del candidato congelado termina con 6.883 checks correctos.
El benchmark del pipeline ordinario sort/set/project usa 3.000 filas,
dos secuencias ABBA por motor y salidas BSON idénticas a la baseline. Sin otros
gates del repositorio activos, las razones de medianas candidato/base son
1,009 en Memory y 1,026 en SQLite. Es una medida acotada de streaming, no el
gate completo de costes de D05 ni una conclusión estadística sobre cada carga.
Los commits de producción son `d237b97` (BSON) y `1420b16` (respuestas/cursores).
Un commit y un replay verde no cierran los pendientes siguientes.

**Pendientes verificados que impiden aceptar D06:**

- Límite por bytes y codecs de usuario por respuesta, además de número de filas.
- Sufijos tras etapas materializantes: la sonda nueva de set/sort/project con
  error en fila 103 entrega 101 filas antes de fallar con código 2 en los tres
  servidores, con allowDiskUse false/true. El candidato falla al abrir y no
  entrega filas. Completar el particionado/evaluación de sufijos en los caminos
  existentes; un bound aplicado solo al pipeline streamable no cubre sort/group.
- Raw batches directos y otras salidas wire: observar bytes antes del hook SDK,
  orden/extras/db null y controles Code.scope. No deducir esa garantía de la
  lectura pública ni corregir la forma BSON después de perderla.
- Conservar la distinción entre apertura SDK y comandos raw con cero. La API
  directa database.command no tiene todavía una garantía de cursor vivo por
  asociación con el store wire.
- Consumidores String restantes y D06c; después revisión, costes y matrices
  integradas de D05. H53, sNaN, D07 y B07 conservan sus obligaciones.

## D06a/b — Sufijos por respuesta y conservación de tipos

Base `7328feb`. La [evidencia del corte](evidence/bson-consolidation-implementation/aggregation-response-suffix/README.md)
identifica las siete acciones nativas, la baseline y el candidato. No reescribe
el diagnóstico del checkpoint anterior ni acepta todo D06 por asociación.

Group, salida de group ordenada, sort incremental y pipeline windowed usan
ahora el mismo planificador de demanda por número de documentos. Continúa
llamando a `apply_pipeline`; no introduce otro evaluador de operadores. Las
fronteras entre prefijo materializante y sufijo streamable conservan slices
preparados con sus direcciones lógicas, dialecto, bindings y reloj originales.
El presupuesto/deadline del prefijo se transmite desde la ejecución que lo abre.

Una expansión no permite evaluar todo su sufijo antes de devolver la primera
tanda. El planificador mantiene iteradores de salida pendientes y aplica los
bloques siguientes según la demanda, incluso si un documento genera 226 filas.
La pila es explícita: el test de 995 etapas evita introducir un límite nuevo
por recursión Python. Las guardas de extensiones sustituidas se conservan.

La ejecución del prefijo devuelve valores BSON internos. `_materialize` sigue
siendo el adaptador para salidas públicas completas; `_materialize_pipeline`
posee la ejecución reutilizada, sin convertir valores antes de otro stage.
Un intento anterior convertía un Long pequeño a int en el perfil sin SDK y
alteraba `$type` en el sufijo. El testigo nativo de tipos y la regresión con
SDK ausente cubren esa diferencia; no se corrige envolviendo todos los enteros.

Los recursos del programa se cargan una vez por ejecución y se comparten entre
prefijo/sufijo. Se liberan de la referencia del cursor al cerrar. Las pruebas
comprueban el reloj y let estables, una carga de la colección extranjera y
ausencia de archivos de sort/group tras error, también forzando spill a dos
documentos en ambos motores.

Las siete acciones se capturan y comprueban en 7.0.43/8.0.32/9.0.2 con FCV
correspondiente. Tienen orden de entrada explícito. El replay focal pasa 171
checks; la baseline aislada falla 120 y pasa 51 controles. Los fallos iniciales
de semilla vacía y la suposición de orden natural de SQLite se conservan como
feedback, no como defectos del runtime. Las referencias históricas y los
observadores protegidos no se modifican.

El wheel final comprueba sus 283 módulos contra el candidato. El perfil real
Python 3.13 sin SDK ejecuta 72 escenarios de respuesta/spill y 24 de tipos,
incluidos ambos modos de disco y sync/async. Las cuatro mutaciones detectan
la retirada del bound, evaluación completa de la expansión, materialización
del sufijo entero y conversión pública prematura del prefijo.

El replay previo de 7.030 checks pasó antes de detectar la conversión prematura;
su input queda identificado y no sustituye al gate final del código corregido.
El código corregido queda commiteado en `d9e9579`: pasan 7.054 checks del replay
completo, 3.188 de core/API/wire y 1.128 subtests, con un skip separado. El wheel
instalado pasa los 171 checks finales. Typing público y canario SPI v2 pasan;
el manifiesto público efectivo coincide con la baseline. El drift histórico de
su snapshot y el resto de matrices siguen perteneciendo a D05.

La medición sin otros gates activos usa 48 muestras ABBAABBA, 1.500 documentos
de entrada, spill local a ocho documentos y hashes BSON completos idénticos.
Razones de medianas candidato/base:

| Carga | Memory | SQLite |
| --- | --- | --- |
| Sort y sufijo | 0,998 | 1,027 |
| Group/sort y sufijo | 1,021 | 1,027 |
| Unwind y sufijo | 1,169 | 1,166 |

El aumento de unwind se registra como coste de esta muestra; no se presenta
como mejora de rendimiento ni como presupuesto global de D05. El corte cierra
la demanda por número de documentos de estos siete testigos y la conservación
del tipo interno entre prefijo/sufijo, manteniendo las regresiones anteriores.
Bytes/callbacks por respuesta, raw y los demás contextos/consumidores de D06,
H53, sNaN, D07, D05 y B07 conservan sus pendientes y sus owners.


## D06a/b — Lecturas raw, apertura y cantidades

Base `7f8110a`; implementación `30c8ae2`. La [evidencia del corte](evidence/bson-consolidation-implementation/raw-response/README.md)
registra seis acciones nuevas con capturas y comprobación 7.0.43/8.0.32/9.0.2,
FCV correspondiente. Son testigos de contenido BSON, cantidades y apertura;
no aceptación de bytes/callbacks ni de todos los contextos raw.

Find raw reutiliza el iterador y no vuelve a consultar por offset ni aplica
codecs de contenido. La primera tanda por defecto conserva el máximo 101;
getMore sin cantidad fijada puede entregar más. Aggregate raw usa la preparación
existente y abre con primera tanda vacía; su getMore solicitado a 1 se promueve
a 2, como el driver. El `$merge` soportado ejecuta al abrir en sync/async,
incluso con batchSize 0. No se amplía el conjunto de etapas de escritura soportadas.

El testigo de codecs encontró un error compartido de transporte: `cursor.id`
pequeño se emitía como int32 y activaba el decoder de int del usuario sobre
metadatos. La respuesta lo codifica como int64, incluido cero; la entrada
normaliza getMore/killCursors como controles, conservando los anchos en datos
BSON. Las pruebas comprueban bytes/tipo y que no se muta el input ni el documento
anidado que también contiene un campo cursor. Un fallo de apertura no permite
consumir; cleanup conserva el error original y termina bajo cancelación repetida.
El close explícito fallido sigue permitiendo reintento.

La baseline aislada verifica 283 módulos e import real: falla 84 checks y pasa
63 controles. Candidato y wheel instalado pasan 147. El replay integrado final
pasa 7.201; core/wire/API 3.202 con un skip y 1.842 subtests; opciones/fronteras
466 y 108 subtests. Python 3.13 realmente sin SDK pasa 120 escenarios: 96 de
respuesta/tipos anteriores y 24 guardas raw de dependencia opcional. Pasan typing,
SPI y lint; el manifiesto público efectivo coincide con la base. Los logs de
setup y la expectativa antigua de aggregate raw a 1 se conservan como feedback.
Observadores protegidos y goldens históricos permanecen intactos.

## D06a/b — Diagnóstico para el siguiente corte de bytes y codecs

La [caracterización](evidence/bson-consolidation-implementation/response-frontier-diagnostic/README.md)
separa los pendientes de los gates anteriores. En los tres servidores, cinco
documentos cercanos a 4 MiB se dividen por bytes; local los devuelve juntos.
Con error en el documento 5, los servidores pueden entregar tres antes del error
de getMore y local no entrega ninguno. En una frontera más estrecha, documentos
de 4.194.288 bytes producen primero cuatro en find raw de MongoDB 7 y tres en
8/9; aggregate raw produce tres en todos. No basta sumar tamaños ni suponer una
única sobrecarga para rutas/dialectos.

Dos documentos `v=[1]` y `v=[0]`, unwind y divide, prueban la falta de atomicidad
de codecs: ante error de apertura, local invoca el decoder de `_id=1` y los tres
servidores no lo hacen. El control de un único documento `v=[1,0]` sí coincide;
el control no-raw a batchSize 1 entrega una fila y ejecuta un callback antes del
error de getMore. No se trata de prohibir callbacks tras toda ejecución parcial.

Sigue: registrar estos testigos en el capturador/replay existente, fijar la
frontera exacta por ruta/dialecto y corregir el coordinador de respuesta sobre
el cálculo BSON/contexto actuales. Conservar el documento pendiente sin
reevaluarlo y separar evaluación de respuesta de codecs públicos. Después,
resto de contextos/consumidores D06c. La inspección independiente confirma que
los 15 contenedores y 30 volúmenes propios de estos cohorts ya no existen.
H53, sNaN, decisión D07, D05 y admisión B07 conservan sus pendientes.

## D06a/b — Fronteras por bytes, capacidad y efectos de codec

`381aa10` entrega el siguiente corte sobre `e941554`. El
[receipt](evidence/bson-consolidation-implementation/response-frontier/receipt.json)
identifica la fuente final, los fallos previos, las capturas y sus límites.
Nueve acciones de respuesta y dos de comandos amplían registro/capturador/replay
existentes; se capturan y comprueban con MongoDB 7.0.43/8.0.32/9.0.2, FCV de su
major, en replica sets propios fijados por digest. No cambian observadores,
goldens históricos ni catálogos protegidos.

`ResponseBatchReader` comparte cota por cantidad/bytes y conserva un documento
evaluado que no cabe, sin reevaluarlo. El framing distingue primer find 7 de
8/9, aggregate y getMore; el documento devuelto admite hasta 16 MiB + 16 KiB
inclusive en las referencias observadas. La capacidad String calculada es otra
frontera: `$concat`/`$replaceAll`, UTF-8 y cola null prueban la guarda compartida
del evaluador existente. No se crea otro intérprete ni se cierra admisión B07.

La respuesta se evalúa antes de materializarla y ejecutar decoders públicos.
Raw conserva BSON sin callbacks; los adaptadores sync consumen una respuesta
cada vez. Fase/clase/código/codeName/errmsg se contrastan en apertura/getMore y
ante fallos del servidor o decoder. El control de entrada pequeña/salida grande
también falla al abrir en los tres servidores: se conserva esa prelectura, sin
introducir una fuente unitaria por una hipótesis local. El acceso primario
probado con `_id_` ordena antes del residual; `v_1` fuerza el control bloqueante.
Ni skip ni límite se adelantan a un residual eliminatorio.

El cierre conserva recursos hasta completar cleanup y permite reintento sin
sustituir el error original. Los tests cubren overflow, rewind, cancelación
repetida y fallos de cierre. Los tres constructores internos, incluidos sus
fallbacks, desactivan codecs y difieren diagnóstico al aggregate propietario.
Se corrigen además dos pérdidas de frontera compartidas: wrappers numéricos que
escapaban sin SDK, concentrando conversión en el materializador público, y slices
streamables que perdían PreparedPipeline, conservando preparación y direcciones
sin otra caché ni debilitar invalidación por contexto/registro.

En wire, getMore con batchSize cero elimina la cota por cantidad y killCursors
codifica sus arrays como int64, con Unknown vacío cuando se conoce el estado.
Las guardas de namespace/sesión/principal y los arrays del usuario se conservan.

Aceptación: base aislada con 283 módulos verificados, 228 fallos/18 positivos en
el corpus final; candidato con **7.447 checks de replay completos**, 3.216 pruebas
amplias y 1.120 subtests, 110 de integración y 1.211 subtests. El wheel limpio
coincide por hash en sus 284 módulos y pasa 246 checks. Python 3.13.15 sin SDK
pasa 180 escenarios de fases/spill/tipos/raw/bytes/String. Typing, SPI v2, lint y
manifiesto público efectivo conservan sus contratos. La auditoría independiente
comprueba ausencia de 30 contenedores y 60 volúmenes propios.

El coste diagnóstico ABBA mantiene resultados BSON: ratios CPU candidato/base
Memory sort/group/unwind/Strings grandes 1,22/1,07/0,96/1,36; SQLite
1,16/1,12/0,95/1,23. Son 32 muestras calentadas, sin otros gates propios activos;
no se afirma ausencia de coste ni aceptación del presupuesto global D05.
La guarda y formación de respuestas añaden trabajo, especialmente con Strings
grandes. Los demás contextos D06a/b, consumidores D06c, H53, sNaN, decisión D07,
D05 y B07 mantienen sus obligaciones.


## D06a/b — Dispatch de codecs, consumo y ownership de respuesta

El commit `a53824d` entrega este corte funcional acotado, con
[evidencia y límites](evidence/bson-consolidation-implementation/codec-dispatch/README.md).
No cierra D06a/b completos, D06c ni D05. Conserva el contrato autorizado y los
cortes anteriores; el default UUID sigue siendo STANDARD hasta su coordinación
con admisión B07, conforme a la alternativa de revisión 15.

El materializador común conserva el batch transformado por el SDK, con dispatch
exacto y de pase único y orden batch/id/ns. Query y command cursors consumen ese
batch según contratos propios: la cuenta del cliente puede diferir del número
de documentos del origen; un iterable sin longitud tiene tratamiento distinto.
Find entrega None como dato, iterar aggregate lo omite y to_list lo conserva.
La omisión ocurre al consumir cada fila, para que intercalar ambos métodos no
pierda las restantes. Las tandas vaciadas conservan su origen y la limpieza GC
del iterador temporal no puede cerrarlo antes de una reanudación.

Se reutilizan lector, contexto de operación, runtime de materialización y
cierre existentes. Find_one aplica capacidad y codecs a una respuesta completa,
incluso vacía, preservando el acceso por _id y el diagnóstico de ejecución.
EOF conocido por límites/ventanas se comunica sin otra evaluación y respeta
el último documento pendiente por bytes. No se añade otro evaluador, caché,
registro de cursores ni dependencia obligatoria del SDK. La lectura UUID conserva
subtipo 4 bajo modos legacy e interpreta los bytes de subtipo 3 por representación.
El owner de orden/residual reconoce las formas equivalentes de hint _id y
collation simple; no adelanta skip/limit al residual.

Validación final de la fuente congelada: 15 acciones capturadas/comprobadas en
MongoDB 7.0.43/8.0.32/9.0.2 con FCV correspondiente; 363 focales públicos y
7.810 casos del replay completo sobre el wheel instalado. Los cuatro workers
verifican la procedencia de sus 262 módulos cargados y los hashes de los 284
archivos disponibles. Pasan 360 unitarias/contratos con 43 subtests, typing con
dos positivas y 20/25 errores negativos, y 186 escenarios en Python 3.13.15
real sin SDK. La base archivada `181a922` falla 300 de los 363 focales nuevos.
El control GC falla en ambos motores sobre el candidato anterior; la barrera
reproduce fila 1 repetida sin depender de sleeps.

El manifiesto público efectivo conserva su hash frente a `181a922`. No se
regeneran observadores, catálogos ni goldens históricos. Se verifica ausencia
de 33 contenedores y 66 volúmenes propios. ABBA dos veces, con gates propios
inactivos y resultados BSON idénticos, da ratios CPU 0,78–1,03 en 16 pares
motor/opción/operación; es tendencia diagnóstica, no todo el coste D05.

La suite amplia anterior es feedback, con materiales cambiantes: 42.290 casos
correctos, 111 reportes de fallo y un skip. Sus 99 nodos únicos dan 97 fallos y
dos correctos en la base; los dos deltas de Binary público se corrigen con SDK
y seed STANDARD explícito. D05 conserva los 97 nodos previos y sus gates
obligatorios, además de matrices Python/SDK/pytest, cobertura y distribución.
Las proyecciones calculadas de find siguen fuera del subconjunto declarado;
la sonda final de String/errores de find_one usa $expr soportado.

→ Sigue: contextos restantes D06a/b, incluidas fábricas documentales con efectos
propios y otras combinaciones de consumo/estado y representación portable;
consumidores y claves D06c; H53, sNaN, decisión D07, D05 y admisión B07.

## D06a/b — Construcción y asignación de documentos de respuesta

`d4caba5` entrega este corte sobre `220cedc`, con
[evidencia y límites](evidence/bson-consolidation-implementation/document-factories/README.md).
No cierra D06a/b completos, D06c ni D05. La envoltura del comando y los
contenedores intermedios de selección forman parte de los efectos observables
de document_class, aunque no se devuelvan al usuario. El materializador conserva
su construcción/asignación y usa APIs públicas BSON para la decodificación;
el portable construye antes de recorrer campos. No hay otro driver o evaluador.

Cuatro acciones capturadas/comprobadas con MongoDB 7.0.43/8.0.32/9.0.2 standalone,
FCV correspondiente, cubren find/find_one/aggregate, con/sin registro, anidados y
Code.scope, errores de constructor/setter, vacío/poblado y batch 0/1/2/getMore.
Se comparan efectos previos, fase y clase del error, además de las filas.
La base aislada 220cedc falla 48 y pasa 51 controles del mismo corpus final.

El fixture existente admite standalone explícito y conserva RS por defecto.
Registros, preflight, capturador y CI declaran esa exigencia; mezclas que exigen
ambas topologías requieren particiones separadas. El control sobre un RS real
rechaza el corpus standalone antes de escribir una referencia. No se simulan
metadatos de reloj ni se normalizan constructores para igualar topologías.
Los manifiestos históricos, observadores y catálogos protegidos no cambian.

El wheel final coincide por hash en sus 284 archivos y pasa 7.909 checks de
replay, con procedencia verificada en cada worker. Pasan 362 unitarias/contratos
con 43 subtests, 45 de infraestructura y 47 de materialización; typing y el
manifiesto público conservan sus contratos. Python 3.13.15 realmente sin SDK
pasa 72 escenarios nuevos y 186 controles anteriores. La guarda portable evita
un callback int espurio sobre cursor.id y no expone su wrapper interno al setter;
un id de los datos sigue ejecutando su decoder. Los replay anteriores a ese
último cambio se conservan como feedback de fuentes distintas.

Se verifica ausencia de 13 contenedores y 26 volúmenes propios. ABBA dos veces
con BSON idéntico conserva su alcance diagnóstico, sin aceptar el presupuesto
D05 ni afirmar aislamiento de tareas externas. No se acredita CI remoto,
cobertura global ni las matrices completas Python/SDK/pytest. El registro de
trabajo restante conserva otros contextos/representaciones y consumo/estado
D06a/b, D06c, H53, sNaN, decisión D07, D05 y B07. El default UUID sigue STANDARD.

## D06a/b — Posición, propiedad y cierre de cursores sync

`72edfa3` entrega el corte sobre `d50ab62`, con
[evidencia acotada](evidence/bson-consolidation-implementation/cursor-consumption/README.md).
No cierra D06a/b completos, D06c ni D05. Se retiran la caché que reproducía
resultados consumidos, el atajo eager de iteración con límite uno y el camino
muerto equivalente de aggregate. Alive combina el estado async con las filas
pendientes sync; no se crea otro estado o caché para coordinar los dos.

Los iteradores son vistas prestadas: desaparecen sus finalizadores y el estado
suspended que solo cubría decoders que vaciaban tandas. El propietario cursor
conserva cierre explícito y finalización diferida, también al abandonarlo. Las
pruebas deterministas drenan cleanup antes de reanudar y comprueban ambas caras.
Aggregate delega en close del cursor async real, incluso antes de iterar; conserva
fallback legacy y reintento tras fallo. Se corrige el fake aclose-only que ocultaba
la ruta rota y se comprueba el delegado concreto de Memory/SQLite.

El primer replay completo pasó con 114 warnings de corutinas sin esperar y no se
aceptó. El runner disponía el awaitable rechazado por reentrada, pero no por
closing/closed. Una única disposición en ese componente existente cubre ambos
rechazos sin cambiar el error original. El test comprueba el estado antes de su
propio cleanup; el guard anterior se conformaba con que se lanzara el error.

Cinco acciones congeladas se capturan/comprueban con MongoDB
7.0.43/8.0.32/9.0.2 standalone, FCV correspondiente e imágenes fijadas. Cubren
lectura completa/acotada, iteración intercalada y temporal, rewind/clone, alive,
respuestas vacías y decoders que expanden/vacían/retornan None. La base aislada
falla 24 checks nativos, cuatro de ownership y dos subtests de admisión.
El wheel final pasa 8.032 checks completos sin warnings, con RuntimeWarning y
avisos de GC/hilos elevados a error. Los cuatro workers verifican los 284 archivos
y la procedencia de sus 262 módulos cargados. Pasan 421 pruebas instaladas de
contratos/runner/SPI con 49 subtests y 20 de integración con 19 subtests.

Python 3.13.15 realmente sin SDK compara 30 acciones/1.278 trazas tipadas contra
los oráculos nativos y conserva los 258 controles anteriores, incluidos async.
Typing, manifiesto público y lint no cambian sus garantías. La auditoría confirma
ausencia de 12 contenedores y 24 volúmenes propios. ABBA dos veces verifica BSON
idéntico en 40 pares; Memory find(limit=1) default/STANDARD muestra ratios CPU
1,16/1,17. Es diagnóstico: D05 conserva el coste y su revisión, sin afirmar
rendimiento equivalente ni aceptar el presupuesto global. No se acredita CI
remoto, cobertura global ni las matrices completas.

→ Sigue: demás contextos/representaciones D06a/b, consumidores D06c, H53,
sNaN, decisión opcional D07, D05 y admisión B07. No se amplían métodos públicos
no admitidos ni se cambia la conveniencia local length=0; UUID sigue STANDARD.


## D06c — Claves de consumidores y ejecución de grupo

`332d07a` entrega este corte sobre `7629ea6`, con
[evidencia y límites](evidence/bson-consolidation-implementation/bson-consumers/README.md).
Grupo recorre la vista documental común de DBRef; el índice de conjuntos usa
átomos BSON en su dominio anterior y conserva Code en residual. Se retiran la
clave duplicada y sus exports privados, manteniendo la guarda escalar efectiva.
No cambia la admisión general B07, las claves persistidas ni los desempates.

El diferencial descubre un defecto adicional: la conversión Code→String del
representante de grupo dependía solo del dialecto y se aplicaba a subpipelines
clásicos. La decisión preparada que ya gobernaba average pasa a llamarse
`group_uses_slot_semantics`, con owner `group_execution.py`; se conserva en plan,
bucket, slices y spill. Gobierna ambos comportamientos y elimina el parámetro de
dialecto redundante de finalización. La identidad del grupo sigue siendo distinta
de la representación de su ID; no se emula un planificador nativo general.

Cinco acciones congeladas se capturan/comprueban con MongoDB
7.0.43/8.0.32/9.0.2 standalone y FCV correspondiente. Observan miembros y tipos,
BSON raw exacto de representantes en seis rutas con/sin collation, tamaños y
pertenencia de conjuntos, addToSet y lookup. Los tests fuerzan spill y comprueban
particiones abiertas; hash suficiente/agotado/ausente se acredita observando el
plan real, separado del límite de materialización. Las propiedades extienden
coherencia de clave/serialización y conservación de candidatos de dominios BSON
no representados en índices materializados Memory/SQLite.

La base Git aislada falla 88 y pasa 101 controles del corpus final. El wheel
final pasa 8.221 checks de replay en 200,26 s sin avisos, con RuntimeWarning y
avisos de GC/hilos elevados a error; cada worker verifica sus 262 módulos y los
284 archivos instalados. Pasan 675 unitarias/propiedades/contratos con 159
subtests; typing y manifiesto público conservan sus garantías. Python 3.13.15
realmente sin SDK pasa doce acciones DBRef/Binary derivadas de los mismos
miembros nativos, con exclusión explícita de Code no admitido, además de los
controles portables anteriores. No se presenta como replay íntegro sin SDK.

La auditoría independiente confirma ausencia de seis contenedores y doce
volúmenes propios. ABBA dos veces, con los gates propios terminados, conserva
BSON idéntico y ratios CPU 0,99–1,01 en seis casos; es diagnóstico, no aceptación
del presupuesto global D05. Se conservan observadores, catálogos y las 248
referencias históricas. Las capturas nuevas se ampliaron antes de congelar el
candidato; los replay intermedios conservan su carácter de feedback.

→ Sigue: demás contextos D06a/b/c, incluida persistencia/reapertura pública de
Search, H53, sNaN, decisión opcional D07, gates D05 y admisión B07. Este corte no
acredita CI remoto, cobertura global ni matrices completas. UUID sigue STANDARD.


## D06c — Contexto de candidatos, ranking y reapertura Search

`fbc7cde` entrega este corte sobre `47336ce`, con
[evidencia, fuentes y límites](evidence/bson-consolidation-implementation/search-bson-persistence/README.md).
La revisión pública descubre tres fallos y una debilidad del oráculo:

- SQLite cortaba la tanda ANN al alcanzar limit coincidencias, antes de ordenar
  todos los candidatos ya recibidos. Examina ahora toda la tanda y solo adjunta
  metadata a los ganadores; ANN, fallback exacto y documentos decorados usan el
  mismo orden de hits. No promete vecinos que ANN no devolvió.
- El comando explain reflejaba verbosity sin enviarla al cursor. Se transmite la
  opción explícita y se conserva el default local; queryPlanner inspecciona un
  índice pendiente sin ejecutar datos, y executionStats sigue rechazándolo.
- Los índices escalares usaban claves nativas con dialectos que redefinen
  igualdad. La guarda común supports_mql_candidate_prefilter conserva residual
  en contextos no acreditados, tanto en ejecución como en explain y ambos
  motores. Se retira un cálculo duplicado de candidatos en el planner Memory.
- Las pruebas sync activaban reference después de la apertura, por lo que podían
  comparar dos ejecuciones optimizadas. Un helper test-only configura el hook
  existente antes de abrir, conserva la instancia async y verifica el modo real
  recibido por el ejecutor. El registro de imports unitario tiene nombre propio
  para evitar que lo sobrescriba el maestro del replay paralelo.

Dos acciones MQL congeladas se capturan/comprueban con MongoDB
7.0.43/8.0.32/9.0.2 standalone, FCV correspondiente e imágenes fijadas. La acción
posee su seed BSON rico; la entrada inicial del fixture es neutra para que EJSON
no canonicalice DBRef en sus metadatos. Se mantiene el validador de bytes y el
hash del corpus completo. No cambia ninguna de las 251 referencias históricas.

Las mismas selecciones cruzan Search vector/vector-downstream/text/compound,
sync/async y wire, ventanas, calentamiento, cambios de tipo y motores SQLite
nuevos al reabrir sin recrear índices. Se acredita el backend concreto y la
ruta Python separadamente. Sesenta controles de overrides añaden guards/spies,
contexto y reapertura; su expectativa literal no depende de otro matcher local.
Son contrato local, no extensiones Python ejecutadas en MongoDB ni ranking Atlas.

La base Git aislada falla 32/pasa 141 en corpus público/nativo, falla cuatro/pasa
13 en explain y falla 48/pasa doce en overrides. El wheel definitivo pasa 8.454
checks integrados en 211,57 s sin avisos y 1.144 pruebas unitarias/Search/SPI/
propiedades con 181 subtests. Los cuatro workers verifican 262 módulos y los 284
archivos instalados; la prueba unitaria exclusiva conserva sus 263 imports.
Typing, manifiesto público y lint mantienen sus garantías. Python 3.13.15
realmente sin SDK pasa 960 comparaciones del subconjunto admitido en cuatro
rutas y fases, con SQLite persistido/reabierto y los tres backends observados;
Code se excluye explícitamente. El probe de overrides también pasa en ambos
motores sin SDK. No se presenta como matriz o corpus íntegro SDK-free.

Se verifica ausencia de nueve contenedores y dieciocho volúmenes propios. El
primer diagnóstico CPU mostró ratios 1,66/1,75 en límites pequeños; materializar
solo ganadores los reduce a 1,11/1,47 con BSON idéntico, y los controles de tanda
completa quedan en 0,99. ABBA dos veces, doce muestras/runtime/caso y cuatro
casos son diagnóstico; D05 conserva el residual Binary/limit=2 y su revisión de
coste. No se afirma presupuesto global aceptado ni ausencia de regresiones de
rendimiento. Los gates anteriores al último cambio siguen siendo feedback.

El nodo D05 test_generated_search_errors_match_reference_engines_and_surfaces se
comprueba con el oráculo corregido; el resto del listado histórico de 97 no se
acepta por asociación. → Sigue: contextos D06 restantes y su auditoría por
consumidor, H53/C03/B08, sNaN, decisión opcional D07, D05 y B07. No se acredita
CI remoto, cobertura global ni todas las matrices. UUID sigue STANDARD.


## Conversión escalar — gramática, tipos y preservación BSON

`51ff167` completa el corte de conversión del plan con
[evidencia propia](evidence/bson-consolidation-implementation/decimal-conversion/README.md).
El [receipt](evidence/bson-consolidation-implementation/decimal-conversion/receipt.json)
identifica fuente/wheel, baseline `a2c3cbb`, capturas finales y comprobaciones.

La causa no era solo aceptar String sNaN: coexistían el parser permisivo de
Python, el gating accidental de decimal en 7/8, la falta de preparación de
`$toDecimal`, la salida directa sin tipo BSON y la presentación String del SDK
que perdía signo/señal al almacenar NaN. El corte reutiliza el parser local de
conversión, la conversión double de 15 cifras, milisegundos enteros, entorno de
pipeline y DocumentCodec. No crea otro comparador, contexto, formato persistido
ni prohibición general de NaN. Los helpers independientes conservan su
conveniencia Python; los pipelines conservan tipos de conversión BSON.

Seis acciones con 548 observaciones por versión contrastan String/Code,
valores no finitos almacenados, precisión/exponentes, signos/cero negativo,
fechas ±1001 ms, vacío/poblado, literal/campo, onError/onNull y ramas. Los
subpipelines let/facet/lookup conservan el contexto; los updates pipeline
repetidos comprueban tipo, postimagen y modified_count. La observación de BID
no transforma Decimal Python en Decimal128 para hacer pasar la API directa.
No se promete preservar cualquier payload BID arbitrario ni recuperar datos
que ya habían perdido información bajo el codec anterior.

- MongoDB standalone 7.0.43/8.0.32/9.0.2: capturas/checks finales idénticos con
  imágenes y FCV registrados. Las siete rondas verifican ausentes 21 contenedores
  y 42 volúmenes propios; 263 archivos históricos protegidos siguen idénticos.
- Baseline: 144 replays públicos fallan y las tres comprobaciones de metadata
  pasan. Candidato final instalado: 8.601 checks pasan en 211,96 s, con warnings
  de runtime/unraisable/thread tratados como errores.
- 1.144 pruebas instaladas de unitarias/SPI/propiedades, con 181 subtests, pasan
  en 75,58 s. Los proofs verifican hashes/sitio en cuatro workers y sesiones
  master; no se mezcla el checkout con el wheel. Typing mantiene 2 controles
  positivos y los 20/25 errores negativos; API manifest y lint ratchet pasan.
- Python 3.13.15 sin SDK: 228 controles del subset, con resultados nativos y
  reapertura SQLite. No sustituye la matriz completa D05. CPU escalar String→
  Decimal ABBA×2: ratio diagnóstico 1,06; no acredita el presupuesto global.

La ejecución amplia anterior, con inputs que cambiaron durante el feedback,
se conserva separadamente y no sirve como aceptación. El gate final usa
runtime/corpus congelados; las referencias históricas no se recapturan.

✅ Cerrado: este corte funcional de conversión escalar. No reabre los cierres
D01/D02/D03/D04 ni declara D06/D05 terminados.
→ Sigue: contextos/representaciones y consumidores restantes D06, disposición
H53/C03/B08, decisión opcional D07, matrices/cobertura/costes y cierre integrado
D05, y admisión B07 con el corte UUID coordinado. El default sigue STANDARD.


## B07 — Preflight nativo de admisión, implementación abierta

Tras `2ae4b56`, el [preflight](evidence/bson-consolidation-implementation/bson-admission-preflight/README.md)
y su [receipt](evidence/bson-consolidation-implementation/bson-admission-preflight/receipt.json)
fijan seis acciones y 676 observaciones por versión MongoDB 7/8/9. No cambia
producción ni acredita correcciones. El baseline aislado tiene 96 fallos y 51
checks correctos: 72 fallos directos, 24 wire en opciones/comandos, 48 controles
wire correctos y tres de metadata. El adaptador async delega explain al método
real para observar la fase; no inventa un resultado del plan físico.

Las causas exigen integrar la codificación cliente en su momento y lote:
find permite mutar el filtro antes de iterar, y cerrar no codifica. Un error
capturado al construir sería incorrecto. Insert_many y cada familia bulk
codifican antes de ejecutar ese grupo; un bulk mixto puede conservar el grupo
insert previo cuando falla la codificación de los updates. La preparación
anticipada de todo el bulk también sería incorrecta. AsyncCursor._as_operation
y el agrupador bulk existentes son mecanismos a reutilizar.

Los controles positivos conservan diferencias que no son fallos de admisión:
proyección calculada de find, id int64/orden firstBatch/id/ns del sobre raw,
orden n/nModified/ok del resultado update y superficie pública del resultado
bulk. El plan las asigna a D06/B08 sin cambiar el observador. Los rechazos de
objetos imposibles por el driver wire ya correctos no se atribuyen al servidor.

Capturas/checks finales coinciden; se verifica con Docker operativo la ausencia
de 11 contenedores y 22 volúmenes propios, incluidas las rondas de feedback del
observador. No se modifican normalizadores, observadores o referencias
históricas. No hay gate global verde, candidato corregido, wheel nuevo ni
aceptación D05/B07. El corpus nuevo permanece rojo y obligatorio, sin xfail.

→ Sigue: implementar la admisión diferida y común sobre las fronteras existentes,
integrar los lotes de escritura y completar el corte UUID antes de cambiar su
default. Conservar los consumidores positivos y el resto del objetivo íntegro.


## B07 — Codificación de lecturas y solicitudes find diferidas

`0b01a62` entrega el [corte de lectura](evidence/bson-consolidation-implementation/bson-input-reads/README.md)
y su [receipt](evidence/bson-consolidation-implementation/bson-input-reads/receipt.json).
Reutiliza el encoder SDK, el lector BSON raw y la normalización existente;
el perfil portable no requiere instalar SDK. La admisión cliente no endurece
DocumentCodec.to_internal ni vuelve a codificar planes internos preparados.

Find conserva sus referencias hasta ejecutar; clone toma un snapshot, rewind
prepara otra solicitud y explain opera sobre un clon sin congelar la original.
Los valores inválidos se rechazan en su fase, no al crear/cerrar el cursor.
El binding usa el contexto final y la caché solo publica el artefacto completo.
Las rutas internas y el fallback de agregación conservan el plan/contexto ya
preparados en vez de serializar NOW como declaración pública. Distinct usa ese
constructor interno y evita una segunda admisión por el frontend find.

Find_one admite IDs escalares como el driver. La representación normalizada
ha descubierto dos consumidores incompletos: regex u en el matcher común e
Int64 en parámetros Search. El corte usa las vistas existentes, conserva la
precisión de enteros y los filtros MQL, y no amplía el dominio vectorial a
Decimal/bool ni acredita ranking Atlas. Se omiten los opcionales nulos del
cuerpo codificado sin eliminar los null anidados ni el valor None de la API.

- Tres acciones / 39 observaciones nativas por versión 7.0.43/8.0.32/9.0.2,
  fuente final congelada y checks idénticos. El baseline ecfac84 falla en 48
  replays y pasa 27 checks; el candidato pasa los 75 del corpus nuevo.
- Gate amplio instalado: 8.727 checks correctos y 96 fallos, todos del corpus
  bson_admission que conserva los pendientes. No es un gate integrado verde;
  no hay xfail ni filtrado de esos fallos. Los demás replays no retroceden.
- 1.199 pruebas instaladas de unitarias/SPI/propiedades y 181 subtests pasan.
  Guards de una codificación por lectura, cero codecs/fábricas de entrada,
  contexto atómico, una compilación y parámetros Search representados.
- Python 3.13.15 sin SDK: 748 controles de rechazos, fase, resultados y bytes
  UUID con UNSPECIFIED explícito. Typing (2 positivos y 20/25 negativos), API
  manifest y lint ratchet pasan. El wheel de 285 módulos y sus imports quedan
  identificados; no se mezcla src con la distribución instalada.
- Cuatro rondas de desarrollo conservadas: 12 contenedores y 24 volúmenes
  propios verificados ausentes; referencias/observadores históricos intactos.
- Coste diagnóstico ABBA×2: Memory find(limit=1) 1,06 y find_one 1,12; con codecs
  1,04/1,12. SQLite entre 0,98–1,04; aggregate alrededor de 1,00–1,02 y cursor
  no consumido alrededor de 0,23. D05 conserva los incrementos de lectura y
  presupuestos anteriores, además de matrices/cobertura/cierre integrado.

Este corte no acepta B07 completo. Siguen escrituras por lote, comandos,
índices/validators, el corte UUID de entrada antes del nuevo default,
proyecciones calculadas y sobres/resultados positivos D06/B08. STANDARD sigue
siendo el default. El resto D06, H53, decisión D07 y D05 conserva sus owners.


## B07 — Inserciones, IDs y errores públicos

`b713e52` entrega el [corte de inserciones](evidence/bson-consolidation-implementation/bson-input-inserts/README.md)
y su [receipt](evidence/bson-consolidation-implementation/bson-input-inserts/receipt.json).
Insert_one/insert_many reutilizan el codificador común antes de ejecutar;
la preparación de documentos/IDs es común y conserva las asignaciones parciales
del iterable. Los IDs devueltos proceden de la entrada, conservando tipo y
precisión; las copias no anticipan un error Python al rechazo BSON del driver.
El ejecutor privado sirve a comandos/bulk internos sin recodificar sus entradas.

El materializador público de errores conserva opciones builtin UUID/fecha,
excluye fábricas/decoders del usuario y añade la operación original bulk después
de decodificar. El DTO común corrige el orden de sus contadores. La normalización
DBRef tiene controles de lookup actuales y de wrappers antiguos sembrados por
el SPI real; conserva residuales/fallback y referencias nativas. El adaptador
async delega drop_collection real, sin simular resultados.

- Cinco acciones / 169 observaciones nativas por versión 7/8/9; fuente congelada
  y checks idénticos. Baseline 60 fallos/63 correctas; los 123 checks nuevos pasan.
- Gate instalado: 8.856 correctas y 96 fallos, todos B07 preexistentes y visibles.
  No hay regresiones adicionales de schema/Binary/lookup ni gate global verde.
- 1.403 pruebas y 4.535 subtests pasan; 942 observaciones de inserción y 748
  controles de lectura pasan en Python 3.13.15 sin SDK. Typing, API y lint pasan.
- 272 fixtures/observadores históricos intactos y 63 recursos temporales propios
  verificados ausentes. El receipt identifica wheel/imports y límites del perfil
  portable. Coste diagnóstico: MemoryEngine lote 1: 1.05; MemoryEngine lote 4: 1.02; SQLiteEngine lote 1: 1.01; SQLiteEngine lote 4: 0.98; no cierra D05.

→ Sigue: integrar las otras escrituras y la codificación por familia bulk antes
de los grupos, conservando prefijos confirmados. Comandos/índices/validators,
UUID por defecto y positivos D06/B08 mantienen su aceptación pendiente. B07,
resto D06, H53, decisión D07 y D05 siguen abiertos.


## B07 — Mutaciones públicas y respuestas builtin

`c83c95c` entrega el [corte de mutaciones](evidence/bson-consolidation-implementation/bson-input-mutations/README.md)
y su [receipt](evidence/bson-consolidation-implementation/bson-input-mutations/receipt.json).
Update/delete/findAndModify admiten BSON una vez antes de normalizar/compilar;
los validadores de forma conservan la entrada y el orden propio de sesión,
capacidad y codificación. IDs upsert y errores públicos conservan opciones
builtin UUID/fecha sin ejecutar fábricas/decoders del usuario. Bulk usa el
executor preparado existente para evitar otra admisión por elemento, pero su
codificación completa por familia sigue pendiente.

- Siete acciones / 701 observaciones nativas por versión 7/8/9, fuente congelada
  y checks idénticos. Baseline: 72 fallos/99 correctos; candidato: 171 correctos.
- Gate amplio instalado: 9027 correctos y 96 fallos, todos B07
  preexistentes y visibles. No se acepta el cierre integrado por ese resultado.
- 1491 pruebas y 4569 subtests correctos; 3.918
  observaciones de mutación, 942 de inserción y 748 de lectura realmente sin SDK.
  El subset portable excluye Code y contrato completo de errores tipados;
  el perfil SDK conserva ambos. Typing/API/lint correctos.
- 275 fixtures/observadores históricos intactos; 63 recursos temporales
  propios comprobados ausentes. Coste diagnóstico: MemoryEngine update_one: 1.16; MemoryEngine update_many: 1.07; MemoryEngine find_one_and_update: 1.16; SQLiteEngine update_one: 1.06; SQLiteEngine update_many: 1.06; SQLiteEngine find_one_and_update: 1.09; D05 queda abierto.

→ Sigue: admisión bulk por familia, después comandos/índices/validators y
UUID coordinado. Positivos D06/B08, resto D06, H53, decisión D07 y D05 siguen
por sus owners; STANDARD sigue siendo el default.


## B07 — Preparación y frames bulk clásicos

`5a89dcc` entrega el [corte bulk](evidence/bson-consolidation-implementation/bson-input-bulk/README.md)
y su [receipt](evidence/bson-consolidation-implementation/bson-input-bulk/receipt.json).
Preparación secuencial sin copias/normalización prematuras, grupos existentes y
encoder común por frames de 48 MB. El lookahead inválido impide publicar el
frame pendiente; se conservan frames/familias anteriores. Orden del driver y
ordered del servidor son decisiones separadas, especialmente con null.

Las reglas estructurales de comandos tienen owner común. Wire corrige vacío
como reemplazo y errores ordered con código 14. Inserciones y upserts reutilizan
el orden raíz _id; datos antiguos siguen comprobados por SPI sin migración.
Respuestas usan codecs builtin y el DTO común; bulk_api_result es la única
adición API prevista, con snapshots históricos intactos.

- Nueve acciones / 518 observaciones nativas por versión 7/8/9; checks idénticos.
  Baseline 144 fallos/75 correctos; 219 checks nuevos correctos en el candidato.
- Replay instalado: 9246 correctos y 96 fallos B07
  preexistentes; ningún fallo nuevo ni aceptación global por ese resultado.
- 1562 pruebas y 4577 subtests correctos; API 18/11,
  typing y lint correctos. Sin SDK: 2.880 observaciones bulk más 3.918/942/748
  controles de mutación/inserción/lectura, con límites declarados en el receipt.
- 278 fixtures/observadores históricos intactos y 45 recursos propios
  verificados ausentes. Coste diagnóstico hasta aproximadamente 1,28 en Memory
  update lote 1; conservarlo en D05, sin aceptar presupuesto global.

→ Sigue: admisión cliente de comandos/índices/validators y rutas internas
preparadas; luego UUID coordinado. Positivos restantes D06/B08, resto D06,
H53, decisión D07 y D05 mantienen sus owners. STANDARD sigue como default.

## B07 — admisión y respuesta de comandos públicos

✅ Cerrado el corte acotado en `3bfe5fb`; [evidencia](evidence/bson-consolidation-implementation/bson-input-commands/README.md)
y [receipt](evidence/bson-consolidation-implementation/bson-input-commands/receipt.json).
El sender público admite BSON una vez antes del ejecutor preparado; wire y
las delegaciones internas conservan su entrada sin encoder cliente. Se reutiliza
el materializador de respuesta completo, con opciones propias de command y
sin TypeDecoders. Distinct mantiene valores internos hasta esa frontera.
Los sobres cursor/write conservan tipo int64 y orden nativos, sin otro DTO.

Siete acciones/236 observaciones por servidor 7/8/9 y seis controles pareados
sync/async acreditan el contrato. La base presenta 120 fallos/51 correctos;
los 171 checks nuevos pasan. El replay instalado obtiene 9441
correctos y 72 fallos restantes, todos del conjunto previo.
La cohorte ampliada tiene 2085 correctas/4677 subtests y dos fallos de batching
reproducidos en la base; las 25 pruebas finales de frontera pasan. Permanecen
visibles: no se acredita un gate global verde ni se cierra B07/D05.

Python 3.13.15 realmente sin SDK verifica 1.362 observaciones de comandos y
conserva bulk/mutación/inserción/lectura. API sin cambios, typing y lint pasan;
281 fixtures/observadores históricos intactos y
60 recursos propios verificados ausentes. El coste CPU diagnóstico aumenta
especialmente en ping (ratios 1.64/1.48 en Memory/SQLite); D05 conserva el presupuesto
integrado y los demás incrementos, sin aceptación por asociación.

→ Sigue: wrappers públicos de catálogo/índices/validators sobre el encoder y
los ejecutores existentes; después UUID coordinado. Positivos/batching y los
demás contextos D06, H53, decisión D07 y D05 mantienen sus owners. STANDARD
global sigue como default; command usa su propio default UNSPECIFIED del SDK.


## B07 — frames cliente de creación de catálogo

✅ Cerrado el corte acotado en `7524e39`;
[evidencia](evidence/bson-consolidation-implementation/bson-input-catalog/README.md)
y [receipt](evidence/bson-consolidation-implementation/bson-input-catalog/receipt.json).
create_collection/create_index/create_indexes admiten el frame completo antes
de la normalización servidor, las copias de valores o los cambios de catálogo. Se reutilizan encoder,
catálogo de aliases, normalizador servidor y materializador; raw/wire conserva
su ejecución preparada sin codificación cliente repetida.

El contraste corrige precheck, coerción size, null opcionales, código por
dialecto, idempotencia de opciones y codecs propios de índices/listCollections.
La fábrica de create se ejecuta una vez después de confirmar; sus errores no
revierten la creación. Los controles del comando no se guardan como opciones.
El adaptador de modelos no copia objetos arbitrarios antes de admitir BSON.

Nueve acciones/233 observaciones por MongoDB 7/8/9: base 156 fallos/63 correctos,
candidato 219 checks nuevos correctos. El replay instalado obtiene
9660 correctos y 72 fallos del conjunto previo,
sin nodos nuevos. Cohorte: 2149 correctas/4690 subtests,
con los mismos dos fallos antiguos de batching; 18 pruebas de frontera pasan.
Perfil realmente sin SDK: 1.362 observaciones nuevas y las regresiones de
comandos/bulk/mutación/inserción/lectura. API limitada a cuatro firmas de
aliases, typing y lint pasan; 284 históricos
intactos y 63 recursos propios ausentes. Coste create 1.95/1.99
(Memory/SQLite) frente a la base: D05 conserva su revisión y presupuesto.

→ Sigue: disponer el censo de entradas administrativas e índices antes de
coordinar UUID global; no se declara B07 cerrado por estos tres wrappers.
Positivos/batching, demás contextos D06, H53, decisión D07 y D05 conservan sus
owners. El censo no obliga a implementar Atlas ni funciones fuera del subset.


## B07 — administración de índices, admisión y codecs

✅ Cerrado el corte acotado en `c715ae7`;
[evidencia](evidence/bson-consolidation-implementation/bson-input-index-admin/README.md)
y [receipt](evidence/bson-consolidation-implementation/bson-input-index-admin/receipt.json).
Listados e información usan las opciones propias del SDK (SON/UNSPECIFIED),
no las de la colección. Las retiradas admiten con las opciones de colección;
una fábrica fallida después del commit no revierte el borrado. La ejecución
preparada comparte lectura/retirada y el DTO con comandos wire, sin repetir
codificación ni codecs públicos. El testigo de asignaciones descubre y corrige
el sobre parcial, orden y msg de la respuesta descartada de retirada.

Cuatro acciones/127 observaciones por versión nativa; base 84 fallos/15 correctos,
candidato 99 checks nuevos correctos. Replay instalado: 9759 correctos y los
mismos 72 fallos previos. Cohorte: 2165 correctos/4688 subtests y los mismos dos
fallos antiguos de batching; 16 controles de frontera. SDK-free: 738 observaciones
nuevas y las seis familias BSON anteriores. API idéntica, typing y lint pasan;
287 históricos intactos y 36 recursos temporales ausentes. Costes diagnósticos
ABBA×2 registrados, sin aceptar presupuesto D05.

→ Sigue: 13 entradas administrativas del censo (incluidas tres delegaciones),
UUID coordinado y unidad 5 de positivos/respuestas. Esta admisión no cierra toda
la semántica de los listados, errores servidor o tandas. STANDARD global, otros
contextos D06, H53, decisión D07 y cierre D05 mantienen sus obligaciones.


## B07 — listados y validación administrativa

✅ Cerrado el corte acotado en `d9c9c80`;
[evidencia](evidence/bson-consolidation-implementation/bson-input-admin-reads/README.md)
y [receipt](evidence/bson-consolidation-implementation/bson-input-admin-reads/receipt.json).
Listados admiten una vez con opciones propias; validate reutiliza Database.command.
Los lectores de validación/stats/hash operan sobre el snapshot SPI preparado y
no decodifican con codecs del usuario. Se conserva Mapping cliente solo para names,
namespace 73/26, filtro servidor 14, comentarios y el contrato valid:false del SDK.
Las reglas BSON legacy y de booleanos tipados permanecen distintas, con vista
numérica común para evitar que Decimal128 cero cuente como verdadero.

Seis acciones: 269 + 18 observaciones por versión nativa. Base 120 fallos/30
correctos; candidato 150 checks nuevos correctos. Replay instalado: 9909
correctos y los mismos 72 fallos previos. Cohorte: 2192 correctos/4686 subtests
con los mismos dos fallos antiguos de batching; 27 controles de frontera.
SDK-free real: 1566 + 108 observaciones y siete familias anteriores. API idéntica,
typing/lint pasan; 290 históricos intactos y 45 recursos temporales ausentes.
La proyección CSV conserva identidad del owner; el test deja de duplicar su SHA
literal. Costes diagnósticos de stats/hash ~0.75 Memory/~0.88 SQLite y names
~1.67/~1.27 frente a la base; no se acepta presupuesto D05.

→ Sigue: siete entradas del censo, UUID coordinado y positivos/respuestas/batching.
Background local continúa accepted-noop y síncrono: las referencias de ese flag
fijan full=True para aislar incompatibilidad/conversión antes de checkpoints.
No se simula el 115 observado sin checkpoint en 8/9 ni corrupción física del
servidor mediante el control SDK de respuesta inválida. Otros contextos D06,
H53, decisión D07 y matrices/cobertura/costes integrados D05 siguen abiertos.


## B07 — solicitudes públicas de namespace

✅ Cerrado el corte acotado en `b938556d`;
[evidencia](evidence/bson-consolidation-implementation/bson-input-namespace/README.md)
y [receipt](evidence/bson-consolidation-implementation/bson-input-namespace/receipt.json).
Drop/rename admiten una vez con opciones propias UNSPECIFIED y reutilizan
la ejecución DDL preparada. InvalidName conserva el puente SDK/portable y
su precedencia cliente. Se conserva Collection con nombre String confirmado y
opciones/contexto anteriores. Code scope se codifica en drop y se descarta al
construir namespaces en rename, según referencia nativa. El parser drop exige
dialecto y conserva sus códigos no String 2/14/73 en 7/8/9.

Cuatro acciones/55 observaciones por versión; base 72 fallos/27 correctos,
candidato 99 checks nuevos correctos. Replay instalado 10008 correctos y los
mismos 72 fallos previos; cohorte 2250 correctos/4686 subtests, con los mismos
dos fallos antiguos de batching. Dieciséis controles de frontera y los contratos
DDL de rollback/journal/checkpoint pasan. Se corrige el testigo obsoleto de
comentario persistido sin retirar las garantías de estado/índices/publicación:
la base reproduce sus seis fallos. SDK-free: 240 observaciones y ocho familias
previas. Firmas API, typing y lint pasan; 296 históricos intactos y 27 recursos
propios ausentes. El nuevo módulo privado eleva la wheel a 286 módulos.

→ Sigue: cinco entradas Search del censo, UUID coordinado y positivos/respuestas/
batching. El retorno Collection local y los parámetros namespace existentes se
conservan; no se acepta todo reply drop ni paridad distribuida de eventos por
las capturas standalone. Otros contextos D06, H53, decisión D07 y cierre D05
mantienen sus obligaciones, incluidos costes y matrices integrados.

## B07 — solicitudes públicas Search

Código `2723fb28` sobre `aceaa5e8`, con [evidencia ligada a fuentes](evidence/bson-consolidation-implementation/bson-input-search/README.md).
Las cinco entradas usan la admisión BSON común y opciones propias UNSPECIFIED.
Creación individual/múltiple comparte ejecución; se retiran dos normalizadores
que copiaban antes de codificar, se reconoce el modelo SDK y se conserva la
definición directa local. Orden de constructor y maxTimeMS/comment conservan
la precedencia del driver. Un solo índice evita la lectura del catálogo;
varios conservan rollback, plazo y nombres preexistentes.

- Native standalone 7.0.43/8.0.32/9.0.2: cinco acciones/303 observaciones por
  versión y --check idéntico. Rechazo BSON/sesión y construcción del request;
  no son resultados positivos Atlas ni soporte nuevo del proxy.
- Base: 60 fallos/63 correctos. Candidato instalado: 123 checks nuevos pasan;
  replay 10.131 correctos/72 fallos, mismos nodos de bson_admission.
- Cohorte: 2.274 correctos/4.686 subtests y los dos fallos previos de batching.
  Incluye 24 controles de frontera, persistencia, callbacks, modelos y rollback.
- SDK-free real 3.13.15: 1.716 observaciones; nueve familias previas pasan.
  Sobres públicos mapping reemplazan al constructor SDK; se omite Code.
- Wheel de 287 módulos con hashes/import guards, manifiestos públicos iguales,
  API 18 tests/13 subtests, typing instalado y lint correctos. Los 299 testigos
  históricos siguen intactos y los 27 recursos Docker propios están ausentes.
- Se conservan los gates de feedback interrumpidos y el fallo del testigo de
  orden previo. Solo el candidato final reconstruido acredita la aceptación.
  Costes ABBA×2 diagnósticos, sin cierre del presupuesto D05.

El censo administrativo histórico queda dispuesto: cero entradas pendientes
allí. No cierra B07 ni redefine su alcance. Sigue UUID coordinado; positivos/
respuestas/batching, contextos D06, H53, decisión D07 y D05 conservan obligaciones.

## B07 — UUID coordinado y ausencia en distinct

UUID `d5bdadb8` y distinct `b29e9bf1` sobre `30365046`, con [evidencia final](evidence/bson-consolidation-implementation/bson-uuid-policy/README.md).
El constructor y el campo dataclass de CodecOptions pasan a UNSPECIFIED. Se
reutilizan admisión, dueños y políticas de lectura/escritura existentes; no se
cambia almacenamiento, SPI ni se reescribe Binary 4 como legacy. La selección
STANDARD/legacy explícita conserva bytes y resultados nativos. Database.command
mantiene su dueño propio. El consumidor común de distinct deja de fabricar null
por ausencia, con testigos API/comando y controles null/arrays.

- Cuatro acciones/216 observaciones por MongoDB 7.0.43/8.0.32/9.0.2, --check
  idéntico. Base del corpus: 72 fallos/27 correctos; instalado: 99 correctos.
- Replay: 10.278 correctos/24 fallos previos de opciones/proyecciones; se resuelven
  exactamente 48 nodos anteriores sin cambiar referencias históricas.
- Cohorte: 2.330 correctos/4.709 subtests y dos fallos previos de batching. Incluye
  24 controles nuevos, commit barrier, herencia, lectura independiente e índice/
  reapertura de un SQLite creado por el runtime anterior STANDARD. Los tests
  espejo de ausencia, la fixture binaria con UUID y el test de owner de opciones
  se corrigen contra los contratos observados, conservando sus controles.
- SDK-free real 3.13.15: 1.296 observaciones completas; diez familias previas pasan.
  API 18/13 subtests y typing instalado correctos. El manifiesto cambia solo los
  dos defaults deliberados de CodecOptions; 287 módulos de wheel verificados.
- Los 302 testigos históricos siguen intactos. Recursos Docker propios: 27
  ausentes. La fixture SQLite antigua nueva tiene procedencia 30365046 y hash;
  no la genera el candidato. Costes ABBA diagnósticos, sin cierre de presupuesto.

Sigue la unidad 5 de positivos/respuestas: 24 nodos admission_options y dos
pruebas de batching obligatorios, con testigo físico antes de corregir sus
expectativas. Contextos D06, H53, decisión D07 y D05 siguen abiertos. El cambio
de default está aceptado; no volver a aplazarlo ni reiniciar el censo cerrado.


## B07 — Proyecciones preparadas, contexto y metadatos

✅ Cerrado el corte de proyecciones de la unidad 5 en `8e261c7a`, con
[aceptación y evidencia](evidence/bson-consolidation-implementation/find-projections/README.md).
Find y aggregate comparten expansión y asignación de campos calculados sobre
el evaluador existente. Flags numéricos BSON, arrays/objetos vacíos, `_id`
calculado y ausencia/`$$REMOVE` conservan la forma y orden observados. La
ruta de estados deja de evaluar dos veces las expresiones de `$project`.

Un portador dict interno conserva el `RuntimeMetadata` existente fuera de los
campos BSON a través de scans, copias y conversiones; no cambia SPI ni formato
persistido. Preparación distingue proveedor declarado y valor por fila,
respeta literales y conserva el contexto en sufijos físicos. Los caminos
optimizados de Memory/SQLite reciben variables y collation. FindAndModify
conserva la separación nativa entre errores constantes anteriores al commit
y errores de proyección de la imagen posteriores a él; barreras locales de
eventos comprueban sus efectos.

- MongoDB real 7.0.43/8.0.32/9.0.2: siete acciones, 252 observaciones por versión,
  captura y `--check` idénticos. SDK-free real 3.13.15: 1.488 observaciones;
  cuatro rutas Code por motor/dialecto se omiten expresamente por no existir
  constructor público portable. Los observadores permanecen intactos.
- Wheel de 287 módulos verificados: 10.473 pruebas diferenciales correctas,
  incluidas las 24 admission_options antes fallidas. Cohorte: 2.432 correctas,
  4.709 subtests y solo los dos pendientes previos de batching.
- Focales: 181 correctas/seis subtests. Reapertura SQLite instalada: una.
  API: 18/13 subtests; typing y ratchet correctos. Dos controles de coste y
  dos subtests pasan, sin atribuirles el presupuesto integrado D05.
- Los 307 fixtures/observadores protegidos de la base siguen intactos. Los 45
  recursos Docker de las cinco rondas nativas están ausentes. El gate amplio
  de feedback encontró una regresión de objetos padres missing/REMOVE: se
  corrigió en el helper, se amplió el corpus nuevo y se repitió el gate final;
  no se reescribieron las referencias históricas para ocultarla.

→ Sigue la unidad 5: batching/getMore, sobres/DTO, resultados bulk y las
combinaciones restantes de respuestas/fábricas/callbacks. La [sonda física de
batching](evidence/bson-consolidation-implementation/command-batches-diagnostic/README.md)
es diagnóstico del siguiente corte, sin aceptar todavía sus rutas locales. Contextos D06, H53, decisión D07 y D05 siguen abiertos.

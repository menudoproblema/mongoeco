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

## Trabajo restante

- D06a/D06b/D06c: gramática (incluido H55 String/Code), preservación BSON wire y consumidores DBRef.
- D04: DDL/cierre por ámbito e interacción con imágenes, continuidad/reanudación
  y fixtures replica set de jobs nativos. Preparación/contexto y modos CRUD de
  imágenes tienen aceptación propia; no cierran las garantías restantes. Las obligaciones y testigos concretos
  están en revisión 8 del plan; ni publicación TTL ni D01b los cierran.
- H53: caracterización y disposición de observabilidad conforme al plan.
- Conversión escalar: diagnóstico string sNaN frente a 241 nativo, contraste 7/8 y regresión permanente pendientes.
- D07: piloto opcional con criterio de continuidad, sin migración obligatoria.
- D05: revisión y gates integrados, matrices y commits de cierre.

Los cortes focales no acreditan el cierre integrado del programa ni D05.

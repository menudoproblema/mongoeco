# Implementación de consolidación BSON

**Estado vigente: parte I F00–F11 cerrada, 12/12 bloques aceptados.**
Véase la [aceptación final del candidato b83789bc](#aceptación-final-f11-del-candidato-b83789bc).
El cierre técnico es independiente del cierre de versión, aplazado por el owner.

Contrato autorizado por el usuario: implementar el
[plan refinado](bson-semantics-consolidation-plan.md), revisando cada bloque y
commiteando semánticamente. Base: `eef9c09`. Estado: alcance O1–O8 y D05 completado.
No cambia SPI v2 ni declara implementados los pilotos opcionales. La lista del
alcance original es [O1–O8](#obligaciones-de-cierre-vigentes); el incremento
posterior F00–F11 tiene su estado vigente más abajo. Los estados por corte
conservan historia. Véase el [cierre integrado](#cierre-integrado--o8-y-d05).

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
- B07: lecturas, inserciones, mutaciones, bulk clásico y comandos públicos conservan sus aceptaciones. `7524e39` añade [frames cliente de creación de catálogo](#b07--frames-cliente-de-creación-de-catálogo), opciones/contexto y respuesta metadata. El corte posterior `8e261c7a` acepta [proyecciones y contexto](evidence/bson-consolidation-implementation/find-projections/README.md): el replay instalado queda sin fallos y esa aceptación conserva dos tests antiguos de batching rojos. `dbe8257f` acepta después batching y cierre de spill, con 10.660 replay y 2.457/4.709 subtests de cohorte sin fallos; el estado de aquella ejecución no describe automáticamente el checkout actual. `c715ae7` añade las cuatro admisiones de índices y los campos observables de la respuesta de retirada; `d9c9c80` añade las seis entradas de listados/validate, lectores fuente preparados y controles de flags. `b938556d` añade drop/rename y `2723fb28` acepta las cinco entradas Search del censo. `d5bdadb8` entrega el default UUID coordinado y `b29e9bf1` omite ausencia en distinct. `05b3f655` acepta DTOs/sobres y diagnóstico de escritura; `a8a81f0e` acepta después constructores/setters y sus efectos confirmados; continúan los contextos D06 y la aceptación integrada B07 unidad 6/D05. UNSPECIFIED es el default global; STANDARD/legacy siguen disponibles explícitamente y el [plan activo](bson-semantics-consolidation-plan.md#b07--admisión-en-la-fase-y-el-lote-correctos) conserva la secuencia.

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


## B07 — Batching, respuesta del driver y cierre de spill

✅ Cerrado el corte en `dbe8257f`, con [aceptación y evidencia](evidence/bson-consolidation-implementation/command-batches/README.md).
La petición de respuesta del servidor conserva su tamaño; la entrada del driver
aplica promoción inicial y límite restante sin alterar la entrada interna.
EOF conocido y filas efectivamente entregadas son hechos distintos, incluida
la fila pendiente por bytes. Las políticas observadas de sort/group e initial
aggregate se expresan en el dialecto, con elegibilidad de orden de índice
compartida y sin otro ejecutor. La proyección posterior mantiene su fase de error.

Sort/group spill conserva cardinalidad y cleanup mediante output owned; cerrar
antes de la primera lectura también elimina archivos. La propiedad se registra
al crear el output; cierre de fuentes fallido conserva el error y admite retry.
`singleBatch` wire valida bool y cierra tras la primera respuesta, también vacía.
Las expectativas de los dos tests antiguos de batching se corrigen contra las
tres referencias nativas; ahora pasan en la cohorte instalada.

- MongoDB standalone 7.0.43/8.0.32/9.0.2: seis acciones/284 observaciones de frames
  y tres/36 de callbacks y raw por versión. Resultados/manifests exactos en
  capture/check y segunda comprobación; hello conserva sus campos variables.
- Wheel de 287 módulos sin drift: 10.660 correctas en replay; 2.457 correctas,
  4.709 subtests y cero fallos en cohorte. Baseline Git aislada: 104 fallos/58
  correctas; candidato focal: 162 correctas. Spill forzado: 37 correctas,
  con creación/eliminación de archivos y cierre antes de arrancar el delegado.
- SDK-free real 3.13.15: 144 observaciones; raw BSON requiere SDK y queda omitido.
  API 18/13 subtests, typing y ratchet pasan. Dos controles de coste/dos subtests
  pasan sin aceptar el presupuesto integrado. 407 archivos protegidos intactos.
- `caed66da` protege el manifest del fixture mediante claim exclusivo y rechazo
  de directorios ya usados: 47 pruebas y segunda comprobación nativa correctas.
  Los 93 recursos Docker de 31 manifests retenidos están ausentes. Tres manifests
  diagnósticos tempranos fueron sobrescritos antes de esta guarda: sus IDs no
  se pueden reconstruir. El inventario global acredita ausencia residual, no
  recuperación del ledger perdido. La limitación consta en el receipt.

→ Sigue B07 unidad 5: sobres/DTO, resultado bulk y combinaciones restantes de
respuestas/fábricas/callbacks. El pipeline parcial que reconstruye su fuente
desde el cursor completo sigue siendo sospecha D06 por inspección, sin testigo
que demuestre divergencia. Resto D06, H53, decisión D07 y D05 siguen abiertos.


## B07 — DTOs, sobres y diagnóstico de escritura

✅ Cerrado el corte en `05b3f655`, con [aceptación y evidencia](evidence/bson-consolidation-implementation/write-responses/README.md).
El DTO común de comandos conserva orden n/writeErrors/upserted/nModified/ok,
omitiendo solo campos ausentes. Los DTOs públicos y bulk_api_result ya correctos
se reutilizan. El guard clásico añade razón semántica sin perder su WriteError;
la política de operación resuelve el prefijo nativo de update/findAndModify.
El error findAndModify usa el materializador de comandos, conserva la fábrica
y evita TypeDecoders ajenos al subtree del error. Diagnóstico y fallback
conservan la secuencia de metadatos del sobre antes de su contexto.

- MongoDB real standalone 7.0.43/8.0.32/9.0.2: cuatro acciones/95 observaciones
  por versión, cinco perfiles de codecs/fábricas, captura/check exactos en los
  campos estables; hello mantiene los datos variables. Casos con error y upsert
  simultáneos evitan aceptar un orden correcto solo en respuestas simples.
- Baseline instalada: 48 fallos/51 correctas. Focal checkout: 99 correctas.
  Wheel final de 287 módulos: 10.759 replay, 2.457 de cohorte/4.709 subtests,
  12.068 de identidad integrada y 53 de identidad del núcleo, todo correcto.
- SDK-free real Python 3.13.13: 570 observaciones de los cinco perfiles con
  cuerpos de observación intactos; Int64 se vincula al int portable y no hay
  especímenes Int64/Code en ese corpus. No se acredita su hook/constructor SDK.
  API 18/13 subtests, typing y ratchet correctos. Los 415 archivos protegidos
  están intactos. Los 36 recursos de 12 manifests y claims retenidos están
  ausentes por ID, sin sobrescritura de evidencia.

→ Sigue B07 unidad 5: constructor/setter que falla en respuestas de escritura,
comandos y findAndModify, con/sin registry; observar fase, efectos previos y
estado ya confirmado, sin asumir rollback por un fallo de decoder. El resto
D06, pipeline parcial aún no reproducido, H53, decisión D07 y D05 siguen abiertos.


## B07 — Fallos de fábricas y frontera cliente de findAndModify

✅ Cerrado el corte funcional de la unidad 5 en `a8a81f0e`, con
[aceptación y evidencia](evidence/bson-consolidation-implementation/write-factory-failures/README.md).
El servidor y la API comparten el DTO del resultado atómico. La respuesta
completa se materializa en la frontera cliente, con selección value y metadata
según el SDK; los errores sin value conservan su camino. Se retiran las dos
capas privadas de AsyncCollection que volvían a copiar el documento público.
El snapshot defensivo previo al codec permanece y SPI/formatos no cambian.

La rama portable comparte la recursión existente y distingue el error de
construcción/setter raíz de InvalidBSON dentro de un elemento embebido. El
comando diferencia codec_options explícito del heredado, que no activa por sí
solo la fábrica. La decodificación posterior no revierte una escritura confirmada.

- MongoDB 7.0.43/8.0.32/9.0.2: standalone, dos acciones/672 observaciones por
  versión; replica set, una/8 con barrera de eventos. Corpus/manifests/resultados
  exactos en capture/check; los relojes de hello/BuildInfo RS se conservan como
  variables. Los doce manifests y claims son distintos; 36 recursos ausentes.
- Baseline instalada: 36 fallos/30 correctas. Wheel final de 287 módulos:
  10.825 replay, 2.457 de cohorte/4.709 subtests y 12.068 de identidad integrados,
  todo correcto. El focal standalone de 51 era feedback, no cierre de la rama
  portable ni aceptación conjunta. API 18/13 subtests, typing y ratchet pasan.
- SDK-free real 3.13.13: 3.564 observaciones; se omiten expresamente 468 que
  exponen RawBSONDocument del SDK. Cuerpos de observación intactos. Sobre la misma
  wheel pasan además los 144 controles previos de batch, 570 DTO y 1.488 de
  proyecciones, con sus límites anteriores. 419 archivos protegidos intactos.
- Tres rondas CPU alternadas con checksum/estado iguales, ambos motores y
  plain/factory/registry con STANDARD: medianas 0,888–1,028. No se introduce una
  optimización compensatoria ni se acepta por ello el presupuesto global D05.

→ Sigue D06: contextos/consumidores restantes, sospecha del pipeline parcial aún
sin divergencia reproducida y transformaciones de respuesta distintas de los
constructores/setters que lanzan errores. H53, decisión D07 y B07 unidad 6/D05
conservan sus obligaciones de integración. Este corte no acredita CI remota.


## Fuente materializada y conservación de contexto

✅ Aceptado el candidato `b09292b2`, sin cambio productivo, con
[evidencia archivada](evidence/bson-consolidation-implementation/bson-grammar-context/materialized-acceptance.json).
Los callers reales mantienen la primera barrera no pushable dentro del prefijo:
fuente completa y prefijo conservan filtro, proyección, sort, skip y limit.
No se reproduce la sospecha histórica; no se refactoriza esta ruta por inspección.
Las regresiones observan PreparedPipeline, identidad del contexto, dialecto y
ruta materializada efectiva, con/sin spill y ambos motores.

MongoDB 7.0.43/8.0.32/9.0.2 aporta 128 observaciones por versión para ventanas,
collation, let, NOW, gramática DBRef/Code y entradas vacías/pobladas. Pasan 78
pruebas instaladas, incluidas tres propiedades de 500 ejemplos. El perfil sin
SDK real aporta 288 observaciones de ventanas/contexto; omite la acción que
necesita el constructor Code. Se reutiliza la wheel a8a81f0e, sin drift en sus
287 módulos. Permanecen intactos 427 archivos protegidos; 27 recursos de nueve
manifests distintos están ausentes. El seed del corpus nuevo se hizo neutro
antes de la captura final para no perder orden DBRef al cargar Extended JSON;
no se cambiaron observadores ni referencias históricas para aceptar resultados.

## Obligaciones de cierre vigentes

Esta es la única lista ejecutable de pendientes, fijada por el encargo de cierre.
Los apartados anteriores conservan historia, no generan obligaciones adicionales.
Estado vigente de esta lista: **4/8 cerradas**. No hay defectos reproducidos
pendientes en el candidato al fijarla. Un gate rojo se registra en la obligación
que incumple; no abre una auditoría general. Las hipótesis sin regresión ni
incumplimiento obligatorio quedan como seguimiento, fuera del cierre.

| ID / clase | Contrato exigido | Evidencia que falta / criterio observable de cierre | Estado |
| --- | --- | --- | --- |
| O1 / comprobación obligatoria | Fuente materializada conserva selección, representación y contexto D06. | Aceptación archivada del candidato y commits semánticos; 78 pruebas, nativos 7/8/9, SDK-free y ausencia de recursos acreditados arriba. | Cerrada |
| O2 / comprobación obligatoria | D06a/b/c conserva gramática, BSON previo al decoder, contexto y equivalencia de claves por consumidor; B07 conserva efectos de fábricas también cuando transforman sin lanzar. | Componer mapa requisito→aceptaciones existentes y completar únicamente el diferencial de transformaciones de documento/metadata aún no acreditado. Cierra con resultados nativos coincidentes por superficie soportada y sin huecos normativos en ese mapa. | Cerrada; aceptación y mapa debajo |
| O3 / comprobación obligatoria | H53 dispone C03/B08 distinguiendo evaluación, permiso y warning, strict/moderate/off, error/warn/bypass y documento anterior/candidato. | Contrastar caracterización retenida y contrato público; documentar disposición explícita y límites. No exige crear un canal nuevo de logging. | Cerrada; disposición debajo |
| O4 / decisión exigida | D07 requiere continuar/descartar el piloto opcional con criterio explícito. | Disposición documentada: descartar el piloto en esta entrega conforme al encargo de no hacer refactorizaciones opcionales; conservar implementación y garantías aceptadas. | Cerrada; descarte documentado debajo |
| O5 / comprobación obligatoria | D05 exige suite global, cobertura ≥99% y propiedades profundas sobre candidato instalado. | Resultado terminal de los gates; cualquier fallo obligatorio conserva bloqueo de cierre hasta su resolución y verificación causal. | Cerrada; aceptación D05 debajo |
| O6 / comprobación obligatoria | D05 exige Python 3.13/3.14/3.15, extremos SDK/pytest admitidos y perfil realmente sin SDK. | Matriz de CI y perfiles ejecutados, imports de artefacto comprobados; reutilizar evidencia idéntica admitida y distinguir omisiones de clases SDK. | Cerrada; aceptación D05 debajo |
| O7 / comprobación obligatoria | D05 exige distribución reproducible, API/typing/SPI, compatibilidad persistida y presupuestos de coste. | Resultados de runners existentes y evidencia previa causalmente válida; todos los gates requeridos verdes, sin sustituir presupuesto por un benchmark diagnóstico. | Cerrada; aceptación D05 debajo |
| O8 / comprobación obligatoria | Entrega trazable, revisión del candidato, recursos liberados, commits semánticos y árbol limpio. | Receipt integrado ligado a fuentes/artefactos exactos, obligaciones O1–O7 cerradas y comprobación final del árbol. No requiere publicación ni CI remota. | Cerrada; receipt integrado debajo |

Hipótesis dispuestas: la reconstrucción de fuente desde pipeline completo queda
sin defecto reproducido y protegida por O1. Otras sospechas que no incumplan las
garantías de esta lista se registrarán como seguimiento, sin ampliar el alcance.


### Disposición O3 — Validación y observabilidad H53

Se conserva el contrato de aceptación de escrituras C03/B08. Strict/error rechaza
un candidato inválido; moderate/error permite actualizar documentos antiguos ya
inválidos; off y bypass omiten validación; warn permite escribir. Un resultado
válido del validador tras omisión no acredita que el candidato satisfaga el schema.
El motor local no publica el warning de servidor 20294. Este límite se documenta
expresamente: no se promete el mismo logging de mongod ni se añade una superficie
de avisos o datos de documentos por implicación. Bajo moderate/warn no se necesita
forzar una evaluación adicional para producir un aviso que esta API no ofrece.

Se reutilizan la caracterización física
[native9-characterization.json](evidence/bson-consolidation-review/native9-characterization.json)
(`native.moderate_warn_logs`, warning 20294 bad→bad) y las referencias nativas
`semantic_schema` 7/8/9 de lifecycle para permisos, errores, estado y bypass.
La matriz añadida observa 288 transiciones públicas: ambos motores, tres dialectos,
tres niveles, dos acciones, antiguo/candidato válido o inválido y bypass activado
o desactivado. Pasan 28 pruebas/302 subtests sobre la wheel instalada, incluyendo
helpers y schema; [log](evidence/bson-consolidation-implementation/bson-grammar-context/materialized-validation-policy.txt.gz).
El log de servidor retenido es caracterización 9.0, no una captura nueva 7/8/9 de
logs; las garantías de permiso no se confunden con paridad de logging.

### Disposición O4 — Piloto D07

Se descarta el piloto opcional para esta entrega por el encargo explícito de
cierre sin refactorizaciones opcionales. Se conservan CompiledJsonSchema y las
primitivas compartidas ya aceptadas; no se crea un segundo intérprete ni una
migración sin mejora medida. Esta decisión cierra D07, no elimina ninguno de los
gates D05 ni modifica el subset o los diagnósticos de schema.


### Aceptación O2 — D06 y transformación de sobres

`8cede116` extrae la comprobación ya existente de Database.command al owner
error_documents y la reutiliza después de materializar findAndModify y cursores.
Se comprueba el sobre transformado antes de seleccionar value/batch, sin volver
a ejecutar codecs ni deshacer una escritura confirmada. Un setter que cambia
ok a cero demuestra el defecto: native falla con KeyError de errmsg ausente y
la directa anterior devolvía value. Se conserva ese error cliente, los efectos
previos y el estado ya escrito, en lugar de fabricar un error del servidor.

[Receipt O2](evidence/bson-consolidation-implementation/bson-grammar-context/response-transform-acceptance.json):
MongoDB 7.0.43/8.0.32/9.0.2, 312 observaciones por versión; ambas APIs y motores,
wire sync/async, con/sin registry. El corpus transforma v, ok, n, value,
lastErrorObject y writeErrors en escrituras, comandos, find y aggregate. Pasan
267 regresiones causales instaladas; sin SDK real pasan 1.872 observaciones,
sin omisiones de este corpus. Lint correcto en siete archivos; 427 protegidos
intactos; nueve manifests con claims distintos y 27 recursos ausentes.

Mapa finito de garantías D06, reutilizando aceptaciones sin reabrirlas:

| Garantía | Evidencia aceptada / consumidor |
| --- | --- |
| D06a: DBRef literal/operador, orden/extras/tipos negativos; Code/String por contexto | Gramática y contexto del registro, completados por sufijos, raw y dispatch; fixtures semantic_dbref_grammar y semantic_bson_grammar_context preservados, incluido contexto let, field arguments, facet/lookup/union y vacío/poblado. O1 añade prefijos reales y reloj compartido. |
| D06b: BSON antes del decoder, orden, undefined anidado, scope y entradas wire de filtro/escritura | Cortes aceptados binary-and-raw-boundaries, raw-response y codec-dispatch, seguidos de document-factories, proyecciones, batching y write-factory-failures. No se usa el checkpoint histórico rojo como aceptación ni se reconstruye orden ya perdido en archivos antiguos. |
| D06c: equivalencia de claves, representantes, group/lookup/sets/spill | Receipt bson-consumers y pruebas test_bson_consumer_plans: activación real hash/spill, fallback, collation, negativos Binary/Code y representante BSON. Se retiró _aggregation_key; storage keys no se migran. |
| D06c: poda, ranking y persistencia Search | Receipt search-bson-persistence: candidatos completos antes de residual/ventana, ambos motores, reapertura e índices. No acredita recall ANN ni positivos Atlas. |
| B07: materialización y efectos no excepcionales | Receipt O2 anterior, junto con los cortes de fábricas que fallan y DTOs ya aceptados. |

Este mapa cierra la aceptación acotada D06. La integración y las garantías de
coste/entorno sobre el candidato final siguen en O5–O8; no se sustituyen por
los replays ni por los checkpoints históricos.


### O7 — Conciliación del manifiesto público generado

El gate detecta 14 diferencias respecto del snapshot generado anterior, no de
un oráculo MongoDB. Se concilian solamente cambios ya implementados y aceptados:
full_document nullable de watch (D04); bulk_api_result; UUID UNSPECIFIED; kwargs
de creación de índices (admisión B07); y el campo opcional change del resultado
atómico compartido de `f8344e75`. Este último amplía el DTO existente de forma
compatible con sus constructores anteriores; no cambia métodos ni versión SPI.
El canario externo SPI v2 sigue verde. Las menciones históricas a «sin cambio
SPI» se refieren al protocolo del proveedor, no a identidad textual del DTO.

El snapshot se regenera con el runner existente y después pasa --check. Se
retienen el [original](evidence/bson-consolidation-implementation/bson-grammar-context/d05-public-api-before.json.gz)
y el [delta acotado](evidence/bson-consolidation-implementation/bson-grammar-context/d05-generated-api-delta.json).
Esta excepción explícita afecta solo a metadatos generados; no se cambian
capturas nativas ni observadores para acomodar un fallo. Los 427 protegidos
permanecían intactos en las aceptaciones O1/O2, anteriores a esta conciliación.
O7 continúa abierta hasta completar el conjunto de gates exigido.


### O5–O7 — Primer gate integrado y conciliación de regresiones

O5 sigue abierta. El [primer global](evidence/bson-consolidation-implementation/bson-grammar-context/d05-global.txt.gz)
usa la wheel a8a81f0e y termina con 366 fallos, 57.366 correctas, 482 skips,
253 warnings, 14.388 subtests correctos y cobertura 98,56%: no supera el 99%.
Es diagnóstico anterior al arreglo O2, no aceptación final. Se retienen los
[275 padres fallidos, incluidos subtests](evidence/bson-consolidation-implementation/bson-grammar-context/d05-all-failed-nodes.json.gz).
Sobre la wheel 8cede116 se repiten solamente esos padres: el primer grupo
produce 284 fallos/2.452 subtests correctos; los 39 restantes producen 82 fallos
con 39 padres correctos. Estos resultados tampoco se aceptan como cierre.

Dos correcciones de tests conservan garantías ya contrastadas, sin tocar
producción ni goldens: el spy de compile_find_operation sigue su owner
_collection_reads; clone/rewind siguen el contrato nativo de bson_request
(snapshot de entradas actuales y contexto nuevo para otra ejecución), sin
perder la inmutabilidad de la ejecución ya ligada. La prueba de $out conserva
rechazo antes de escribir y ahora comprueba código 40324 y el diagnóstico
aceptado, en lugar del texto retirado. Ese bloque pasa 765 pruebas instaladas.

Otras 152 pruebas instaladas conservan NaN histórico al reabrir SQLite y
transacciones/publicación. Las nuevas escrituras usan el valor BSON público,
conservando el wrapper privado exclusivamente en el payload histórico inyectado.
Drop/rename comprueban sus efectos DDL e invalidate ya aceptados, en lugar de
exigir una cola vacía; create/index conservan su ausencia de efectos. No se
normaliza el observador para hacer pasar un resultado.

Pruebas independientes ya terminadas sobre el runtime 8cede116: 66 propiedades
profundas; 149 pruebas/2 subtests de reapertura, UUID previo, proyecciones,
Search y coste determinista; 13 pruebas mínimas con SDK realmente ausente;
smokes wheel/sdist/pytest-extra; typing y canario SPI v2. Ambos smokes benchmark
size=250, warmup=0, repetitions=1 (search/vector) terminan correctamente. Son
smokes, no una promesa de ausencia de sobrecoste. Los logs están archivados aquí
con prefijo d05; los JSON de benchmarks conservan entorno, fuentes y métricas.

El primer intento de imports mínimos falló porque sus hijos no heredaban las
dependencias del intérprete -S. El segundo mostró que esos tests limpian
PYTHONPATH y cargaban una instalación anterior. La instalación real del wheel en
minimal313 cierra el gate con 13 pruebas correctas, sin modificar su lógica.
Un unittest global con ese problema se interrumpe limpiamente (130) y no se
acepta. La .venv principal queda instalada con el candidato para el gate final.
La matriz del kit permanece activa; sus tres primeras celdas 3.13 pasan.


### O5 — Regresión de admisión de proyección corregida

Las referencias nativas existentes api_options reproducen una regresión de B07:
find_one con proyección set devolvía InvalidDocument en lugar del documento;
con generador devolvía InvalidDocument en lugar de TypeError. El encoder común
encode_operation_bson reutiliza ahora projection_document antes de BSON, igual
que las otras entradas. No se añade parser ni se modifica el oráculo.

La nueva wheel y sus 287 módulos están fijados en d05-projection-wheel.json y
d05-projection-runtime.json. Pasan 93 pruebas instaladas/3.744 subtests, que unen
el diferencial api_options a las garantías de preparación; no se debilita el
rechazo de generadores ni se aplica el normalizador después del encoder. Este
cambio invalida solo la evidencia alcanzada por esa frontera y exige el gate
global final posterior; O5–O8 continúan abiertas.


### O5 — Eventos, fases y metadatos estáticos

Pasan 36 controles focales de identidad/errores/fallback. El documentKey del
evento conserva el tipo BSON del ID anterior mientras el documento almacenado
conserva el ID convertido; commit/abort siguen comprobados. El error del primer
batch de aggregate sync se observa al abrir, conforme al corte de fronteras ya
aceptado; async conserva sus consumos first/iteración. Se reduce el eje sync a
una prueba de apertura, conservando diagnóstico completo y recuperación.

La admisión pública conserva tipos numéricos etiquetados y exige su fallback
conservador. Se retira la expectativa de SQL de esos operandos públicos; el
contrato SQL de números originales sigue en test_sqlite_engine y la lectura
nativa del archivo previo conserva schema/documentos/auxiliares. No se crea un
pushdown nuevo para hacer verde el test ni se afirma que el coste sea idéntico.

Los cinco checks estáticos de reloj/emisores pasan. Se añade únicamente el
callsite de presupuesto monotónico Search extraído del owner previo. Para los
emisores se transfieren exactamente tres entradas históricas: dos constructores
pasaron de string literal a variable message y _aggregation_key cambió de nombre.
Los diffs de 8e261c7a/7524e392 y el constructor antiguo fijan la equivalencia;
el presupuesto total de emisores sin código no aumenta. Originales archivados:
d05-clock-inventory-before.json.gz y d05-error-inventory-before.json.gz. Son
inventarios locales, no capturas nativas, y su conciliación no admite nuevos
emisores sin identidad. O5 sigue abierta hasta resolver los demás fallos y el
99% de cobertura.


### O5/O7 — Inventarios de campos y unidades de error

Pasan 55 pruebas del corte de inventarios y sobres de error. Se conservan los
campos, significados acotados, evidencias positivas/negativas y comprobaciones
de fuentes. Los cinco CSV existentes se vinculan a fuentes de 073dc33e; sus
originales están retenidos con prefijo d05-before-. La fila de ID de cursor
precisa EOF establecido por la respuesta, sin equipararlo al conocimiento previo
de todo el sufijo; añade el test de protocolo con spill ya existente. El helper
de referencias admite funciones pytest además de métodos unittest y continúa
exigiendo su existencia real. No se crea otro inventario ni un runner.

Los dobles de find_modify_errors reciben el owner mínimo y CodecOptions,
conservando su camino cliente de materialización y las aserciones de identidad,
labels y diagnóstico; no se evita ese camino con server_command. La conciliación
no modifica código productivo. Quedan 79 padres de los fallos originales por
resolver en O5; esta cifra interna no cambia las ocho obligaciones de cierre.


### O5 — Metadata sort y proyección acotada corregidos

`c4917a3f` corrige dos regresiones reproducidas en la garantía de metadata B07.
El frontend convertía $meta:textScore a una clave privada con NUL antes de BSON;
el constructor find y el setter sort comparten ahora sort_document, que valida
la forma pública y conserva el documento BSON hasta la admisión. La normalización
de ejecución permanece después del encoder; no se añade un parser ni se aceptan
claves NUL del usuario. El segundo defecto estaba en el sort acotado 9.0: se
proyectaba un _RuntimeReadDocument como mapping plano y se perdía el metadata.
Se pasa su RuntimeDocumentState al proyector existente y se conserva al envolver
el resultado, con una única evaluación por documento.

[Receipt](evidence/bson-consolidation-implementation/bson-grammar-context/d05-text-sort-acceptance.json):
MongoDB 7.0.43/8.0.32/9.0.2 aporta siete observaciones por versión: argumento,
setter, ventana limit=1 y find_one, dos rechazos NUL y recuperación. La wheel
anterior falla 16 y pasa 11 controles del corpus final; la nueva pasa 273
regresiones de metadata/proyecciones/batches y 131 pruebas de contratos con
3.744 subtests de API, preparación, reapertura y spill. El cuerpo idéntico sin
SDK real pasa 42 observaciones sin omisiones. Se conservan 424 protegidos;
solo los tres inventarios/metadatos generados ya justificados difieren.
Los seis manifests y claims son distintos y sus 18 recursos están ausentes.

El testigo usa empate de score y orden por ID; no acredita paridad numérica del
score textual local. Las capturas históricas permanecen intactas. O5–O8 siguen
abiertas: este corte no acepta la suite global roja, la cobertura ni matrices
que se ejecutaron antes de estos cambios productivos.


### O5 — Vista index_information corregida

`571fa575` restaura una regresión de c715ae75: al formar index_information desde
list_indexes se copiaba unique=False, que la vista anterior omitía conforme a
IndexDefinition.to_information_entry. Se omite ese default en la vista de
información; el catálogo tipado y su almacenamiento no cambian. No se retocan
las expectativas existentes para aceptar el campo extra.

[Receipt](evidence/bson-consolidation-implementation/bson-grammar-context/d05-index-information-acceptance.json):
MongoDB 7.0.43/8.0.32/9.0.2 confirma tres variantes (default, false explícito,
true). La wheel anterior falla 12 y pasa tres controles; la nueva pasa 114
regresiones de API/admisión/codecs y 18 observaciones portables sin SDK. El primer
intento portable comparaba tuples con las listas creadas por JSON; se restaura
la forma list-of-tuples de key al cargar el esperado, sin cambiar el observador.
Los tres manifests tienen claims propios y sus nueve recursos están ausentes.
El censo existente IndexModel actualiza solo identidad de fuente/creación a este
commit y conserva sus 16 campos/evidencias; su check pasa. El alcance es esta
vista de las APIs directas, no equivalencia completa de todo el catálogo wire.

La matriz previa de 12 celdas termina verde y queda retenida con prefijo
d05-prior-kit-. Acredita su wheel 29a485d7, anterior a los últimos arreglos; no
se usa como aceptación final del runtime posterior. O6 permanece abierta.


### O5 — Fixtures de paridad y contrato BSON público

Pasan 90 pruebas de paridad entre motores con 16 subtests. Los fixtures que
escriben UUID eligen STANDARD explícitamente y conservan sus comparaciones;
no se altera el default UNSPECIFIED. $count sobre entrada vacía conserva la
salida nativa vacía. Los dos casos de undefined siguen comprobando sus resultados
7.0/8.0, pero siembran ese tipo histórico por el SPI, sin confundirlo con una
entrada pública encodable por el SDK. No se añade una dependencia bson a ese
módulo de pruebas ni un helper para eludir la frontera pública.

El control de los 79 padres restantes, posterior a las correcciones causales,
termina con 77 fallos contando subtests, 48 padres correctos y 40 subtests
correctos. Se continúa atribuyendo ese conjunto; no es una aceptación global
ni modifica el umbral de cobertura o las ocho obligaciones.


### O5 — Preparación de extensiones y binding diferido

La regresión del test de descubrimiento de extensiones procedía del análisis de
metadata: reconocía el override pero descendía después por los campos del builtin
sustituido. El handler conserva ahora su gramática, metadata desconocido y
descubrimiento de recursos existente. No se invoca durante preparación.
La wheel fijada en d05-extension-wheel.json pasa 267 pruebas y 83 subtests,
incluidas preparación, metadata, contexto y proyecciones.

Pasan 37 pruebas y 11 subtests de límites de lectura. Los spies capturan el
contexto al ligar la ejecución, después de verificar ausencia de trabajo al
construir; rewind renueva contexto/identidad y deadline conservando sesión.
Clone de una ejecución ligada prueba reloj independiente; el fixture sync
observa referencias vivas antes de consumir, conforme al corpus nativo
bson_request. Se conservan proveedor único, cierre y recuentos de compilación.
Los logs se archivan con prefijos d05-extension y d05-read-context; O5 sigue
abierta y mantiene el umbral de cobertura y todos sus fallos restantes.


### O5 — Salidas BSON y dobles administrativos

Pasan 22 pruebas/8 subtests de salidas y errores públicos. $toDecimal devuelve
Decimal128 y $toUUID devuelve Binary subtipo 4 con UNSPECIFIED, conforme a sus
aceptaciones nativas existentes; no se devuelve Decimal/UUID por comparación
Python. List_collections inválido conserva error de servidor 14, mientras
list_collection_names conserva TypeError cliente. Namespace de validación
inexistente conserva 26. Watch wire sigue explícitamente fuera del subset y
su rechazo usa 40324 con el diagnóstico vigente, sin aceptar watch positivo.

Los dobles de la fachada command delegan en command, que aplica la frontera
cliente, y la lectura de system.profile respeta su proyección (op, sin _id).
Se mantienen aserciones de campos, familias y códigos; son conciliaciones de
pruebas respecto de cortes ya aceptados, sin nuevo runtime ni oráculo nativo.


### O5/O7 — Catálogo actual y lectura de archivos anteriores

El catálogo actual se regenera con update_compat_snapshots.py; los fixtures
históricos permanecen intactos. El contraste de JSON solo añade tres flags de
EOF por dialecto y comentarios de listados, y corrige tres notas de validate
respecto de su conversión BSON ya aceptada. El delta histórico conserva orden
y entradas previas salvo esas actualizaciones explicadas, con originales
archivados d05-compat-before-. No se normaliza un resultado MongoDB.

Pasan 46 pruebas del catálogo y tres controles focales de archivos anteriores y
flags legacy. El perfil de lectura de UUID del fixture SQLite 4.5/4.10 pide
STANDARD explícito para comprobar el UUID original sin alterar UNSPECIFIED.
Scandata string no se rechaza: se comprueba su truth conversion nativa, y None
permanece falso; nameOnly string sigue rechazado. Projection flag numérico 2
mantiene inclusión, conforme a la proyección ya contrastada. Las aserciones del
catálogo conservan inmutabilidad, hooks exactos y reconstrucción por deltas.
El gate conjunto ampliado está en d05-compat-final; O5/O7 continúan abiertas.


### O5 — UUID de Search después de admisión BSON

Se reproduce el siguiente consumidor roto por la admisión estricta: los datos
UUID escritos bajo STANDARD llegan a Search como Binary subtipo 4, pero su
clasificador de facetas solo reconocía uuid.UUID y eliminaba esos buckets.
Se reutiliza binary_parts; únicamente subtipo 4/16 bytes representa un UUID.
Los subtipos 0/3/5, bytes sin subtipo y longitud inválida siguen fuera del dominio.
No se altera el score, recall ANN ni se declara un positivo Atlas en mongod.
La garantía es conservación de representaciones en el subset local ya existente.

La wheel congelada pasa 38 pruebas y 25 subtests de helpers, escalares, facetas,
API y reapertura Search. Otros 16 controles/5 subtests observan equivalencia y
negativos. Los fixtures afectados piden STANDARD cuando necesitan UUID, usan
Decimal128 y datetime en operandos públicos, y mantienen find_one con ID escalar
array válido. Nueve pruebas/16 subtests pasan sobre esos fixtures, incluyendo
reloj único y rechazo 14 del q inválido en bulk sin capturar un reloj.

La revisión causal de eventos pasa diez pruebas/20 subtests. Se observa orden
en updateDescription, sin exigir que fullDocument/updateLookup congele un estado
anterior a su lectura. El fallo de publicación se inyecta en publish_committed,
el sink efectivo del adaptador, manteniendo escritura confirmada, degradación y
error al leer. Index_information conserva su decoder builtin UTC naive aunque
la colección tenga tz_aware; el evento conserva sus opciones de codec. No se
modifican oráculos nativos para acomodar esos contratos ya aceptados.
O5–O8 siguen abiertas y la suite global final/cobertura siguen exigidas.


### O5 — Regresiones administrativas, índices y merge

Pasan dos padres de formas de comandos, tres controles de merge/namespace con
seis subtests y seis pruebas de índices/wire con ocho subtests. Se conservan
los parsers no afectados: la lista vacía de createIndexes ya produce código 2;
comentarios BSON de listados son válidos; validate ausente produce 26. La prueba
keepExisting usa whenNotMatched=insert, combinación válida del corpus nativo
semantic_merge_key, conservando que no revalida ni reescribe el documento antiguo.

Drop_index por lista transforma claves al nombre generado, como SDK _drop_index;
no busca aliases personalizados. Los controles exigen código 27 y ausencia de
cambio antes de borrar por nombre explícito; se conservan aliases restantes.
Wire mantiene el rechazo de namespace drop vacío y el comentario BSON de
listCollections sin transformar un comentario válido en error. El resto de
rechazos de la prueba wire permanece intacto. No cambia producción en este corte.

Seguimiento fuera del cierre: una sonda de las 32 formas retenidas detecta
diferencias generales de parsers administrativos frente al servidor. Los mismos
TypeError para sort/projection y showPrivileges están en 181a922, anteriores al
candidato; no son una regresión demostrada ni una nueva migración aprobada de
gramática. Se retienen fuentes/capturas/claims con prefijo followup-command-shape
y followup-semantic_command_shape, sin registrarlas como gates de esta entrega.
La captura es 7.0.43/8.0.32/9.0.2 y sus nueve recursos están ausentes por ID.
Esto no acredita paridad general de toda forma inválida de comando: las garantías
exigidas son las de los cortes afectados y su subset declarado. No se introduce
un catch genérico de TypeError ni otro validador para ocultar estas diferencias.
O5–O8 conservan suite final, cobertura, matrices, distribución/costes y entrega.


### O7 — Gate de lint contra la release

El ratchet real de CI contra la última etiqueta de release detectó cuatro
bloques de imports desordenados. Se conserva el resultado rojo y el posterior
verde para 402 archivos Python modificados, sin cambiar reglas ni presupuestos.
La corrección solo ordena/coalescea imports; no cambia funciones, constantes ni
contratos. El wheel posterior verifica 87 pruebas y 291 subtests de codecs,
imports con/sin SDK y runtime de agregación. Las suites globales ya activas
conservan su wheel congelado y su manifiesto anterior: la aceptación final
registrará ambos hashes y esta verificación causal, sin atribuir al primero
los bytes del segundo. O5–O8 siguen abiertas. Evidencia: prefijos d05-lint y
d05-final-ratchet en el directorio existente.

### Aceptación D05 — O5, O6 y O7

El candidato de runtime final es `9e6b1886`; los cambios posteriores de esta
aceptación son pruebas y documentación. Se conservan por separado los bytes del
wheel anterior al lint y del posterior. El global anterior pasa 57.666 pruebas
y 14.811 subtests en Python 3.14; los de 3.13/3.15 pasan 57.655 pruebas y 14.811
subtests cada uno. Las omisiones son 485/496/496 respectivamente, sin acreditar
los fixtures físicos omitidos. Los diferenciales nativos aceptados por bloque
siguen siendo su oráculo, no esos skips.

El primer global queda rojo por cobertura: 98,61%. Se añaden controles del
contrato existente en los owners de pruebas actuales: admisión portable y
frames contra el SDK, corrupción BSON contra ambos decoders, rutas DBRef
persistidas, contexto/continuidad/cierre de proveedores, resultados SPI inválidos
y propiedad de contenedores. No se modifican producción, exclusiones ni umbral.
El loader de errores portable conserva sus aserciones y pasa a nombrarse bajo
el paquete para que coverage mida el código que realmente ejecuta.

La cobertura final es **99,00116530714166%**, 47.576/48.056 líneas: el gate
termina verde, sin aceptar el redondeo visual de un intento previo 98,999%.
Para reutilizar la suite global se trasladan únicamente las trazas de 286
archivos byte a byte idénticos. Se descartan todas las del codec editado y se
mide este de nuevo sobre el wheel actual; no se sintetizan marcas ni números
de línea. Se conservan trazas originales/finales, mapa y hashes. Los controles
posteriores al lint pasan 469 pruebas/347 subtests en 3.14 y 459/347 con diez
omisiones SDK en 3.13/3.15; los testigos adicionales del codec también pasan.
Las propiedades profundas instaladas pasan 66 pruebas.

Unittest pasa 3.654 pruebas en 3.14/3.15 y 3.663 en la repetición final de 3.13,
con tres omisiones cada uno. Se conserva el rojo anterior de 3.13: un explain
de dos documentos excedió sus 50 ms bajo las suites concurrentes. La repetición
completa sin esa carga pasa sin subir el timeout ni cambiar su aserción. Las
pruebas añadidas se verifican causalmente en los tres Python; no se suman los
replays solapados como pruebas únicas.

Las doce celdas Python 3.13/3.14/3.15 × pytest 8.4.2/9.1.1 × PyMongo
4.9.2/4.19.0 pasan. Se comprueban imports/manifiestos del artefacto instalado.
El perfil realmente sin SDK conserva 288/3.564/570 observaciones portables;
las omisiones RawBSONDocument/Code y aliases Int64 se mantienen explícitos. El
entorno mínimo y los controles posteriores del import de codec pasan. Los
probes independientes del SDK/release publicada se reutilizan porque sus tres
hashes de entrada siguen idénticos al receipt aceptado de predicate-authorities.

API pública, typing, canary SPI v2 independiente, reapertura de archivos
anteriores, compat snapshots actuales y ratchet contra la última release pasan.
Los 424 artefactos protegidos siguen idénticos; las únicas tres excepciones
son los metadatos generados ya conciliados: manifiesto público, allowlist de
relojes e inventario de emisores. Los presupuestos deterministas de coste y
evaluación anticipada pasan en el global; no se introduce un techo de CPU nuevo.

Las medidas finales usan los runners existentes y las mismas semillas/resultados:
matching/SQL, group/spill, materialización, validadores, consumo, Search y
fábricas. Los hashes de resultado de los 20 casos de report coinciden entre
base/candidato; los probes BSON y checksums de escritura también. Se conservan
medidas por corte anteriores, sin convertir ratios con bases distintas en un
ratio acumulado. Como límites diagnósticos permanecen hasta 1,28× en demanda
unitaria de Memory y 1,52× en Binary residual/limit=2; no se promete ausencia
de sobrecoste. Validadores medidos: ratios 1,02–1,08; matching/SQL y group/spill
del report: 0,92–1,04. No se hace una optimización opcional para ocultarlos.

Evidencia: prefijos `d05-final`, `d05-post-lint` y `d05-coverage` en el directorio
existente. Los logs rojos y selecciones diagnósticas fallidas permanecen
archivados, separados de los terminales verdes. O8 conserva aceptación integrada,
artefactos con changelog final y comprobación del árbol tras los commits.

### Cierre integrado — O8 y D05

**8/8 obligaciones cerradas.** El [receipt final](evidence/bson-consolidation-implementation/bson-grammar-context/d05-final-acceptance.json)
liga runtime `9e6b1886`, pruebas/packaging `ac8d6ee1`, 1.196 entradas de fuentes,
artefactos reproducibles y evidencias por corte. `0ad43826` registra los fixes
en el changelog; `ac8d6ee1` conserva las pruebas/aceptaciones O5–O7. El commit
documental que contiene este receipt completa la entrega; el estado limpio se
comprueba después del commit, sin crear un hash autorreferente en el receipt.

Los wheel/sdist definitivos se construyen dos veces desde fuente limpia con
changelog final y coinciden byte a byte. Los 308 archivos del paquete coinciden
con la instalación probada; los documentos públicos del sdist coinciden con el
árbol. Los smokes de esos artefactos exactos pasan para wheel, sdist y extra
pytest. La revisión final no encuentra cambios productivos posteriores al
runtime aceptado ni rebajas de contratos para alcanzar los gates.

Se libera el mongod propio de los fixtures y el contenedor efímero propio, sin
montajes del host. La ausencia de sus bases se acredita con los receipts por
recurso ya aceptados, sin repetir capturas válidas. Se conservan rojos
intermedios y límites: Search local no acredita Atlas; watch mantiene su subset
y rechazo wire; warning nativo no concede un API local de logs; clases SDK
omitidas siguen omitidas; los sobrecostes diagnósticos medidos no se eliminan
por declaración. El piloto D07 se descarta. Las formas raw de comando
preexistentes registradas como seguimiento no se incorporan al alcance.

No quedan obligaciones exigidas sin comprobar en O1–O8. No se ejecutan CI
remota, push, tag ni publicación. Las secciones anteriores conservan la historia
de cada candidato y no constituyen una segunda lista pendiente.


## Seguimientos P2 — entrega independiente B1–B4

Contrato autorizado en conversación: resolver las 32 formas retenidas de
comandos raw y los dos workloads de coste, base `3fe96e42`. O1–O8/D05
permanecen cerradas. No se incluyen ampliaciones P3. Se reutilizan owners,
capturador/replay y runners; no se crea otro framework ni otro validador.

| Bloque | Aceptación observable | Estado |
| --- | --- | --- |
| B1 | 32 formas dispuestas con fixture aislado, y dos workloads reproducibles de base congelada. | Cerrado |
| B2 | Cero divergencias confirmadas pendientes del conjunto; código/familia/fase/diagnóstico/efectos y recuperación nativos 7/8/9, controles de API Python y todas las superficies soportadas. | Cerrado |
| B3 | Causa medida y redundancia eliminada, resultados idénticos; tres tandas ABBA mejoran ambos workloads más que ruido A/A y no regresan controles fuera del ruido. | Cerrado |
| B4 | Gates aplicables verdes, revisión, evidencia, commits semánticos y árbol limpio. | Cerrado |

B1: se mantienen las 32 formas y se añaden 36 controles de reglas afectadas.
Cada una se ejecuta aisladamente con namespace ausente, vacío y poblado: 204
observaciones por servidor 7.0.43/8.0.32/9.0.2. El código 13 de listDatabases
es una restricción de base admin, no un fallo de permisos; sus controles admin
separan esa precedencia del tipo de nameOnly. La puntuación variable al cerrar
la lista de tipos de dropIndexes se canonicaliza sin borrar/reordenar tipos.
Se preservan las capturas históricas. La base inmutable falla 24 replays y pasa
tres verificaciones de captura; los 287 módulos de su wheel son byte a byte los
de `3fe96e42`. Ambos workloads de base conservan sus resultados BSON.
El perfil distingue ordenación completa en demanda unitaria y recompilación
por documento en residual Binary. Evidencia inicial con prefijo p2-b1 en el
directorio existente; B2–B4 siguen abiertos.

B2: el parser raw conserva coerciones, ausencia/null, límites, precedencias y
diagnósticos por comando/dialecto. Se comparten renderizado de errores BSON,
límites y lotes vacíos en _command_bson; no se cambia la validación de métodos
Python. Wire delega las reglas P2 y conserva sus guards de transporte y los
checks no afectados. Las fachadas de list_indexes siguen devolviendo vacío
para namespace ausente; el comando raw rechaza con 26 como el servidor.
Se dispone también listDatabases fuera de admin y validate background según
dialecto sin ampliar capacidades. Se reutiliza el corpus nativo en los tests
de consumidores en vez de otra tabla de TypeError supuestos.

Sobre wheel instalado pasan 27 replays/metadatos —4.896 observaciones— y
1.196 controles de consumidores, con 1.374 subtests. Los --check nativos 7/8/9
pasan contra salida separada; las observaciones coinciden con B1, cuya copia
se conserva, y solo se concilia la identidad del observador tras lint. El
ratchet contra la base P2 pasa en 13 archivos. Logs rojos intermedios y evidencia
final con prefijos p2-command y p2-b2. B3 y B4 siguen abiertos; O1–O8 no cambian.

B3, candidato en verificación: la ordenación detecta una sola vez claves Python
int homogéneas del dialecto estándar 7.0 y conserva el comparador canónico
para mezclas, dialectos personalizados, collation y deadlines. No se adelanta
la ventana ni se omite el residual. SQLite prepara cada residual de forma
perezosa al primer documento y comparte ese plan entre ANN, ampliaciones y
reserva exacta; conserva bindings, collation y contexto de la operación sin
caché global. La sonda ordinaria nativa acredita elegibilidad MQL, no Atlas.
Los controles incluyen candidatos elegibles tardíos y límites 2/64. Pasan
176 pruebas/4 subtests y 51 replays/metadatos sobre wheel instalado. La
aceptación de coste y los gates B4 todavía están pendientes.

Seguimiento ajeno al corte: el control adicional con find(limit=0) explícito
descubrió que la API directa devuelve vacío donde PyMongo lee sin límite.
Se reproduce también en el wheel base 3fe96e42; no es regresión del candidato.
Se registra sin corregirlo ni ampliar P2/P3. El control de lectura completa
usa ausencia de límite; mantiene BSON, orden, primera página y agotamiento.

Conciliación del gate B4: los consumidores antiguos de parsers raw se alinean
con la evidencia nativa existente; el test wire reutiliza la misma tabla de
expectativas de las fachadas en lugar de mantener otros diagnósticos locales.
La revisión detecta una regresión introducida en explain vacío: el objeto
vacío escapaba como StopIteration. MongoDB 7/8/9 devuelve 59 CommandNotFound
con el diagnóstico completo «Explain failed due to unknown command: ». Se
añade ese único control al corpus existente: 207 observaciones por servidor.
Las 204 observaciones originales permanecen idénticas. El guard conserva
la validación previa de verbosity y evita capturas generales de excepciones.
Los gates globales sobre el nuevo wheel siguen pendientes; se conservan
los fallos intermedios y no se reabren O1–O8.

El primer global de pytest descubre metadatos de fuentes obsoletos y
consumidores con precondiciones antiguas (cursor obligatorio, base admin y
coerciones de flags/límites). Se concilian únicamente los owners existentes
y se conservan sus estados. La edición automática de requests produjo una
entrada espuria en un diccionario de tests; el feedback focal la detecta y
se retira antes de congelar el candidato. Los logs rojos se mantienen.

Se corrige además una regresión de la delegación wire: batchSize no numérico
escapaba como TypeError. El parser común devuelve el diagnóstico nativo 14;
una sola conversión contextual BSON safeNumberLong se comparte con el cursor
wire: trunca, satura int64 y convierte NaN a cero. No se cambia la validación
de métodos Python ni el alias local estricto de batchSize superior de aggregate.
Las sondas 7/8/9 cubren doubles, Decimal128, NaN, infinitos y rango; añaden
30 controles/90 observaciones por servidor. Las 207 anteriores siguen idénticas; el corpus
queda en 297 observaciones por servidor, 32 formas retenidas y 67 controles.
Pasan 78 replays/metadatos, 4.536 unitarios/1.619 subtests y seis controles
explícitos de primera ventana (int/double/Decimal128). Los globales afectados
se repiten con pytest-xdist existente y el mismo suelo 99.00; no se sustituye
el gate global por feedback focal. B3/B4 continúan abiertos.

El conjunto de tipos numéricos esperado de batchSize se imprime en orden
inestable por los servidores. El witness ordena únicamente esa lista IDL,
conservando alias y multiplicidad; campo, tipo recibido, texto restante,
familia, código, codeName, fase y efectos se comparan sin omisiones.
Los campos FindCommandRequest.batchSize/find.batchSize por dialecto y
cursor.batchSize de aggregate siguen siendo distintos.

El global del candidato d89 pasa funcionalmente en los tres Python, pero
3.14 queda en 98,9876% (487 líneas sin cubrir), por debajo de 99,00. No se
acepta ni se rebaja el suelo. El hueco identifica controles ausentes de los
guards modificados y un helper wire genérico con un solo consumidor. Se
pliegan sus guards conservados en find y se retira el adaptador de batchSize
sin consumidores; no se añade otro runner ni un test que invoque código muerto.

Los controles nativos distinguen la conversión legacy de find.skip/find.limit
y count.skip del entero exacto exigido por count.limit. Se comparte la
primitiva de número BSON; count conserva sus errores propios, valor absoluto
y rechazo de min long. Se verifica también la distinción de explain aggregate:
7/8 exige cursor incluso en el comando explain exterior; 9 lo implica. La
ruta ya existente de explain recibe cursor en su copia únicamente para 9.

Se añaden 76 controles numéricos de esos guards y dos de precondición de
cursor/explain. El corpus queda congelado en 531 observaciones por servidor
(32 formas retenidas y 145 controles); las 297 anteriores no cambian. Pasan
78 replays/metadatos y 4.540 unitarios/1.619 subtests del nuevo wheel. Los
globales/cobertura de este corte siguen pendientes y no se acreditan con el
feedback focal. Las sondas de aggregate.explain inline, opción no anunciada,
quedan como seguimiento fuera de este corte; no se incorpora una nueva ruta.


Aceptación final P2: B1–B4 cerrados (4/4), sin obligaciones exigidas pendientes.
O1–O8/D05 permanecen cerradas; su texto anterior se conserva byte a byte.
El recibo derivado [p2-final-acceptance.json](evidence/bson-consolidation-implementation/bson-grammar-context/p2-final-acceptance.json)
selecciona el candidato y las evidencias aceptadas; las ejecuciones rojas e
intermedias se conservan y no se usan como aceptación.

B3: el primer intento del corte final no se acepta: aunque mejora ambos
objetivos en las tres tandas y pasan los 44 controles, la mejora Binary no
supera el ruido A/A. Se alarga únicamente la muestra del worker existente
(3 × 1.000 operaciones Binary; consumo sigue en 50), sin cambiar datos,
umbrales ni implementación. El protocolo largo A/A + tres ABBA, con los demás
gates propios terminados, conserva resultados BSON en los 16 turnos y pasa
44/44 controles. Razones candidato/base de lectura unitaria: 0,81184 / 0,85003 /
0,81816; agrupada 0,82287 frente a ruido A/A 0,08647. Binary: 0,90239 / 0,90336 /
0,91971; agrupada 0,91591 frente a ruido 0,03687. Ambas mejoras superan su ruido.
Workers, contexto, muestras y cálculo quedan con prefijo p2-b3-accepted;
la preparación se comparte también con la reserva exacta, verificada de forma
explícita en los tests públicos de candidatos tardíos y límites 2/64.

B4: el mismo wheel 4de2e33e9300b1aac800ee9f7e525237a7deb576c7a6a65c9204c898318764a7
pasa pytest completo en 3.13/3.14/3.15: 57.805 / 57.816 / 57.805 pruebas,
con 14.915 subtests cada uno. El primer global 3.13 agota el maxTimeMS original
de 50 ms; pasa aislado y se repite después el global completo, sin otra
suite global en paralelo y sin elevar el umbral: 57.805 pruebas,
499 omisiones y 14.915 subtests verdes. El feedback focal no lo sustituye.
La cobertura final 3.14 es 47.643/48.123 líneas (99,00256%); suelo intacto 99,00.

También pasan unittest (3.670 por Python, tres omisiones), la matriz de doce
combinaciones Python/pytest/PyMongo y los replays P2 con el SDK mínimo, quince
gates de tipado/SPI/manifiesto, snapshots idénticos en tres Python, 66 propiedades
deep y trece pruebas sin bson ni pymongo. Nueve smokes wheel/sdist/plugin y
los dos benchmarks públicos pasan. Dos builds producen wheel y sdist idénticos;
sus 287 fuentes y los cinco documentos públicos coinciden con lo verificado.
Las importaciones de los gates quedan ligadas al wheel por hash y ruta.
Los contratos PyMongo independientes del producto se reutilizan comprobando
sus entradas; el smoke del artefacto publicado se conserva separado de este
candidato, sin publicación nueva.

El ratchet real contra el ancestro de release detecta un import desordenado
en el consumidor wire de P2. Se ordena, pasan sus 19 pruebas/42 subtests y
el ratchet queda verde en 409 archivos. La revisión final mantiene 343 entradas
históricas de fixtures/snapshots/recursos intactas, no incorpora runners ni
capas permanentes y conserva la limpieza de las bases y procesos nativos propios.
Los commits separan witnesses, correcciones de gramática y regresiones,
formato del consumidor, mejora de coste y aceptación documental.

Límites del cierre: los oráculos nativos son standalone 7.0.43/8.0.32/9.0.2.
La elegibilidad MQL de Search no acredita Atlas ANN. Los seguimientos ya
registrados de find(limit=0) directo y aggregate.explain inline no anunciado
permanecen fuera del corte; no se incorporan ampliaciones P3.


## Parte I F00–F11 — incremento autorizado posterior

Contrato: apartado 9 de [plan consolidado](bson-semantics-consolidation-plan.md#incremento-propuesto-20261101),
refinado en 19138ebb y autorizado por el encargo de implementar F00–F11.
O1–O8/D05 y B1–B4 P2 conservan sus cierres. X01–X06 no se implementan.
Gate de entrada: contrato y autorización en conversación, censo de impacto
actualizado, base de runtime 2fa637f6 verificada contra sus 287 módulos del
wheel conservado. El proyecto no exige otra transición RFC para este cambio.
Pruebas focales por bloque; suites globales reservadas a F11.

| Bloque | Estado | Evidencia / siguiente criterio |
| --- | --- | --- |
| F00 | Cerrado | Inventario/oráculos congelados; f00-inventory.json y capturas 7/8/9. |
| F01 | Cerrado | 24 diferenciales 7/8/9; 225 regresiones de respuesta y 103 controles locales/21 subtests. |
| F02 | Cerrado | 192 diferenciales + 3 metadatos; 19 regresiones focales/3 subtests y lint. |
| F03 | Cerrado | 315 diferenciales/metadatos; 36 planes forzados y regresiones de consumidores. |
| F04 | Cerrado | 1.179 diferenciales/metadatos, 44 controles focales y ratchet. |
| F05 | Cerrado | 492 diferenciales/contexto/metadatos, 54 spill y fronteras focales. |
| F06 | Cerrado | Catálogo/unique/hashed, cuatro superficies, eventos RS 7/8/9, archivos previos e inventarios CXP. |
| F07 | Cerrado | Escrituras/identidad/campos, 7/8/9, contadores/eventos y validadores; aceptación integrada en F11. |
| F08 | Cerrado | Claves contextuales/natural, inicializadores y acumulación, materialización/spill real; aceptación integrada en F11. |
| F09 | Cerrado | Tres unidades y perfiles nativos; aceptación integrada y conservación CXP en F11. |
| F10 | Cerrado | Fachadas/metadata nativas y rechazos sin efectos; aceptación integrada en F11. |
| F11 | Cerrado | Producto b83789bc / wheel 176a9857: G1–G6 aceptados; 44/44 controles de coste. Véase la aceptación final; las campañas anteriores conservan su disposición histórica. |

F00: se reutiliza el runner y se registran 135 testigos focales de familias
F02–F10 más el caso de límites en semantic_response_frontier. Capturas físicas
7.0.43/8.0.32/9.0.2; las nueve observaciones previas de response_frontier por
versión permanecen idénticas. Se preservan los rojos locales iniciales y de
límites. Un primer check rechaza hashes de fuente obsoletos por capturas
tempranas concurrentes sobre el mismo destino; tras terminar sus escritores
y recapturar fuentes estables pasan siete checks de metadata/registro.
Range grande solo se capturó en mongod (146 antes de materializar), nunca en
la base local sin guard. Los efectos/watch requieren controles adicionales
de sus bloques; este inventario no los acepta por asociación.

F01: la normalización pública de find/limit separa cero sin límite y negativos
de una respuesta del cero interno vacío. El cursor conserva singleBatch en
clone y cierra la fuente después de esa respuesta sin perder su buffer.
To_list valida longitud entera <1 antes de leer/decodificar; en cursor cerrado
conserva la precedencia del SDK. First mantiene su extensión local y el cero
interno sigue vacío. Se concilian las expectativas antiguas que exigían el bug.
Pasan 24 diferenciales 7/8/9 de cuatro superficies, 225 controles focales
de consumo/callbacks/límites y 103 pruebas locales/21 subtests. Los controles
nativos adicionales cubren clone, skip, lote menor/igual al límite negativo,
primera respuesta de 101, y validación tras close. Observaciones previas
conservadas; la única conciliación de hash sin recaptura es un comentario noqa
con AST idéntico, no una expectativa alterada. Logs con prefijo f01.
Suites globales y aceptación integrada siguen reservadas a F11.

F02: shift, locf y linearFill materializan ausencia en sus propias fronteras
nullable usando el helper existente; shift usa default también para el campo
ausente de una fila destino existente. ArrayToObject valida NUL al construir
el objeto, con 4940400 y evaluación constante/contextual en su fase; bson_size
permanece estricto. La validación de sort de shift/linearFill se comparte entre
preparación y aplicación, y se rechazan claves repetidas de linearFill en su
partición con 6050106. Se añaden cinco controles de bordes/default, NUL dinámico
y sort inválido/duplicado: todas las 135 observaciones F00 anteriores quedan
idénticas en 7/8/9. Pasan 192 diferenciales, tres metadatos y 19 controles
focales/3 subtests. Lint verde, logs f02; F11 sigue pendiente.

F04: el plan canónico distingue eq Regex literal de ejecución implícita; not
admite Regex, y regex/in conservan también la igualdad del patrón y flags
almacenados. Size usa truncado BSON más comparación exacta para aceptar solo
enteros representables en su dominio; bool, fracciones, NaN/infinito y límites
inválidos se rechazan con OperationFailure 2. All valida la mezcla de elemMatch
antes de consultar datos. ElemMatch objeto ve elementos array como documentos
BSON con claves numéricas, sin traversal implícito (solapamiento explícito F03).
Se añaden 39 testigos, incluidas validación en vacío, flags, Regex en arrays y
consumidores find/count/distinct/update/delete. Las 140 observaciones previas
permanecen idénticas en los tres servidores. Pasan 1.179 diferenciales/metadatos,
44 controles locales y el ratchet focal. El primer replay sin permiso de sockets
se descarta como evidencia de producto; el replay autorizado pasa las cuatro
superficies en Memory/SQLite y 7/8/9. Logs f04; PCRE completo sigue en X01.

F03: las rutas de agregación recorren documentos de arrays sin usar segmentos
numéricos como posiciones; RuntimeDocumentState aplica el mismo contrato sobre
su documento público conservando metadata interna. Distinct abandona heurísticas
de reconstrucción y consume miembros terminales una sola vez. El mismo extractor
de miembros sirve al lado local de lookup; el lado extranjero conserva igualdad
array y distingue ausencia de rama no atravesable. Hash y evaluación residual
usan la misma extracción. Unset reutiliza exclusión de proyecciones; unwind
reutiliza su asignación documental para includeArrayIndex, reemplazando padres
escalares/array sin cambiar hermanos válidos. No se altera acceso posicional MQL
ni borrado de updates clásicos.

Cinco testigos adicionales cubren ROOT/CURRENT/let, find calculado, facet
materializado, claves "0", arrays anidados, distinct, lookup con/sin subpipeline
y ramas de unset/unwind. Las 179 observaciones previas son idénticas en 7/8/9.
Pasan 315 diferenciales/metadatos, 36 controles con presupuesto hash None/1/100,
55 controles runtime/lookup y 61 del núcleo. La tanda focal de consumidores
compartidos registra 727 pases/101 subtests y un fallo de una prueba que usaba
la semántica incorrecta tags.0 para probar conversión; se cambia a arrayElemAt
y sus 61 controles pasan. No se repite esa tanda completa: el resto sigue siendo
admisible y F11 exigirá integración fresca. Ratchet focal verde; logs f03.

F05: OperationContext distingue collation pendiente de simple ya resuelta. La
resolución asíncrona consulta el namespace una vez por operación y conserva
identidad, reloj y bindings; explícita/default/simple se propagan a find, count,
distinct raw/API, aggregate/group/lookup, selección de writes e insert validado.
Los subpipelines conservan el contexto padre; no heredan accidentalmente el
default extranjero. La opción original del cursor permanece separada del valor
efectivo para clone/rewind tras recrear el namespace. No se añade caché ni SPI.
Los índices nuevos heredan el default sin reescribir metadata histórica, y las
opciones explícitas conservan su spelling previo.

Locale es requerido en documentos no vacíos y las opciones desconocidas se
rechazan con 40414/40415; caseFirst queda explícitamente fuera del subset. Los
parsers raw mantienen la distinción nativa por consumidor: null es inválido en
find/createIndexes y admisible en los otros seis comandos caracterizados; una
collation vacía hereda en consultas/agregaciones pero se rechaza en índices.
Los errores de tipo de raw son OperationFailure 14, sin delegarlos a validación
Python. No se altera la gramática de batch writes que ya era correcta.

Las sondas obligatorias de caseLevel/numericOrdering reproducen dos defectos
compartidos del backend PyUCA: usar todo el terciario como caso y comparar los
niveles de un prefijo antes del resto de primarias. Las claves se construyen
por niveles completos, con caso independiente de acentos/variantes y dígitos
decimales. Comparación y agrupación comparten esas claves; el codec de
diagnósticos ICU sigue separado de la autoridad de almacenamiento.

Se añaden trece testigos: strengths 1/2/3, caseLevel, numericOrdering, override,
validadores, parcial unique, frío/caliente, recreación/rename y 56 observaciones
raw. Las 184 observaciones previas permanecen idénticas en 7/8/9. El observador
de validator reutiliza public_error para reconocer la familia WriteError sin
eliminar la subclase local aceptada por O3. Pasan 492 controles diferenciales,
contexto y metadata; 54 con spool real (caso/numéricos/acumulación); 180 unidades
(una omisión de backend opcional, 46 subtests), 169 fronteras/61 subtests, 62 de
índices/8 subtests y 53 de parser/contexto/8 subtests. El último ajuste de
normalización conserva 103 controles/16 subtests verdes (misma omisión).
Reapertura, metadata no reescrita, planes hash y residual Search quedan dentro
del replay focal; no se acredita ANN Atlas. Ratchet verde en 17 archivos Python.
Se conservan los rojos intermedios corregidos y logs f05. Gates globales/coste
quedan exclusivamente en F11; X01–X06 no se implementan.

F06, candidato funcional: construcción y mantenimiento comparten extracción de
claves unique, null/ausencia, arrays vacíos y correlación multikey; las claves de
selección pueden sobreaproximar pero la colisión confirma la clave canónica.
SQLite conserva claves físicas y formato; ante entradas escalares históricas
incompletas verifica el payload. Las reglas de catálogo/hashed/paralelos se
comparten entre motores; la fachada conserva la presencia de opciones _id,
incluidos false explícitos, y aplica la diferencia hidden de 7 frente a 8/9.
Insert_many reutiliza su partición de lotes rechazados para preservar prefijos,
contadores y eventos, sin publicar candidatos rechazados.

Las 197 observaciones anteriores permanecen idénticas en 7/8/9. Se capturan 229
casos standalone y catorce flujos de eventos por replica set. El replay focal
registra 1.352 pases y ocho rojos del único contrato hidden de 7; corregido,
sus ocho superficies pasan. Hay 248 controles directos 8 verdes; 107 controles
de consumidores/archivos previos, 27 reaperturas adicionales y 118 de modelos,
comandos y gramática. Los eventos confirman exclusivamente escrituras aceptadas.
El commit funcional precede a la actualización de las dos matrices CXP para que
source_revision identifique bytes reales de código. F06 no se acepta aún: queda
ese gate documental con sus tests; suites globales/coste siguen en F11.

F06 aceptado sobre 13c15c45: las matrices identifican su revisión y hashes
reales, conservan los 22 campos de comando y separan 16 campos de modelo del
control privado de presencia. Pasan sus 120 controles y ratchet focal. Los
logs f06 conservan los fallos intermedios, incluida la corrección del observador
BSON y el error de permisos de una prueba wire no autorizada, sin atribuirlos
al servidor. No queda una migración pendiente ni se infiere procedencia de
collation histórica. F07 es el siguiente bloque; F11 sigue pendiente.

Punto estable por petición del usuario (tras F06, 7/12 bloques cerrados): F07
solo tiene diagnóstico; no se ha modificado su runtime ni sus expectativas.
Su replay directo focal 8.0 reproduce 36 fallos en los nueve testigos F07,
conservados en f07-initial.txt.gz. No constituyen una regresión de F06 ni un
cierre de F07. Quedan F07–F11; la parte II permanece excluida y no se han
adelantado pruebas globales.

Las sondas temporales adicionales de 8.0 orientan la continuación, sin aceptarse
por asociación para otros dialectos: campos raíz con dólar se admiten en insert
y pipeline, y en upsert de inserción; cambiar uno mediante modificador o usarlo
en replacement coincidente da 52, mientras un set idéntico puede ser no-op.
Los campos documentales anidados con dólar se admiten fuera de _id; insert con
_id documental que contiene dólar, incluso más profundo, da 52. Debe conservarse
la precedencia del filtro/ID inmutable antes de extrapolar a otros contextos.
Timestamp(0,0) se sustituye en campos raíz de insert y replacement, incluido
replacement upsert; no en _id, objetos/arrays embebidos, modificadores o pipeline.
Estas sondas requieren incorporarse al corpus y contrastarse en 7/8/9 al retomar
F07. Los servidores temporales propios se detienen en esta pausa; los receipts
y capturas aceptados de F00–F06 quedan conservados.

Ejecución ajustada por el usuario: se conserva el alcance F00–F11 y las
aceptaciones anteriores. En adelante cada bloque se registra como implementado
y comprobado focalmente; la aceptación de la entrega completa corresponde a
F11. Sólo se agrupan pruebas focales y diferenciales causalmente necesarios.
Suites globales, cobertura, matrices, packaging, compatibilidad y coste se
reservan a F11 sobre el candidato final. No se añaden mecanismos de validación.
F07 se retoma desde las reproducciones conservadas, sin repetir F00–F06.

F07 implementado y comprobado focalmente: replacement reutiliza _id primero
sin reordenar valores embebidos; ambos motores aplican la misma regla al upsert.
Bit reutiliza la promoción existente y conserva int64 aunque el resultado quepa
en int32. Pull distingue ausencia de traversal bloqueado mediante el lector de
ruta de update, sin introducir traversal MQL implícito. Se valida dólar en _id
recursivamente y campos raíz modificados según insert/replacement/modificador/
pipeline, conservando no-op, nested y upsert. La precedencia de _id inmutable
frente a dólar anidado difiere en 7 respecto a 8/9 y queda en el helper que ya
posee esa política. No se aplica una prohibición global de dólar al BSON.

La sustitución de timestamps vacíos sólo afecta a insert y replacement, antes
de validar/almacenar/publicar, con el reloj de operación y contador local al
lote; excluye _id y documentos/arrays embebidos. No crea reloj de replicación.
La imagen confirmada alimenta el evento y findOneAndReplace conserva su imagen
anterior. Continúan los límites públicos de change streams; no se añade una
API de preimágenes ni otra capacidad X.

Dos matrices nuevas y un flujo RS completan los testigos: 231 standalone y 15
flujos RS por dialecto; las 229/14 observaciones anteriores son idénticas en
7/8/9. Pasan 290 controles focales agrupados, incluidos cuatro transportes,
metadatos, reloj inyectado/imagen anterior y controles de validación; 259 tests
locales/67 subtests de consumidores afectados. La matriz detectó precedencias
que se corrigieron: 24 controles afectados pasan. Se conserva evidencia roja
intermedia. El diff se revisa y ratchet es verde. Los controles nativos de
insert y validadores previos se reutilizan y repiten sólo por el cambio causal
en identidad/finalización, sin ejecutar de nuevo sus corpus completos.
Compatibilidad de archivos, matrices, suites globales, cobertura, packaging,
coste y aceptación de toda la entrega quedan para F11. F08 es el siguiente gate.

F08 implementado y comprobado focalmente. Sort documental conserva rutas MQL,
rechaza ambigüedad numérica 16746 y arrays paralelos antes de seleccionar la
ventana; sortArray sólo lee campos de documentos sin traversal de arrays.
La clave de array vacío reutiliza Undefined como clave interna serializable,
con orden contextual antes de null y después de MinKey. Top/bottom comparten
claves documentales y collation. N se inicializa con la clave documental del
grupo, sin reevaluarlo por fila; errores/fase y truncado se conservan según
7/8/9, sin extender esa regla a otros operadores N.

Natural sort/hint usa el escaneo físico en Memory y ORDER BY rowid explícito en
SQLite, con filtro/skip/limit y sort lógico cuando corresponde. Una gramática
común valida conflictos/direcciones; raw y método ya consumían el mismo
validador de hint, por lo que se retira el control redundante añadido durante
el trabajo. Wire conserva los códigos semánticos que ese validador emite.
Natural en stage de agregación conserva el rechazo nativo 16410.

BucketAuto redondea el tamaño objetivo y extiende los cortes para no partir
valores iguales. StdDev admite Decimal128 en los contextos retenidos; el nuevo
testigo reproduce además pérdida de precisión en la fórmula de cuadrados,
corregida con estado Welford. El estado sólo pertenece a la ejecución/spill,
sin migración de archivo. El extractor de ancestros array se mueve al owner de
rutas sin cambiar su AST salvo nombre; F06 conserva exactamente ese cuerpo.

Se añaden siete testigos (238 standalone/15 RS por dialecto); todas las 231/15
observaciones anteriores permanecen idénticas. Pasan 468 controles focales,
incluidas las cuatro superficies, materialización y seis verificaciones con
apertura efectiva de archivos temporales. La primera prueba de spill no cruzaba
el umbral de grupos y queda conservada como roja del test, no del producto.
El testigo corregido también exige coincidencia numérica exacta con MongoDB.
Pasan 49 controles locales/4 subtests y nueve del estado/9 subtests. Se concilian
expectativas que partían ties o reevaluaban n, respaldadas por los nativos.
La simplificación final del validador conserva 24 controles naturales verdes;
diff revisado y ratchet focal verde. Compatibilidad/reapertura, coste y demás
gates integrados quedan en F11. F09 es el siguiente bloque.

F09 implementado y comprobado focalmente en sus tres unidades: argumentos/gramática/recursos, calendario y representación textual. Se reutiliza un primitivo exacto BSON→int64, manteniendo límites, nulabilidad, precedencia y perfiles por consumidor. Range calcula cardinalidad y rechaza el presupuesto antes de materializar; un control intercepta la asignación y comprueba continuidad del cliente. La aridad se valida también sin filas. Las variables conservan sus reglas de expresión, separadas de command let.

El calendario usa milisegundos enteros para duraciones y límites subdiarios, ancla nativa de 2000 para bins, calendario civil para días/meses y acarreo de dateFromParts; se contrastan DST, offsets, IANA y ambos signos. Las opciones constantes y las ramas nulas mantienen su fase y precedencia. El formateador BSON común conserva cohortes Decimal y texto exacto; casing conserva su propia conversión nativa de doubles y trim su conjunto explícito. Database aggregate(1) conserva contexto/reloj/bindings sin crear namespace; sólo se habilita el origen documents ya declarado. La validación wire delega el namespace al compilador contextual existente.

Se añaden nueve testigos: 247 standalone y 15 RS por dialecto; las 238/15 observaciones previas permanecen idénticas y los seis hashes del corpus coinciden. Del contraste focal pasan 1.838 controles; las 48 discrepancias de perfiles/arity restantes quedan corregidas y pasan los 72 controles afectados. Pasan 104 tests locales/6 subtests y los controles de recurso/contexto incluidos en el focal. Se conservan resultados rojos y capturas nativas 7/8/9. Diff revisado y ratchet verde. No se declara aceptación de entrega: F11 debe refrescar la conservación CXP del parser admin modificado y ejecutar los gates integrados, compatibilidad y coste. F10 es el siguiente bloque.

F10 implementado y comprobado focalmente. Database.client conserva el owner real sync/async y clones, propagado por las factorías existentes sin cambiar el SPI ni los constructores públicos. Los handles async creados directamente con un motor devuelven None. command(nombre, valor) delega al compilador existente; rename reenvía dropTarget por la admisión BSON y el reemplazo atómico ya disponible.

La proyección pública de índices omite unique intrínseco de _id_, conserva v proporcionado y declara v:2 para el formato actual sin versión explícita. Conserva orden BSON nativo y collation según dialecto. Se elimina ns añadido artificialmente por raw listIndexes; ausencia sigue siendo error 26 raw frente a {} / [] en métodos, sin crear namespaces. El SPI y la unicidad interna permanecen intactos.

Out/graphLookup reconocidos usan diagnóstico local 115, distinguido de desconocido 40324 y con precedencia de extensiones registrada intacta. ViewOn/capped y opciones raw returnKey/showRecordId se rechazan explícitamente sin efectos; las opciones Python no admitidas conservan sus guardas. Las capacidades nativas se ejercitan en bases temporales propias de MongoDB 7/8/9 y se eliminan; no se declara paridad del rechazo local con el servidor. Los límites quedan en COMPATIBILITY.md.

Pasan 176 controles focales/18 subtests: metadata, fachadas posicional/keyword, rename destino existente/ausente/dropTarget/fallo sin efectos, identidad y límites. Se concilian dos consumidores antiguos: capped ya no se acepta como metadata inerte y distinct prueba su ruta pública en vez de mocks del extractor eliminado. Se conservan rojos intermedios (incluido identificador duplicado del test corregido). Las 247/15 observaciones anteriores permanecen idénticas; 249 standalone/15 RS por dialecto, hashes verificados. Control adicional confirma v:1 proporcionado conservado y proyección sin mutar el catálogo. Diff revisado y ratchet verde. 11/12 bloques comprobados focalmente; aceptación de entrega, conservación CXP, matrices y demás gates pendientes de F11.

F11 activo: el primer wheel instalado (09de1ab75e350b469c1f6b7ca352dbcc890163fec840f8181b85f6eb210611e3, 287 módulos) alimenta los gates globales, con imports/hashes vigilados y TMPDIR aislados. Tipado público, manifiesto, mypy de los módulos exigidos y canario SPI v2 pasan. Los unittest completos iniciales detectan expectativas antiguas y regresiones del candidato; estos rojos se conservan y no acreditan entrega. La matriz inicial se detiene tras dos celdas completas verdes porque las correcciones requieren congelar otro candidato.

Correcciones integradas comprobadas focalmente: SQLite evita incluso invocar el lector de documentos para unicidad de _id sin collation no simple; al crear un índice registra el namespace dentro del mismo savepoint antes de insertar su catálogo, de modo que dropIndexes conserva una colección vacía. La proyección pública también omite unique:false en listIndexes. Limit se valida durante preparación y documents aplica su restricción de namespace raíz sin romper la regla de joins de dialecto 7 ya retenida. Los helpers conservan su dialecto predeterminado para llamadas locales. No cambia formato de archivo ni SPI.

Se concilian consumidores con garantías nativas ya aceptadas: Regex explícita es literal; metadata incluye v y omite ns/flags intrínsecos; ausencia no sintetiza índice; un índice unique y otro no unique sobre la misma clave forman un lote válido; un segundo text no permite inventar ambigüedad. Las pruebas de metadata usan opciones de colección realmente soportadas, y el rechazo capped conserva su prueba sin efectos de F10. Los stubs SPI incluyen la lectura de catálogo exigida; se elimina el mock de un constructor que omitía sus invariantes. Se conserva el control de vacío Search con un match válido, en vez de limit:0 inválido.

Pasan 2.021 controles diferenciales/correctivos y cuatro subtests, 91 de expresiones/92 subtests y seis de API/12 subtests; ratchet contra v4.11.0 verde (423 archivos Python cambiados desde release). Las seis capturas nativas tienen hash actual; sólo se amplía f10_facades para metadata no unique y convivencia unique/no unique. Los demás testigos permanecen idénticos. Diff de estas correcciones revisado. Siguen pendientes el resultado global, cobertura real, candidato final exacto con matrices, builds/smokes/compatibilidad/coste y limpieza. No es aceptación de entrega.

F11, segundo grupo correctivo: la suite inicial termina con 63.795 passed, 492 failed, 753 skips y 14.834 subtests. Cobertura inicial real 98,73311157652662%, insuficiente: no acredita aceptación. Se conservan los resultados como rojos. Las discrepancias de diagnóstico/metadata/preparación se agrupan por owner en vez de repetir los doce bloques.

La adquisición de documentos para unicidad de _id collated se hace perezosa en el helper existente: ninguna de las dos implementaciones invoca siquiera su fuente con collation simple. Se conserva el control determinista de no enumerar la colección y el fast path SQLite. Cursor evita otra llamada a bind cuando sólo cambia el estado de resolución, conservando identidad de operación, reloj, bindings y contexto efectivo compartido. Su nuevo wrapper conserva el error primario ante fallo de limpieza y retiene el recurso para reintentar el cierre. Los controles de captura previos a la resolución distinguen el frame aún no resuelto del frame efectivo, manteniendo los mismos identificadores/objetos internos y la preparación única.

El diagnóstico de índice vacío conserva undefined en minúsculas incluso con collation; numIndexesBefore cuenta el índice intrínseco al crear un namespace. La vista pública 7/8 omite simple explícito sin reescribir su procedencia almacenada; 9 mantiene su perfil. DBRef conserva sus marcadores y su payload opaco en _id, mientras campos extra con dólar y otros documentos inválidos siguen rechazándose. Se contrasta esta regresión con siete formas en MongoDB 7/8/9 y ambos motores; las capturas bulk aceptadas vuelven a conservarse. El límite de orígenes de aggregate de base usa diagnóstico codificado 115.

Focales terminales: 4.264 controles de garantías retenidas pasan inicialmente (los 22 restantes eran capturas de contexto/cancelación y quedan conciliados); después pasan 833 controles de consumidores y BSON, 70 de metadata/capacidad, y 97 de fronteras. Se preservan 78 controles BSON previos, incluidos todos los errores de admisión. Sólo los dos casos de capped exitoso nativo comprueban la limitación declarada F10 con código 115, estado conservado y ausencia de namespace/índices; no se alteran goldens ni se simula paridad para esa capacidad excluida. El diff correctivo queda en commits semánticos separados; el ratchet contra release es verde. El primer build descubre dos módulos eliminados en build/lib: se conserva el residuo fuera del árbol y se exigen builds limpios. Siguen pendientes las suites/cobertura/matrices finales, packaging/compatibilidad/coste y limpieza del candidato definitivo.


F11, comprobaciones integradas del candidato 9c627385/8cad88e9: unittest completo verde en 3.13/3.14/3.15 (3.673 por versión), matriz 12/12 verde, deep y contratos de distribución/API/SPI verdes. Pytest completo 3.14 pasa 64.201 pruebas y 14.926 subtests, pero termina rojo por cobertura real 98,93263270249189%, inferior a 99,00%; no acepta la entrega. Las pruebas focales añadidas componen diagnósticamente 99,01% sobre ese mismo artefacto, sin sustituir el gate del candidato definitivo.

Una prueba de posición numérica anidada demuestra un incumplimiento de F08: ordenar a.0.0.b mezcla la posición explícita con una rama de campo numérico ausente y genera un empate falso. MongoDB 7/8/9 devuelve el orden 2,1. El recorrido compartido conserva ambas alternativas en consultas; la extracción de claves de ordenación desactiva la alternativa de campo numérico después de la validación de ambigüedad existente. No introduce otro recorrido, runner ni capacidad. El testigo diferencial existente incorpora find con limit, aggregate y facet en ambas direcciones, con capturas 7/8/9 actualizadas; sus otros resultados permanecen conservados. Replica sets sólo refrescan identidad/captura, sin ampliar sus casos.

Diff revisado; pasan 24 combinaciones del diferencial ampliado, 199 controles de consumidores del recorrido/recursos/fechas con 66 subtests, y 298 controles agrupados de capturas y fronteras. Las nuevas pruebas comprueban el error primario ante fallo de limpieza, el reintento de recursos retenidos, limpieza de todas las fuentes, modalidad materializada de getMore, exports retenidos, argumentos y ausencia de efectos al rechazar capacidades. La tanda focal antigua se interrumpe porque la actualización de capturas invalidó su identidad durante la ejecución: sus 24 discrepancias no se reutilizan como aceptación; el replay estable posterior es verde. Logs y rojos diagnósticos quedan conservados en la evidencia existente. 11/12 bloques comprobados; F11 pendiente de congelar y evaluar el candidato corregido, incluido coste. Parte II excluida.


F11, candidato corregido congelado en 64ff6f2e: wheel SHA-256 8b73489953358826761eda9c075c99f7e7e52534427dfe6ddbb56045f1970de0, sdist d5f617faa85bc6f67763d83327ff2d78066420be7bc1710015d1148958789590. Dos builds desde git archive limpio son idénticos; 308 archivos de paquete y 287 módulos instalados coinciden con wheel y checkout. Typing, manifiesto, mypy, SPI, snapshots, deep, instalaciones limpias y perfil sin SDK pasan sobre este artefacto. El ratchet detecta doce infracciones en pruebas recién añadidas: se corrigen sin alterar bytes productivos y pasan sus 25 controles/12 subtests en los tres Python; ratchet verde para 426 archivos desde release. Los rojos anteriores se conservan y no se convierten en verdes retroactivamente. Las suites y la matriz integradas siguen ejecutándose; el coste permanece pendiente. El descubrimiento unittest terminal exigido usa exactamente la raíz tests de CI, incluyendo integración; la tanda anterior desde tests/unit era parcial y se conserva como tal. No es aceptación de entrega.


F11, gates funcionales terminales del wheel 8b734899: el descubrimiento unittest de CI desde tests pasa 3.681 pruebas por versión, con tres skips existentes, en Python 3.13/3.14/3.15. Pytest completo recoge todo el conjunto: 3.13 termina con 64.209 passed/9 failed/764 skipped; 3.14 con 64.219/10/753; 3.15 con 64.207/11/764. Cada versión pasa 14.941 subtests. Los procesos completos tienen exit 1 y se conservan así; no se presentan como terminales verdes.

La concurrencia de las suites y la matriz agotó los puertos de loopback: los fallos directos muestran EADDRNOTAVAIL/Errno 49, y los lectores de estado de los testigos pueden convertirlos en aserciones sin conservar el mensaje. Una sonda controlada sobre los helpers existentes confirma esa pérdida de detalle tanto en state como en namespaces. Se detiene exclusivamente la matriz concurrente, se conservan sus tres celdas rojas y tres interrumpidas, y no se modifica código ni expectativa por esos fallos. Se revisan y repiten todos los nodos fallidos globales, incluidas las aserciones normalizadas: pasan los 9/10/11 respectivamente, con salida completa, sin paralelismo y sobre el mismo artefacto. Los casos ya verdes se reutilizan. Esta composición acredita que cada caso recogido tiene una observación verde del candidato, sin reejecutar las suites enteras ni falsear sus exits originales. Los nodos y las ejecuciones están en f11-final3-global-resource-recovery.json.gz y sus logs.

La cobertura instalada completa y su composición focal coinciden: 48.238/48.719 sentencias, 481 faltantes y 1.027 exclusiones existentes; cobertura real 99,01270551530205%, superior al 99,00% exigido sin redondeo favorable ni exclusiones nuevas. Las tres celdas terminales de la matriz recuperan sólo sus 27/3/10 nodos fallidos en verde; las nueve restantes se ejecutan completas después de las suites globales. Las matrices aún no están aceptadas en conjunto. Tipado/manifiestos/SPI/deep/SDK-free/smokes/conformance tienen sus logs del artefacto corregido archivados; el rojo de lint conserva su corrección focal ya aceptada. Los seis endpoints nativos y cuatro contenedores propios están cerrados con identidad comprobada. F11 continúa pendiente de matriz completa, coste y aceptación final; 11/12 bloques comprobados, parte II excluida y cierres O1–O8/P2 conservados.


F11, matriz terminal: las doce combinaciones Python 3.13/3.14/3.15 × pytest 8.4.2/9.1.1 × PyMongo 4.9.2/4.19.0 quedan verdes sobre el wheel congelado. Tres celdas reutilizan sus casos pasados y recuperan los 40 nodos de transporte fallidos; las nueve restantes pasan completas. El bundle f11-matrix-final4.json.gz conserva cada log, resultado y selección de reintento; f11-final-imports.json.gz conserva procedencia/imports/hashes del mismo artefacto. No se reutilizan las tres celdas interrumpidas como aceptación.

Los once workloads existentes de rutas afectadas se ejecutan en base 2fa637f6 y candidato, sobre Memory y SQLite, sin carga de suites y sin cambiar el harness respecto a la base. Las 46 huellas de resultado por motor coinciden (92/92), sin workloads omitidos. Se conservan medidas y oráculos en f11-affected-cost-* como diagnóstico comparable de índices, materialización/spill, predicados, ordenación, consumo y Search/vector. No se inventa un SLA adicional para estas mediciones; los 44 controles retenidos mantienen su presupuesto propio.

El primer intento de calibración, con 500 operaciones de consumo y 1.000 de búsqueda por muestra, falla el criterio de calidad: seis controles A/A superan 0,05; máximo 0,0687187942501366. Se conservan calentamiento y cuatro A/A íntegros, sin ejecutar ABBA ni aceptar conclusiones de rendimiento. El siguiente intento repite todo el protocolo con 1.000/2.000 operaciones, los mismos workers/datasets/artefactos y los mismos umbrales. No se descartan casos ni se eleva el margen. Las suites, cobertura, matrices y distribuciones permanecen aceptadas a su nivel; F11 y la entrega siguen abiertos exclusivamente por coste y registro/revisión final. 11/12 bloques comprobados.


F11, segunda calibración rechazada: con 1.000/2.000 operaciones, once controles de consumo superan 0,05; máximo 0,37282227647360133 (SQLite default find-limit-one). Los cuatro controles de búsqueda sí cumplen calidad, pero no se aceptan por separado ni se inicia ABBA: el protocolo exige calidad en los 44. El intento completo permanece en f11-cost-final5-*; no modifica las aceptaciones funcionales ni demuestra regresión del candidato, que todavía no se ha medido en ABBA.

Se contrasta la inestabilidad antes de repetir: la sonda sobre el worker intacto de la base registra 284 eventos de GC, con 0,045019 s en generación 0, 0,012377 s en generación 1 y 0,01017 s en generación 2; no muestra pausas suficientes para explicar la variación observada. Sus tiempos son diagnósticos y no se aceptan como medición. El sistema no reporta avisos térmicos/de rendimiento y mantiene carga de fondo (343% de CPU agregada tras las muestras, cinco procesos por encima del 10%); no se alteran procesos ajenos. Para estabilizar el siguiente intento se fija PYTHONHASHSEED=0 y se ejecutan exclusivamente nuestros workers con taskpolicy -a, política de aplicación documentada por macOS. La sonda de arranque confirma Python 3.14.5 y semilla 0. Se conservan intérprete, GC activo, bytes de los workers, todos los costes de CPU, 44 casos, 1.000/2.000 operaciones, agregación interna y umbrales. Esta política se declara antes de observar ninguna comparación A/B de los 44 controles y se aplicará por igual a base/candidato. F11 continúa abierto.


F11, checkpoint estable sin aceptación de entrega: el tercer intento, con semilla fija y política de aplicación, vuelve a rechazar la calibración (doce controles, máximo 0,26678097756627306). Se archivan f11-cost-final6-samples/summary íntegros. Ningún intento ejecutó las tres ABBA de los 44 controles. El usuario comunica que no puede reservar ahora una ventana sin otras cargas. Se detienen los reintentos automáticos; no se modifica el umbral, no se omite GC, no se cambian workers ni se declara una regresión del candidato por variación A/A de la propia base.

Revisión independiente completada: diff y documentación de comportamiento alineados; los cambios posteriores al freeze son sólo pruebas de estilo ya comprobadas y documentación/evidencia. Los 308 archivos de paquete coinciden entre checkout, wheel e instalación; 287 módulos conservan identidad; ambos builds retienen los mismos hashes. El prefijo histórico completo del receipt de 2fa637f6 permanece byte a byte, preservando O1–O8/D05/P2. No quedan procesos propios de medida/mongod ni los cuatro contenedores propios. El checkpoint reproducible está en f11-stable-checkpoint.json.gz. No se publica ni etiqueta.

Estado separado: contrato vigente y entrada satisfecha; F00–F10 implementados/comprobados, sin corrección productiva pendiente identificada; entrega F11 bloqueada por precisión de medición y comparación de coste aún no ejecutada. Suites completas con sus recuperaciones explícitas, cobertura real >=99%, deep, matriz 12/12, API/typing/SPI, snapshots, lint, instalaciones/distribuciones reproducibles, SDK-free, conformance, persistencia y diferenciales están registrados sobre el payload final. Permanecen pendientes exclusivamente la aceptación del coste y el registro final de entrega que depende de ella. No hay revisión de diff/documentación pendiente ni ampliación a parte II.

Evaluación del criterio solicitada por el usuario (propuesta, no cambio aprobado): exigir buena evidencia de rendimiento es razonable; hacer del máximo A/A <=5% en cada uno de 44 casos una precondición universal no está demostrado viable en este equipo compartido. El máximo de cuatro valores es sensible a picos, y la tolerancia de regresión ligada al ruido mezcla calidad de medida con presupuesto admisible. Las guías primarias de pyperf separan advertencias de estabilidad y comparación estadística (https://pyperf.readthedocs.io/en/latest/analyze.html y https://pyperf.readthedocs.io/en/latest/cli.html). Se propone mantener las garantías deterministas y BSON, definir por separado un presupuesto de regresión explícito y clasificar la comparación como aceptada/regresión/inconcluyente con un número acotado de intentos, reutilizando workers y ABBA. Declarar inconcluyente no acredita rendimiento. Aplazar su aceptación o cambiar el presupuesto exige disposición explícita del owner; esta nota no modifica el plan aprobado ni cierra F11.


2026-11-02: el usuario autoriza reformular únicamente el coste F11 y continuar.
Se materializa el presupuesto fijo +5% por cada uno de los 44 casos, dos intentos
máximos de tres ABBA, acumulación de todos los bloques y decisión con límites de
Student sobre log-razones pareadas (97,5% unilateral por evaluación, dos evaluaciones).
Aceptación sólo con las 44 cotas superiores <=1,05; una cota inferior >1,05 señala
regresión por investigar; el resto es inconcluyente y no acredita rendimiento.
Se documentan supuestos y límites del modelo. Se conservan identidad BSON,
presupuestos deterministas, workers y agregación, Python 3.14.5/GC/política/semilla.
Se fijan 500/1.000 operaciones antes de medir A/B; no se repiten A/A ni los gates
aceptados. Los rojos anteriores conservan su significado histórico. No se añade
runner, documento ni capa productiva; sólo cambia el contrato de coste autorizado.


Primer coste bajo el contrato reformulado: tres bloques ABBA completos, identidad
BSON conservada; 9 casos aceptados, 6 con regresión observada y 29 inconcluyentes.
Se conserva el intento íntegro (f11-cost-bounded-*), sin ejecutar otro intento
para esconder el rojo. Los seis casos se concentran en find con limit=1 y una
primera página: razones estimadas 1,0839–1,1817, con cotas inferiores >1,05.

El perfil causal de la API pública detecta dos sobrecostes: 18.000 vueltas de
event loop por 2.000 lecturas frente a 12.000 en la base, y validación genérica
de sort repetida para cada documento incluso con una única clave simple.
Se usa ejecución eager de la tarea de cierre (disponible en Python 3.13) sólo
cuando no hay fábrica configurada; si el cierre suspende conserva shield y
manejo de cancelaciones repetidas, y las fábricas siguen siendo autoridad.
La clave simple evita el recorrido compuesto; rutas punteadas y arrays paralelos
conservan su único validador. También se evita copiar FindOperation cuando los
objetos canónicos skip/limit son idénticos, preservando distinciones bool/int y
manteniendo copias para overrides. No se omite resolución de contexto, admisión,
validación, GC ni publicación. La sonda confirma que el guard se aplica al caso medido.

Pasan 1.658 controles focales por cada Python 3.13/3.14/3.15; después del ajuste
final pasan 715 controles de sort, 1.664 de cursor/contexto y 114 controles de
ventana/cierre en cada runtime (más sus subtests). Mypy del helper y ratchet
verdes. El perfil vuelve a 12.000 vueltas; sus tiempos instrumentados son sólo
diagnóstico. Diff revisado. Se congela un candidato material corregido: invalida
la aceptación de runtime del wheel anterior, no las capturas nativas/SDK cuyos
inputs no cambiaron ni los cierres O1–O8/P2. Se construirá otro wheel y se
repetirán los gates integrados causalmente afectados y el coste acotado; no se
reutilizan las razones A/B del candidato anterior para aceptar el nuevo.


F11, coste terminal del candidato 13bae057 / wheel 417f8625: seis ABBA y 24
mediciones completas, identidad BSON conservada en los 44 controles. Primera
evaluación: 3 aceptados, 0 regresiones, 41 inconcluyentes; segunda acumulada:
24 aceptados, 1 regresión, 19 inconcluyentes. SQLite/default/find-limit-one tiene
razón 1,082127 y límites [1,056298; 1,108588], incumpliendo el presupuesto 1,05.
Se conservan config, muestras, ambas evaluaciones y summary íntegros bajo
f11-cost-fixed-bounded-*. No se ejecuta un tercer intento del mismo candidato.

El perfil causal conserva la lectura obligatoria de collation efectiva y su
frontera de executor. Detecta además una respuesta vacía redundante del cursor
síncrono cuando el cursor async ya conoce EOF. Se evita únicamente ese cruce
si no quedan buffers ni fuentes retiradas; el propietario sigue cerrándose y
los errores/reintentos de cleanup conservan su recorrido. Los cruces del runner
bajan de 15.000 a 10.000 por 5.000 lecturas SQLite. Los tiempos instrumentados
son diagnóstico, no aceptación de rendimiento. Una variante SQL CASE apenas
mejoró el diagnóstico y se descartó; no hay caché nueva ni cambio de contexto.

Pasan 178 controles focales y cuatro subtests en cada Python 3.13/3.14/3.15,
incluidos testigos nativos 7/8/9 de consumidores F08, buffers y límites de
respuesta. Los nuevos controles verifican un único cruce de cierre en EOF y
la propagación/reintento de limpieza retirada tras EOF natural. La primera
versión del segundo test confundía la fase de EOF con limit y se corrigió;
se retienen sus rojos. Una selección diferencial demasiado amplia se interrumpe
y conserva como parcial, sin acreditarla como gate global. Ratchet verde para
426 archivos; diff y whitespace revisados. Este cambio material permite medir
un nuevo candidato con el mismo contrato acotado, sin mezclar tiempos previos.
F11 sigue abierto; O1–O8/D05/P2 y parte II conservan su disposición anterior.


F11, verificación final del candidato congelado e4eb17ccf75634f7ff365c6f43ed8387f16e72e7
(2026-11-02). Se mantiene la parte I; X01–X06/parte II quedan excluidos y
O1–O8/D05/B1–B4 P2 conservan literalmente su receipt histórico. No se introducen
cambios productivos después del freeze ni se repiten sus auditorías. Las
correcciones de scheduling/sort/EOF invalidaron el runtime del wheel anterior;
F11 exige expresamente suites globales y matriz sobre el candidato final. Se
reutilizan las capturas físicas y perfiles SDK cuyos estímulos/observadores no
cambiaron, y se ejecutan sus regresiones sobre el nuevo código.

Identidad del artefacto probado:

- Wheel: a10cd6375fded32d42905e2fee214309f5884c9b92c334e39184324edae4c046.
- Sdist: 7f3bc693ef282a578c27b5e1bf5383664b661eacf0e15a18bc959febb52eeb36.
- Ruta retenida: /var/folders/gh/m3pllfqn6m14srd4s448b0zw0000gn/T/mongoeco-f11-eof-3j6yanii.
- Dos builds limpios con SOURCE_DATE_EPOCH, normalizador de sdist existente,
  check_distribution_sources/twine y 308 archivos coincidentes. El primer
  cotejo anterior a aplicar el normalizador se conserva en el log; al aplicar
  el paso obligatorio ya existente coinciden ambas distribuciones, sin
  reconstrucciones adicionales. Se verifican los 287 módulos del runtime y
  se conserva la identidad del payload después de todos los consumidores.

El preflight valida intérpretes, dependencias, rutas, imports instalados y los
3.587 nodos de la selección matricial antes de ejecutar. Unittest y pytest
completos se ejecutan en los tres perfiles, primero unittest y después pytest.
La matriz se ejecuta después, máximo tres celdas. Sólo los controles estáticos sin otra suite amplia se solapan con
pytest. Coste, propiedades/matriz y diagnósticos adicionales quedan separados.
No se crea runner, documento ni capa de validación nuevos: se reutilizan los
workers/guard versionados y scripts del proyecto. Los comandos, rutas y
resultados terminales quedan en los JSON de evidencia, con 28 pruebas de import
que acreditan el wheel instalado, todas con salida 0.

Las suites globales y la matriz final no necesitaron reintentos:

- Unittest: 3.681 pruebas por cada Python 3.13/3.14/3.15, salida 0, tres skips
  previstos en cada perfil.
- Pytest: 64.224/64.235/64.224 passed en Python 3.13/3.14/3.15, respectivamente;
  764/753/764 skips y 14.941 subtests por perfil. Los tres procesos completos
  terminan con salida 0. Python 3.14 conserva las 307 ResourceWarning SQLite
  ya presentes en el global anterior, sin convertirlas en errores ni aumentar
  el alcance; no se afirma ausencia de warnings.
- Cobertura instalada real: 48.244/48.727 = 99,00876310874874%, 483 líneas
  pendientes y las 1.027 exclusiones existentes. Se verifica el cociente real,
  además del resultado del plugin; no se añaden exclusiones.
- Deep: 66 passed. Matriz Python 3.13/3.14/3.15 × pytest 8.4.2/9.1.1 ×
  SDK 4.9.2/4.19.0: 12/12 celdas, 3.587 passed por celda, sin reintentos.
- Quince gates estáticos/de consumidores, todos con salida 0: typing público
  (20 negativos) y de engine (25), manifiesto API, mypy 9+2 módulos, canario
  SPI v2, ratchet de 426 archivos, los dos snapshots exigidos, deep, smokes
  wheel/sdist/pytest-kit, tres proveedores conformance y perfil sin SDK.
  Conformance: Memory/SQLite 10 passed cada uno; canario externo 5 passed y
  5 not-applicable según capacidades. SDK-free: 14 passed, con ausencia real
  de bson/pymongo comprobada.
- Once workloads existentes en cada backend, sin skips: 46 identidades de
  salida por Memory/SQLite, 92/92 coincidentes. Se reutiliza A porque base,
  corpus y harness permanecen idénticos; sólo se mide B nuevo. Sus tiempos
  son diagnóstico y no sustituyen el presupuesto normativo de los 44 casos.

Mapa de aceptación del candidato, usando los inventarios/casos existentes:

| Contrato | Caso / owner existente | Consumidor | Evidencia en bson-grammar-context | Artefacto |
| --- | --- | --- | --- | --- |
| F00–F10 y conservación causal | SEMANTIC_CORPORA, fixtures 7/8/9 y suites completas | APIs/wire, Memory/SQLite, tres Python; capturas RS retenidas donde aplican | f11-eof-global-gates.json.gz y logs unittest/pytest; capturas nativas aceptadas | e4eb17cc / a10cd637 |
| Cobertura y propiedades | CI global instalado y perfil deep | Runtime instalado | f11-eof-coverage.json.gz y f11-eof-deep.txt.gz | e4eb17cc / a10cd637 |
| Matriz, typing/API/SPI, compatibilidad e instalación | Selecciones/scripts de CI, SDK-free y conformance | Doce perfiles, wheel/sdist y consumidor externo | f11-eof-matrix-summary.json.gz, f11-eof-static-consumer-gates.json.gz, f11-eof-build.txt.gz | e4eb17cc / a10cd637 |
| Resultados de paths afectados | Once workloads previos por backend | Memory/SQLite | f11-eof-affected-workload-comparison.json.gz; B nuevo y A retenido | e4eb17cc / a10cd637 |
| Identidad, contexto de ejecución, historia y recursos | Guard/manifiestos y revisión final | Repositorio y procesos propios | f11-eof-import-proofs.json.gz y f11-eof-final-review.json.gz | e4eb17cc / a10cd637 |
| Coste obligatorio, todavía no aceptado | 44 controles retenidos / dos workers inalterados | Mismo baseline 2fa637f6 y candidato | f11-cost-eof-bounded-config/samples/look3/look6/summary.json.gz | e4eb17cc / a10cd637 |

Coste terminal: primera evaluación de tres ABBA, 16 aceptados / 0 regresiones
confirmadas / 28 inconcluyentes; segunda evaluación acumulada de seis ABBA,
32 aceptados / 0 regresiones confirmadas / 12 inconcluyentes. Se conservan
las 24 mediciones completas, sin descartar ninguna ni mezclar candidatos.
Identidad BSON, tipos/resultados y presupuestos deterministas se conservan.
Python 3.14.5, GC activo, semilla 0, política de aplicación y 500/1.000
operaciones por muestra permanecen fijados. Se verifican bytes idénticos con
p2-b3-accepted-consumption-worker.py.gz, p2-b3-accepted-vector-worker.py.gz y
p2-final-artifact-guard.py.gz; no se copian ni modifican esos mecanismos.

Única obligación exigida pendiente, de comprobación y no un defecto funcional
reproducido: acreditar aumento de CPU <=5% en los 44 casos. Falta evidencia
admisible con cota superior <=1,05 en estos doce controles; una estimación
puntual o ausencia de regresión confirmada no satisface ese contrato:

| Control inconcluyente | Razón estimada | Cota inferior | Cota superior |
| --- | --- | --- | --- |
| MemoryEngine-decoder-find-first-page | 1.040630 | 1.026049 | 1.055418 |
| SQLiteEngine-default-find-limit-one | 1.053564 | 1.034618 | 1.072856 |
| SQLiteEngine-default-find-first-page | 1.046533 | 1.009391 | 1.085043 |
| SQLiteEngine-default-aggregate-first-page | 1.028681 | 0.998362 | 1.059920 |
| SQLiteEngine-standard-find-limit-one | 1.050599 | 1.024264 | 1.077611 |
| SQLiteEngine-standard-find-first-page | 1.043496 | 1.016118 | 1.071611 |
| SQLiteEngine-standard-aggregate-first-page | 1.020783 | 0.991189 | 1.051261 |
| SQLiteEngine-decoder-find-limit-one | 1.038216 | 1.025448 | 1.051142 |
| SQLiteEngine-son-aggregate | 1.025232 | 1.000831 | 1.050227 |
| SQLiteEngine-son-find-limit-one | 1.044043 | 1.033483 | 1.054711 |
| all/limit2 | 1.051750 | 1.036018 | 1.067721 |
| binary/limit2 | 1.025828 | 0.995699 | 1.056870 |

Cierre observable pendiente: las 44 cotas superiores cumplen <=1,05 en una
comparación admisible conforme al protocolo autorizado, manteniendo las
garantías funcionales/deterministas. Los dos intentos están agotados: se
detiene la medición automática. Otra medición requiere entorno adecuado y
disposición explícita del owner; no se reinician intentos ni se cambia el
presupuesto o el código por resultados meramente inconcluyentes. La lista queda
cerrada en esta obligación; no se incorporan hipótesis o ampliaciones opcionales.

Revisión final completada: diff productivo y pruebas revisados, documentación
de comportamiento/alcance alineada, hashes de ambas distribuciones y runtime
conservados, prefijo histórico intacto, sin cambios ajenos ni procesos propios
de coste/gates pendientes. La limpieza nativa anterior se reutiliza porque no
se han arrancado nuevos mongod/contenedores en esta continuación. Los cambios
finales son sólo receipt y evidencia; no invalidan los gates del payload.
F00–F10 quedan comprobados focal/integralmente (11/12 bloques); F11 y la
aceptación de entrega permanecen pendientes exclusivamente del coste. No se
publica ni etiqueta, ni se declara finalizado F11.


## Correcciones B→A de arranque de lectura — alcance focal separado

Autorización: implementar el plan B→A acordado en conversación, en rama
separada desde fb7c4a9a. Referencia productiva e4eb17cc y wheel a10cd637
congelados; este incremento no sustituye su aceptación ni reinicia el coste
F11. O1–O8/D05/P2 y parte II conservan su disposición. El repositorio no
exige transición RFC adicional para este arreglo.

Se reutilizan OperationContext, la vinculación validada, el propietario
_AsyncCursorIterator, finish_cleanup y el capturador/replay existentes; no se
crean coordinadores, cachés ni runners. El censo de callers identifica _iter,
first y pull_chunk además de _fetch_response. La frontera de inicialización
debe conservar las modalidades streaming/batch, AsyncIterable SPI v2, fases,
buffers, identidad contextual y limpieza con cancelación repetida.

| Bloque | Estado | Cierre exigido |
| --- | --- | --- |
| B | Comprobado focalmente | Vinculación directa guardada cuando no cambia la semántica; programa preparado conservado y recompilación legítima intacta. |
| A | Comprobado focalmente | Apertura diferida única sin wrapper por fila; modos, fases, contadores y propiedad/limpieza conservados. |
| Verificación compartida | Comprobada focalmente | Selección focal prevalidada, tres Python/ambos motores/superficies afectadas, diffs revisados y commits semánticos. No acredita entrega ni coste F11. |

B: se reproduce el rebind redundante con un test que conserva el programa
preparado. La vinculación directa exige mismo dialecto, collation efectiva y
objeto de expresiones; los cambios semánticos conservan bind/recompilación.
Pasan 163 pruebas focales/1.054 subtests: identidad/sesión/reloj/bindings,
resolución idempotente, variantes de dialecto/collation/bindings, error sin
reemplazo parcial, namespace recreado en ambos motores y consumidores
materializados/wire. Ratchet sobre tres archivos Python verde; diff revisado.
Se conservan el rojo inicial, la selección y los logs de lint. La verificación
compartida en tres Python sigue pendiente, igual que toda aceptación F11.

A: el propietario existente resuelve el contexto en la primera lectura y abre
una única fuente; desaparece _scan_with_effective_collation y su avance por
fila. _prepare_scan conserva el contador query y el bloqueo de opciones al
crear el iterador. El streaming, pull_chunk y first comparten esa apertura;
el camino batch conserva su lector y sus fronteras originales. Se conserva
AsyncIterable incluso con iterador separado, propiedad de ambos objetos,
limpieza protegida con finish_cleanup, retención de recursos fallidos y
reintento existente. No se añade caché ni cambia el SPI.

La reproducción inicial falla en los tres entrypoints (wrapper e iterador
separado sin cierre). El focal definitivo pasa 335 pruebas/1.094 subtests en
Python 3.14: errores de apertura/iteración/lectura/cierre, prioridad y notas,
reintento sin reapertura, cancelación repetida, EOF, límites, bytes pendientes,
codecs, rewind/clone, contexto/sesión/reloj y namespace recreado en ambos
motores. La prueba antigua de lazy sobre un stub sin resolver se ajusta para
exigir que ni siquiera se abra la fuente antes del primer I/O. Se conservan
los fallos intermedios: dos eran la extensión del test de cancelación usando
__anext__ en AsyncCursor en vez del iterador; corregida la prueba, la limpieza
pasa. Verificación compartida y replay nativo pendientes; no aceptación F11.

Cierre focal B→A: producto 71aa410f015fd422cee7a67af4e4f0310c0cac88 en rama
perf/f11-context-startup (B: 4181b18dad8cecc61a35eb9f3ec7b0e62e7000d5;
A: 71aa410f015fd422cee7a67af4e4f0310c0cac88). Comprobados los dos bloques
y la verificación compartida; no quedan criterios focales pendientes.

- La selección prevalidada de 335 pruebas pasa en Python 3.13, 3.14 y 3.15,
  con 1.094 subtests por versión. Incluye Memory/SQLite y las fronteras
  async/sync/wire afectadas. No se repite el focal B separado: la selección
  compartida ya contiene sus cinco archivos.
- Replay diferencial prevalidado: 1.200 pruebas aprobadas en Python 3.14
  contra las capturas nativas existentes de MongoDB 7.0/8.0/9.0, para
  cursor_consumption, response_frontier, find_projection, materialized_context
  y command_let, ambos motores y superficies sync/async/wire-sync/wire-async.
  Se reutiliza ese oráculo aceptado; no se arrancan servidores nativos nuevos.
- Ratchet de lint verde sobre los seis archivos Python cambiados desde
  fb7c4a9a. Contratos de typing público y SPI verdes, incluidos los 20 y 25
  errores negativos esperados. Diff productivo y pruebas revisados,
  git diff --check verde. El último cambio posterior sólo registra evidencia.
- La prueba de congelación verifica main aún en
  fb7c4a9aad755fad54a70ce857e2af03ff8d82f5, candidato histórico
  e4eb17ccf75634f7ff365c6f43ed8387f16e72e7 y wheel SHA-256
  a10cd6375fded32d42905e2fee214309f5884c9b92c334e39184324edae4c046.
  Configuración, muestras, looks 3/6 y resumen del coste conservan los mismos
  bytes que su evidencia aceptada. El receipt histórico sigue siendo prefijo
  inalterado; O1–O8/D05/P2 permanecen cerradas y parte II excluida.

Límite de este cierre: implementación B→A y garantías focales. No se han
realizado builds, suites globales, perfiles ni mediciones de coste nuevas.
No se infiere una mejora porcentual ni se acredita el umbral de CPU. El nuevo
producto queda separado del artefacto congelado; los gates globales de F11
aceptados para e4eb17cc no se atribuyen a esta rama. F11 permanece pendiente
exclusivamente de aceptación del coste (32 aceptados, cero regresiones
confirmadas, 12 inconcluyentes, intentos agotados). No se publica ni etiqueta.

### Revisión B→A por bloques y correcciones de limpieza

Workset de revisión: fb7c4a9aad755fad54a70ce857e2af03ff8d82f5 →
4b0440e17143f7ec3ebe531a443bc1ed63e0a424, rama perf/f11-context-startup.
Se revisan diff, callers, contratos de contexto/operación y ciclo de vida,
incluyendo _iter, first, pull_chunk, respuesta batch, EOF, close y rewind.
La corrección queda identificada por los dos paths Python y el SHA-256 del
diff en ba-review-frozen-proof.json.gz. No se toca otro worktree.

B revisado: sin nuevos findings en el bloque. La guarda conserva dialecto,
collation efectiva y el mismo objeto de expresiones; replace pasa por la
validación del AggregateOperation ligado. El resolver real deriva sólo la
collation; bind sigue siendo la autoridad cuando cambia la semántica. Se
reutilizan los tests aceptados de identidad, recompilación y fallo atómico,
además del focal compartido previo: no hay cambios causales que los invaliden.

A: dos findings locales observados y corregidos, ambos P2; no son reaperturas
de los P2 anteriores:

1. Si aclose del iterador separado falla, la fuente no recibe ni un intento
   de cierre. La cola de retirados repetía el mismo bloqueo tras rewind.
   Se reproduce en close/EOF/rewind: seis fallos de nueve combinaciones.
   Ahora la liberación activa delega en la cola existente; ésta intenta todos
   los recursos, conserva el primer error y retiene sólo los cierres fallidos
   para reintento. finish_cleanup protege el drenaje completo, también tras
   rewind. Se retiran los tres envoltorios de limpieza redundantes del owner.
2. Ante error de lectura y dos cierres fallidos, la nota del segundo cierre
   quedaba en el error de limpieza y se perdía al conservar el error de lectura.
   Se reproduce con una combinación roja de tres. Ahora se conservan también
   esas notas secundarias, sin sustituir el error de lectura ni su fase.

Verificación del código corregido:

- Selección focal prevalidada: test_response_boundaries.py, test_cursors.py y
  test_sync_cursor_consumption_contract.py. Pasan 157 pruebas y cuatro subtests
  en cada Python 3.13/3.14/3.15. Incluye 12 combinaciones de errores y cuatro
  cancelaciones repetidas sobre fuente/iterador separados, además de las
  regresiones anteriores. El orden esperado de un test previo se actualiza
  para exigir que la fuente se cierre antes del reintento del iterador fallido.
- Replay focal prevalidado: 360 pruebas aprobadas contra capturas nativas
  7.0/8.0/9.0, corpus cursor_consumption/response_frontier, ambos motores y
  superficies sync/async/wire-sync/wire-async. No hay nuevas capturas físicas.
- Ratchet de lint verde sobre los dos paths Python corregidos; diff revisado
  y git diff --check verde. No cambia una interfaz de typing: se reutiliza
  su evidencia aceptada, junto a los controles no alcanzados del incremento.
- Congelación comprobada por hashes: main, e4eb17cc, wheel a10cd637 y los cinco
  registros normativos del coste permanecen idénticos a la evidencia anterior.
  El receipt aceptado de 4b0440e1 permanece como prefijo sin modificaciones.

Revisión y corrección de 2/2 bloques completas, sin criterios focales pendientes.
Límites: se fija el workset exacto; la ausencia de otros findings se limita al
diff, sus callers y garantías comprobadas, sin una auditoría global nueva.
No se ejecutan builds, suites globales, perfiles ni coste. F11 continúa con
32 controles aceptados y 12 inconcluyentes, sin nuevos intentos. No se atribuye
su aceptación global al nuevo código. O1–O8/D05/P2 conservan sus cierres y la
parte II permanece fuera del alcance.

### Evaluación integrada del nuevo candidato b93681b7

Autorización: ejecutar el plan acotado anterior; payload congelado en
b93681b74a284ed09138ef3d647bedf6ff7f78da, en rama perf/f11-context-startup.
No se modifica producto. e4eb17cc y wheel a10cd637 se conservan como históricos.
La nueva aceptación no hereda las 32 decisiones de coste anteriores.

| Obligación | Estado inicial | Criterio de cierre |
| --- | --- | --- |
| G1 identidad/reutilización | Aceptada | Identidad fijada; dos módulos productivos alcanzados; focales/revisión y oráculos reutilizables trazados. |
| G2 artefactos | Pendiente | Dos builds iguales, checks y runtime instalado ligados por hashes. |
| G3 coste | Pendiente | 44 cotas superiores <=1,05 con protocolo y presupuesto aprobados. |
| G4 integración final | Pendiente | Tres Python, unittest/pytest, cobertura >=99,00%, deep, replay integrado. |
| G5 compatibilidad/consumidores | Pendiente | Doce celdas y consumidores alcanzados verdes sobre el artefacto nuevo; reutilización justificada. |
| G6 entrega | Pendiente | Receipt completo, review, commits semánticos, árbol limpio y ningún criterio obligatorio pendiente. |

La sonda de atribución previa (600 lecturas, GC activo, 20,81 s) acredita los
mecanismos eliminados, no el presupuesto: wrapper 12.870→0 y bind 198→99,
BSON y trabajo físico iguales. Los tiempos no mejoran uniformemente y no se
promueven a gate. G1 reutiliza esa evidencia y los focales 335/157 por Python,
replay 1.200/360 y revisión aceptada. Capturas y fixtures anteriores son oráculos,
no ejecuciones nuevas del producto. Resultados del nuevo run en la estructura
de evidencia existente; disposición final se añadirá sin editar historia.

G2 aceptado: dos builds desde git archive b93681b7, epoch 1793596735,
normalizados con el script vigente, binariamente reproducibles; checks de
308 archivos/distribución y twine verdes. Instalación aislada, 287 módulos:
wheel b36a7ec8186dba6056b123ca3d6cc51078f7be0aa72749e0c084c1bff079b0e9;
sdist d669b72658259e900973b5e3d4d3663fb7b2613532730b72c837e13c0cbf7530.
Preflight de coste sin lecturas: hashes de los dos workers inalterados,
manifest completo de base/candidato, imports instalados y taskpolicy -a /
Python 3.14.5 / HASHSEED=0 / GC activo comprobados. G1–G2: 2/6 cerradas.

G3 ejecutado, no aceptado: máximo de dos intentos agotado sin fallos técnicos,
seis ABBA/24 registros íntegros. Primera evaluación 18 aceptados/0 regresiones/
26 inconcluyentes; segunda acumulada 30/0/14. Duración: 1.444,03 s y 1.437,01 s,
2.881,05 s acumulados, dentro de 2.700 s por intento/5.400 s totales. Se
conservan todas las muestras y se detiene el coste automático. Los 44 intervalos
y clasificaciones se recalculan desde crudos sin mediciones nuevas; identidad
BSON y metadatos de los 44 controles coinciden con su referencia aceptada.

| Control inconcluyente b93681b7 | Razón | Inferior | Superior |
| --- | --- | --- | --- |
| SQLiteEngine-default-aggregate | 1.028610 | 1.001998 | 1.055930 |
| SQLiteEngine-default-find-limit-one | 1.063237 | 1.032719 | 1.094656 |
| SQLiteEngine-default-find-first-page | 1.034397 | 0.994184 | 1.076235 |
| SQLiteEngine-default-aggregate-first-page | 1.023508 | 0.994645 | 1.053209 |
| SQLiteEngine-standard-find | 1.015152 | 0.979478 | 1.052125 |
| SQLiteEngine-standard-find-limit-one | 1.051057 | 1.025974 | 1.076755 |
| SQLiteEngine-standard-find-first-page | 1.032700 | 1.003450 | 1.062802 |
| SQLiteEngine-decoder-find-limit-one | 1.051230 | 1.023971 | 1.079214 |
| SQLiteEngine-decoder-aggregate-first-page | 1.023410 | 0.992964 | 1.054790 |
| SQLiteEngine-son-find | 1.011931 | 0.971845 | 1.053671 |
| SQLiteEngine-son-find-limit-one | 1.049388 | 1.035552 | 1.063410 |
| SQLiteEngine-son-aggregate-first-page | 1.029275 | 1.007423 | 1.051601 |
| all/limit2 | 1.051028 | 1.039780 | 1.062399 |
| binary/limit2 | 1.040051 | 1.028363 | 1.051872 |

No hay regresión confirmada, pero tampoco aceptación de rendimiento. No se
mezclan estos datos con e4eb17cc, se descartan muestras ni se heredan verdes.
Los 14 intervalos cruzan 1,05. Los gates independientes G4/G5 continúan conforme
al plan para dejar pendiente únicamente lo que falte; F11 no se declara cerrado.
Preflight de integración completado: tres Python, doce perfiles pytest/SDK,
selección de 3.587 nodos y perfil realmente sin bson/pymongo. Un assert del
preflight inicial situaba la ruta antes de -q; se corrigió la selección antes
de lanzar pruebas, sin ejecuciones costosas fallidas ni cambios de producto.

G4 aceptado por ejecución integral y corrección focal del observador, sin
cambiar payload b93681b7 ni artefactos/coste. Unittest termina 0 en los tres
Python. Las tres suites pytest completas conservan un único rojo idéntico:
test_cancelled_snapshot_releases_lazy_source_frame leía iterator._source antes
de la apertura diferida aprobada. El fallo es AttributeError sobre None, no
una fuga ni una nueva regresión productiva. No se ocultan esas salidas.

Se sustituye esa observación privada obsoleta por dos fases explícitas: resolver
suspendido antes de abrir (inyectado, ya que Memory resuelve metadata sin await)
y fuente física abierta bloqueada por el lock real. Barreras deterministas,
sin depender de sleep(0), comprueban cero aperturas antes de resolver y ag_frame
liberado después de cancelar, snapshot cerrado/sin tarea pendiente, owner y
cola vacíos, cero snapshots activos. Las primeras dos expectativas del test
nuevo se corrigen: metadata Memory no espera el lock y una tarea de cierre ya
terminada es válida; no se cambian contratos productivos para satisfacerlas.

La corrección focal del archivo completo pasa 21 pruebas por Python 3.13/3.14/
3.15. La cobertura 3.14 se compone con --cov-append sobre el mismo artefacto:
48.269/48.751 líneas, 99,0113023323%, 482 ausentes, 287 módulos. El perfil deep
pasa. Se acepta la composición explícita de la suite completa con el único
observador reemplazado; no se presenta ningún proceso global originalmente
rojo como terminal 0. Las otras 64.255/64.266/64.255 pruebas aprobadas y sus
skips/subtests mantienen inputs idénticos y se reutilizan; no hay rerun global
por una edición limitada a tests. G1/G2/G4: 3/6 cerradas; G3 sigue inconcluyente.

G5 aceptado por evidencia explícita: nueve celdas completas terminal 0; tres
celdas 3.13 conservan 50 fallos EADDRNOTAVAIL al abrir sockets, antes de comparar
resultados (48 read-reply, uno y uno unack async). Son errores de disponibilidad
de dirección local; el agotamiento transitorio de puertos por densidad de
conexiones es la explicación operativa compatible, no una regresión semántica
confirmada. Se reduce la concurrencia para la repetición, sin modificar producto,
expectativas ni timeouts. Selección exacta de 48+1+1 nodos prevalidada y repetida
secuencialmente: todos verdes. Se conserva la matriz original y la repetición;
no se afirma que sus tres procesos originales fueran verdes. Las doce celdas
quedan cubiertas por 3.587 nodos cada una mediante esa composición, sin repetir
las otras nueve ni los nodos ya aprobados de las tres afectadas.

Consumidores: doce gates actuales terminal 0 y tres controles estáticos
reutilizados (mypy estricto, mypy engine y snapshots). El ledger prueba targets,
scripts/datos del catálogo y fixtures sin cambios; interfaz importada conservada
por review y tipado actual sobre el wheel. Tipado público, manifest, ratchet
contra v4.11.0, SPI, deep 66, smokes wheel/sdist/pytest-kit, conformance de ambos
motores/externo y perfil sin SDK (14 tests, ausencia real de bson/pymongo)
comprobados. Los smokes limpios eliminan PYTHONPATH para que su venv instalado
sea realmente el importado. Se verifican 92/92 outcome_sha256 en los once
workloads funcionales existentes por motor: base inmutable reutilizada y salida
actual comparada. Los tiempos incidentales no generan otra decisión de coste.
Compatibilidad de archivos SQLite anteriores/reapertura se acredita por los
archivos de tests incluidos en la ejecución integral, con fixtures históricas
intactas; no se crea una base antigua nueva para probar compatibilidad.

Revisión final de la evaluación: producto exacto b93681b7, dos distribuciones
reproducibles y sus bytes instalados conservados; 42 pruebas de procedencia
registran imports del nuevo site-packages, incluidos originales rojos y
repeticiones, sin ocultar sus exit codes. Los 287 archivos de cobertura son
del paquete instalado. Main sigue fb7c4a9a; e4eb17cc, wheel a10cd637, manifest
y cinco registros de coste históricos sin cambios. El prefijo histórico del
receipt es idéntico. Después de congelar producto sólo cambian observador de
test, plan/receipt y evidencia. Ningún nuevo mongod o contenedor nativo se ha
arrancado; gates y workers propios terminados. No hay nueva implementación,
publicación, tag ni ampliación a parte II.

| Obligación del nuevo candidato | Disposición final |
| --- | --- |
| G1 identidad/reutilización | Aceptada. Ledger y hashes registrados. |
| G2 artefactos | Aceptada. Wheel b36a7ec8 / sdist d669b726, reproducibles. |
| G3 coste | Pendiente de aceptación: 30 aceptados, 0 regresiones confirmadas, 14 inconcluyentes; dos intentos agotados. |
| G4 integración final | Aceptada por ejecución completa + observador corregido; cobertura instalada 99,0113023323%; composición íntegra trazada. |
| G5 compatibilidad/consumidores | Aceptada por nueve celdas completas + tres compuestas; consumidores verdes y reutilización justificada. |
| G6 entrega final | Pendiente por G3. Receipt/revisión/commits completados; no se concede aceptación de entrega. |

Resultado: 4/6 obligaciones aceptadas, evaluación autorizada ejecutada y sin
pruebas funcionales pendientes. El único bloqueo material es acreditar el coste
de los 14 controles enumerados; G6 depende de ello. No se repite automáticamente
ninguna medición, se cambia el umbral ni se concede cierre por ausencia de
regresión confirmada. Para avanzar hace falta disposición explícita del owner
sobre entorno/evaluación, conservando el contrato vigente. F11 no está cerrado,
ni para el candidato histórico ni por asociación al nuevo artefacto.

### Corrección del coste fijo de opciones vacías SQLite

Encargo «Corrígelo» tras el análisis de los 14 controles: se corrige el viaje
redundante, no el umbral ni la incertidumbre mediante una nueva medición. Rama
perf/sqlite-empty-options desde afcf3990; candidatos e4eb17cc y b93681b7,
respectivos wheels y campañas agotadas intactos. Parte II y cierres anteriores
conservados. El censo alcanza SQLiteAdminRuntime.collection_options,
SQLiteEngine.collection_options, el ciclo de vida SQLiteConnection y sus
consumidores de resolución de contexto; sin nuevos callers públicos ni SPI.

S1/S2 implementados y comprobados focalmente: las tres reproducciones de job/SQL
redundante son rojas antes del cambio y verdes después. Una única observación
vacía vive en SQLiteConnection, junto al owner de metadatos ya existente; se
libera en close. Mismo objeto de conexión primaria, total_changes intacto y sin
transacción son necesarios para reutilizarla; el lock se intenta sin esperar.
La ruta caliente no ejecuta SQL. No se memoriza archivo, perfil virtual ni
opciones no vacías. La comprobación original de sesión precede a todo hit.
No se cambia la lectura de opciones ni se incorporan invalidaciones por DDL.

Pasan 34 pruebas del contrato de jobs incluyendo find/aggregate/vector. El grupo
de frescura/materialización pasa 56 antes de añadir el control vectorial y las
últimas comprobaciones. El primer prototipo restringía al tipo sqlite3.Connection
base y no alcanzaba el tipo propietario real SQLiteConnection; las tres pruebas
lo detectaron y se retiró ese estado provisional en favor del owner correcto.
S3 (tres Python/replay/review) pendiente; no se atribuyen resultados de coste.

S3 aceptado focalmente: selección prevalidada de 301 pruebas aprobadas en cada
Python 3.13/3.14/3.15, con contratos de jobs, materialización/contexto, transacción,
compatibilidad SQLite 4.5/4.10 y tests del engine. Replay prevalidado de 960 casos
verde contra capturas MongoDB 7/8/9 existentes: materialized_context, command_let,
find_projection y cursor_consumption, ambos motores y superficies registradas
sync/async/wire-sync/wire-async. Ratchet de lint verde en cinco archivos Python;
diff revisado y git diff --check verde. Se conservan los rojos y el prototipo
fallido: no se oculta su resultado como verde ni se atribuyen a la versión final.

Se comprueban ausencia de SQL/job en caliente y aislamiento del dict devuelto;
invalidez conservadora tras escritura de datos y collMod, drop/recreación,
rename/dropTarget y dropDatabase; lectura actual tras commit/abort y rechazo de
sesión ajena, sin reutilizar dentro de transacción; archivo actualizado por otro
engine sin cambiar total_changes local; perfil virtual que desaparece sin
escritura SQL; conexión/reconexión y contención con progreso del event loop.
El caso de namespace recreado empieza ahora por metadata vacía antes de pasar
a collation en/strength2 y simple, sin reutilización stale entre operaciones.

Implementación y verificación focal S1–S3 completas. Son tres archivos productivos,
un único valor acotado por conexión y dos archivos de tests; no hay gestor,
generación de invalidación nueva, hook DDL, SQL en event loop ni dependencia/SPI
nuevos. SQLiteConnection sigue siendo propietario y libera la observación aun
si close falla. El camino de fallback mantiene la autoridad original.

Congelación comprobada: ambas distribuciones wheel e4eb17cc/a10cd637 y
b93681b7/b36a7ec8, sus manifests instalados y diez registros de ambas campañas
/config/muestras/looks/resumen siguen idénticos; main y la rama de evaluación
anterior no se mueven. Ninguna medición de coste, perfil, build o suite global
F11 se ha ejecutado en esta corrección. El presupuesto del +5% queda sin
verificar para el nuevo producto; no se transfiere aceptación de runtime de
b93681b7 ni se resetean sus dos intentos agotados. F11 sigue pendiente.

### Revisión de ce2ca71c: identidad de namespace y almacenamiento

Workset fijado: afcf3990 → ce2ca71c779d35316ed3b534e8412621534dbe8d en
perf/sqlite-empty-options, con contrato S1–S3 y censo de los tres módulos
productivos y sus consumidores. Se revisan generación total_changes, locks,
lectura/publicación de la observación, sesión/thread-bound connection,
commit/rollback, rutas de archivo/virtuales y cierre/reconexión. El diff revisado
queda ligado por SHA-256 a los dos paths corregidos en el proof. No se abre una
auditoría general ni se reabren aceptaciones históricas.

Dos findings P2 observados y corregidos, con tres reproducciones rojas:

- La comparación de la tupla de observación usa igualdad Python. Un nombre str
  con igualdad casefold hace que records y RECORDS, distintos en SQL y con
  opciones diferentes, compartan {}. Se reproduce por AsyncCollection.options
  tanto al publicar la observación como al consultarla. Desaparece la collation
  real del segundo namespace, aunque no se haya escrito nada entre lecturas.
- La prueba _path == :memory: también admite igualdad sobrescrita. Una subclase
  str que señala un archivo real y compara igual a :memory: permite reutilizar
  {} después de que otro engine cambie sus opciones. El contador local no cambia
  con una escritura externa, y la API devuelve opciones obsoletas. La prueba usa
  dos engines y un archivo compartido, sin simular el resultado SQL.

Una única elegibilidad _can_observe_empty_options gobierna publicación y lectura:
str nativos para ruta/base/colección y ruta literal :memory:, además de excluir
system.profile. Las subclases conservan el camino SQL anterior; no se convierten
ni reinterpretan sus nombres. No se introduce otro estado, caché o invalidador.
El resto de las condiciones de conexión, contador, transacción y lock se conserva.
Se añaden también dos controles de nombre de base con __eq__ que lanza: la
identidad SQL no debe introducir llamadas a ese hook al consultar o publicar.

Verificación de la corrección: selección prevalidada de 306 pruebas aprobadas
por cada Python 3.13/3.14/3.15, incluyendo las cinco nuevas combinaciones y los
contratos de jobs/sesión/archivo/lifecycle anteriores. Replay prevalidado de 840
casos contra capturas MongoDB 7/8/9: materialized_context, find_projection y
command_let, ambos motores y superficies registradas. Se reutiliza el resto del
replay de consumo ya aceptado: no se toca el lector, sus buffers ni el cierre.
Ratchet verde sobre los dos archivos Python modificados; diff revisado y
whitespace check verde. Los tres avisos iniciales de lint sólo exigían __slots__
en las clases str de los tests y quedan corregidos. No se hacen builds, perfiles,
suites globales F11 ni nuevas mediciones de coste.

No aparecen otros findings accionables en el diff y callers revisados. El workset
está identificado, pero esta revisión acotada no acredita ausencia de defectos
fuera de sus consumidores/garantías comprobadas. Ambos wheels congelados y sus
manifests de módulos, diez registros de las dos campañas, main y la rama de
evaluación anterior permanecen intactos. Las dos correcciones locales quedan
comprobadas focalmente y commiteadas; F11 y el presupuesto del nuevo código
siguen sin aceptación. No se reinicia ningún intento ni se modifica el contrato.

### Preparación de la evaluación b83789bc y sonda de atribución

Plan de evaluación preparado por encargo del owner; no se ejecutan builds,
suites ni coste con esta preparación. Contrato, dependencias y criterios G1–G6
pertenecen a la sección «Evaluación propuesta de b83789bc» del plan vigente.
Candidato productivo b83789bc36bc1bebe693d09b782e5e242be24db4, tres módulos
SQLite cambiados respecto a b93681b7; no se transfiere la aceptación del artefacto
anterior. El futuro presupuesto conserva dos intentos de tres ABBA, 45 minutos
por intento/90 acumulados y los 44 controles. Su ejecución queda pendiente de
inicio explícito, sin tocar los contadores agotados anteriores.

Se incorpora la sonda ya autorizada y ejecutada, no otra medición: evidencia
b837-attribution-* en la carpeta existente, incluyendo worker, ejecución,
resultados, perfiles y análisis. Worker SHA-256
cb1b853b0f0c686006db4de58e643dfa2566731d5fa325586ba8f78eded519d8,
Python 3.14.5, GC activo (2000,10,10), 600 lecturas en 21,376081 s.
C=b93681b7 instalado, D=b83789bc fuente; 100 lecturas por caso/revisión,
una preparatoria y 99 perfiladas. Tres casos: SQLite primera página y vector
con límites 2/64. Resultados BSON iguales entre C/D y ambos registros históricos.

Cada caso pasa de 99 a cero jobs _collection_options_sync y elimina 297
iteraciones del event loop. Fetches/cierres y trabajo vectorial observado no
cambian; bind conserva la reducción B→A anterior. CPU descriptiva C→D:
-1,7542%, -4,2899% y -5,3738%, respectivamente. No son cotas de coste ni prueba
de que todo ese delta proceda del job eliminado: ejecución única secuencial,
profiling y fuente frente a distribución. GC permanece activo; ninguna traza
pendiente al terminar. La evidencia acredita eliminación del trabajo observado,
no el umbral +5%, los otros workloads ni packaging del candidato nuevo.

La comprobación posterior de wheels se restringe a distributions-a/b: un primer
assert auxiliar demasiado amplio incluía wheels artificiales de tests antiguos
y falló; se corrigió la selección sin repetir la sonda. Wheels sellados y
manifests de ambos candidatos, fuentes y registros de coste conservan sus bytes.
No se modifica código productivo ni tests, no se reinicia el gate F11 y no se
hacen otras lecturas de atribución. Las obligaciones G1–G6 de este candidato
siguen 0/6 aceptadas; focales y sonda son evidencia reutilizable, no cierre F11.

### Ejecución autorizada de la evaluación b83789bc

El owner autoriza G1–G6 mediante «Impleméntalo». G1/G2 aceptados: producto exacto
b83789bc36bc1bebe693d09b782e5e242be24db4, 284 módulos sin cambios frente a
b93681b7 y tres módulos SQLite cambiados. Oráculos/focales se conservan; controles
estáticos sólo se reutilizarán por inputs transitivos acreditados antes de G5.
Dos builds limpios desde git archive, epoch 1793606466, producen wheel y sdist
idénticos. Normalizador y checks de fuentes/distribución existentes verdes.
Instalación aislada: 308 archivos y 287 módulos coinciden con fuente y wheel.
Wheel SHA-256 176a98576dafa433137fca4e16467a66707a8904b7c8c54a6a90d1d19cb30e60;
sdist eab99f0def7ed591541c061fb975b5c9b0377a0b74a273e0a90d7f813f9e5bd9.
Evidencia b837-eval-* en la carpeta existente. Ambos candidatos anteriores,
artefactos/manifests y registros de coste intactos. No cambia código ni tests.
2/6 obligaciones aceptadas; siguiente gate G3 aislado, con 0/2 intentos usados.
No se conceden aceptación de coste ni cierre F11 por estos dos gates.

G3 ejecutado y sin aceptación: primer look 10 aceptados/0 regresiones/34
inconcluyentes; segundo, acumulando seis ABBA y 24 registros, 43/0/1.
Único inconcluyente SQLiteEngine-standard-aggregate-first-page: razón
1,0183395032, límites 0,9827550394–1,0552124406. Dos intentos usados, sin
fallos técnicos, 1444,140782 + 1421,870213 s; total 2866,015076 s.
Recalculados independientemente los 88 intervalos/clasificaciones de ambos looks;
orden ABBA y registros completos verificados. Identidad BSON/contadores de los
44 controles coincide con el corpus retenido; workers intactos, GC y política
de lanzamiento precomprobados. La campaña termina: no tercer intento ni más
sondas. Se conserva el inconcluyente, sin rebajar el +5% ni heredar verdes.
G4/G5 independientes siguen autorizados; G6 depende de la aceptación G3.

Checkpoint de pausa solicitado por el owner: «Detente en el próximo commit».
Se recoge la ejecución global ya iniciada, sin lanzar deep, matriz ni consumidores.
Los seis procesos completos unittest/pytest terminan 0 sobre el wheel 176a9857.
Unittest: 3684 tests por Python, tres skips previstos. Pytest: 64281 pasan/764 skips
en 3.13 y 3.15; 64292 pasan/753 skips en 3.14; 14944 subtests por perfil.
No hace falta composición ni repetición correctiva de estas suites.
Cobertura instalada: 48288/48770 líneas = 99,0116875128%, 482 sin cubrir,
287 archivos de site-packages y procedencia de imports comprobados.

G4 conserva pendiente deep: las suites y cobertura no se renombran como gate
integral aceptado. G5 sólo preparado: dependencias reales de doce perfiles y
selección de 3587 nodos comprobadas, sin ejecutar la matriz. Ledger de snapshots
ampliado con hashes de exporter/inputs; strict/engine typing se volverán a probar
por el cambio en el cierre de imports SQLite. No se ejecutan consumidores ni
controles funcionales adicionales después de la orden de pausa.

Estado: 2/6 obligaciones aceptadas (G1/G2). Pendientes: G3 aceptación de coste
con un caso inconcluyente y dos intentos agotados; G4 deep; G5 matriz/consumidores;
G6 aceptación final dependiente de los anteriores. Para retomar, reutilizar estas
seis suites y cobertura sin cambios causales, continuar deep/G5 con los comandos
preparados y conservar el bloqueo G3 sin medir automáticamente. Producto b83789bc,
ambos candidatos/wheels históricos, registros de sus campañas, main y rama anterior
intactos. Evidencia b837-eval-pause-checkpoint y logs/coverage archivados; procesos
propios de validación terminales. Esta pausa no concede cierre F11 ni publicación.

### Revisión independiente de las propuestas de salida F11

Se contrastan los tres informes adjuntos contra ef578283, datos crudos, código,
artefactos y fuentes primarias. Plan refinado en «Revisión de los tres informes»;
no se reanuda la implementación pausada ni se ejecuta coste, builds o globales.
Los 88 intervalos/clasificaciones se reproducen; 43/0/1 se conserva. Dispersión
mediana 0,02659 frente a 0,01632/0,01478 históricas. Correlación del pendiente
con los otros 39 controles 0,92671 y sd residual 0,01273: descripción, no corrección
ni prueba de mecanismo causal. No se declara inexistencia de sobrecoste por medias
ni se identifica una carga externa concreta a partir de covariación/mtime.

Se cubre el hueco de atribución del control exacto base→candidato: una sonda
pública aggregate-sort/to_list(1)/close con codec standard, 200 lecturas totales,
12,698536 s, GC activo y artefactos instalados 2fa637f6/b83789bc. Cada lado:
99 lecturas perfiladas, 5049 fetch calls, 9999 materializaciones públicas
(101 por lectura), 19800 decodificaciones JSON de filas (200 por lectura),
396 llamadas/reanudaciones prepare_pipeline y 31185 iteraciones event loop.
Mismo BSON; cero recolecciones cíclicas en este recorrido aislado. No tiene los
warmups de los 35 controles anteriores del worker normativo ni prueba la causa
histórica del jitter. La CPU perfilada 5703,225/5696,999 ms no acepta coste.
Ambas revisiones instaladas (287 módulos cada una) coinciden con Git/manifests.
Workers normativos, registros de las tres campañas y artefactos congelados intactos.

Se rechaza un nuevo test al 97,5% sin pagar miradas previas y el pooling con
crítico arbitrario. Bajo normalidad iid del protocolo, integración analítica con
sumas inferiores prueba P(E3 intersección E6)>0,0053; por tanto la unión antigua
<0,0447 deja alfa 0,005 para una sola prueba nueva independiente. Simulación
independiente de cuatro millones de réplicas: unión de tres oportunidades 0,04944;
no sustituye la cota. Propuesta seleccionada: seis ABBA nuevos con worker de
consumo intacto, crítico 4,033, sólo datos nuevos para el pendiente, 43 verdes
conservados, una decisión final y máximo 40 min (total máximo 87,8 min). Requiere
autorización de la enmienda antes de medir; no reinicia el contador 2/2. No promete
normalidad ni potencia en la máquina; escenarios simulados de aceptación 0,15/0,59
para sigma 0,034/0,018 muestran el límite de esta alternativa conservadora.

Evidencia b837-report-review.* y b837-exact-attribution-* archivada en la carpeta
existente. Producto y tests sin cambios, no se crean runners ni documentos nuevos.
G1/G2 siguen aceptados (2/6); G3 inconcluyente; suites/cobertura G4 conservadas y
deep pendiente; G5 preparado, G6 pendiente. No se cierran obligaciones sin prueba.
La tabla histórica aportada también se corrige: e4eb tenía 9 consumo SQLite,
2 vectoriales y 1 Memory inconcluyentes; los 14 inconcluyentes de b936 están
aceptados en b837, cuyo único pendiente procede de un caso antes aceptado.

Reanudación autorizada por el owner: deep/consumidores y matriz sobre el mismo
artefacto. G4 aceptado: seis suites/cobertura reutilizadas y deep terminal 0.
Quince controles estáticos/consumidores: catorce ejecuciones verdes y snapshots
reutilizado por ledger. Tipado, SPI, smokes, conformance y SDK-free verdes;
92 outcome hashes funcionales coinciden con base, sin decisión nueva de CPU.
El primer assert auxiliar de archivado suponía 17 controles; el inventario real
contiene 15. Corregido el recuento, sin repetir pruebas ni alterar sus resultados.
3/6 obligaciones aceptadas; G5 espera matriz activa. Continuación final G3
autorizada y preparada: seis ABBA nuevos, crítico 4,033, sólo datos nuevos,
tope 40 min, sin reset 2/2; no empieza hasta cerrar G4/G5.

G5 aceptado por ocho celdas completas terminal 0 y cuatro compuestas: 38 nodos
fallaron antes de consulta por EADDRNOTAVAIL en Python 3.14 (incluye AutoReconnect
con la misma causa). Selecciones exactas precoleccionadas y repetidas en secuencial:
11/1/24/2 verdes. Los rojos se conservan; no cambian timeouts, producto ni asserts.
El primer guard de selección sólo admitía read_reply_parity y detuvo el proceso
tras los once verdes; se amplió a los tres casos unacknowledged con causa idéntica,
sin repetir los once ya comprobados. Consumidores/compatibilidad del mismo wheel
verdes y snapshots reutilizados por ledger. 4/6 obligaciones aceptadas.

La enmienda G3 está autorizada por «Continúa con la implementación». Controller
preparado y archivado antes de medir: un intento final, seis ABBA nuevos del worker
intacto, crítico 4,033 y datos exclusivamente nuevos para el único pendiente;
tope 2400 s y sin miradas intermedias. Se conservan las dos campañas agotadas,
los 43 verdes y los registros originales. Preflight de Python, SDK, GC y política
comprobado sin lecturas de benchmark. La medición empieza sólo al terminar los
procesos de pruebas propios. G6 permanece pendiente de aceptación final G3.

### Aceptación final F11 del candidato b83789bc

Etapa confirmatoria única terminada sin fallo técnico: 24 registros nuevos,
seis ABBA completos, GC activo y worker de consumo intacto. Identidad/resultado
BSON, cantidad de filas y CPU finita/positiva verificadas en los 40 controles
por registro (960 comprobaciones de metadata). Sólo se decide el pendiente;
no se modifican las clasificaciones de los otros 43 ni los looks originales.
Recálculo independiente: razón 0,996011177012, límites
0,948980607228–1,045372536780, desviación de log-razones 0,029378155948,
crítico 4,033 y alfa 0,005. Cota superior <=1,05: G3 aceptado por composición
explícita, 44/44 aceptados, cero regresiones y cero inconcluyentes pendientes.
La regla procede del plan autorizado antes de medir; no de seleccionar el
resultado favorable. No hay análisis intermedio ni más intentos autorizados.

Pared de la etapa: 2027,880412 s (33,80 min), dentro de 2400 s. Acumulado con
la campaña original: 4893,895488 s (81,56 min), dentro de 5400 s. Los intentos
originales continúan 2/2 agotados y la excepción final 1/1 consumida. Las muestras
antiguas permanecen inconcluyentes donde lo eran: se archiva composición nueva,
no un look6 reescrito. No se promete inferencia sin supuestos: bloques normales
independientes dentro de las etapas, etapa nueva independiente de la antigua y
mismo parámetro objetivo. No hay garantía universal fuera de los 44 controles.

| Gate | Evidencia de aceptación del candidato |
| --- | --- |
| G1 | Ledger, producto y entradas/identidades comprobadas. Oráculos y snapshots reutilizados sólo por entradas conservadas. |
| G2 | Dos distribuciones reproducibles; 308 archivos/287 módulos fuente-wheel-instalación; procedencia comprobada. |
| G3 | 43 controles de look6 y un control de la única confirmación nueva; recálculo y equivalencia preservados. |
| G4 | Unittest/pytest completos en tres Python, cobertura instalada 99,0116875128% y deep verde; sin repetir suites válidas. |
| G5 | Doce perfiles: ocho completos y cuatro compuestos con 38 nodos focales verdes; rojos de socket conservados. Catorce gates actuales, un snapshot reutilizado, 92 outcomes funcionales idénticos. |
| G6 | Diff final/documentación revisados, identidades históricas intactas, evidencia archivada y procesos propios terminales. Se completa con el commit semántico de este registro y verificación posterior del árbol limpio. |

Artefacto aceptado: producto b83789bc36bc1bebe693d09b782e5e242be24db4;
wheel 176a98576dafa433137fca4e16467a66707a8904b7c8c54a6a90d1d19cb30e60;
sdist eab99f0def7ed591541c061fb975b5c9b0377a0b74a273e0a90d7f813f9e5bd9.
Los commits posteriores al payload son documentales/evidencia; no cambian src.
Los 308 archivos y ambas distribuciones reproducibles se verifican de nuevo por
hash; e4eb17cc/b93681b7, sus wheels, registros, main y rama anterior intactos.
La evidencia nativa es replay de capturas aceptadas, no una recaptura ni otra
campaña física. Parte II, O1–O8/D05/P2 históricos, publicación y tag no se reabren.

Errata documental de la sonda aislada de esta revisión: el control exacto tiene
29 controles previos en el worker normativo, no 35 como decía el párrafo anterior.
La sonda no ejecutaba ninguno de ellos; no cambia su salida ni acredita coste.
Esa limitación motivaba precisamente mantenerla sólo como atribución diagnóstica.

Evidencia final en f11-b837-confirmatory-* y b837-eval-confirmatory-acceptance,
b837-eval-final-review/all-import-proofs/evaluation, dentro de la carpeta existente.
No hay nuevas capas de validación ni cambios de producto para forzar el gate.
F11 queda aceptado para este candidato al completar el commit y árbol limpio;
no quedan obligaciones técnicas de la parte I por comprobar.

### Cierre administrativo de parte I sin cierre de versión

Por encargo del owner se reconcilian las entradas vigentes del plan, este receipt,
el índice documental y las notas Unreleased. La tabla F00–F11 queda en 12/12
cerrados; no hay otra implementación, prueba o aceptación técnica pendiente.
Se conserva el detalle histórico de focales, fallos, campañas y composiciones;
no se reabren gates ni se repiten suites por cambios exclusivamente documentales.

Integración local preparada como fast-forward desde main fb7c4a9a hacia la rama
perf/sqlite-empty-options, que lo contiene como ancestro. No hay conflictos ni
nuevos cambios productivos: src sigue idéntico al payload b83789bc aceptado.
La integración no reconstruye ni sustituye los wheels congelados, no reescribe
commits y no elimina ramas. Su efectividad se comprueba al terminar la operación.
La conservación de main durante los gates anteriores permanece como hecho
histórico; su promoción local se realiza ahora, después de la aceptación.

Versión 4.11.0 sin cambios; changelog permanece en Unreleased, sin fecha de nueva
release ni tags nuevos. No se publica, hace push ni abre parte II. El cierre de
versión queda aplazado explícitamente y separado de la parte I ya completada.

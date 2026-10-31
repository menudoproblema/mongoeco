# Estado de implementación de paridad semántica

Contrato: [plan revisado](plan-semantic-parity-refactor.md). Base: `b38ef967`.
Estado: implementación en curso; B01/B02 completados con validación focal; B00/B03/B04 abiertos.

## B00: infraestructura inicial

El runner existente integra los testigos de group, ausencia, consumidores,
comparación/orden, Decimal128, suma/promedio y overflow. Las familias más
recientes incluyen `command-let` y `expression-grammar`, además de `overflow-values`,
`overflow-numeric-controls` y `overflow-writes`.
El inventario ejecutable posee las acciones y seeds; el
[receipt vigente](evidence/semantic-refactor/command-let-validation.json) posee
las ejecuciones y hashes de este corte. Las capturas canónicas son versionadas
7/8/9, con buildInfo/FCV, SDK, hash del módulo y manifiestos. Se conserva el
replay histórico.

`tests/differential/semantic_corpora.py` posee únicamente el registro de corpus,
casos, seed, módulo y selector de captura. Catálogo, capturador, replay y matriz
pública derivan sus inventarios de él. Se verifican IDs y selectores únicos,
conjuntos no vacíos, manifiestos completos y coincidencia de seeds con tipos BSON.
No introduce runner, lenguaje de fixtures ni evaluador alternativo. El replay
usa las entradas Python del corpus; los manifiestos se comparan por bytes BSON
para reconocer int64 sin confundir el decoder Extended JSON con una diferencia
de valor. Esto conserva la cobertura de int grandes Python e Int64 explícitos.

B00 sigue abierto: faltan familias de los demás bloques y la comprobación de
oráculos/errores/efectos completa. La identidad bool/int/double y ausencia/null
se comprueba explícitamente en el observador. La consolidación del registro se
valida junto con el [primer corte B03](evidence/semantic-refactor/numeric-validation.json).

## B01: implementación y validación focal completadas

Se eliminó `compiled_aggregation.py`, su generación de código y la caché.
No quedan imports ni callers de CompiledGroup en source/tests. `_apply_group`
usa `_IncrementalGroup` y no reintenta fallos de ejecución. Los tests de
mecanismos eliminados se retiraron; los tests de comportamiento se migraron al
núcleo común. El manifiesto público permanece vigente, sin cambios SPI.

Referencias reales: MongoDB **7.0.43, 8.0.32 y 9.0.2**, con FCV comprobada.
Los tres testigos pasan contra ambos motores por el runner existente:
[7](evidence/semantic-refactor/group-native-7.json),
[8](evidence/semantic-refactor/group-native-8.json),
[9](evidence/semantic-refactor/group-native-9.json).
La [captura inicial](../tests/fixtures/semantic_group_mongodb_9_0.json) y el
[fallo anterior](evidence/semantic-refactor/group-before-9.json) se conservan
como historia; el replay vigente usa las capturas versionadas nuevas.

Validación final y hashes: [receipt](evidence/semantic-refactor/group-validation.json).
82 tests de replay públicos: sync/async directos y PyMongo wire sync/async,
ambos motores, tres dialectos, más deadlines y verificación de capturas.
Regresión de agregación/preparación: 469 tests y 1.594 subtests verdes; los
32 skips corresponden a tests nativos sin URI y no cuentan como diferencial
(los reportes nativos separados no tienen skips). Lint ratchet de los 13
archivos Python cambiados y manifiesto público verdes.

El [benchmark](evidence/semantic-refactor/group-benchmark.md) compara la base
original y el candidato con el mismo harness/dataset, 1.000 documentos,
un warmup y tres repeticiones. Los hashes de resultados coinciden. El grupo
anidado mide **+24,6 %** de wall time; group principal **+0,5 %**, top-k **−5,4 %**.
Se conserva la ruta correcta y simple; no existe un presupuesto que exija
mantener el compilador. Es una muestra local pequeña, no una garantía general
de rendimiento. No se oculta el coste ni se restaura la semántica defectuosa.

Los contenedores nativos 7/8 de este corte se detuvieron. Wheel, suite global,
Python/SDK extremos y otros bloques siguen pendientes para B10. B01 completado
no acredita cierre del programa.

## B02: implementación y validación focal completadas

### Primer corte (evidencia histórica)

Ocho testigos públicos nuevos capturan comparaciones, referencias ROOT/CURRENT y
léxicas, productores de ausencia, composición, arrays, REMOVE, booleanos y
reasignación de CURRENT. Referencias canónicas por versión en
`tests/fixtures/semantic_missing_{7_0,8_0,9_0}.json`, con identidad del corpus.
El capturador existente acepta `--case-set missing-values`. El replay y el
inventario suplementario consumen las tres referencias; no quedan esos ocho
casos en pendientes de captura.

Se usa un único sentinel Enum para ausencia/REMOVE, estable bajo deepcopy y
pickle. La evaluación conserva missing; arrays y operadores nullable lo
materializan en su propia frontera. `_evaluate_expression_with_missing` delega
en la misma evaluación. Las comparaciones separan missing de null conservando
la política de igualdad personalizada. CURRENT gobierna también `$field`.
Se eliminó el memo especial de copias y las ramas duplicadas de REMOVE/MISSING
al asignar campos. El cambio alcanza el consumidor de `$set`; H05 todavía
requiere sus pruebas específicas de escritura en B05.

[Antes](evidence/semantic-refactor/missing-before-9.json): ocho testigos fallan
en ambos motores frente a 9.0.2. Después, los ocho pasan contra
[7.0.43](evidence/semantic-refactor/missing-native-7.json),
[8.0.32](evidence/semantic-refactor/missing-native-8.json) y
[9.0.2](evidence/semantic-refactor/missing-native-9.json), sin skips nativos.
La referencia 9.0 se verificó con `--check` sin alterar el golden.

[Receipt y hashes](evidence/semantic-refactor/missing-validation.json): 278 tests
verdes de replay (incluye regresiones B01), cuatro superficies y ambos motores;
446 tests y 1.594 subtests de regresión, 40 skips locales que no acreditan
validación nativa. Lint ratchet de 23 archivos verdes, manifiesto público y
`git diff --check` verdes. Los servidores temporales 7/8 se detuvieron.

Los bordes de REMOVE.path y variables null fueron contrastados adicionalmente
en [MongoDB 7/8](evidence/semantic-refactor/missing-edge-native.json).

### Ampliación de consumidores y cierre focal

Se corrigieron `$reduce` (estado inicial y resultado conservan missing) y
`$ifNull` (el reemplazo final puede producir ausencia). Treinta testigos nuevos
cubren esos casos, `$replaceRoot`/`$replaceWith`, variables de lookup y grupos en
lookup/union, y cuatro valores compuestos a través de seis rutas de escritura:
update_one, update_many, find_one_and_update, bulk ordered/unordered y comando.
Las escrituras observan documento final, contadores y retorno cuando aplica.

Las capturas `semantic_absence_consumers_{7_0,8_0,9_0}.json` se integran en replay.
Los 41 testigos conjuntos (3 B01 + 8 missing + 30 consumidores) pasan en ambos
motores contra [7.0.43](evidence/semantic-refactor/absence-consumers-native-7.json),
[8.0.32](evidence/semantic-refactor/absence-consumers-native-8.json) y
[9.0.2](evidence/semantic-refactor/absence-consumers-native-9.json), sin skips.

[Receipt final focal](evidence/semantic-refactor/absence-consumers-validation.json):
1.001 tests de replay y controles en cuatro superficies, dos motores y tres
dialectos; 446 tests y 1.594 subtests de regresión; 16 tests y 28 subtests de
propiedades/atomicidad. Los 70 skips de regresión son tests nativos sin URI y
no acreditan paridad; los diferenciales separados no omiten casos. Lint ratchet
verde. El replay de 1.001 tests se repitió al cerrar esta trazabilidad.

El [benchmark frente a B01](evidence/semantic-refactor/missing-benchmark.md)
conserva hashes de resultados: grupo anidado +1,9 %, principal +1,0 %, top-k
+2,7 %, con 1.000 documentos y tres repeticiones; solo tendencia local.

B02 queda completado en su alcance focal. Esto no cierra la semántica de paths
de H05 (B05), acumuladores de H08 (B06), frontera BSON estricta de H06 (B07) ni
los gates globales B10. Los receipts anteriores conservan su alcance histórico.

## B03: primer corte numérico (abierto)

Se separa la conversión exacta de comparación de la conversión aritmética:
`numeric_comparison_decimal` preserva el valor binario del double; no lo convierte
mediante su representación decimal abreviada. Comparación y claves de igualdad
usan esa misma representación. Las claves aprovechan igualdad/hash de Decimal
sin `normalize()`, evitando redondeo por el contexto ambiental. La identidad
física y las claves textuales persistidas SQLite no cambian en este corte.

Se corrige además la clasificación BSON de `bson.Int64` cuando no aparece como
tipo exacto en la tabla, respetando las entradas explícitas de dialectos. La
sonda había expuesto un orden incorrecto Int64/bool en Memory que SQLite no
reproducía; se ajustan ambos caminos del comparador mediante el helper de rank.

Cuatro casos nativos cubren comparación cruzada, group, lookup directo e índice
unique: double/decimal 0.1 (diferentes), 0.5 (iguales), enteros alrededor de
2**53, int/long/double/decimal, booleanos y cero con signo. Los fallos previos en
[8](evidence/semantic-refactor/numeric-before-8.json) y
[9](evidence/semantic-refactor/numeric-before-9.json) se conservan.
Las capturas `semantic_numeric_{7_0,8_0,9_0}.json` se consumen por replay;
la referencia 9.0 se verificó mediante captura nueva con `--check`.

Los 45 casos conjuntos pasan contra MongoDB
[7.0.43](evidence/semantic-refactor/numeric-native-7.json),
[8.0.32](evidence/semantic-refactor/numeric-native-8.json) y
[9.0.2](evidence/semantic-refactor/numeric-native-9.json), ambos motores y cero skips.
[Receipt del corte](evidence/semantic-refactor/numeric-validation.json): 1.107
pruebas de superficies y controles, incluidos contextos Decimal 6/28/34; 99 tests
y 62 subtests numéricos/índices/replay; 165 tests y 6 subtests de compatibilidad,
agregación y propiedades. Lint ratchet de 29 archivos y manifiesto público verdes.

H24 permanece parcial: faltan predicados indexados, poda de candidatos/rangos,
multikey y archivos SQLite previos, además de ampliar límites numéricos. No se
extrapola de unique a los lectores optimizados ni se cambia el formato de las
claves persistidas. Siguen pendientes H26, H27 y H33; los comparadores duplicados
se revisarán al resolver H33 conservando overrides de dialecto y collation.

### Segundo corte: predicados numéricos e índices SQLite

Se añadieron dos acciones con referencia nativa: lectura sin índice y lectura
con índices/hints. Cada acción contiene 462 consultas (11 operandos × 7
operadores × 6 campos), con escalares y arrays mixtos Decimal/Int64/double,
campos sin decimal y campos de int/float Python sin etiquetar. Incluyen
`$eq/$lt/$lte/$gt/$gte/$in/$nin`. No se impone sort al servidor para que puedan
activarse las rutas rápidas; se ordenan solo los IDs observados después.

El [candidato anterior](evidence/semantic-refactor/numeric-predicates-previous-cut-8.json)
falla en las dos acciones con SQLite frente a MongoDB 8; se reconstruyó con
los hashes del primer corte B03 y SQLite de la base, usando el corpus final.
Las primeras sondas también fallan en
[7](evidence/semantic-refactor/numeric-predicates-before-7.json) y
[8](evidence/semantic-refactor/numeric-predicates-before-8.json).
El dialecto 9 no reproducía este fallo porque ya delegaba en Python: su verde
no acreditaba las rutas SQL de 7/8.

Las claves auxiliares numéricas persistidas representan texto decimal
aproximado, por lo que no pueden establecer igualdad ni orden BSON exactos.
Se retira su uso numérico de fast paths escalares, multikey y sort; no se cambia
el formato SQLite. Las comparaciones SQL que usan valores JSON originales y
arrays homogéneos siguen activas. Los valores/operandos etiquetados Int64 y
Decimal, incluidos los de arrays, se detectan antes del SQL y usan el comparador
común. El guard se comparte con los callers existentes; su razón de explain se
actualiza a `Tagged BSON numeric comparison requires Python fallback`.

La normalización de claves auxiliares deja de depender de la precisión Decimal
ambiental, conservando el formato anterior. Esto se detectó al reabrir una
[fixture SQLite de 4.11.0](../tests/fixtures/sqlite/numeric-indexes-before-semantic-fix.json),
generada con los 293 blobs source verificados contra la base. Los tests leen
resultados nativos, reabren dos veces y conservan esquema, documentos/claves de
almacenamiento y filas semánticas de índices con precisiones 6 y 34. Además,
explain comprueba SQL exacto sin claves auxiliares para números JSON y delegación
para tipos etiquetados; no se infiere la estrategia solo de los resultados.

Los 47 testigos conjuntos pasan contra
[7.0.43](evidence/semantic-refactor/numeric-predicates-native-7.json),
[8.0.32](evidence/semantic-refactor/numeric-predicates-native-8.json) y
[9.0.2](evidence/semantic-refactor/numeric-predicates-native-9.json), ambos motores,
cero skips. [Receipt](evidence/semantic-refactor/numeric-predicates-validation.json):
1.158 tests de replay/controles; 84 tests y 93 subtests numéricos, planner,
compatibilidad de almacenamiento y replay histórico. La ampliación a lectores,
mutaciones y fixtures 4.5/4.10 dio 1.741 tests verdes y 24 fallos de bind del
sandbox; los archivos wire afectados se repitieron con permiso de sockets y
pasaron sus 1.616 tests. Estos conteos se solapan y no se suman. Lint ratchet,
manifiesto público y diff check verdes.

El [benchmark](evidence/semantic-refactor/numeric-predicates-benchmark.md) usa
el harness existente, el candidato anterior verificado y el mismo dataset de
1.000 documentos, un warmup y tres repeticiones. Los seis hashes de resultados
coinciden. Igualdad numérica con índice mide −2,1 %; los controles oscilan entre
−4,2 % y +2,6 %. Es una muestra local; no garantiza rendimiento general ni
justifica recuperar la ruta semántica incorrecta.

H24 sigue parcial: faltan ampliar límites/no finitos y comprobar unique
multikey, partial y collation. B03 sigue abierto con H26/H27/H33 pendientes.

### Tercer corte: orden BSON de documentos (H33 corregido)

Cinco acciones nativas cubren 18 documentos: `$cmp` simple/con collation en 324
pares por acción, sort de find/agregación y filtros de objetos, y creación y
selección de un índice parcial por `createIndexes`, con/sin hint. Incluyen tipos
antes de nombres de campo (`{z: null}` frente a `{a: 1}`), igualdad numérica,
booleanos, claves reordenadas, nombres A/a y valores de texto A/a, objetos y
arrays anidados. Las capturas son `semantic_document_order_{7_0,8_0,9_0}.json`.

El [corte anterior](evidence/semantic-refactor/document-order-previous-cut-8.json),
reconstruido con hashes verificados de los dos primeros cortes B03 y usando el
corpus final, falla sus cinco acciones en ambos motores (diez subfallos).

`bson_type_rank` y `compare_bson_documents` en `core/bson_ordering.py` poseen la
regla común. El comparador por defecto y el adaptador de collation delegan en
ella; los nombres mantienen orden binario. `MongoDialect.compare_values` delega
en el comparador común conservando su callback recursivo, retirando su segunda
implementación. La tabla de tipos explícita del dialecto conserva prioridad.
No se modifica identidad física ni claves de igualdad de documentos.

La sonda de filtros reveló otro error compartido: `_InternalBsonDocument` en un
operando pasaba por tipo desconocido, por lo que un filtro de documento podía
devolver vacío aunque la comparación de dicts ordinarios fuera correcta. Las
marcas de ownership se clasifican como su contenedor BSON; una subclase con
rango explícito mantiene ese override. Tests adicionales conservan comparadores
personalizados dentro de objetos/arrays, con/sin collation, y prioridad de tipos.

Los 52 testigos pasan contra MongoDB
[7.0.43](evidence/semantic-refactor/document-order-native-7.json),
[8.0.32](evidence/semantic-refactor/document-order-native-8.json) y
[9.0.2](evidence/semantic-refactor/document-order-native-9.json), ambos motores y
cero skips. [Receipt](evidence/semantic-refactor/document-order-validation.json):
1.281 tests de replay/controles en cuatro superficies; 232 tests y 293 subtests
de comparación, filtering, compilación de consultas, propiedades y contratos.
El único skip local es el backend ICU opcional no instalado; pyuca y los
controles nativos de collation pasan. Otros 39 tests y 68 subtests cubren índices
parciales, Code, identidades y archivos SQLite previos. Lint ratchet de 40
archivos, manifiesto público y diff check verdes.

El [benchmark](evidence/semantic-refactor/document-order-benchmark.md) añade
ordenación de documentos de forma fija al harness existente y compara contra
el corte anterior con hashes verificados. Dataset de 1.000 documentos, un
warmup y tres repeticiones; los cinco hashes de resultados coinciden. Tras
retirar un callback redundante y trabajo de clasificación repetido, ordenación
de documentos mide +5,0 %; los controles +1,2 % a +12,9 %. La muestra es local y
pequeña, sin umbral contractual de tiempo; no acredita rendimiento general.

B03 permanece abierto por H24/H26/H27. Se conserva además para B08/H35 la
observación de superficie detectada al preparar el canario parcial: la API
directa local no acepta `create_index(partialFilterExpression=...)`, mientras
el comando raw sí. Su clasificación y regresión de alias pertenecen a B08; el
oráculo H33 usa exactamente la misma petición raw en servidor y candidato.

### Cuarto corte: aritmética y materialización Decimal128 (H26 parcial)

Ocho acciones nativas nuevas cubren aritmética, group, ventanas, escritura por
pipeline, booleanos y límites. Las seis primeras fallaban en ambos motores
frente a [9.0.2](evidence/semantic-refactor/decimal-before-9.json): diez
subfallos y dos errores. El [antes con el corpus final](evidence/semantic-refactor/decimal-previous-cut-8.json)
usa un candidato previo reconstruido por hashes y MongoDB 8: ocho acciones,
diez subfallos, seis errores y cero skips. Las capturas finales son
`semantic_decimal_{7_0,8_0,9_0}.json` y `semantic_decimal_edges_{7_0,8_0,9_0}.json`.

`bson_add/subtract/multiply/divide/mod` aíslan la aritmética mediante contexto
Decimal128 propio de 34 cifras, redondeo half-even, exponentes y clamp definidos.
Sus flags no alteran el contexto de la aplicación. La comparación exacta sigue
separada: el constructor explícito `Decimal.from_float` evita el trap/flag
ambiental FloatOperation, que la nueva prueba metamórfica había descubierto.

La sonda mixta confirmó dos conversiones diferentes: aritmética desde double a
15 cifras frente a 34 en acumuladores. `bson_accumulator_add` posee esta segunda
conversión para los consumidores de suma/promedio existentes; no se introduce
otro evaluador. Las expresiones sum/avg y los acumuladores comparten esa regla.
La finalización de `_AverageAccumulator` usa `bson_divide`, conservando el tipo
y precisión en group y ventanas en lugar de dividir un Decimal desenvuelto.

Los resultados decimales se conservan como `BsonDecimal128` y se materializan en
la frontera pública común: `bson.Decimal128` si está disponible, el tipo Decimal128
local si no. El codec reversible mantiene su contrato de lectura histórica;
la validación BSON estricta de entradas sigue pendiente en B07. La veracidad y
`$toBool` reconocen cero decimal, -0, NaN e infinito mediante el valor numérico.

Los límites capturados incluyen máximo finito, overflow a Infinity, mínimo
subnormal, división que subdesborda a cero, signo de cero, NaN e infinito, con
estado escrito posterior. El resto decimal necesita un cociente interno de más
de 34 cifras en algunos de esos límites: se amplía solo ese cálculo interno y
se conserva la conversión Decimal128 del resto final.

Los 60 testigos conjuntos pasan en ambos motores contra MongoDB
[7.0.43](evidence/semantic-refactor/decimal-native-7.json),
[8.0.32](evidence/semantic-refactor/decimal-native-8.json) y
[9.0.2](evidence/semantic-refactor/decimal-native-9.json), sin errores ni skips.
[Receipt del corte](evidence/semantic-refactor/decimal-validation.json): 1.509
pruebas de replay y controles en cuatro superficies; incluye precisiones 6/28/34,
rounding UP/DOWN, traps Inexact/FloatOperation y conservación de flags. Regresión:
502 tests y 359 subtests de núcleo/agregación/contratos; 36 tests y 74 subtests de
frontera BSON/wire, identidades, SQLite previo y replay. Lint ratchet de 45
archivos, manifiesto público y diff check verdes. Las expectativas unitarias
antiguas que exigían promedios desenvueltos se ajustan al resultado BSON nativo.

El [benchmark](evidence/semantic-refactor/decimal-benchmark.md) compara el corte
anterior con el mismo harness/dataset, memory-sync, 1.000 documentos, un warmup y
tres repeticiones. Ambos hashes de resultados coinciden; group anidado mide
+10,5 % y principal +15,0 %. Son tendencias locales sobre el workload existente,
no una medición completa del dominio Decimal128 ni una garantía de rendimiento.

H26 sigue parcial: faltan capturar y corregir las funciones numéricas avanzadas
(abs, potencias, logaritmos, exponenciales, raíces y redondeo), además de pruebas
de cancelación y acumulación compensada. Este corte no cierra H24 ni H27,
los consumidores de B06 ni los gates globales B10.

### Quinto corte: funciones Decimal128 avanzadas (H26 sigue abierto)

El [candidato anterior a H26](evidence/semantic-refactor/decimal-advanced-prior-candidate-8.json),
reconstruido y verificado en el receipt del cuarto corte, falla las ocho acciones
del corpus avanzado final frente a MongoDB 8 (dos subfallos y catorce errores).
La [sonda inicial](evidence/semantic-refactor/decimal-advanced-before-9.json)
se conserva con su corpus inicial más pequeño. Las capturas finales son
`semantic_decimal_{advanced,advanced_edges,generated,controls}_{7_0,8_0,9_0}.json`.

Se corrigen abs, exp, ln/log10/log, sqrt, pow, round/trunc y floor/ceil, con
tipos y límites observados. Se normalizan los operandos Decimal128 antes de
operar; no se convierten a float para ejecutar funciones `math`. La cuantización
conserva signo/escala y reproduce los NaN/Infinity nativos por operador. La
aritmética y las plantillas compartidas conservan Int64 explícito; potencias
enteras sin overflow mantienen entero y floor/ceil de double mantienen double.
Los casos de overflow int64 y promoción completa siguen en H27.

El kernel portable `core/decimal_math.py` adapta los pasos BID de Intel usando
solo Decimal/Fraction estándar: redondeo binary128 de 113 bits, conversión de dos
partes truncadas, corrección cercana a uno, escalado y corrección residual de
potencias. No añade backend, compilador ni dependencia nativa. Un reemplazo
simple por Decimal.ln/exp/power no conservaba el último dígito. Se mantienen
comportamientos contraintuitivos del servidor, sin tablas de respuestas ni
tolerancias. [Procedencia y alcance](evidence/semantic-refactor/decimal-math-source-notes.md).

Las capturas revelan un delta por versión: `$log` de bases 2/10 en 7/8 usa
rutas especializadas, mientras 9 coincide con ln(x)/ln(base). La decisión vive
en el adaptador del dialecto; no se extrapola de una versión a las otras.
El corpus incluye diez operandos finitos seleccionados, 32 generados por una
recurrencia fija, cuatro controles int/long/double, diez entradas límite,
catorce pares de potencias, cinco errores de dominio y escritura por pipeline.

Se corrigió también el observador B00: dos double NaN independientes no pueden
compararse con igualdad Python. La clave observada canoniza ese valor conservando
la etiqueta de tipo; NaN double sigue distinto de NaN Decimal128, cadena "NaN"
y null. Los demás valores y el orden/estructura de las claves se conservan.
Las capturas siguen almacenando resultados BSON crudos, no claves del matcher.

Los 68 testigos pasan contra MongoDB
[7.0.43](evidence/semantic-refactor/decimal-advanced-native-7.json),
[8.0.32](evidence/semantic-refactor/decimal-advanced-native-8.json) y
[9.0.2](evidence/semantic-refactor/decimal-advanced-native-9.json), ambos motores,
cero errores/skips. [Receipt](evidence/semantic-refactor/decimal-advanced-validation.json):
1.750 tests de replay/controles en cuatro superficies, incluidos traps/flags
ambientales; 504 tests y 359 subtests de núcleo/agregación/contratos/propiedades;
39 tests y 84 subtests de BSON/wire, identidades, SQLite previo e índices parciales.
Lint ratchet de 48 archivos, manifiesto público y diff check verdes.

Se incluye el aviso Intel BSD y se declara `Apache-2.0 AND ICU AND BSD-3-Clause`.
La [verificación de distribución](evidence/semantic-refactor/decimal-math-distribution.json)
comprueba fuentes/aviso/metadata en wheel y sdist y reproduce 48 acciones nativas
desde el wheel (origen importado verificado), ambos motores y tres dialectos.
Esto es un smoke focal; no cierra B10 ni su matriz completa de Python/SDK.

El [benchmark de control](evidence/semantic-refactor/decimal-advanced-benchmark.md)
usa el mismo harness/dataset y el reporte verificado del cuarto corte como base:
1.000 documentos, un warmup y tres repeticiones, memory-sync. Los dos hashes de
resultados coinciden; grupos anidados +3,2 % y principales +2,3 %. No mide todo
el dominio de trascendentes ni garantiza rendimiento general.

H26 queda pendiente por cancelación y acumulación compensada. H24/H27 y las
reglas de consumidores de B06 siguen abiertos, al igual que los gates de B10.
El oráculo de errores de este corte observa familia/código/recuperación;
los envelopes y fases completos siguen siendo obligaciones de B04/B08.

### Sexto corte: estado compartido de suma/promedio (H26 abierto)

Se compone un estado numérico de tamaño fijo (`NumericSum`) con partes double
alta/baja y total decimal independiente. Expresiones, group y promedio delegan
en él; se retira `bson_accumulator_add`, que perdía información entre entradas.
El promedio mantiene conteo y finalización. La selección de arrays sigue en su
contexto: `$sum` como expresión recorre un array de un nivel, group ignora el
operando array. Se corrige un test previo que exigía sumar esos arrays en group.

`RemovableNumericSum` adapta ese mismo estado para ventanas con límite inferior
acotado: conserva historia aritmética, tipos y contadores actuales, retira
valores y permite recuperar el resultado al salir NaN/infinidades. No sustituye
las ventanas crecientes por esa semántica. Las ventanas por rango desempaquetan
los índices BSON numéricos antes de seleccionar miembros; esto no acredita
límites de precisión de todos los ejes de rango. También se comparte la
clasificación de tipos numéricos para reconocer Decimal128 público después de
un update por pipeline en Memory.

Dos corpus contienen siete acciones, 35 secuencias seleccionadas y ocho
generadas, en 7/8/9: expresión, grupo ordenado tras unwind, facet, ventanas de
documentos/rango, escritura y grupo incremental con entrada explícitamente
ordenada. Se conserva el caso incremental en la matriz aunque falle. Su prueba
fuerza apertura de spool con umbral 2 y contrasta también el recorrido sin spill.

**Pendiente descubierto, sin ocultar el fallo:** en 8.0.32 y 9.0.2, el grupo
simple ordenado usa SBE y devuelve `1/3` como promedio de
`[Int64(9223372036854775807), 1, Int64(-9223372036854775807)]`. Las formas tras
unwind y dentro de facet usan el motor clásico y devuelven `0`. Mongoeco devuelve
`0` en las tres formas. Se capturan valores/tipos y planes en
[MongoDB 7](evidence/semantic-refactor/sum-route-7.json),
[MongoDB 8](evidence/semantic-refactor/sum-route-8.json) y
[MongoDB 9](evidence/semantic-refactor/sum-route-9.json), con un
[reproductor nativo](evidence/semantic-refactor/characterize_sum_route.py).
No se añade una excepción por nombre de test ni se deduce la selección SBE solo
de la versión: el contrato depende también de la forma/plan de ejecución.

El [receipt de este corte](evidence/semantic-refactor/sum-validation.json)
conserva fuentes, referencias, resultados y fallos: 75 acciones nativas verdes
en 7; dos subfallos (un caso, ambos motores) en 8 y en 9, sin errores ni skips.
La matriz pública conserva 1.920 verdes y 32 fallos de ese mismo caso. Regresión
del núcleo: 474 pruebas y 373 subtests verdes; fronteras/spill/cursor: 65 pruebas
y 218 subtests verdes. Lint ratchet y manifiesto público comprobados.

El [benchmark focal](evidence/semantic-refactor/sum-benchmark.md) conserva el
rechazo del comparador por cambio de resultado. La sonda nativa de group/facet
con sus 1.000 documentos confirma los promedios actuales y rechaza los
anteriores; no se presenta el cambio de tiempos como optimización equivalente.
 H26/B03/B06 siguen abiertos;
la siguiente acción es delimitar la finalización por estrategia y adaptar sus
consumidores sin duplicar motores. H27 conserva además los pendientes de
expresiones aritméticas y errores/atomicidad de `$inc`. Los gates globales y la
validación del wheel de este corte siguen pendientes.

### Séptimo corte: finalización de promedio preparada (H26 corregido focalmente)

La anotación `PreparedStage.group_average_uses_sum` selecciona la finalización
numérica nativa antes de que pushdown, paginado o subpipelines recorten el
pipeline. Las vistas físicas conservan la decisión y una reubicación lógica la
recalcula. Cursor incremental, ejecutor común y recorrido con provenance la
transportan al mismo bucket/estado de suma; el bucket la conserva por spill.
No se introduce otro intérprete ni se usa el backend local para decidir el valor.

El promedio clásico divide la parte double; la forma nativa SBE finaliza la suma
BSON antes de dividir. El caso que quedaba rojo devuelve así `1/3` en su grupo
simple 8/9, y conserva `0` cuando corresponde al contexto clásico. También se
conservan los cambios de contexto entre 8 y 9. El
[detalle de selección y fuentes](evidence/semantic-refactor/average-route-source-notes.md)
explica el owner y los límites sin anunciar un planificador SBE general.

Se captura una acción con 31 contextos en cada versión: prefijos, expresiones,
facet y pipelines anidados de lookup/unionWith. Las capturas previas permanecen
inmutables. El código anterior, contra el corpus final, falla en dos acciones
(cuatro subfallos de motor) en MongoDB 8; no hay errores ni skips.

El [receipt](evidence/semantic-refactor/average-route-validation.json) conserva
fuentes y resultados: 76 acciones nativas verdes en 7/8/9, sin skips ni errores;
2.017 pruebas verdes en la matriz pública y 517 pruebas/591 subtests de regresión
compartida. Incluye spill abierto realmente en los tres dialectos, copias,
vistas de lista/slice, pickle de stages, reubicación y ejecución con provenance.
Los 32 fallos de la matriz anterior se corrigen sin tolerancias ni exclusiones.

El [benchmark focal](evidence/semantic-refactor/average-route-benchmark.md)
conserva hashes de resultados, harness y dataset frente al corte de suma. Es una
medición de tres repeticiones sobre 1.000 documentos, sin atribuirle garantías
generales de rendimiento. Lint ratchet, enlaces y manifiesto público verificados.

H26 se considera corregido en el alcance focal capturado. B03 sigue abierto por
H24/H27; B06 por sus consumidores y B04/B08 por gramática, fases y envelopes de
error. B10 y la distribución del candidato completo siguen pendientes.

### Octavo corte: promoción y escrituras estrictas (H27 corregido focalmente)

Las operaciones escalares comparten cálculo y conservan una opción explícita de
promoción para agregación; `$inc`/`$mul` permanecen estrictos. Se corrigen ancho y
promoción variádicos, división no decimal y resultados enteros de funciones
transcendentales con entrada int64. La conversión aritmética double → Decimal128
retiene quince cifras significativas y sus ceros finales. No se modifica la
conversión exacta de comparación ni la transición de suma/acumulación.

El overflow de escritura produce código 2 con diagnóstico BSON y prefijos
nativos por operación/versión. Se observa documento intacto tras el fallo,
recuperación, retorno, contadores y prefijos válidos de update-many y bulk
ordered/unordered, incluidos fallo de array, comandos raw y tipos de `_id`.
La presentación comparte el renderer existente de duplicate keys. El orden de
campos de error se ajusta sin cambiar precedencia ni copia defensiva.

Las sondas de no-op requieren dos reglas: asignación/pipeline/replacement usan
identidad BSON, y `$inc`/`$mul` igualdad aritmética del mismo tipo, con NaN como
escritura. Se conserva la cohorte/signo anterior en un no-op aritmético. Crear
un campo con `$mul` sigue calculando `0 * factor`. Memory y SQLite comparten el
criterio de replacement, conservando el efecto nativo del `_id` implícito. Esto
corrige pérdida de cambios de ancho/cohorte en pipeline y replacement; no cierra
broadcast ni la semántica completa de paths de B05.

**H34, independencia del comparador:** se retira su dependencia de
`canonical_document_id` productivo. El soporte de observación usa BSON de
PyMongo y conserva tipos, orden, cohortes y cero con signo. Controles negativos
y sustitución de helpers productivos comprueban la independencia. La comparación
más estricta descubre `$mod` con cero positivo incorrecto en lectura y escritura;
el núcleo usa ahora resto double sin formar el cociente. Las referencias
conservan la regla distinta 7 frente a 8/9. Un testigo adicional captura bordes
de signo, cocientes extremos, infinitos y NaN. No se relaja el comparador para
hacer pasar el código anterior ni se migran masivamente los formatos históricos.

El [receipt de aceptación focal](evidence/semantic-refactor/overflow-validation.json)
posee las capturas 7/8/9, fallos anteriores, resultados nativos y de superficies,
fuentes, helpers de observación y regresiones. El
[detalle de reglas y límites](evidence/semantic-refactor/overflow-source-notes.md)
explica las decisiones contextuales. Se conservan los receipts anteriores con
sus fuentes y alcances originales.

Cinco tests internos SQLite aún exigían claves auxiliares numéricas redondeadas.
El fallo se reprodujo con el código previo a este corte; se migran a la regla
H24 ya implementada: valores originales o delegación conservadora. No se
restaura el atajo ni se presenta esta migración como optimización nueva.

H27 queda corregido en su alcance focal. B03 sigue abierto por H24; B00 por
inventario completo, procedencia de helpers integrada en capturas y restantes
controles de observación. B04–B09 y los gates de distribución/globales B10 siguen
pendientes. La siguiente unidad prioritaria es gramática/bindings B04, tras
cerrar este corte; la ampliación exploratoria numérica no es prerrequisito.

## B04: gramática y preparación (abierto)

### Primer corte: familias de expresión y diagnósticos contextuales

Fecha: 31 de octubre de 2026. Se retira la delegación field-bound a consultas y
se separan las gramáticas en catálogo, preparación y evaluación. Las familias
migradas comparten aridades; se añaden `$not`/`$last` y se aceptan las formas
escalares booleanas e índices numéricos integrales nativos. First/last sobre
escalares y el segundo argumento no array de in dejan de aceptarse. Los
predicados de consulta fuera de expr y los objetos literales verdaderos se
conservan. No se cambian los defectos lógicos de compiladores consumidores.

Las capturas previas a la corrección observan código, familia, diagnóstico,
resultado, estado y recuperación en vacío/poblado. Cubren proyección, set,
expr, find, count y update, más facet/lookup/union. El
[receipt focal](evidence/semantic-refactor/expression-grammar-validation.json)
posee casos, hashes, fallos anteriores y resultados terminales contra 7/8/9,
ambos motores y cuatro superficies. El
[detalle de owners y fases](evidence/semantic-refactor/expression-grammar-source-notes.md)
explica las reglas sin trasladar errores de evaluación a una validación global.

Los errores conservan contexto: parser raíz de project, expresión anidada,
optimización y evaluación. Update sin filas no fuerza constantes de datos;
la agregación conserva su optimización nativa incluso en los testigos de ramas
inactivas. Las pruebas de namespace comprueban que solo se normaliza la colección
correcta del harness y que los textos de resultados no se alteran.

Cuatro tests de núcleo fijaban las extensiones o first escalar incompatibles.
Se migran con referencia real; los joins positivos usan expresiones binarias y
consulta ordinaria conservando sus resultados. No se mantienen aliases de los
helpers retirados. Catálogo actual JSON/Markdown y sus deltas se actualizan;
las fixtures históricas permanecen intactas.

El contraste adicional de not/first/last conserva la finalización de promedio
nativa: soporte de expresión y elegibilidad SBE siguen siendo conceptos
separados. Los testigos numéricos anteriores forman parte de la regresión nativa
del corte; no se deduce el resultado solo de que el operador esté en el catálogo.

H09–H13 quedan corregidos en el alcance focal capturado. Al cerrar este primer
corte, H14 conservaba pendientes de otras estructuras/acumuladores y H30 seguía
sin implementar. B04 sigue
abierto. B00 aún debe completar la relación ejecutable de H/caso/frontera y
procedencia de helpers; B08, consumidor externo y gates globales siguen
pendientes. La siguiente unidad B04 es `let` de comando y su separación de
bindings internos ya evaluados.

### Segundo corte: programas de let de comando (H30 corregido focalmente)

Preparación, contextos y adaptadores distinguen el programa de comando de los
bindings internos ya evaluados. El visitante de alcance existente detecta
dependencia de documento sin atravesar literales como sintaxis ni perder
CURRENT cerrado. Las definiciones se evalúan en orden; la validación estática
no ejecuta azar/reloj. Se preservan ramas inactivas, vacío y precedencia frente
a errores del cuerpo. No se añade otro IR, caché ni jerarquía de contextos.

Los callers de API, comandos y bulk delegan en ese owner conservando sus
fronteras. Un let inválido en update/delete bulk mantiene los inserts anteriores
y sus índices de error. Los raw count/distinct rechazan let; count_documents
sigue admitiéndolo por aggregate. El catálogo registra la disponibilidad de
NOW en bindings de lectura y el nombre IDL del rechazo distinct, con sus deltas
7/8/9; no desactiva NOW en el cuerpo ni en las escrituras.

El [receipt de aceptación](evidence/semantic-refactor/command-let-validation.json)
conserva corpus/capturas, copia anterior, parche reversible, hashes de fuentes,
reportes nativos y gates terminales. El contraste con MongoDB 7/8/9 incluye
todos los testigos semánticos anteriores y ambos motores. Pasan la matriz de
superficies, núcleo/catálogos, regresión de errores/escrituras/API y manifiesto
público. Una expectativa antigua de error lookup para let de comando se migra
al código 9 observado; snapshots históricos y observador de writes permanecen
intactos. Las exportaciones vigentes y sus deltas sí se actualizan.

Las pruebas de clone directo usan una petición nueva nativa equivalente como
referencia. La observación CommandListener separada demuestra que PyMongo 4.19
omite let en clone: no se atribuye ese comportamiento al servidor ni se oculta
con una normalización. Los tests previos conservan la identidad del contexto
en rewind sin programa. El
[detalle de owners, fases y límites](evidence/semantic-refactor/command-let-source-notes.md)
incluye una caracterización adicional de NOW en ramas inactivas de MongoDB 9.

H30 queda corregido en el alcance focal capturado. B04 sigue abierto por H14:
otras estructuras y acumuladores, incluido input obligatorio de getField.
H35 conserva fases completas de cursor/getMore, reloj nativo de clone/rewind,
otros perfiles SDK y el TypeError local de distinct(let) frente al 40415 del
servidor. El censo ejecutable H/caso/frontera de B00, H24 restante y B05–B10
conservan sus pendientes. La siguiente unidad es H14 dentro de B04; no exige
volver a ampliar las familias numéricas ya aceptadas.

### Tercer corte: estructuras H14 y revisión transversal (correcciones verificadas)

El [reproductor nativo](evidence/semantic-refactor/characterize_object_grammar.py)
captura getField/setField/unsetField y acumuladores unarios en vacío/poblado,
proyección y escritura cuando corresponde. Las
[observaciones de MongoDB 9](evidence/semantic-refactor/object-grammar-characterization-9.json)
conservan buildInfo/FCV, entradas, errores, estado posterior y recuperación.
Son caracterización previa, no aceptación de H14. La revisión posterior añade
referencias 7/8 e integra los casos en `semantic_object_grammar_cases.py`, mediante
el registro/capturador/replay existentes. Sus capturas canónicas poseen entradas,
efectos y recuperación; no sustituyen las observaciones iniciales.

El [contraste focal previo](evidence/semantic-refactor/object-grammar-before-focal-9.json)
conserva hashes de fuentes y cuatro divergencias en Memory y SQLite: getField
sin input se acepta sin filas; input escalar produce null en vez de ausencia;
su forma abreviada ignora CURRENT reasignado; group sum admite un operando
array sintáctico que el servidor rechaza con 40237, incluso de un elemento.
El control de array explícito mediante literal coincide. Esta comparación
observa resultado/tipo BSON/código/estado, sin acreditar envelopes ni fases
completas. Los nuevos testigos de getField amplían los pendientes H02/H18;
los cierres focales anteriores conservan su evidencia y alcance original.

La preparación y evaluación delegan en los parsers de la familia. La forma
abreviada getField usa CURRENT; el input escalar produce ausencia. Se validan
campos requeridos, parámetros desconocidos y nombres constantes, después de
parsear las expresiones hijas. Los argumentos se recorren en orden BSON: los
pares de defectos conservan qué error prevalece. Match conserva su presentación
de variable indefinida, sin añadir el prefijo de project.
El delta de nombres dinámicos getField vive en
el catálogo de dialectos. Los errores de tipo de datos no se adelantan al parseo.

Group/bucket rechazan arrays sintácticos como operandos de los ocho acumuladores
unarios capturados; ventanas los admiten. El adaptador de ventana declara esa
distinción. Bucket crea estado solo al recibir documentos. Este ajuste de
ocupación no cierra validación de límites/groupBy ni H32 completo.

El corpus amplía la conservación H35 a find_one, distinct, count_documents y
los contextos anteriores. El cursor resuelve diagnósticos con el comando que
lo usa: distinct conserva su presentación aun reutilizando semántica de find.
Los namespaces se normalizan únicamente en el prefijo incidental que identifica
la colección concreta; los mensajes o datos ajenos permanecen observables.

Las referencias finales 7/8/9 ejecutan 149 acciones semánticas en cada versión,
ambos motores y sin skips/errores. El código previo falla en 22 subcasos de motor
del corpus final. La matriz pública focal y la suite completa del candidato
estable pasan. El [receipt de revisión](evidence/semantic-refactor/review-validation.json)
conserva resultados, skips, comandos, hashes y límites. Se cierra este corte
de correcciones; B04 y el plan completo continúan abiertos.

### Revisión bloque por bloque solicitada el 31 de octubre

La revisión se realiza sobre el árbol sin commit que parte de `b38ef967`.
[Fuentes congeladas](evidence/semantic-refactor/review-source-freeze.json) y
[parche del corte](evidence/semantic-refactor/review-change.patch) identifican
las correcciones. Los receipts anteriores conservan su base y alcance; no se
reescriben como si correspondieran al nuevo árbol.

| Bloque | Revisión y correcciones de este corte | Límite de aceptación |
| --- | --- | --- |
| B00 | Nuevo corpus comparte registro, observador independiente y capturador. Referencias nativas previas y testigos de error/efectos se conservan. | Relación completa H/caso/frontera y procedencia integrada de helpers siguen pendientes. |
| B01 | Retirada del compilador conservada; las reglas nuevas siguen en el ejecutor común. | La regresión nativa incluye sus testigos y el gate global estable pasa; no amplía la aceptación histórica del benchmark. |
| B02 | GetField abreviado respeta CURRENT y su input escalar conserva ausencia. | Cierre focal nuevo de H02/H18 verificado por el gate final del corte. |
| B03 | Las claves numéricas iguales se codifican igual para spill, sin contexto ambiental ni cambios de identidad física. La generación común del registro invalida también la estrategia de promedio al retirar un handler de expresión. | Ventanas adicionales coinciden con la sonda nativa; H24 completo sigue abierto. |
| B04 | Parsers compartidos y pares de errores. Rewind conserva la operación preparada y planning issues, renovando semántica física/deadline. Los overrides conservan argumentos opacos y una generación común invalida preparaciones tras cambios de expresión o stage. | Otras estructuras/operadores H14 y todo B04 siguen abiertos. |
| B05 | Se conserva el cambio de identidad de asignación y su separación de igualdad aritmética. | Paths/broadcast/proyección siguen pendientes de implementación; no se acreditan por el cambio del helper. |
| B06 | Unarios distinguibles por contexto, buckets ocupados y claves de spill corregidos con testigos. | Distinct semántico, lookup, límites de buckets y demás consumidores pendientes. |
| B07 | La salida Decimal128 y los sentinelas conservan sus regresiones; no se añade serialización permisiva para absorber los nuevos errores. | Frontera BSON estricta sigue pendiente. |
| B08 | Diagnósticos por operación en find/find_one/count_documents/distinct, con contexto explícito en los cursores usados por distinct. | Aggregate de base, fases completas/getMore y restantes deltas H35 pendientes. |
| B09 | Se actualizan contrato, catálogo vigente y procedencia del inventario static-admin wire. Su revisión se identifica por hash del conjunto de fuentes, no por un commit histórico con bytes nuevos. | Migración externa del testkit y resto H36 no realizados. |
| B10 | La suite inicial descubrió fallos fuera de los corpus focales. Se aíslan los temporales por test en work-control y cursores, manteniendo las aserciones de creación/limpieza y los fallos inyectados. | Suite completa estable verde, con skips documentados; wheel/sdist, cobertura y matrices Python/SDK todavía pendientes. |

El [primer recorrido global](evidence/semantic-refactor/review-initial-full.log)
es diagnóstico: hubo cambios concurrentes de catálogo durante su ejecución y
no acredita un candidato inmutable. Los defectos de spill/rewind y la
procedencia static-admin se reprodujeron por separado y sus regresiones focales
pasan. Las diferencias de temporales provenían de observar archivos de otros
procesos; las pruebas ahora usan directorios propios y siguen detectando fugas
de los recursos que crean. El recorrido final usa además TMPDIR aislado.

El registro de extensiones comparte una única generación entre expresiones y
stages. El [testigo anterior](evidence/semantic-refactor/review-expression-registry-before.json)
prepara un grupo con un override de `$not`, lo retira y obtiene promedio `0.0`
en vez del `1/3` capturado de MongoDB 8. La regresión comprueba esa referencia
independiente después de retirar el handler. Los otros tres casos conservan la
propiedad del handler sobre argumentos opacos de getField/setField/unsetField;
detectaron y corrigieron una regresión introducida durante esta revisión.
El mismo recorrido global exigió conservar la identidad de operación al hacer
rewind, renovando su semántica física y deadline. Estas correcciones amplían
la garantía focal H26/H30 sin cerrar los demás consumidores o fases.

## Hallazgos

| Hallazgo | Bloque | Estado |
| --- | --- | --- |
| H01 | B02 | Corregido en B02: capturas 7/8/9 y cuatro superficies; receipt final focal. |
| H02 | B02 | Referencias/ausencia B02 conservadas. Input escalar de getField corregido en tercer corte B04, con referencia 7/8/9; gate global final del corte verde. |
| H03 | B02 | Corregido en B02: capturas 7/8/9 y cuatro superficies; receipt final focal. |
| H04 | B02 | Corregido: composición, reduce/ifNull y reemplazo verificados en 7/8/9 y cuatro superficies. |
| H05 | B05 | Escrituras planas B02 e identidad numérica de asignación/pipeline/replacement verificadas; paths/broadcast y cierre B05 pendientes. |
| H06 | B02 | Copia/pickle, arrays/objetos, salida y escrituras verificadas; frontera BSON estricta B07 pendiente. |
| H07 | B02 | Corregido en B02: capturas 7/8/9 y cuatro superficies; receipt final focal. |
| H08 | B06 | Pendiente. |
| H09 | B04 | Corregido focalmente: operadores de consulta rechazados como expresión en vacío/poblado, subpipelines y rutas públicas; receipt B04. |
| H10 | B04 | Corregido focalmente: sin reinterpretación field-bound, literales verdaderos y operandos escalares; aridad y subpipelines verificados. |
| H11 | B04 | Corregido focalmente: not directo/unario y last; reglas de arrays/null/ausencia y rechazo de escalares capturadas en 7/8/9. |
| H12 | B04 | Corregido focalmente: and/or escalar e índices double/decimal/int64 integrales; rechazos de bool/fracciones/rango verificados. |
| H13 | B04 | Corregido focalmente: in con null/ausente/no array falla con 40081 y diagnóstico por contexto; receipt B04. |
| H14 | B04 | Familias previas conservadas. Estructuras de get/set/unsetField, expresiones hijas y ocho unarios de group/bucket corregidos con corpus 7/8/9; ventanas mantienen arrays válidos. Gate global final verde; estructuras restantes pendientes. |
| H15 | B08 | Pendiente. |
| H16 | B07 | Pendiente. |
| H17 | B09 | Pendiente. |
| H18 | B02 | CURRENT B02/H30 conservado. GetField abreviado corregido, incluida dependencia implícita en let de comando; referencias 7/8/9 y matriz focal verdes, gate global del corte verde. |
| H19 | B09 | Pendiente. |
| H20 | B09 | Pendiente. |
| H21 | B01 | Corregido: compilador/caché retirados; capturas 7/8/9, cuatro superficies y benchmark verificados (receipt B01). |
| H22 | B01 | Corregido: compilador/caché retirados; capturas 7/8/9, cuatro superficies y benchmark verificados (receipt B01). |
| H23 | B01 | Corregido: compilador/caché retirados; capturas 7/8/9, cuatro superficies y benchmark verificados (receipt B01). |
| H24 | B03 | Comparación e índices focales conservados. Codificación de claves de igualdad en spill corregida para cero con signo/cohortes, con spill físico contrastado con referencia nativa. Gate global nuevo verde; límites/no finitos y unique multikey/partial/collation pendientes. |
| H25 | B06 | Pendiente. |
| H26 | B03 | Corregido focalmente: aritmética/funciones Decimal128, suma/promedio y materialización; ampliado con quince cifras/cohortes de conversión y tipos no decimales. Finalización por contexto conservada en vistas físicas/provenance/spill e invalidada tras cambios del registro de expresiones. Receipts séptimo/octavo y de revisión; suite global vigente verde, distribución y matriz B10 pendientes. |
| H27 | B03 | Corregido focalmente: promoción de agregación, errores/atomicidad de `$inc`/`$mul`, efectos parciales y recuperación; referencias 7/8/9 y cuatro superficies en receipt octavo. B03 completo pendiente H24. |
| H28 | B05 | Pendiente. |
| H29 | B05 | Pendiente. |
| H30 | B04 | Corregido focalmente: programa ordenado/cerrado, literales y CURRENT; lecturas/escrituras/comandos/bulk, vacío, efectos y recuperación capturados 7/8/9. Receipt segundo corte B04; fases completas H35 pendientes. |
| H31 | B06 | Pendiente. |
| H32 | B06 | Ocupación de buckets corregida en tercer corte B04/B06; testigos vacíos/poblados nativos y matriz focal verdes. Validación completa de límites/groupBy/default y cierre B06 pendientes. |
| H33 | B03 | Corregido: regla de documentos compartida, ownership transparente y comparador duplicado retirado; cinco acciones nativas 7/8/9, cuatro superficies y overrides verificados. |
| H34 | B00 | Comparador desacoplado de identidad productiva, controles BSON independientes y fallo de `$mod` expuesto/corregido; inventario, procedencia y cobertura completa de oráculos pendientes. |
| H35 | B08 | Overflow/no-op, gramática y let B04 verificados en rutas directas/wire con errores/efectos. Pendientes fases completas de cursor/getMore/reloj, otros perfiles SDK, distinct(let) directo y alias partialFilterExpression de create_index frente a createIndexes observado en H33. La omisión de let en clone PyMongo 4.19 queda observada por separado. |
| H36 | B09 | Contratos/catálogo y procedencia static-admin vigentes actualizados en revisión. Inventario restante y consumidor externo pendientes. |

## Ampliación: consultas, validadores y prefiltros

El [análisis y plan H37–H47](predicate-authorities-analysis-and-plan.md) añade
los cortes C00–C06 sobre la base `79529a2`, después de los commits del trabajo
anterior. Posee sus causas, alcance y dependencias; este registro posee su estado.

Estado: **implementación en curso**. La evidencia de caracterización previa
conserva 65 estímulos de solo lectura contra MongoDB 9.0.2,
ocho escenarios de rutas públicas en ambos motores privados y 38 pruebas
existentes verdes. No es aceptación de los hallazgos nuevos ni captura nativa
7/8. Se ha recibido un informe; el segundo anunciado sigue pendiente.

Los receipts y cierres focales anteriores conservan su identidad y no se reescriben; los
pendientes B00–B10 no quedan absorbidos ni cerrados por esta ampliación.

El plan incorpora una revisión de simplicidad sobre `1f9c794`: C00/C06
acompañan cada entrega y C02/C03 se integran por familia. Se mantienen los
hallazgos, la matriz de garantías y sus aceptaciones pendientes. Esta revisión
solo cambia la organización del trabajo; no añade resultados de ejecución.

### C01: matching común y especializaciones SQL

Base del incremento: `c835181`. Mecanismo: reutilización de `QueryNode`,
`QueryEngine.match_plan`, clasificación BSON y comparación numérica existentes;
composición booleana generada y adaptaciones de SQL/índices sobre esas reglas.
Se retiran las implementaciones hoja del generador y la comparación SQL por
orden entre familias. No se añade otro planner ni una caché de valores.

El matcher restringe familias antes de comparar, conserva límites inclusivos
con null/ausencia, MinKey/MaxKey, NaN y la distinción Code/Code-with-scope.
La plantilla cacheada resuelve el método del owner al ejecutarse y conserva
valores por consulta; no retiene instrumentación temporal. Los fallos internos
dejan de convertirse en `False`.

La integración corrige además la implicación de límites inclusivos/estrictos en
índices parciales. El testigo SQLite creado con las fuentes anteriores verifica
reapertura, inserción válida, rechazo de un duplicado real y estado posterior;
en ese testigo no hace falta migración. La selección de updates, `$pull` y
array filters comparte la regla nativa capturada.

Las capturas ampliadas demuestran también que `$ne: null` y `$nin: [null]`
excluyen ausencia en **7/8/9**. Se corrigen matcher, SQL, dos expectativas
locales anteriores y `DIALECTS.md`; el delta de `UNDEFINED` explícito permanece
separado. Esta corrección elimina otro error compartido por ambos evaluadores.

El [receipt C01](evidence/predicate-authorities/query-matching-validation.json)
posee identidad de fuentes, corpus, resultados y límites. Conserva once acciones
nativas por versión en ambos motores, el contraste rojo de la base anterior,
la matriz pública y las regresiones causales. Los benchmarks de predicados
reutilizan workloads existentes y se conservan como tendencia, sin umbral CPU.
La prueba de contexto decimal compara el mapping de sus nueve flags públicos:
`SignalDict` puede diferir de su copia por subflags internos de InvalidOperation
aunque los flags observables no hayan cambiado. No se limpian flags heredados.

H37–H39 quedan corregidos en el alcance C01 capturado; los gates globales de
C06 siguen pendientes. **H40–H47 / C02–C05 siguen abiertos.** El siguiente
incremento es igualdad/preparación de schema y su integración de escritura
C02/C03, conservando la aceptación específica del dialecto de `moderate`.


### C02/C03: schema preparado y validación de escritura coherente

Base: `4bad1b7`. H40–H44 corregidos en el corpus focal, con preparación,
decisión y diagnóstico compartidos por `$jsonSchema` y validator de colección.
Enum y uniqueItems comparten igualdad recursiva propia de schema. Los límites
usan comparación exacta; multipleOf mantiene su conversión aritmética decimal.
Se ejecutan exclusividad y additionalProperties como schema; las restricciones
fuera del subset dejan de aceptarse silenciosamente. Los patrones se preparan
antes de filas. El resultado conserva detalles, sin reconstrucción ni segunda
validación al rechazar una escritura.

`moderate` conserva el dialecto del original y del candidato. El testigo físico
BSON undefined pasa por las cuatro superficies; el puente wire ya no lo colapsa
a null antes de aplicar la política. El validator MQL usa la collation de la
colección y enum sigue siendo binario. La preparación por lote se reutiliza sin
una caché global. Las opciones SQLite conservan BSON y orden de objetos; collMod
cambia únicamente validator/nivel/acción bajo el ownership existente del motor.
Se prueban reapertura, cambios rechazados sin efectos y rollback/commit locales.

La prueba nativa de `$merge` descubre un delta adicional: un resultado idéntico
se valida en 9.0.2 y se omite en 7.0.43/8.0.32; ambos modos merge/replace quedan
capturados. `$out` no tiene caller productivo en esta base. El requisito de
integración por callers reales conserva su rechazo explícito y sin efectos;
no se implementa un stage nuevo ni se declara paridad inexistente.

El [receipt C02/C03](evidence/predicate-authorities/schema-validation.json)
conserva las diez acciones por versión, el contraste previo y las identidades.
Regresión: 2.814 pruebas, 936 subpruebas, un skip opcional de ICU. La matriz
schema cubre sync/async y wire-sync/wire-async en Memory/SQLite y dialectos 7/8/9.
C01 y C02/C03 quedan corregidos focalmente. H45–H47 / C04–C05 y los gates globales
C06 siguen pendientes; no se cierran B00–B10 por esta ampliación.


### C04: conservación de candidatos y traversal MQL

H45/H46 se corrigen reutilizando CandidateFilterPlan/Result y el matcher MQL.
Las claves escalares numéricas son exactas; las materializaciones marcan paths
no representados y declinan arrays/rutas punteadas/ausencia-null. Se retira el
matcher booleano paralelo; el residual comparte dialecto, collation y contexto.
SQLite no adelanta top-k si el downstream necesita residual. Search-v1 mantiene
enteros en rangos/facets y no confunde postings textuales con field presence.

El oráculo nativo descubrió además errores compartidos de traversal: posición
numérica versus campo numérico, arrays anidados, ramas missing, elemMatch
atómico y all/size/bits. Sus consumidores comparten las mismas hojas; el helper
observable de índices conserva su contrato. Los límites null de un índice
parcial usan MISSING como marcador de ausencia de límite.

El corpus mantenido `semantic_search_eligibility_cases.py` tiene cinco acciones,
incluidos cien estímulos de rutas, lookup físico, escrituras, hint/índice parcial
y mutaciones del estado materializado. Las rutas Search verifican ventanas,
compound, collectors y merge en sync/async/wire y ambos motores. La propiedad
usa los IDs nativos como verdad y prueba T⊆C⊆U y exact⇒C=T. La fixture SQLite
previa comprueba lectura, unicidad, salida de membresía y reapertura.

El reproducer `characterize_routes.py` es evidencia histórica del análisis y
solo se ejecuta en su base Git; el corpus mantenido sustituye su import del
helper retirado. No se modifica esa evidencia histórica para simular vigencia.

El receipt `evidence/predicate-authorities/search-validation.json` registra el
corte, nativos, rojo previo, regresión y tendencias de coste. H47/C05 y C06
siguen abiertos. Este corte no declara paridad Atlas ni cierra B00–B10.

C04: regresión 3.399 passed + 936 subtests, ICU opcional omitido; nativos
3×5 acciones con ambos motores sin fallos/omisiones. Tendencias secuenciales:
Memory 0,976–1,209×; vector SQLite 0,997–1,047×. Los compound should de SQLite
con exists llegan a 6,370× al retirar una prueba de exactitud inválida que
adelantaba poda de score. Se conserva la garantía semántica; recuperar ese
coste exige demostrar presencia real, no inferirla de postings de texto.

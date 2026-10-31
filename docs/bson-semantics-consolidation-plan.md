# Consolidación BSON: contraste de informes y plan integrado

Fecha: 31 de octubre de 2026. Base de runtime inspeccionada:
`734c57f4026c602491bdc6f62aa5dc44369dfcb4`.

Revisión 2 del plan sobre `57a9767`, contrastada con el tercer informe recibido.
El runtime permanece idéntico. La evidencia inicial conserva su identidad;
esta revisión añade trazas de representación, distinct, eq/in y fronteras TTL.

Revisión 3 sobre `e04e285`, contrastada con la autocrítica posterior. Conserva
los IDs y garantías de revisión 2; añade parser de literales DBRef, orden en
wire y selección SQL de bytes. No reinterpreta el retraimiento de una crítica
como corrección de runtime ni como motivo para retirar testigos válidos.

Revisión 4: nueva lectura de la misma autocrítica (hash idéntico al input de
revisión 3), contrastada con HEAD `93b9b34` y el árbol de trabajo del 31 de
octubre. La base histórica de las secciones 1–3 no describe automáticamente
el runtime actual. Esta revisión precisa aceptación y dependencias; no agrega
otra implementación ni altera los cambios de fechas en curso.

Revisión 5: contraste del mismo adjunto con HEAD `f69759a` y cambios Binary
sin commitear, identificados en el receipt. La identidad del texto coincide con
las revisiones 3/4; no se abren hallazgos nuevos por repetirlo. Se actualizan el
estado de fechas y las dependencias entre Binary, gramática y transporte.

Revisión 6: mismo adjunto (SHA-256 verificado), contrastado con HEAD
`ef604c7`. D03a/D03b ya tienen aceptación funcional propia; la descripción
histórica no vuelve a abrirlos. Esta revisión actualiza el orden restante y
precisa las obligaciones de D01b con sondas nuevas contra MongoDB 7/8/9.

Revisión 7: mismo adjunto, contrastado con HEAD `375382a` y el trabajo de
eventos sin commitear. La [evidencia de revisión 7](evidence/bson-consolidation-review/review7/receipt.json)
identifica ese árbol, repite regresiones y caracteriza cinco efectos de escritura
en los tres servidores temporales. El orden vigente está al final de la sección
5; las revisiones anteriores conservan su alcance histórico.

**Estado: contrato autorizado; implementación en curso.** Los cortes y gates
aceptados se registran en el documento de implementación; no se acredita aquí
el cierre completo del incremento. Este documento posee H48–H54 y su integración con B00–B10 y
C00–C06. No reabre H37–H47 ni sustituye sus receipts. La implementación y los
gates históricos siguen en el [registro vigente](semantic-parity-implementation.md).
El [plan histórico](plan-semantic-parity-refactor.md) conserva las obligaciones
generales; el [incremento C](predicate-authorities-analysis-and-plan.md) conserva
su caracterización inicial, cuando faltaba el segundo informe.

El usuario ha autorizado la implementación completa del plan refinado. El
[registro de implementación](bson-consolidation-implementation.md) conserva
el progreso por bloque y sus pendientes; las caracterizaciones de este plan
no se convierten en aceptación de runtime por esa autorización.

## 1. Evidencia y alcance del contraste

Se contrastaron los dos textos adjuntos, las propuestas previas de este chat,
los consumidores actuales y fuentes de MongoDB **r8.0.0**, fijadas por etiqueta,
URL y hash. No se utiliza master como contrato ni se copia código del servidor.
Las fuentes externas sirven para explicar mecanismos; la aceptación observable
depende de diferenciales por versión, no de la similitud de arquitecturas.

La [evidencia de caracterización](evidence/bson-consolidation-review/receipt.json)
incluye fuentes, hashes de los informes, scripts, resultados y límites.
Se ejecutaron sondas con PyMongo contra un **MongoDB 9.0.2, FCV 9.0, replica set
de un miembro desechable**, y contra Memory/SQLite con dialectos locales 7/8/9.
No se modificó la instancia habitual. El servidor temporal quedó detenido.

Las sondas son caracterización, no la aceptación final de un arreglo: no hay
capturas nativas 7/8 nuevas, matriz wire/async nueva ni golden del runner nuevo.
Los valores complejos del JSON de diagnóstico usan repr; no sustituyen al
observador BSON independiente del runner. Las 167 pruebas focales existentes
pasaron sin cambios: esto conserva regresiones, pero no cubre los nuevos casos.

La [caracterización de revisión 2](evidence/bson-consolidation-review/refinement/receipt.json)
añade un MongoDB **9.0.2, FCV 9.0, standalone desechable**, trazas de matcher en
los seis pares locales motor/dialecto y 45 comprobaciones TTL locales. No
repite ni amplía la aceptación de las 167 pruebas anteriores. Esta revisión
solo cambia documentación/evidencia; los defectos continúan presentes.

La [caracterización de revisión 3](evidence/bson-consolidation-review/review3/receipt.json)
contrasta ocho formas de filtro en API directa y PyMongo wire, ambos motores y
los tres dialectos locales, con MongoDB 9.0.2 / FCV 9.0 standalone. Incluye
sentencias SQL realmente ejecutadas y conteo de llamadas al matcher, no solo
el SQL que un traductor podría producir. Las 14 fuentes iniciales se volvieron
a descargar de sus URLs r8.0.0: todos los hashes coinciden. Se añade el parser
del servidor con URL/hash propios, sin alterar sources.json ni receipts previos.
Las ejecuciones son caracterización; no hubo nuevos fixes ni suite de aceptación.

La [revisión actual](evidence/bson-consolidation-review/review4/receipt.json)
repite ocho formas de filtro en los seis pares directos motor/dialecto y nueve
trazas SQL, sobre el árbol de trabajo identificado en su receipt. Bytes eq/in
y Binary eq ya seleccionan IDs 1/2 con fallback y tres llamadas al matcher;
DBRef implícito y las vistas de igualdad siguen divergiendo. Pasan las tres
propiedades Binary existentes. No se repitieron nativos, wire, watch ni TTL;
sus referencias anteriores mantienen su alcance original. Se releyeron las
fuentes versionadas del parser; no se atribuye a esta revisión una nueva
descarga y comprobación de los 14 hashes históricos.

La [revisión 5](evidence/bson-consolidation-review/review5/receipt.json) repite
48 observaciones directas y nueve trazas SQL en los seis pares motor/dialecto:
eq/in binario conserva IDs 1/2 mediante fallback; el parser y la igualdad DBRef
siguen fallando. Las tres propiedades Binary actuales pasan, sin acreditar
su ampliación pendiente. Se verifican otra vez los 14 hashes r8.0.0: coinciden.

Quince sondas de solo lectura con `$documents` contra los servidores temporales
7.0.43/8.0.32/9.0.2, FCV correspondiente, confirman orden Binary subtipo 2,
rechazo de literales cortos 3/5 con código 9 y ausencia de coincidencia con
orden DBRef inverso. No equivalen a filtros de colección, escrituras, watch ni
a la matriz completa del replay. El decoder raw local convierte el documento
reordenado en DBRef y no conserva sus bytes al recodificar; para Binary 3 corto
lanza InvalidBSON antes de la preparación de expresiones. No se repite wire
ni se declara cerrado ese defecto con una prueba del decoder.

### Estado actual frente a la caracterización inicial

El registro de implementación sigue siendo el owner de gates y progreso. Esta
tabla solo evita volver a planificar como nuevos defectos ya corregidos:

| Obligación | Disposición al revisar el informe |
| --- | --- |
| H48 rango/partes Binary, SDK y eq/in SQL | D03a cerrado funcionalmente en `542bce9`; vistas SDK/local/owned, distinct, longitudes y fallback binario tienen evidencia en el registro. La sonda de revisión 6 conserva IDs/subtipos y observa ambos lados del matcher. No reintroducir pushdown por simplificación. |
| H54 identidad física y Code | D01a entregado en `93b9b34`, con codec corregido en `19ed3fa`. Los incrementos de D01b entregan guardas, estado/índices/múltiples y contenido/entrega; el [registro](bson-consolidation-implementation.md) acredita su cierre funcional con entradas permanentes sync/async. D05 conserva matrices y cierre integrado. No repetir la extracción del helper ni reabrir sus contraejemplos resueltos. |
| H49 fechas | D02 cerrado funcionalmente en `0979fd8` y `f69759a`, con coordenadas, migración TTL y entrega de eventos en el registro. D05 conserva integración/matrices. |
| H50 parser/vistas/wire/claves | `542bce9` entrega gramática y preservación raw compartidas, con testigos nativos/directos/wire. D06a/D06b conservan aceptación restante; D06c conserva consumidores y claves. No volver a describir el literal DBRef básico como defecto presente. |
| H51 tipos SQL | D03b cerrado funcionalmente en `ef604c7`: tabla común de argumentos y predicados por familia, arrays y SQL ejecutado. D05 conserva gates integrados. |
| H52 y H53 | Watch/contexto y disposición de observabilidad siguen abiertos. La corrección de publicación TTL no cierra preparación ni contenido de eventos de actualización. |

## 2. Qué aceptar y qué corregir de las críticas

| Afirmación o propuesta | Contraste y disposición |
| --- | --- |
| El ejemplo anterior bytes/Binary era incorrecto | Necesita especificar representación. `bytes` frente al Binary local crudo compara igual; frente al SDK y al interno no. La afirmación anterior procedía del caso normalizado, pero no explicitaba esa frontera. La API pública confirma un fallo mayor. |
| Cualquier operando Binary falla | Confirmado para igualdad, desigualdad, rango y `$expr` del corpus reproducido, en los seis pares motor/dialecto. No extrapolar a todo operador existente sin testigo. |
| SQLite da un resultado diferente para filtro bytes | Confirmado en dialectos 7/8: devuelve solo el documento almacenado como bytes. En 9 devuelve los tres, igual que Memory, porque usa fallback Python. Registrar dialecto y estrategia, no solo motor. |
| `$type` de arrays está incompleto en SQLite | Confirmado por API pública para binData, objectId y date en dialectos 7/8. En 9 los tres pasan. La guarda `_dialect_requires_python_fallback` explica la diferencia; falta demostrar el camino SQL y su equivalente al implementar. |
| DBRef tiene consumidores sin vista documental | Confirmado: igualdad selecciona solo una de las dos representaciones y `$group` falla. MongoDB selecciona ambas y forma un grupo de tamaño dos. `virtual_indexes` también consume clasificación y debe figurar en el censo. |
| Fechas pierden aproximadamente 0,8 % | El defecto es confirmado; la frecuencia depende de la muestra. Semilla 20261031, 20.000 fechas uniformes por milisegundo entre 1900 y 2100: 148 diferencias, 0,74 %, frente a aritmética entera independiente. No es una tasa universal ni una captura nativa poblacional. |
| TTL usa la misma conversión flotante | Confirmado en cutoff y expiración indexada. Sin embargo hay residual exacto antes de borrar; la sonda de frontera conserva el documento en ambos motores. Incluir el consumidor sin declarar borrado anticipado demostrado. |
| Todo lo citado está abierto | Incorrecto. Bool/números, Code como string, schema/DBRef, moderate sin dialecto y fallback de schema ya tienen correcciones acotadas. Se conservan como regresiones. Los dos ejemplos originales de floats/update pipeline también siguen corregidos. |
| Varias obligaciones y propiedades ya existían | Correcto. B03 ya exige owner/coherencia igualdad-clave; B04/B08 preparación/contexto; Search ya posee conservación de candidatos. Ampliar dominio y fronteras de las pruebas existentes, sin crear otro sistema de propiedades. |
| Hay que retirar dos nombres antes de extraer clasificación | No es una garantía válida. `_aggregation_key` sirve también de guarda; el lookup hashable tiene un dominio deliberadamente restringido. Retirar autoridades duplicadas, no borrar adaptadores necesarios para alcanzar una cuota de nombres. |
| MongoDB usa comparadores que acoplan igualdad y hash | Confirmado. Adoptar esa responsabilidad conjunta; no implica que una clase universal con tres modos represente todos los contratos de mongoeco. |
| MongoDB tiene una sola comparación y dos bits para todo | Excesivo. BSONElement y Value tienen implementaciones distintas; la identidad binaria no es el comparador semántico. Los bits no modelan por sí solos arrays MQL, missing/null, elegibilidad Search, claves persistidas y modificación física. |
| TypeBits permite obtener toda identidad física desde una clave semántica | No justificado. TypeBits sirve para restaurar tipos en KeyString. La clave semántica puede perder escala Decimal128, signo/payload y orden de campos según su política. No usarla como sustituto de identidad de escritura. |
| Tipo intrínseco para familias ambiguas | Adoptar normalización y clasificación común, preservando codec_options y UUID representation. No envolver cada escalar ni cambiar tags almacenados para poder corregir Binary. |
| Todas las firmas internas deben perder defaults inmediatamente | Adoptar contexto explícito en rutas preparadas alcanzadas. No cambiar firmas SPI v2 ni defaults públicos/compatibles en masa; un default existente no demuestra una fuga en un caller que pasa contexto. |
| Change streams debe esperar al cierre completo de B04 | No. Estaba fuera del programa histórico, pero existe un contrato local ADR-006. Ahora posee caracterización nativa propia y alcance separado H52; puede usar el preparador existente sin esperar todos los pendientes de B04. |
| Compilar schema al matcher simplifica necesariamente | Posible evolución, no conclusión demostrada. Introduce nodos con semántica propia y diagnósticos anotados. No es requisito de los defectos observados ni autoriza traducir validators a SQL. Evaluar en un bloque opcional independiente. |
| H48 atribuye la representación interna al almacenamiento | Aceptado el matiz del tercer informe. La traza pública muestra operandos `_InternalBsonBinary` de rango 100 frente a documentos decodificados con bytes/Binary local de rango 6. Un helper con ambos lados internos no representa ese camino productivo. |
| Distinct pierde el subtipo 5 después de materializar | Confirmado: devuelve bytes y Binary SDK 0, mientras MongoDB devuelve bytes y Binary SDK 5. Igual número de resultados no implica igualdad de contenido BSON. Añadirlo al primer corte H48, no aplazarlo a la consolidación D06. |
| D01/D03 son ambiguos y dependen del chat | Aceptado. Los bloques se definen aquí; D01a/D01b y D03a/D03b identifican entregas únicas. La correspondencia con la revisión 1 queda en la sección 5. |
| Falta igualdad/claves desde el primer corte | Aceptado. D00 exige propiedades generadas y testigos deterministas antes de modificar H48; D03a no se acepta sin ellas. La clase de comparador sigue siendo opcional, la garantía no. |
| TTL solo tenía control de borrado anticipado | Aceptado. Revisión 2 comprueba −1/0/+1 ms, fechas positivas/negativas, creación del índice y reapertura SQLite; los 45 casos coinciden. No demuestra toda la frontera ni la futura mezcla de auxiliares antiguos con un cutoff exacto. |
| La autocrítica retira objeciones sobre la causa de H48 y las familias de prueba | Correcto: el rango del operando interno ya era parte de la causa original; eq/in y distinct ya figuraban como consumidores. Se conservan las precisiones y testigos concretos de revisión 2. La propiedad D00, clases de error y cortes únicos ya están incorporados; no crean otra fase ni otro ID. |
| Clasificar el interno no sirve para el primer subcaso | Matizar: igualar rangos sí permite que interno/local entren en la rama Binary, pues hay herencia. Arregla ese rechazo concreto, pero no completa H48: bytes y SDK, longitud, distinct y SQL tienen mecanismos adicionales. |
| Falta el parser de literales DBRef | Confirmado. La forma implícita con $ref/$id falla por API directa y funciona en MongoDB. También difiere el caso con $ref no string, y un documento con primer campo ordinario y otro campo con $. Añadir gramática por contexto, no solo vistas de valores. |
| Directa y wire probablemente difieren | Confirmado para los seis pares locales. Wire acepta el DBRef válido al decodificarlo como objeto SDK; además reordena el filtro $id/$ref y produce falsos positivos frente al servidor. No considerar suficiente que wire acepte la forma válida. |
| El comentario de sqlite_query garantiza fallback global para bytes | El comentario está en ordenación y su guarda se aplica a sort; no demuestra un contrato de fallback para igualdad. El defecto de igualdad/in se confirma con SQL ejecutado: tag bytes omite Binary 0, sin matcher en 7/8. Añadir guarda o traducción exacta para esas formas. |
| En TTL solo cabe un retraso irrelevante | No demostrado. La revisión 2 no encontró retraso en sus 45 casos y el camino actual emplea la misma transformación monótona. Memory usa datetime exacto; solo SQLite tiene preselección por enteros flotantes. Mantener el riesgo de mezcla de auxiliares anteriores con cutoff nuevo y distinguirlo del monitor asíncrono nativo. |

## 3. Hallazgos nuevos y causas

| ID | Prioridad / estado | Primer invariante roto e impacto |
| --- | --- | --- |
| H48 | P1; confirmado | La normalización del operando y la decodificación del almacenamiento producen representaciones con rangos diferentes (100 frente a 6). El comparador reconoce Binary local pero no SDK: dos SDK de igual payload y distinto subtipo comparan iguales y pierden un valor en distinct. Igualdades/rangos/eq frente a in/selección/no-op y orden por longitud son incorrectos. |
| H49 | P1; confirmado | Una fecha exacta se convierte a float antes de recuperar milisegundos. `$toLong`, `$subtract`, `$dateDiff` y roundtrip pierden un milisegundo; consumidores de ventanas y TTL requieren controles propios. |
| H50 | P1; confirmado | Vista de valores, gramática de literales y decodificación wire no conservan la misma semántica DBRef/documento. Directa rechaza literales válidos y separa representaciones; group falla. Wire convierte a SDK y reordena $id/$ref: devuelve coincidencias inexistentes en MongoDB. |
| H51 | P1; confirmado | SQL de `$type` omite pertenencia en arrays para tres familias y binData omite su tag binary. En dialectos 7/8 elimina matches antes de la evaluación correcta. |
| H52 | P1; confirmado en el subset local | Watch prepara solo forma externa y ejecuta por evento sin dialecto/reloj capturados. OperationFailure 31325 aparece tarde; replaceWith de ruta válida se rechaza localmente con TypeError mientras MongoDB abre el stream; NOW cambia entre eventos e ignora now_factory. La fuga 7→9 con undefined ya tiene sonda local, faltando captura nativa tipada. |
| H53 | P2; diferencia caracterizada | Moderate evalúa primero el antiguo y puede devolver aceptación sin evaluar el nuevo. MongoDB evalúa el nuevo y registra warning 20294 para bad→bad bajo warn. Local tampoco emite warning en strict/warn: el hueco de observabilidad es más amplio que el orden moderate. No atribuir rechazo incorrecto de escrituras a este testigo. |
| H54 | P1; confirmado | La identidad de escritura termina usando igualdad Python para Code; ésta ignora el orden del scope. MongoDB modifica y conserva el nuevo orden; classic y pipeline locales devuelven cero y conservan el anterior en ambos motores. |

Testigos físicos destacados: `Binary(ab,0)` selecciona IDs 1/2 en MongoDB y
ninguno localmente; reasignar Binary idéntico devuelve 0 frente a 1;
`2038-04-22T20:55:58.687Z` produce 2155582558687 frente a 2155582558686;
Code con scope `{a:1,b:2}` → `{b:2,a:1}` produce modificación 1 frente a 0.
Cambiar +0.0 a -0.0 y Code sin scope a scope vacío sí coincide: conservar esos
controles positivos, no corregirlos por aproximación.

### H48: cadena causal observada por frontera

1. El filtro público atraviesa `DocumentCodec.to_internal`: su Binary se vuelve
   `_InternalBsonBinary` (rango 100 en el comparador actual).
2. El almacenamiento codifica y decodifica por otra ruta: el matcher recibe
   `bytes` o Binary local (rango 6). La traza confirma ambas formas en ambos
   motores. No atribuirle la clase interna por cómo se normaliza el filtro.
3. La guarda de familias o el comparador rechaza el par antes de comparar sus
   partes binarias. Directamente, dos internos pueden coincidir; ese positivo
   no valida la API pública. Igualar rangos deja entrar interno/local en la
   rama Binary, porque el interno hereda de la clase local. Esto arregla ese
   subcaso; agregar una clase a una tabla no completa todos los mecanismos H48.
4. Tras `find`, la materialización pública produce Binary del SDK. El comparador
   solo tiene rama explícita para Binary local; para dos SDK usa `<`/`>` de
   bytes, que no distingue subtipo. Distinct deduplica esos valores públicos
   mediante `_values_equal`, y omite el subtipo 5.
5. `$in` toma además un hash fast path que considera otra representación. Para
   Binary SDK 5, eq devuelve ningún ID e in devuelve ID 3 en los seis pares
   locales; MongoDB devuelve ID 3 en ambos. Para subtipo 0 localmente eq da
   vacío e in solo ID 2; MongoDB selecciona IDs 1 y 2 en ambos.
6. En SQLite 7/8, bytes eq/in puede resolverse íntegramente en SQL: consulta
   el tag bytes y devuelve solo ID 1, sin llamada al matcher. Corregir el
   comparador Python no recupera ID 2 omitido por SQL. Para SDK Binary eq la
   traza toma fallback y sí llama al matcher; dialecto 9 toma fallback también
   para bytes. La guarda de sort no acredita la elegibilidad de estos filtros.

El testigo directo SDK 0 frente a SDK 5 tiene `equal=True` y claves semánticas
distintas. Esto viola ya la ley igualdad⇒clave sin modificar ningún helper.
El arreglo debe cubrir normalización, decode/cache copy-on-write, representación
pública y deduplicación; no basta con el punto donde falló la primera sonda.

### H50: gramática y transporte además de igualdad

La sonda de revisión 1 usaba igualdad explícita `$eq`: no validaba el parser
de la forma implícita `{v: {$ref: 'c', $id: 1}}`. En revisión 3 el servidor
selecciona IDs 1/2; directa lanza OperationFailure de código 2; wire selecciona
1/2. No son dos garantías equivalentes: la forma reordenada `{v: {$id: 1,
$ref: 'c'}}` no selecciona ninguno en MongoDB, pero wire devuelve 1/2.

El parser local trata un documento con cualquier clave prefijada por $ como
expresión de operadores. El servidor examina la gramática del contexto y el
primer campo, con excepción para documentos DBRef completos; esa excepción
comprueba nombres, sin exigir que $ref sea string. El caso con $ref numérico
es literal válido para MongoDB y falla localmente. Un primer campo ordinario
con otro posterior prefijado por $ también discrimina la regla genérica.
La forma incompleta {$ref:'c'} continúa siendo inválida en el contexto probado;
no aceptar indiscriminadamente todas las claves $ para arreglar el caso válido.

El decoder BSON de PyMongo transforma documentos completos en DBRef y su
reconstrucción usa el orden canónico $ref/$id. Se comprobó la pérdida del orden
antes de llegar al parser local. Por tanto, no convertir todos los literales
de filtros a DBRef SDK para unificar representaciones. La igualdad MQL conserva
orden, y las vistas no pueden restaurar información que el transporte perdió.
El owner raw BSON existente debe conservar lo necesario en la entrada wire,
sin mutar el SDK ni cambiar la representación pública prometida de las salidas.
La preservación alcanza filtros y documentos que se insertan o actualizan:
corregir solo el filtro dejaría datos reordenados al escribir por wire. Lo que
ya perdió orden en archivos anteriores no puede reconstruirse por inferencia;
no prometer una reparación de esa información ausente.
Este incremento integra B04/B08 y los testigos H35 de fase/superficie.

## 4. Modelo común propuesto y límites

### 4.1. Clasificación y vistas, con un solo owner

Extraer de `_filtering_specials` el reconocimiento BSON a un módulo interno
neutral. Operaciones propuestas: tipo BSON reconocido, familia de comparación,
vista documental, partes binarias y partes de Code. Los nombres finales deben
expresar esos contratos, no un modo genérico de operación.

Tipo físico, familia comparable y rango del dialecto son conceptos distintos:
los números comparten familia, pero conservan int/long/double/decimal; Code
con/sin scope se distinguen; missing/null/undefined no se colapsan. Respetar
overrides explícitos de rangos y los hooks de igualdad/comparación existentes.
No copiar los números de la tabla C++ a claves persistidas de mongoeco.

DBRef del SDK es un documento BSON con $ref/$id, **no** el BSONType DBRef=12
obsoleto del servidor. La tabla del informe no debe asignarle rango 55.
La vista preserva orden y extras, sin imponer una materialización pública nueva.
UUID se normaliza solo después de aplicar sus codec_options; no imponer
subtipo 4 a configuraciones legacy o UNSPECIFIED.

El codec conserva ownership e inmutabilidad. Unificar, cuando sea compatible,
bytes/subtipo 0 en la representación binaria interna evita ramas ambiguas;
las vistas deben aceptar también las representaciones históricas leídas de
SQLite. Conservar bytes/subtipo del formato almacenado y salida pública. No
crear un segundo normalizador recursivo paralelo al codec.

D03a debe declarar qué formas conserva cada frontera de almacenamiento/cache
copy-on-write y asegurar que las vistas comunes aceptan todas ellas. No exige
que cada documento decodificado tenga físicamente la clase interna del filtro:
exige la misma semántica BSON y que lectura, clonación y asignación no muten
datos prestados. Se conservan tags y salida pública, incluidos archivos previos.

### 4.2. Comparación y claves: compartir responsabilidad, conservar contratos

Se adopta un owner conjunto de compare/equal y claves semánticas. La entrega
inicial usa las funciones explícitas existentes sobre primitivas compartidas,
sin un servicio universal nuevo. Un comparador inmutable ligado a
dialecto/collation queda como evolución opcional, si retira ramas/callers
duplicados y preserva hooks; no es dependencia de los fixes ni una clase para
pasar tres funciones. La garantía de coherencia se exige con o sin esa clase.

No llamarlo comparador de consulta si realmente ofrece orden total de valores:
MQL añade traversal, type bracketing y reglas de null/missing. Schema conserva
igualdad de objetos sin orden; modificación conserva representación BSON, sin
collation. No introducir `equal(mode=...)` en cada llamada ni hacer que la
identidad física sea un modo basado en una clave que ya perdió información.

| Función actual | Contrato / disposición |
| --- | --- |
| `bson_equality_key` | Productor semántico base; consolidar reconocimiento y coherencia con comparación. |
| `aggregation_equality_key` | Adaptador contextual: missing→null donde group lo exige, collation y recursión; delegar átomos, conservar reglas. |
| `_aggregation_key` | En el camino vigente funciona también como guarda de valores no admitidos. Retirar tras sustituir esa garantía explícitamente; no delegar al actual validate_bson_value, que aún es permisivo. |
| `hashable_in_lookup_key` | Fast path restringido de `$in`. Delegar claves admitidas conservando None/fallback y no admitir por accidente números/Code/collation sin prueba. |
| `filter_value_key` | Proyección parcial para candidatos Search, con tipos soportados y no finitos excluidos. Conservar la elegibilidad y el residual; no convertirla en igualdad universal. |
| `bson_engine_key` | Usos heterogéneos: identidad, orden interno, desempates e IDs de Search. Separar consumidores antes de cambiar bytes o forma de la clave. |
| `canonical_document_id` | Identidad física usada por storage_key persistida de SQLite. Conservar su codificación y compatibilidad con archivos existentes. No absorberla en la clave semántica. |

Si una clave es identidad exclusiva de un diccionario, debe representar la
misma equivalencia que su comparador. Si es solo hash, se permite colisión y
se exige comparación posterior. `equal(a,b) ⇒ key(a)==key(b)` no basta para
garantizar un diccionario que no verifica colisiones. Con overrides cuyo hash
compatible no pueda demostrarse, usar comparación/fallback; no inventar claves.

No imponer igualdad cruda que lance excepciones mediante una bandera global de
test: rompe contratos de contenedores/SDK y hace diferente el runtime de test.
Preferir revisión de callers, APIs explícitas y propiedades sobre producción.

### 4.3. Tiempo y contexto

Reutilizar bson_scalars para fecha normalizada→milisegundos enteros y conversión
inversa donde corresponda. Normalizar a precisión BSON antes de restar fechas;
probar negativos y zonas horarias. No almacenar todas las fechas como int64
interno ni ampliar tácitamente el soporte fuera del rango datetime.

OperationContext, ExpressionExecutionContext, PreparedPipeline y sus recursos
ya existen. Reutilizarlos; distinguir lifetime del comando, plan preparado,
batch/getMore, evento, deadline y constantes de un cursor. TTL y workers pueden
necesitar reloj fuera de comando: conservar una frontera explícita de captura
del reloj de pared para ellos. Eliminar el ContextVar no es condición del fix
de fechas y no debe romper el soporte a motores SPI v2.

Para watch el contexto de evaluación es el del stream, no el de la escritura
que publica cada evento. La sonda nativa conserva NOW entre dos getMore.
Caracterizar reapertura/reanudación e invalidate antes de fijar su tratamiento.

TTL requiere además inclusión de candidatos, no solo residual exacto. En el
camino actual cutoff y expiración usan la misma transformación monótona: una
coordenada redondeada puede añadir candidatos; el residual evita borrados
anticipados. No extender ese razonamiento a mezclas de versiones. Por ejemplo,
para −1001 ms el helper antiguo da −1000: una entrada antigua frente al futuro
cutoff exacto −1001 quedaría fuera. Es una deducción de compatibilidad a probar
al implementar, no un retraso observado en el runtime actual. D02 debe mantener
candidatos al leer auxiliares anteriores, mediante reconstrucción de auxiliares
TTL o estrategia conservadora demostrada, conservando el formato público.

## 5. Plan por bloques y dependencias

Los bloques D00–D07 se definen canónicamente en este documento. Los bloques
padre tienen un único significado; sus cortes identifican entregas citables.
Las prioridades ordenan riesgos, sin exigir una migración horizontal previa.

| Bloque | Prioridad / hallazgos | Dependencias y entrega |
| --- | --- | --- |
| D00 — Oráculos y fronteras | P1 transversal; H48–H54 | Propiedades y testigos del primer corte antes de arreglar H48; ampliar registro/capturador/replay existentes, capturas 7/8/9 y fallo previo por familia. No otro runner. |
| D01 — Identidad de modificación | P1; H48/H54 | Cortes D01a/D01b definidos abajo. Helper físico y consecuencias públicas, separados de igualdad semántica. |
| D02 — Milisegundos exactos | P1; H49 | Independiente de Binary. Conversión, dateDiff/subtract, roundtrip; revisar ventanas, ambos lados de TTL y auxiliares antiguos frente a nuevo cutoff. Retirar resta sin callers tras comprobar usos/export privados. |
| D03 — Tipos, Binary y especialización SQL | P1; H48/H51 | Cortes D03a/D03b definidos abajo. Clasificación/vistas y consumidores reales, incluyendo deduplicación pública. |
| D04 — Preparación y contexto de watch | P1; H52 | Testigos D00 con replica set. Allowlist local y preparación común una vez; corregir replaceWith, capturar dialecto/reloj/codec. No depende del cierre completo de B04 ni de toda D06. |
| D05 — Cierre integrado | P1 transversal | Cierre por familia y final conjunto de entregas obligatorias; comparar wheel/código, contratos y archivos SQLite, registrar costes y commits semánticos. |
| D06 — Gramática, transporte, vistas y claves | P1; H50 y consolidación contextual | Cortes D06a/D06b/D06c. Reconocimiento de literal DBRef, preservación en wire y claves por consumidor. H48 distinct y eq/in ya deben pasar en D03a. Conservar storage_key/tie-breakers. |
| D07 — Piloto opcional schema | P2; no bloquea los fixes | Solo después de D03/D06 y una hipótesis medible. Evaluar nodos schema propios con arrays atómicos y diagnósticos anotados; no convertirlos a hojas MQL ordinarias. No compromiso de migración completa. |

| Corte estable | Dependencias | Entrega y correspondencia con revisión 1 |
| --- | --- | --- |
| D03a — Clasificación, Binary y lecturas públicas | D00 de H48 | Parte de tipos/Binary de la antigua fila «D03 + corte D01». Reconocimiento común, longitudes/subtipos/bytes en local/SDK/interno, contrato storage/cache, eq/in y distinct públicos. Incluir guarda de elegibilidad SQL para no omitir Binary 0 al filtrar bytes. No se acepta con un parche solo de rangos ni solo Python. |
| D01a — Helper de identidad BSON | D00 de H48/H54 y vistas necesarias de D03a | Parte de identidad de la misma fila antigua: mover same_bson_update_value y corregir Code scope/Binary sin perder no-op, tipo ni orden. Puede acompañar a D03a en una entrega, con ambos IDs explícitos. |
| D03b — SQL y tipos | D00 de H48/H51 y D03a | Antigua fila «D03, corte SQL/tipos». Canonicalizar aliases; constructor por tipo/forma escalar-array y tag binary para type. Para eq/in bytes-Binary 0: traducción exacta acreditada o conservar fallback de D03a. Reintroducir pushdown solo con pruebas/benchmark ADR-015; incluir virtual/partial indexes. |
| D01b — Efectos públicos de modificación | D01a | Antigua fila «D01, corte restante». Estado, contadores, índices, outbox/eventos, classic/pipeline/replacement y operaciones múltiples. Casos físicos adicionales solo cambian con testigo nativo. |
| D06a — Literales y vistas DBRef | D00 de H50 y primitivas necesarias de D03a | Desambiguar literal/operadores en la gramática común por contexto, sin exigir constructor SDK ni modificar las reglas de expresión de agregación. Igualdad directa DBRef/documento con orden/extras y controles negativos. |
| D06b — Preservación BSON en wire | D00 de H50/H35 y D06a para aceptación conjunta | Evitar perder orden en filtros y documentos de escritura. Usar owner `bson_raw`/bridge existente; conservar inputs de SDK, undefined y tipos BSON. Directa/wire/nativo deben coincidir también con orden inverso y tipos de campo no canónicos; no reconstruir historia ya perdida. |
| D06c — Claves y consumidores DBRef | D06a y clasificación D03a | Parte restante de la antigua fila D06: group/lookup/sets/Search y spill conforme a sus políticas; retirar `_aggregation_key` tras sustituir su guarda. D06 no se cierra sin D06b aunque este corte pase. |

D00 no es una etapa de cierre: el primer corte incorpora una propiedad generada
sobre pares público/local/SDK/interno y transiciones de codec/almacenamiento/
materialización. Exige igualdad⇒misma clave en cada política que la use,
distinción de subtipos distintos con igual payload y preservación de una misma
identidad BSON entre representaciones. Incluye los contraejemplos deterministas
de H48 y registra que fallan en la base antes de modificar producción. No se
deja una suite roja permanente: el corte del fix debe entregar propiedad y
regresión verdes. No se acepta D03a sin esta garantía, aunque su helper pase.

H53 se integra en D00 como caracterización y en C03/B08 como decisión de
política/observabilidad: distinguir documento válido, validación omitida y
escritura permitida. Para warn evaluar el candidato cuando se necesite registrar
su incumplimiento; para error conservar transiciones moderate. La propuesta
es conservar aceptación de escrituras y documentar la falta actual de warning.
Un canal nuevo de avisos, sus datos y superficie requiere especificación
propia; no introducir un dump de documento ni alterar globalmente logging.
No hace falta migrar schema al matcher para corregir este orden/política.

Orden sugerido: testigos + Binary/Code, fechas exactas, DBRef/claves y SQL de
tipos; watch puede avanzar al disponer de su referencia física. Pruebas y
cierre acompañan cada corte. No retener un fix P1 hasta terminar todas las
familias ni abrir un tercer sistema de contexto, AST o fixtures.

### Precisiones de ejecución y simplificación de la revisión 4

1. **Reutilizar los owners ya extraídos.** D03a amplía `bson_values`; D01b
   consume `bson_identity`. No crear otro clasificador, una clase universal de
   comparación o una segunda identidad. La extracción no elimina las políticas
   distintas de consulta, agrupación, identidad y almacenamiento.
2. **Separar gramática de representación.** D06a comparte una decisión de
   literal/operadores con contexto explícito en los callers alcanzados. La
   referencia r8.0.0 usa DBRef completo para campo e `$in`, y permite incompleto
   al decidir la forma de `$elemMatch`; este último sigue su posterior parser
   de objeto. No deducir aceptación final solo de esa decisión intermedia ni
   trasladarla al top-level o a expresiones de agregación. Capturar cada
   contexto en 7/8/9 antes de convertir esa lectura en regla definitiva.
3. **Conservar información antes de comparar.** D06b observa orden raw antes
   de la decodificación SDK y después del bridge. Incluir filtros y escrituras,
   objetos/arrays anidados y scopes Code, con controles de undefined. Mantener
   salidas públicas y opciones soportadas: no modificar decoders globales ni
   canonicalizar el orden de documentos para facilitar igualdad.
4. **Mantener el fallback probado.** D03b no necesita optimizar bytes para
   cerrar H51. Corregir tipos/arrays y acreditar el fallback binario existente
   es suficiente; optimizarlo solo con equivalencia y medida propias. La traza
   con matcher demuestra ese fallback, nunca exactitud de una traducción SQL.
5. **Proteger el cambio de coordenadas TTL.** D02 prueba por separado cálculo,
   conservación de candidatos y residual exacto. Para archivos anteriores,
   reconstrucción versionada de auxiliares o selección conservadora, con
   reapertura, rollback ante error y rechazo de versiones futuras sin alterar
   documentos. Ni las 45 sondas históricas ni el monitor nativo aceptan por sí
   solos esa transición. No se promete una frecuencia de purgado nativa.
6. **Una ficha de aceptación por corte.** Reutilizar receipt y runner actuales:
   invariante/testigo, base y candidato identificados, referencia nativa,
   superficie/camino efectivamente ejecutados, observación independiente y
   pendientes. D00 acompaña al corte; D05 verifica su integración sin duplicar
   fixtures, IDs o reinterpretar caracterizaciones como pruebas verdes.

No es necesario materializar todo el producto cartesiano de la sección 6:
cada camino semántico distinto debe tener testigo discriminante, con pares de
control para representaciones y límites relevantes. Justificar las dimensiones
no alcanzadas; no eliminar las matrices contractuales de D05 ni sustituir una
superficie por otra. Para watch/gramática, comparar fase, familia pública de
excepción y código cuando exista, sin exigir identidad de módulo entre PyMongo
y mongoeco ni igualdad literal de mensajes no contractuales. La preparación
eager se verifica sin eventos y con colección vacía para evitar falsos verdes.

### Ajustes de ejecución de la revisión 5

El informe retira correctamente sus objeciones a la causa de H48, a la presencia
de `$in`/distinct y a la asignación de propiedades. Esos puntos ya tienen
owner/testigos; no necesitan otra abstracción ni otra fase. Se conservan los
huecos de parser DBRef, SQL y error/fase de watch. Se rechaza la conclusión de
que cualquier retraso TTL sería irrelevante: D02 ya resolvió la compatibilidad
de coordenadas antiguas con reconstrucción versionada y sus pruebas propias.

La causa transversal es una política aplicada después de perder información,
o una especialización que evita su autoridad. La simplificación consiste en
reutilizar `bson_values`, `bson_identity`, `bson_raw` y la preparación existente,
con pruebas en sus entradas reales. No requiere un objeto universal de valores.

| Corte | Precisión de aceptación y mantenimiento |
| --- | --- |
| D03a | Incluir subtipo 2: la longitud de comparación es la longitud BSON almacenada, con sus cuatro bytes de longitud interna; no siempre `len(payload)`. Verificar orden contra BSON codificado por SDK y servidor. Cubrir bytes crudos en atajos de rango/sort, además del comparador canónico, sin alterar overrides de dialecto. |
| D03a + D06b | Clasificar separadamente BSON estructuralmente inválido, BSON válido rechazado como literal de expresión y valor almacenado leído como campo. Los subtipos 3/5 cortos requieren testigo eager vacío/poblado y error público nativo; no imponer la regla del literal a todo almacenamiento o filtro. El decoder SDK no es por sí solo un oráculo de aceptación del servidor. |
| D06a + D06b | Preservar orden/tipos antes de convertir documentos a DBRef y validar gramática después. Una sola frontera raw con controles anidados, arrays, Code scope y undefined; no otro decoder paralelo por cada defecto, ni mutación de tablas SDK. Elegir el cambio mínimo que conserve las comprobaciones estructurales; no se exige reimplementar todo BSON. |
| D03a + D01b | `_id` bytes/Binary 0 exige selección y unicidad semánticas, pero las guardas de escritura y las claves persistidas conservan identidad física. Probar ambos órdenes de inserción, duplicado, subtipo distinto, actualización no relacionada y reapertura de archivo previo; añadir controles de clave/documento corruptos. La equivalencia de alias no autoriza aceptar un documento seleccionado incorrectamente ni cambiar su clave persistida. |
| D04 + D05 | Integrar fixtures nativos con replica set para watch/TTL, distinguiendo el timeout del fixture con monitor rápido de cualquier promesa del producto. Comparar fase/familia/código en el subset soportado; el rechazo wire actual de changeStream sigue explícito y no cuenta como paridad positiva. |

**Dependencia acotada:** D03a no depende de cerrar toda D06. La gramática Binary
wire que atraviesa `bson_raw` exige el incremento de preservación D06b; aceptarlo
junto con los testigos DBRef de D06a que ese incremento afecte. D06a usa las
primitivas de clasificación ya entregadas, no el cierre de D03a. Así se evita
una dependencia circular y se conserva un solo owner raw. D06c y la optimización
SQL siguen siendo entregas separadas; no retienen fixes focales verdes.

Orden propuesto en revisión 5 (sustituido por revisión 6): (1) ampliar D00 y completar D03a con esa dependencia raw/gramática;
(2) cerrar D01b, D03b y D06c sobre sus primitivas disponibles; D04 puede avanzar
con fixtures físicos propios; (3) disposición de H53, decisión explícita de
continuar o descartar D07, y D05 sobre el candidato integrado. D02 y D01a no se
reimplementan. La caracterización H53 no obliga a crear logging nuevo y D07
no bloquea correcciones obligatorias.

Cada especialización alcanzada se contrasta con el camino canónico y con el
servidor: matcher compilado/interpretado, sort rápido, hash, SQL y selección por
`_id`. Usar testigos discriminantes y un spy puntual de la ruta; no hacer de
clases internas exactas ni del número de llamadas un contrato público o una
telemetría permanente. Las trazas conservan valor diagnóstico. Las tres
propiedades actuales son regresión focal, no prueba de todos esos caminos.

### Refinamiento de la revisión 6 (orden sustituido por revisión 7)

La [evidencia de revisión 6](evidence/bson-consolidation-review/review6/receipt.json)
verifica que el texto es idéntico al de revisiones 3–5. Las catorce fuentes
históricas r8.0.0 vuelven a coincidir por hash; parser y ejecutor de updates por
pipeline se registran separadamente. No se sustituyen sus receipts ni las
167 pruebas históricas por resultados actuales. El replay focal actual pasa
345 checks sobre capturas existentes; 69 checks de propiedades/fronteras pasan.
No equivale a ejecutar otra vez la suite global ni a recapturar esos corpora.

**Disposición de la autocrítica.** Conservar sus correcciones sobre H48, `$in`,
distinct, representación y propiedades. Parser DBRef, eq/in SQL, SDK Binary y
traza del matcher ya tienen implementación/testigos: mantenerlos como regresión,
no abrir nuevas fases. Fase/familia/código de watch siguen siendo aceptación
obligatoria D04. Retirar «solo retraso irrelevante» como conclusión contractual:
la selección conservadora y migración TTL ya poseen pruebas D02. El número de
resultados de distinct nunca sustituye a observar payload y subtipo.

**D01b: separar tres decisiones, con un único helper físico disponible.**

1. Detectar si cambió el documento por identidad BSON, conservando tipo, orden,
   signed zero, NaN y scopes; no por igualdad de consulta.
2. Validar la inmutabilidad de `_id` según el tipo de escritura. La nueva sonda
   confirma que classic rechaza int→double, también dentro de Code scope, con
   WriteError 66; pipeline y replacement aceptan esos cambios semánticamente
   equivalentes y conservan el tipo nuevo. Memory admite indebidamente el caso
   Code clásico y el cambio +0.0→−0.0, mientras SQLite los rechaza por otra
   guarda de almacenamiento. Cambiar payload NaN presenta el error espejo:
   SQLite permite classic (nativo 66), pero Memory rechaza pipeline/replacement
   (nativo modifica). Reordenar Code scope en `_id` se rechaza con 66 en las tres
   formas de escritura y es un control positivo, distinto de un campo ordinario.
   Reutilizar `same_bson_update_value` en la guarda clásica y retirar su detector
   numérico parcial si ya no tiene callers, preservando precedencia de errores.
   No extender esa restricción física a pipeline/replacement ni confundirla con
   selección/unicidad semánticas de `_id` o con reescribir storage keys.
3. Publicar los efectos confirmados. El tipo de llamada no determina por sí
   solo operationType: en los tres servidores, el mismo pipeline int→double
   sobre un documento pequeño produce replace/fullDocument; con padding de
   1.000 caracteres produce update/updateDescription. Ambos motores locales
   publican update y omiten updateDescription. El [ejecutor r8.0.0](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/update/pipeline_executor.cpp) calcula diff
   y puede emitir reemplazo; no inferir un umbral universal de estos dos casos.

El punto 2 combina política incompleta en un caller con verificación de storage
no reflexiva para NaN en Memory: su `bson_engine_key` conserva el float en
una tupla, mientras SQLite compara el repr persistido de `canonical_document_id`. Tras decode, otro NaN no
es igual por Python; la guarda rechaza incluso la selección del documento antes
de ejecutar la mutación. La sonda ampliada lo expuso durante delete de limpieza;
por eso la ejecución final usa colección aislada por caso. Añadir delete/update
sin tocar `_id`, selección semántica, duplicados y reapertura como controles del
arreglo. Contrastar el origen/fase del error además del código 66: el mismo
código puede ocultar otra guarda incorrecta. No modificar canonical_document_id
ni claves persistidas para arreglar la guarda sin una decisión de compatibilidad.
El helper físico no necesita otra implementación para estos contraejemplos.
El punto 3 cruza el productor de eventos y su entrega; corregir solo watch no
lo resuelve. Mantener ambos dentro de D01b, con D04 para preparación/contexto:
son obligaciones separadas y no exigen una dependencia circular ni reabrir D02.

La nueva [caracterización D01b](evidence/bson-consolidation-implementation/write-effects/receipt.json)
precisa qué información preservar: classic whole/dotted pueden dejar la misma
postimagen y producir updateDescription distinta. Conservar rutas/procedencia
durante la mutación; un diff genérico posterior no basta para ambas formas.
DocumentKey conserva la representación anterior de `_id` cuando pipeline cambia
su tipo. Ambos testigos tienen referencia 7/8/9 y quedan en la aceptación D01b.
No son otro sistema de contexto ni una razón para alterar claves persistidas.

**Refactorización acotada de efectos.** Contrastar primero el outcome existente
de mutación. Si no basta, ampliarlo con el cambio efectivo confirmado y los
datos de evento que deban compartir Memory y SQLite; la construcción semántica
del evento tiene un owner, mientras journal/outbox/rollback conservan sus owners.
No introducir otro contexto de ejecución, otro decoder ni un emulador general
del oplog. Si se necesita tamaño BSON, demostrar primero exactitud del helper
reutilizado para Binary 2, Code scope y anchos numéricos; no basarse en repr,
tamaño JSON ni un estimador no contrastado. Ninguna de estas extracciones se
acepta por reducir nombres: debe retirar decisiones duplicadas y pasar sus
pruebas nativas de contenido y publicación.

| Prioridad / corte restante | Dependencia y aceptación específica |
| --- | --- |
| P1 D01b: guarda clásica y efectos | D01a disponible. Primero regresión del rechazo 66 y estado intacto, controles pipeline/replacement y ambos motores; después update_many/bulk/command/findAndModify, índices, contadores y rollback. Eventos: no-op sin evento, publicación una vez tras commit, operationType, documentKey, fullDocument/updateDescription y valores físicos. Usar barrera para comprobar ausencias; no contar un timeout aislado como prueba. No normalizar update/replace ni ignorar campos para hacer pasar el replay. |
| P1 D06a/D06b restantes, luego D06c | Clasificación/gramática/raw disponibles. Completar contextos/subpipelines y preservación física; reutilizar referencias ya aceptadas para sus regresiones. Claves/group/lookup/sets/spill/Search conservan políticas y guardas, sin alterar claves persistidas. |
| P1 D04 en paralelo lógico | Replica set para pruebas nativas y fixtures de jobs. Preparación eager sin eventos, fase/familia/código y dialecto/reloj de stream capturados. La sonda de publicación D01b usa pipeline watch vacío para aislar al productor; no acepta D04 por accidente. |
| P2 H53 y decisión D07 | Disposición de observabilidad en C03/B08 sin cambiar aceptación de escrituras por sorpresa. D07 es opcional: decidir continuar o descartar con hipótesis medible; no bloquea los fixes. |
| P1 D05 final | Integrar entregas obligatorias y matrices de la sección 6 sobre candidato congelado. Los checks focales de esta revisión no lo cierran. |

Este orden sustituye el orden restante de revisión 5. El registro de
implementación sigue siendo el único owner de estado/gates: las tablas del
plan expresan obligaciones y apuntan a él. No se crean IDs para los
contraejemplos adicionales, ya incluidos en identidad y eventos de D01b. El trabajo
de tests sin commitear preexistente no se considera aceptado por esta revisión.

### Refinamiento vigente de la revisión 7

La autocrítica no introduce otra familia de defectos: parser DBRef, fallback
binario eq/in, SDK Binary, distinct y testigos de representación ya están
incorporados y tienen entregas funcionales. En esta revisión pasan 69 controles
de propiedades/fronteras y 345 de replay sobre capturas existentes; las trazas
actuales observan ambos lados del matcher en los seis pares motor/dialecto.
Las 14 fuentes históricas y las dos adicionales de revisión 6 coinciden por
descarga/SHA-256. Esto no acredita gates globales, nuevas capturas de esos
corpora ni los cambios de eventos sin commitear. Las 167 pruebas históricas
mantienen su significado original. Watch sigue pendiente por fase/familia/código
y contexto; TTL no se reabre ni se rebaja su garantía de candidatos.

**Precisar la causa transversal sin crear una igualdad universal.** La pérdida
de información explica Binary/DBRef, fechas y procedencia de eventos. Evitar la
autoridad común explica SQL, rutas rápidas y el uso de igualdad semántica en
escrituras. Pero no todo efecto de escritura se deduce de identidad BSON entre
la preimagen y la imagen guardada. La sonda nueva confirma, en MongoDB
7.0.43/8.0.32/9.0.2 con FCV correspondiente:

- `$unset: {"a.0": 1}` sobre `a: [null]` cuenta como modificación y emite
  actualización aunque la imagen guardada sea idéntica. Ambos motores locales
  devuelven cero y omiten el evento. La excepción pertenece al operador clásico;
  no se debe cambiar la igualdad física general para simularla.
- `$rename: {a: "b"}` con `a: 1, b: 1` elimina `a`, conserva la posición previa
  de `b` y publica su asignación. Eliminar y reinsertar el destino localmente
  altera el orden BSON. La refactorización debe conservar el orden en el setter,
  no compensarlo reordenando el resultado o el observador.
- `$merge` con `whenMatched: "merge"` y `v: int(1) → double(1)` cambia tipo y
  publica evento. Ambos motores locales conservan el entero por usar igualdad
  semántica. Es otra entrada de escritura a D01b; su ruta manual no debe quedar
  fuera al consolidar updates. No extrapolar esta sonda a todos los modos merge.
- Un pipeline que omite `_id` conserva el identificador al guardar, pero puede
  producir replace en el documento pequeño o update con `_id` en removedFields
  en el grande. El trabajo local actual coincide en estos dos casos; falta su
  testigo permanente y aceptación del corte. Conservar imagen calculada e imagen
  guardada cuando el constructor de eventos necesite distinguirlas.

Estas son caracterizaciones públicas nuevas, con barrera de entrega, estado y
contenido BSON independientes del helper productivo. No son cierre de D01b ni
aceptación de transacciones. Las bases UUID se eliminan al terminar; no se
modifica la instancia habitual ni se detienen servicios de la tarea anterior.

**Responsabilidades comunes mínimas.** Mantener los componentes existentes y
completar únicamente la información que falta en sus fronteras:

| Responsabilidad | Owner y simplificación | Límite que debe conservar |
| --- | --- | --- |
| Reconocer representación BSON | `bson_values`, codec y `bson_raw`; consumidores delegan vistas | Gramática, opciones de codec y validación de admisión tienen contratos propios. |
| Igualdad/orden/claves semánticas | Primitivas y adaptadores de cada política existentes | Query traversal, schema, group y Search no se convierten en un único modo de comparación. |
| Identidad física de un valor | `bson_identity` | No decide por sí sola inmutabilidad de `_id`, todos los modified_count ni publicación. |
| Aplicar la operación y conservar procedencia | Plan de update y outcome existentes; reutilizarlos desde entradas equivalentes, incluido merge cuando corresponda | Separar aplicado/no-op, imágenes, rutas concretas y contenido del cambio. No recalcular todo desde las dos imágenes ni crear otro AST. |
| Construir contenido de evento | Un owner interno compartido Memory/SQLite; el trabajo `change_events` es candidato, no contrato aceptado | Mantener diferencias classic/pipeline/replace, documentKey anterior y diferencias por versión; no convertirlo en emulador general del oplog. |
| Calcular tamaño BSON si el contenido lo requiere | Un cálculo contrastado, compartido con `$bsonSize` | Framing Binary 2, Code scope, widths numéricos, regex/UTF-8/anidados; SDK independiente y servidor como oráculos. No usar estimaciones JSON. |
| Guardar y entregar | Owners actuales de storage, journal/outbox, publicación y transacción | Contenido preparado no acredita commit; rollback, retry y ausencia antes del commit se prueban aparte. No cambiar formato de claves ni esquema persistido sin necesidad demostrada. |

No añadir clases wrapper, una jerarquía de efectos ni una API pública por el
mero hecho de compartir esta responsabilidad. Si el outcome existente admite
los datos necesarios, ampliarlo de forma interna y verificar ownership/copia.
No imponer la captura costosa de rutas/diffs a escrituras sin observador cuando
su política de publicación no la requiere. Conservar controles de huecos del
journal y reanudación; la optimización no puede ocultar trabajo pendiente.

**Secuencia restante y garantías de salida.** Esta tabla sustituye el orden
restante de revisión 6; no crea otros IDs ni cambia los owners de estado:

| Orden / bloque | Trabajo y dependencia | Aceptación discriminante |
| --- | --- | --- |
| 1. D01b: primitivas pendientes y efectos clásicos | D01a/D03a disponibles. Completar tamaño BSON si el diseño elegido lo necesita; corregir unset de array null y rename sin mover destino; conservar procedencia real de las rutas. | Propiedad de tamaño frente a BSON SDK y capturas 7/8/9; límites estrictos update/replace y subdocumentos/arrays; imagen, orden, contador y evento completos. Whole/dotted con misma postimagen deben seguir produciendo descripciones distintas. |
| 2. D01b: todas las entradas de escritura afectadas | Reutilizar los planes y políticas existentes en merge en lugar de mantener otro evaluador de mutación. Censar classic/pipeline/replacement, merge, bulk, update_many, command y findAndModify; reutilizar el corte ya aceptado de estado/índices. | Tipos físicos, `_id` permitido/rechazado por forma de escritura, selección/rekey sin cambiar formato ni orden natural, índices y reapertura; fase/familia/código de errores de comando y escritura según referencia nativa. No asumir que todos exponen WriteError. |
| 3. D01b: entrega confirmada | Contenido común disponible; integración con scopes y outbox existentes, sin otro contexto ni journal. | Evento una vez después de commit, ninguno antes ni tras abort/error/no-op nativo, rollback de estado/índices/outbox y retry/reapertura. Usar barrera, no un timeout aislado; distinguir pruebas locales de captura transaccional física. |
| 4. D04 y D06 restantes | Sin dependencia circular entre productor y consumidor watch. D04 prepara una vez con contexto del stream y fixture replica set; D06a/b completan contextos y preservación, D06c comparte átomos conservando políticas de consumidores. | Watch inválido falla al abrir sin eventos; replaceWith válido abre; fase/familia/código y NOW entre getMore. DBRef/raw en subpipelines y consumidores con/sin hash/spill. Wire no soportado se registra como límite, nunca como éxito de paridad. |
| 5. H53 y conversión escalar | Disposición H53 en C03/B08 sin canal de logging implícito. El diagnóstico sNaN del registro conserva owner de conversión; no se pierde en la consolidación. | strict/moderate/off × error/warn/bypass con antiguos/candidatos; contrastar sNaN 7/8/9 y añadir regresión antes de modificar conversiones, sin prohibir NaN almacenado. D07 sigue opcional. |
| 6. D05 | Entregas obligatorias y fuentes congeladas; revisión por bloque, matrices y commits semánticos del owner vigente. | Reusar evidencias focales válidas, ejecutar gates integrados/matrices/wheel/SPI/SDK-free y medidas exigidas; no contar caracterización o WIP como entrega. |

Por cada fallo compartido, exigir un testigo contra el servidor además de la
equivalencia Memory/SQLite o compilado/interpretado. Por cada especialización,
observar el camino real y contrastarlo con la autoridad canónica. No duplicar
fixtures ni repetir una matriz equivalente sin cambios; sí repetir el gate
afectado cuando cambie su fuente/corpus. Cada receipt identifica ese candidato,
el fallo previo y las dimensiones no ejercidas. Los observadores BSON protegidos
y goldens históricos permanecen independientes e inalterados.

## 6. Pruebas y aceptación por garantía

| Familia | Casos obligatorios y observación |
| --- | --- |
| Tipos y representaciones | Público SDK/local e interno, antes/después de codec/persistencia/materialización. En la frontera real del matcher registrar por separado documento/operando y clases/rangos para los testigos de H48; no fijar una clase interna como salida pública contractual. Bytes/Binary 0/5, longitudes/subtipos, arrays/anidados, ObjectId, DBRef/extras, Code, missing/null/undefined y UUID bajo opciones soportadas. |
| Matching | eq/ne/in/nin/rangos/type/expr; testigo explícito de eq frente a in unitario con el mismo Binary 0/5/bytes y sin regex; compilado/canónico, arrays/rutas numéricas/punteadas y selección para escrituras/count/distinct. Preservar fallback/fast path y observar cada frontera. |
| SQL | Dialectos locales 7/8/9 y referencia nativa correspondiente; pushdown normal/desactivado, sin índice/con scalar/multikey/partial, escalar/array, aliases numéricos/string. Eq/in/ne/nin de bytes/Binary 0/5 y datos con tags históricos bytes/binary; trace de SQL ejecutado y spy del matcher por caso. Ni un fallback verde acepta SQL ni un helper verde acepta el camino SQL. No sort/window antes de residual eliminatorio. |
| Gramática DBRef y wire | Forma implícita, eq explícito, SDK/local, orden ref/id e id/ref en filtro y seed/escritura, db/extras, ref no string, incompletos y documentos ordinarios con $ posterior. Observar entrada raw y plan, fase/clase/código/IDs, vacío/poblado y contextos eq/in/not/elemMatch/subpipeline alcanzados. Top-level mantiene su gramática propia: los campos especiales $ref/$id/$db tienen igualdad de campo; otros nombres desconocidos siguen rechazándose. No propagar excepciones de literal indiscriminadamente. Native/directa/wire deben preservar orden y equivalencia; un caso wire correcto no certifica parser ni transporte. |
| Identidad | No-op Binary 0/5, bytes equivalente, int/long/double/decimal, signed zero, NaN/payload, Code scope reordenado/ausente/vacío, tipos numéricos y arrays/documentos anidados en scope. Separar identidad del `_id` y de campos ordinarios. Classic, pipeline, replacement, bulk ordered/unordered, update_many, findAndModify y comandos; contadores, tipo físico/orden, estado y publicación. |
| Fechas | ±1001 ms, ejemplo 2038, epoch, negativos submilisegundo normalizados, UTC naive/aware y offsets, extremos soportados; toLong/toDate, conversión inversa independiente, subtract/dateDiff. Ventanas derivative/integral con casos nativos propios. TTL a expiración−1/exacta/+1 ms: no borrar antes ni omitir candidatos vencidos; índice nuevo/creado tras datos/reabierto, auxiliares de versión anterior y publicación. No exigir borrado inmediato al monitor nativo. |
| Claves/consumidores | Desde D03a, distinct sobre bytes/Binary SDK 0/5 con el mismo payload, distintos órdenes de inserción, arrays/anidados y materialización pública: comparar subtipo/contenido, no solo cardinalidad. Después group/addToSet/set/lookup/spill y collation, DBRef/documento y Code scope según política. Mismo resultado con/sin hash/spill; desempates Search y persistencia con controles separados. |
| Watch | Apertura sin eventos y pipeline inválido, replaceWith válido, filtro de evento, undefined tipado, NOW entre etapas/eventos/batches, now_factory, getMore/reanudación/invalidate. Capturar clase de excepción además de código y fase: TypeError local de replaceWith frente a apertura válida física, y OperationFailure 31325 diferido frente a apertura fallida. Colección/DB/cliente, sync/async y wire soportados, cursor/offset/token/cierre; sin ampliar semántica distribuida. |
| Validators | strict/moderate/off × error/warn/bypass, antiguo/candidato válido/inválido, insert/upsert/bulk/merge y estado tras error; observar errores de evaluación y diagnóstico, no inferirlos del permiso final. Logs físicos se comparan como efecto nativo sin prometer un API de logging local inexistente. |

Las propiedades existentes se amplían: preservación de representación,
coherencia de comparación/clave dentro de la política declarada, orden
antisimétrico/transitivo, coherencia hash/igualdad y conservación de candidatos.
Propiedades internas y coincidencia entre motores no sustituyen al servidor.
No importar el nuevo helper en el observador nativo protegido ni modificar
goldens históricos para hacer pasar una corrección.

D05 exige la matriz del owner vigente: nativos 7/8/9 con buildInfo/FCV y replica
set para watch, ambos motores, superficies afectadas directas y wire; Python
3.13/3.14/3.15, extremos SDK del kit y pytest soportados, wheel inmutable, typing,
SPI v2, smokes, suite global y cobertura mínima actual. No contar skips físicos
como ejecución. Tests por corte primero; gates completos sobre candidato final.

Medir carga de matching/keys, group/spill, SQL/tipos, validators y streams según
lo afectado, antes/después con semillas y resultados idénticos. No introducir
una clase/comparación/key adicional por valor en hot paths sin medirla; registrar
costes y fallbacks. SQLite reabierto con archivo previo debe conservar IDs,
índices, opciones, lectura y escritura. Cambiar claves persistidas requeriría
otra decisión/migración, fuera de este plan.

## 7. Qué no hacer ahora

- Un único productor con sidecar para todas las siete claves.
- Un Value/KeyString completo ni fechas int64 internas generalizadas.
- Un nuevo contexto o una modificación SPI v2 para arreglar callers internos.
- Prohibir `__eq__` globalmente bajo test o usar repr para identidad semántica.
- Migrar schema por su tamaño, traducir validators a SQL o
  añadir flags a hojas MQL para simular schema sin un piloto independiente.
- Cerrar B07 por normalizar Binary/DBRef. Tipos arbitrarios y Decimal crudo en
  la frontera general mantienen su pendiente; las garantías de cada corte se
  nombran explícitamente.
- Presentar una propuesta como mejora medida o reescribir la aceptación C00–C06.

El piloto D07 solo se continúa si conserva errores/diagnósticos/arrays/subset,
retira el evaluador reemplazado sin convivencia indefinida y reduce puntos de
coherencia y coste de mantenimiento con evidencia. Si añade un intérprete o
no mejora respecto de CompiledJsonSchema con primitivas compartidas, se descarta.

## 8. Fuentes externas fijadas

Los hashes y URLs de las 14 fuentes leídas están en
[sources.json](evidence/bson-consolidation-review/sources.json). Referencias
centrales, todas r8.0.0:

- [bsontypes.h](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/bson/bsontypes.h): tipo BSON frente a rango canónico; DBRef obsoleto distinto del documento SDK.
- [bsonelement.cpp](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/bson/bsonelement.cpp): comparación por familia; Binary longitud/subtipo/contenido y Code scope con comparación de strings binaria.
- [ValueComparator](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/exec/document_value/value_comparator.h) y [Value](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/exec/document_value/value.h): igualdad/hash bajo contexto y comparación diferida. No significa un comparador físico universal.
- [KeyString](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/storage/key_string.h): TypeBits para restauración de tipos, sin justificar un sidecar universal de identidad.
- [Matcher](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/matcher/expression_leaf.cpp) y [schema parser](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/matcher/schema/json_schema_parser.cpp): type bracketing y nodos específicos de schema.
- [Parser de expresiones MQL](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/matcher/expression_parser.cpp): distinción literal/operadores, primer campo y excepción DBRef dependiente del contexto. Hash adicional en la evidencia de revisión 3.
- [collection_write_path](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/catalog/collection_write_path.cpp) y [collection_impl](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/catalog/collection_impl.cpp): candidato antes del antiguo y warning convertido a permiso; collation/contexto del validator.
- [variables](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/pipeline/variables.cpp), [pipeline](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/pipeline/pipeline.cpp) y [Date_t](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/util/time_support.h): constantes, preparación y milisegundos.

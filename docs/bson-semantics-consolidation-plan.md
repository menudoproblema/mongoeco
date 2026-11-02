# Consolidación BSON: contraste de informes y plan integrado

**Estado vigente (2 de noviembre de 2026): parte I F00–F11 cerrada, 12/12 bloques.**
La [aceptación integrada](bson-consolidation-implementation.md#aceptación-final-f11-del-candidato-b83789bc)
corresponde al producto b83789bc y conserva los cierres anteriores. Parte II
X01–X06 excluida. El cierre de versión, tag y publicación quedan aplazados
por instrucción del owner; no son pendientes técnicos de esta parte I.

**Propuesta posterior al cierre (1 de noviembre de 2026):**
[plan en dos partes: correcciones acotadas y ampliaciones](#incremento-propuesto-20261101).
Base 2fa637f6. Esta propuesta no reabre O1–O8 ni B1–B4 de los dos P2,
no cambia sus receipts y no acredita implementación ni aprobación de capacidades nuevas.

**Entrada vigente (1 de noviembre; alcance O1–O8 y D05 cerrado):**
[contraste y orden actual](#contraste-actual-y-orden-mínimo-de-ejecución),
[detalle de B07](#b07--admisión-en-la-fase-y-el-lote-correctos) y
[estado de implementación](bson-consolidation-implementation.md).
La [relectura de la autocrítica](evidence/bson-consolidation-review/current-autocritique/README.md)
conserva el contraste previo; el [corte posterior de batching](evidence/bson-consolidation-implementation/command-batches/README.md)
tiene aceptación propia. `05b3f655` acepta después [DTOs/sobres y diagnóstico de escritura](evidence/bson-consolidation-implementation/write-responses/README.md);
`a8a81f0e` acredita [constructores/setters, errores y eventos confirmados](evidence/bson-consolidation-implementation/write-factory-failures/README.md).
B07, unidad 6, queda aceptado con D05 en el [cierre integrado](bson-consolidation-implementation.md#cierre-integrado--o8-y-d05).
Las revisiones numeradas anteriores conservan historia, no otro backlog activo.

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
en los tres servidores temporales. El orden de revisión 7 se conserva en la sección
5; las revisiones anteriores conservan su alcance histórico.

Revisión 8: mismo adjunto (SHA-256 idéntico), contrastado con HEAD `bc8fb1c`
y el trabajo de watch sin commitear, identificados en la
[evidencia de revisión 8](evidence/bson-consolidation-review/review8/receipt.json).
D01b ya tiene cierre funcional; no se reabre. Se precisan tres garantías de
D04 y su aceptación frente a imágenes y DDL observados en MongoDB 7/8/9.
El orden restante vigente es el de «Refinamiento de la revisión 8» en sección 5.
Esta revisión solo modifica documentación/evidencia, sin aceptar el candidato.

Revisión 9: el adjunto vuelve a ser idéntico por SHA-256, contrastado con
HEAD `163ccb4` y runtime/tests limpios. La [evidencia de revisión 9](evidence/bson-consolidation-review/review9/receipt.json)
separa las regresiones repetidas de la aceptación histórica. Preparación/contexto
e imágenes CRUD de watch ya tienen aceptación propia; siguen pendientes DDL,
protocolo/reanudación y su integración. El orden vigente se precisa al final
de la sección 5, sin crear bloques ni reabrir cortes cerrados.

Revisión 10 sobre `6ff34d4`: mismo adjunto por SHA-256. El contraste actual
se conserva en la [evidencia de revisión 10](evidence/bson-consolidation-review/review10/receipt.json).
La sección «Refinamiento de la revisión 10» precisa el siguiente corte de D04
y sustituye las instrucciones de extracción/DDL ya ejecutadas de revisión 9.
No constituye nueva aceptación de runtime ni cierre de bloques.

Revisión 11 sobre `f0a14fb` y candidato watch sin commitear: mismo adjunto
por SHA-256. La [evidencia de revisión 11](evidence/bson-consolidation-review/review11/receipt.json)
identifica ambos estados. El orden activo y las precisiones de aceptación están
en «Refinamiento de la revisión 11»; sustituyen instrucciones anteriores solo
donde se indica, sin cambiar receipts ni aceptar el candidato por inspección.

Revisión 12 sobre `a981610` y candidato sin commitear: el adjunto mantiene
el mismo SHA-256. La [evidencia de revisión 12](evidence/bson-consolidation-review/review12/receipt.json)
separa regresiones repetidas, dos sondas físicas nuevas y aceptación pendiente.
El orden activo queda en «Refinamiento de la revisión 12». No acepta el candidato
ni vuelve a abrir cortes cerrados por una ampliación de sus consumidores.

Revisión 13 sobre `e8abf99`: el nuevo adjunto tiene el mismo SHA-256 que
las revisiones 3–12. El [contraste actual](evidence/bson-consolidation-review/review13/receipt.json)
identifica runtime y trabajo de CI preexistente. La ruta activa está en
«Refinamiento de la revisión 13»; las tablas de revisiones anteriores conservan
su alcance histórico. No se acepta el fixture en curso por esta revisión.

**Estado: contrato autorizado; implementación en curso.** Los cortes y gates
aceptados se registran en el documento de implementación; no se acredita aquí
el cierre completo del incremento. Este documento posee H48–H56 y su integración con B00–B10 y
C00–C06. No reabre H37–H47 ni sustituye sus receipts. La implementación y los
gates históricos siguen en el [registro vigente](semantic-parity-implementation.md).
El [plan histórico](plan-semantic-parity-refactor.md) conserva las obligaciones
generales; el [incremento C](predicate-authorities-analysis-and-plan.md) conserva
su caracterización inicial, cuando faltaba el segundo informe.

El usuario ha autorizado la implementación completa del plan refinado. El
[registro de implementación](bson-consolidation-implementation.md) conserva
el progreso por bloque y sus pendientes; las caracterizaciones de este plan
no se convierten en aceptación de runtime por esa autorización.

Revisión 14, 1 de noviembre de 2026: el adjunto conserva el mismo SHA-256
`a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`.
Su disposición frente a HEAD `e941554` y el candidato de respuestas sin commitear
está en «Contraste actual y orden mínimo de ejecución», dentro de la sección 5.
La [evidencia de esta relectura](evidence/bson-consolidation-review/review14/receipt.json)
identifica inputs, verificaciones y trabajo preexistente sin aceptarlo. Las bases
y resultados de las revisiones anteriores son históricos; repetir el adjunto
no crea nuevos bloques ni vuelve a abrir los aceptados.

Revisión 15, 1 de noviembre de 2026: mismo adjunto por SHA-256, contrastado con
HEAD `eb85ea3` y el candidato de dispatch/planes sin commitear. Se actualiza el
apartado activo en lugar de añadir otra secuencia de ejecución. El
[contraste de revisión 15](evidence/bson-consolidation-review/review15/README.md)
separa regresiones aceptadas, fallos del candidato y controles todavía necesarios.

Revisión 16, 1 de noviembre de 2026, sobre HEAD `5348004`: el adjunto vuelve a
ser idéntico por SHA-256. El [contraste actual](evidence/bson-consolidation-review/review16/README.md)
conserva las retractaciones y precisa la aceptación de fábricas documentales,
topología y efectos de consumo. No añade otra secuencia de ejecución ni acepta
el trabajo de fixture/corpus preexistente sin commitear.

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
| H52 y H53 | D04 cerrado funcionalmente: el fixture RS de `0b5ac53` completa los cortes de preparación/contexto, imágenes/DDL, H56, protocolo/checkpoint y primera tanda. El [registro](bson-consolidation-implementation.md) conserva sus pruebas y límites; no se acredita ejecución remota CI ni cierre D05. H53 conserva su disposición de observabilidad en C03/B08. |

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
| H55 | P1; confirmado durante D04, pendiente D06a | Un Code BSON con prefijo $/$$ entra en la rama Python `str`: el runtime lo evalúa como campo y el preparador exige una variable inexistente. Referencias 7/8/9 y seis pares locales en la evidencia de imágenes confirman el mismo defecto en agregación ordinaria; $literal es control positivo. Reutilizar clasificación String/Code al decidir gramática y evaluar, con regresión permanente por contexto antes de corregir. No se confunde con identidad física H54. |
| H56 | P1; cerrado funcionalmente en el corte de comandos D04 | En 6e6d31e, el comando local exigia origen/destino en la base seleccionada; MongoDB lo recibe en admin. La correccion comparte gramatica BSON y errores entre directa/wire, conserva el SPI publico para rename ordinario y usa una unidad atomica para dropTarget, con un unico evento rename. Capturas 7/8/9 y gates del [registro](bson-consolidation-implementation.md) acreditan el subset de una base. No cierra protocolo/reanudacion ni todo D04/D05. |

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
| `_aggregation_key` (retirada) | `332d07a` elimina el dispatch duplicado y conserva la guarda escalar efectiva en el adaptador de grupo. No reintroducirlo ni sustituir la admisión B07 por validate_bson_value permisivo. |
| `hashable_in_lookup_key` | Fast path restringido de `$in`. Delegar claves admitidas conservando None/fallback y no admitir por accidente números/Code/collation sin prueba. |
| `filter_value_key` | Proyección parcial para candidatos Search, con tipos soportados y no finitos excluidos. Conservar la elegibilidad y el residual; no convertirla en igualdad universal. `supports_mql_candidate_prefilter` acredita además el contexto, con fallback para dialectos propios/collation. |
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
| D00 — Oráculos y fronteras | P1 transversal; H48–H56 | Propiedades y testigos del primer corte antes de arreglar H48; ampliar registro/capturador/replay existentes, capturas 7/8/9 y fallo previo por familia. No otro runner. |
| D01 — Identidad de modificación | P1; H48/H54 | Cortes D01a/D01b definidos abajo. Helper físico y consecuencias públicas, separados de igualdad semántica. |
| D02 — Milisegundos exactos | P1; H49 | Independiente de Binary. Conversión, dateDiff/subtract, roundtrip; revisar ventanas, ambos lados de TTL y auxiliares antiguos frente a nuevo cutoff. Retirar resta sin callers tras comprobar usos/export privados. |
| D03 — Tipos, Binary y especialización SQL | P1; H48/H51 | Cortes D03a/D03b definidos abajo. Clasificación/vistas y consumidores reales, incluyendo deduplicación pública. |
| D04 — Watch: preparación, imágenes y ciclo de vida | P1; H52/H56 | Tres garantías con aceptación separada dentro del mismo bloque. Preparación/contexto e imágenes CRUD ya entregadas; completar DDL, entradas de comando, protocolo/reanudación e integración sobre el registro/entrega existentes. Testigos D00 con replica set; no depende del cierre completo de B04 ni de toda D06. |
| D05 — Cierre integrado | P1 transversal | Cierre por familia y final conjunto de entregas obligatorias; comparar wheel/código, contratos y archivos SQLite, registrar costes y commits semánticos. |
| D06 — Gramática, transporte, vistas y claves | P1; H50 y consolidación contextual | Cortes D06a/D06b/D06c. Reconocimiento de literal DBRef, preservación en wire y claves por consumidor. H48 distinct y eq/in ya deben pasar en D03a. Conservar storage_key/tie-breakers. |
| D07 — Piloto opcional schema | P2; no bloquea los fixes | Solo después de D03/D06 y una hipótesis medible. Evaluar nodos schema propios con arrays atómicos y diagnósticos anotados; no convertirlos a hojas MQL ordinarias. No compromiso de migración completa. |

| Corte estable | Dependencias | Entrega y correspondencia con revisión 1 |
| --- | --- | --- |
| D03a — Clasificación, Binary y lecturas públicas | D00 de H48 | Parte de tipos/Binary de la antigua fila «D03 + corte D01». Reconocimiento común, longitudes/subtipos/bytes en local/SDK/interno, contrato storage/cache, eq/in y distinct públicos. Incluir guarda de elegibilidad SQL para no omitir Binary 0 al filtrar bytes. No se acepta con un parche solo de rangos ni solo Python. |
| D01a — Helper de identidad BSON | D00 de H48/H54 y vistas necesarias de D03a | Parte de identidad de la misma fila antigua: mover same_bson_update_value y corregir Code scope/Binary sin perder no-op, tipo ni orden. Puede acompañar a D03a en una entrega, con ambos IDs explícitos. |
| D03b — SQL y tipos | D00 de H48/H51 y D03a | Antigua fila «D03, corte SQL/tipos». Canonicalizar aliases; constructor por tipo/forma escalar-array y tag binary para type. Para eq/in bytes-Binary 0: traducción exacta acreditada o conservar fallback de D03a. Reintroducir pushdown solo con pruebas/benchmark ADR-015; incluir virtual/partial indexes. |
| D01b — Efectos públicos de modificación | D01a | Antigua fila «D01, corte restante». Estado, contadores, índices, outbox/eventos, classic/pipeline/replacement y operaciones múltiples. Casos físicos adicionales solo cambian con testigo nativo. |
| D06a — Literales y vistas DBRef | D00 de H50/H55 y primitivas necesarias de D03a | Desambiguar literal/operadores en la gramática común por contexto, sin exigir constructor SDK ni modificar las reglas de expresión de agregación. Igualdad directa DBRef/documento con orden/extras y controles negativos. H55 añade String/Code como decisión gramatical en preparación/runtime y sus contextos; no otro normalizador. |
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

### Refinamiento de la revisión 7 (orden sustituido por revisión 8)

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

**Secuencia restante y garantías de salida.** Esta tabla histórica sustituyó el orden
restante de revisión 6. Revisión 8 contiene el orden actual; se conservan IDs y
owners de estado:

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

### Refinamiento de la revisión 8: responsabilidades y aceptación restantes

**Disposición del informe.** No hay un nuevo conjunto de siete hallazgos: el
texto coincide con el ya contrastado en revisiones 3–7. La causa de H48, la
presencia de `$in`/distinct y la propiedad desde D00 estaban correctamente
planteadas. Las precisiones sobre SDK/local/owned, literal DBRef y eq/in SQL
se conservan como regresión: en este árbol pasan 69 checks de propiedades/
fronteras y 345 de replay sobre referencias existentes, con 18 filas nuevas
que observan ambos lados del matcher. No convertir sus clases concretas ni el
número de llamadas en contrato público. La guarda binaria conserva fallback;
ese resultado no acredita pushdown. Las 16 fuentes r8.0.0 vuelven a coincidir
por hash, sin convertir esa etiqueta en contrato de todas las versiones.

La afirmación «solo un retraso TTL irrelevante» sigue sin adoptarse como
invariante. D02 ya tiene aceptación de coordenadas/migración y no se reabre.
Esta revisión no repite TTL ni H53: el diagnóstico local de este último por
inspección no se presenta como reproducción física nueva. Los 167 tests
históricos conservan únicamente su alcance original.

**La causa común exige conservar decisiones en la frontera correcta.** El
reconocimiento BSON, la comparación semántica, la identidad física y los efectos
aplicados ya tienen owners. Las sondas nuevas muestran otra distinción dentro
de D04: una imagen capturada al escribir, una lectura al consumir y una decisión
de cierre por ámbito tienen lifetimes diferentes. Ni igualdad BSON ni el
preparador de expresiones pueden decidir las tres. Se amplían los owners
existentes únicamente con los datos que les faltan; no otro Value, contexto,
runner, jerarquía de eventos o intérprete de watch.

| Garantía D04 | Causa y refactorización mínima | Aceptación necesaria |
| --- | --- | --- |
| Preparación y contexto | Reutilizar `PreparedPipeline`/compilación y `ExpressionExecutionContext`; capturar dialecto/reloj/codec al abrir. El candidato actual ya lo intenta, pero no está aceptado. Validación estructural compartida con aggregate/update, con política de stages permitidos propia de watch. Reconocimiento sintáctico de un stage y capacidad de ejecutarlo son decisiones distintas: no usar capacidades de dialecto como catálogo gramatical y romper Search/registro de extensiones. | Sin eventos: pipelines válidos/erróneos, colección vacía/poblada, precedencia, fase/familia/código y ningún watcher registrado tras apertura fallida. Error dependiente del evento ocurre al consumir y cierra cuando lo haga el nativo. `$$NOW` estable entre etapas/eventos/getMore vacío y reloj nuevo al reabrir/reanudar; override de dialecto y codec. Handles derivados (`collection.database`, clones/opciones) conservan la inyección del reloj también para agregación ordinaria. Undefined: referencia propia de 7, 8 y 9, sin aplanar diferencias. |
| Imágenes de actualización | `apply_full_document_mode` trata actualmente todos los modos no default como la postimagen almacenada. Conservar disponibilidad de imagen al escribir cuando corresponda; resolver `updateLookup` al consumir mediante recursos existentes y lookup interno tipado. Separar ese recurso del reloj constante de expresiones. El diseño definitivo y su compatibilidad persistida se revisan antes de cambiar journal/outbox. | Con v0→v1→v2 antes de consumir, `updateLookup` da v2, o null si se borró; no v1. Sin imágenes habilitadas, `whenAvailable` da null y `required` falla con OperationFailure 47 y cursor cerrado. Con imágenes habilitadas, ambos dan v1 incluso tras borrar. Default omite fullDocument en update. Controles insert/replace, filtros/proyecciones sobre fullDocument, opciones cambiadas entre escritura/lectura, scopes y reapertura/journal. Una opción aceptada en create_collection no demuestra que se capturen imágenes. |
| DDL y ciclo de vida por ámbito | El productor de drop publica solo invalidate y `ChangeStreamScope.matches` lo entrega a todos los ámbitos; drop_database rápido evita esa publicación. Reutilizar enrutamiento/publicación y cursor actuales, distinguiendo metadatos internos para decidir destinatarios/cierre de los campos del evento público. Validar commit/offset/token con los owners actuales: no emitir fuera del orden confirmado ni fabricar documentKey/ns para invalidate. | Drop de colección: colección recibe drop→invalidate y cierra; DB/cliente reciben drop y siguen hasta una inserción barrera. Drop de base: colección recibe drop→invalidate, DB drop→dropDatabase→invalidate, cliente drop→dropDatabase y sigue. En las sondas invalidate carece de ns/documentKey. Añadir pruebas permanentes sync/async, continuidad tras recrear, cierre explícito/error/cancelación, filtro que oculta invalidate, rename soportado, resumeAfter/startAfter de token invalidate y replay persistente. Los últimos casos requieren caracterización adicional: no se dan por verificados con el drop básico. |

La revisión de implementación añade una caracterización del protocolo local
([sonda](evidence/bson-consolidation-implementation/watch-images/lifecycle-probe.json)):
los tres servidores coinciden en iteración/context manager/resume_token,
try_next cerrado devuelve None y next termina con StopIteration. None es un
full_document válido por defecto; las cadenas desconocidas/vacías fallan con 2.
Invalidate filtrado devuelve None y cierra; resumeAfter de invalidate falla al
abrir con 260, mientras startAfter permite la inserción posterior. Localmente
faltan protocolos, fallan clase/código/cierre y no llega la inserción posterior.
La alteración anidada del token ya coincide (280 y cierre): conservar ese control.

La [sonda posterior de continuidad](evidence/bson-consolidation-implementation/watch-lifecycle/receipt.json)
confirma una simplificación: updateLookup lee el documento nuevo tras drop y
recreate, también en los tres servidores; el local ya coincide. No introducir
una identidad de encarnación para cambiar esa conducta. La imagen capturada
conserva v1 cuando estaba disponible. Drop inexistente, en cambio, debe mantener
vivo el cursor y no emitir invalidate; la API local lo cierra indebidamente.
El efecto real debe determinarse dentro de la mutación/catálogo existente.

La causa de la pérdida posterior se deduce del camino actual: drop publica al
hub directamente, adelantando su token sin incrementar la secuencia del engine;
publish_committed descarta entonces el siguiente commit como ya entregado.
D04 debe incorporar DDL al mismo registro atómico/outbox y a la entrega actual,
con la vía existente de cambios de mantenimiento donde corresponda, sin cambiar
SPI v2 ni mantener dos productores de secuencias. El cierre por ámbito y el
protocolo del cursor se corrigen sobre ese orden único. Las sondas son sync y
caracterización; aún requieren testigos permanentes sync/async, iteración/cierre,
argumentos, offsets/tokens, fallos/reanudación y los controles de DDL sin efecto.

Las dos últimas filas tienen sondas nuevas, no aceptación: 16 escenarios de
imágenes por versión y par local; dos acciones DDL en tres ámbitos por versión
y par. Se usan bases UUID desechables en replica sets propios 7.0.43/8.0.32/
9.0.2, FCV correspondiente. Los fallos son compartidos por ambos motores.
La imagen y los eventos de DDL se observan por API pública, sin usar el helper
productivo para calcular el esperado. La sonda DDL retiene namespaces UUID;
solo se sustituyen para comparar estructura entre ejecuciones. No se compara
el valor opaco de tokens ni se acredita distribución/majority local.

Esta precisión concreta obligaciones dentro de D04; no modifica el contrato
ADR-006 ni promete retención nativa de pre/postimágenes o semántica distribuida.
La API ya admite los cuatro modos; no se cierra el bloque eliminando testigos o
reduciendo silenciosamente ese alcance. Cualquier límite explícito adicional
necesita disposición documentada antes de aceptar el bloque.

**Orden de trabajo vigente y simplificaciones.** D01a/D01b, D02 y D03a/D03b
conservan su cierre funcional y receipts; no se repiten sus extracciones.

| Orden | Entrega | Dependencias y salida |
| --- | --- | --- |
| 1 | Completar las garantías restantes de D04 | Preparación/contexto e imágenes CRUD conservan sus receipts aceptados; no repetir su implementación. Completar DDL, protocolo/reanudación e interacción con imágenes. Incorporar las sondas pendientes al registro/capturador/replay existentes, capturar y --check en 7/8/9. Un receipt por corte identifica la garantía aceptada; el padre D04 sigue abierto mientras quede alguna obligatoria. |
| 1, junto a los testigos físicos | Fixture replica set del job nativo | El workflow actual arranca standalone. Reutilizar imágenes fijadas y runner; inicializar/verificar primary/FCV, timeout acotado, cleanup incluso tras fallo, artefacto/wheel y corpus completo alcanzado. No crear otro runner ni llamar ejecución física a un replay de JSON; no cambiar goldens para acomodar la topología. |
| 2 | Completar D06a/b y D06c | Primitivas/gramática/raw ya entregadas: quedan aceptación de contextos/subpipelines y consumidores, con/sin hash/spill y Search según sus políticas. El éxito de DBRef literal básico no cierra esos caminos; no depende de terminar D04 para avanzar. |
| 3 | Disponer H53, conversión escalar y D07 | Mantener owners C03/B08 y conversión; contrastar sNaN en 7/8/9 y añadir regresión. D07 es decisión opcional continuar/descartar, sin migración ni logging nuevo implícitos. |
| 4 | D05 sobre el candidato congelado | Revisar bloques, compatibilidad persistida, matrices contractuales, wheel, costes/gates globales y commits semánticos. Reutilizar evidencia cuyo input no cambió; repetir solo gates afectados y el integrado final. |

Simplificar la documentación también forma parte del cierre: este plan posee
obligaciones y dependencias; el registro de implementación posee estados/gates;
los receipts poseen comandos, hashes y resultados. Los apartados de revisiones
anteriores son historia identificada, no otras listas activas. No crear nuevos
IDs por cada revisión del mismo adjunto ni copiar todas las matrices en cada
corte. Este refinamiento no acepta ni modifica el trabajo productivo en curso.

### Refinamiento de la revisión 9: disposición y mínima refactorización restante

El adjunto no aporta una reproducción nueva respecto de revisión 8. Sus
retractaciones son consistentes con las trazas históricas; sus cuatro huecos
adicionales ya forman parte del plan y tienen disposiciones diferentes:

| Observación | Contraste actual y decisión |
| --- | --- |
| H48 interno/local y Binary SDK | `bson_values.binary_parts`/`binary_order_key` reconocen las representaciones bytes con subtipo; comparación y clasificación delegan. Distinct observa payload/subtipo. Conservar propiedades D00 y regresiones D03a; no agregar otra tabla de clases. La herencia explica el fallo histórico, pero no debe ser la nueva autoridad semántica. |
| Eq/in SQL y tags históricos | `_is_translatable_equality_value` rechaza las formas binarias y membership conserva fallback. El test de rutas ejecuta SQL, verifica matcher y compara con traducción desactivada. No optimizar por tag ni reabrir D03b para satisfacer una preferencia por pushdown. |
| Literal DBRef y wire | `query_operators` comparte la decisión gramatical por contexto; `bson_raw` preserva el orden antes de la conversión SDK. La regresión básica pasa por directa y wire. D06a/b conservan contextos/subpipelines y H55; D06c conserva consumidores. El éxito del parser básico no equivale al cierre de esas obligaciones. |
| Watch: fase/clase/código | El corpus permanente `semantic_watch_cases` registra las tres dimensiones; preparación/contexto tiene receipt aceptado. Mantener esa regresión. DDL, opciones del cursor, cierre y reanudación siguen abiertos con sus testigos propios; no describir todo watch como corregido ni toda su preparación como candidata. |
| Frontera real del matcher | La sonda actual observa 18 filas en seis pares motor/dialecto, separando documento y operando. Es diagnóstico reproducible, no contrato de nombre de clase. Las propiedades entre representaciones y los resultados públicos son la garantía permanente. |
| TTL y fuentes | El cutoff SQLite actual usa `bson_datetime_milliseconds` entero y conserva residual exacto; la explicación flotante pertenece a la base histórica. No adoptar «retraso irrelevante» como garantía. Los 16 hashes versionados vuelven a coincidir; la lectura de r8.0.0 explica mecanismos, sin sustituir referencias 7/8/9. |

**Refactorización conjunta restante.** El patrón común es perder información
antes de decidir: subtipo al comparar bytes, orden documental al decodificar,
tipo físico al detectar modificaciones, contexto al preparar expresiones o
secuencia confirmada al publicar DDL. La solución compartida ya está repartida
entre vistas BSON, identidad, gramática/raw, preparación/contexto y efectos.
Mantener esas autoridades pequeñas y delegar desde los consumidores; un helper
universal de igualdad o un nuevo normalizador volverían a mezclar contratos.

Para el siguiente corte D04, la extracción útil y acotada es la coordinación
de entrega hoy situada en `CollectionRuntimeCoordinator`: preparación de engine,
hooks de commit y despacho en `finally` pueden compartirse con DDL de base/cliente.
No crear una colección ficticia para acceder a ellos ni replicar el protocolo
en cada API. Los motores registran el efecto real en la unidad atómica/outbox
existente; ámbito/cierre y materialización pública siguen siendo decisiones
del consumidor. Esta extracción depende de testigos DDL previos y debe conservar
entrega tras commits parciales, abort, fallos de publicación y extensiones SPI v2.

La aceptación restante se ejecuta en este orden, dentro de D04:

1. Incorporar drop/rename/dropDatabase y ausencia de efecto al mismo orden
   confirmado. Cubrir entradas públicas y comandos que ya soporten esas acciones;
   verificar barrera posterior, sin omisión ni duplicado, en los tres ámbitos.
2. Completar protocolo, argumentos, cierre/error/cancelación y resume/start,
   con caracterización nativa antes de fijar códigos o reglas no observadas.
   Probar que posición inicial, token y eventos procesados son coherentes ante
   escrituras entre apertura y lectura; cubrir filtros, retención y reapertura.
   El valor opaco del token no se compara con el del servidor.
3. Integrar imágenes y DDL —incluida recreación que ya coincide— y fixture RS
   de CI. Reutilizar receipts de preparación/imágenes cuando no cambien sus
   inputs; repetir los gates alcanzados por la extracción y el integrado final.

**Precisión de implementación D04.** Las nuevas sondas por ámbito obligan a
conservar dos distinciones: una colección que nunca existió no se invalida al
borrar su base; un rename invalida tanto el stream de origen como el del destino.
No usar una invalidación de base para cerrar indiscriminadamente todos los
streams de colección. Los nombres concretos del catálogo y el destino ya
conocido de rename bastan; no introducir identidad de encarnación.

La misma caracterización identifica H56 en la entrada de comando. Su corte
funcional queda registrado en D04 con capturas permanentes 7/8/9: admin frente
a base de origen, fase/clase/código y datos/eventos tras rechazo. Directa y
wire comparten la gramatica, y los fallos inyectados acreditan rollback de
dropTarget con recursos Search. El recurso atomico se exige antes de mutar;
rename ordinario conserva el metodo publico del proveedor. Mantener los
controles de ambas rutas sin fabricar handles para acceder a la entrega.
El subset sigue limitado a una base y la observacion local de escrituras wire
no acredita watch wire positivo.

El orden D06/H53/conversión/D07/D05 permanece en la tabla anterior. Las 88
comprobaciones focales y 408 de replay de la revision 9 usan sus capturas
existentes; no fueron recaptura fisica, suite global ni cierre del incremento.
Aquel corte fue solo documental y no cambio produccion/tests/goldens/receipts
anteriores. Los cortes de implementacion posteriores conservan su aceptacion
y limites en el registro vigente.

### Refinamiento de la revisión 10: cursor y aceptación sin duplicar autoridades

**Disposición del informe.** Sus retractaciones son correctas respecto de la
base histórica. Los huecos que señala ya tienen cobertura y owners en este
plan: representaciones/SDK/distinct en D03a, eq/in SQL en D03a/D03b,
gramática/raw DBRef en D06a/b y fase/clase/código de watch en D04. Los 16 hashes
versionados vuelven a coincidir; las trazas actuales observan 18 filas del
matcher en seis pares. Pasan 88 checks focales y 408 de replay de referencias
existentes. Esto no es una recaptura física ni amplía su aceptación histórica.
Mantener D00 desde el primer corte; no añadir otra infraestructura de propiedades.

**Eliminar trabajo ya realizado del orden activo.** La coordinación propuesta
en revisión 9 ya vive en `api/_change_delivery.py`; colección/base/cliente
delegan en ella. Efectos de namespace y rename administrativo/dropTarget tienen
receipts propios. No repetir esa extracción ni introducir otra secuencia,
outbox, contexto o identidad de encarnación. El orden activo pasa a ser:

| Prioridad y corte | Cambio mínimo | Dependencia y aceptación |
| --- | --- | --- |
| P1 D04: protocolo y cierre | Un bucle de lectura compartido para next/try_next; adaptadores sync/async para iteración y context manager. Cierre idempotente, cancelación y liberación de watchers/waiters. | Conservar preparación e imágenes aceptadas. Diferenciales 7/8/9 de cerrado, error de transformación, invalidate visible/filtrado y cancelación. No fijar un único resultado para carreras nativas sin caracterizarlas. |
| P1 D04: posición y reanudación, mismo corte lógico | Un decoder de token local y un punto coherente de apertura/avance bajo la sincronización existente del hub. Separar offset leído, checkpoint público y token del evento entregado. | Cubrir escritura entre apertura/lectura, filtro que descarta eventos, batch vacío, invalidate, resumeAfter frente a startAfter, retención, gaps y journal reabierto. No adelantar checkpoint sobre eventos no procesados ni cruzar un hueco silenciosamente. |
| P1 D04: integración | Imágenes más drop/rename/recreación y fixture replica set en CI, usando el mismo registro/capturador/replay. | Capturas y --check 7/8/9, scopes y superficies soportadas, wheel y cleanup del job. Mantener los controles de recreación que ya coinciden; watch wire positivo sigue fuera del subset actual. |
| P1 D06a/b/c | Completar contextos de gramática/raw, String/Code H55 y consumidores DBRef usando las vistas existentes. | No depende del cierre completo de D04. Resultado con/sin hash/spill; Search mantiene su garantía de candidatos. El literal DBRef básico ya corregido permanece como control. |
| Después: H53/conversión/D07/D05 | Disposiciones y gates integrados según sus owners y la tabla de revisión 8. | No convertir diagnóstico en aceptación; D07 sigue siendo opcional y requiere decisión continuar/descartar. |

**Robustez del decoder, dentro de D04.** La sonda local actual reproduce tres
fugas Python: token no documental y payload JSON array producen AttributeError;
un dígito Unicode produce ValueError. Además acepta versión booleana y posición
negativa. Añadir estos cinco testigos al contrato local del decoder, incluyendo
controles de tokens heredados válidos. Validar forma y dominio antes de usar
`.get`/`int`; no añadir un segundo parser al exponer resume_token. El formato
opaco nativo no se interpreta como el local y sus bytes no se comparan.
Cualquier metadato necesario para distinguir invalidate exige comprobar lectura
de tokens/journals anteriores y conservar los tokens ordinarios existentes;
no obliga de antemano a cambiar el formato ni la persistencia.

**Argumentos y fases pertenecen a la superficie observada.** El driver puede
seleccionar startAfter/resumeAfter antes de enviar el comando: PyMongo instalado
lo hace en `change_stream_shared._change_stream_options`. No trasladar la regla
del comando sobre combinaciones directamente a watch sin un testigo PyMongo.
Caracterizar opciones solas y combinadas con pipeline inválido, apertura con
backlog y primer getMore; compartir validación donde corresponda, conservando
su fase y precedencia. El reloj entero local no pasa a ser un Timestamp nativo
por compartir el nombre start_at_operation_time.

**Criterio de cierre del corte (precisado por revisión 11).** Registrar capacidades públicas reales, sin
que el adaptador de tests fabrique iteración/context manager/token faltantes.
El invalidate filtrado debe comportarse según la referencia de su fase concreta;
la sonda actual cierra pero vuelve al bucle y lanza OperationFailure sin código.
Un try_next vacío aislado no demuestra que el servidor haya procesado una
escritura ni que su checkpoint sea igual al token de esa escritura: comprobar
continuidad al reanudar y usar barreras. Pruebas deterministas locales cubren
intercalados, gaps y cleanup; diferenciales nativos cubren el comportamiento
público. Ninguno sustituye al otro. No cambiar observadores/goldens protegidos
ni aceptar todo D04 por el éxito de una sola de sus garantías.

La simplificación conjunta continúa siendo conservar información hasta la
decisión y delegar en owners pequeños. Para este corte basta el bucle del cursor,
el decoder y la coordinación atómica del hub; una jerarquía universal de valores,
tokens o contextos aumentaría autoridades sin resolver otra garantía.

**Disposición posterior de implementación.** Los cinco testigos del decoder
de revisión 10 se corrigen en la dependencia registrada en el
[owner de implementación](bson-consolidation-implementation.md). Su corpus añade
admisión negativa y continuidad positiva con capturas/--check 7/8/9; conserva
tokens heredados y no cambia formato/journal. No repetir esa corrección al
completar el bucle y checkpoint, ni confundirla con el cierre del corte conjunto
de protocolo/reanudación o con la precedencia de todas las opciones de watch.

El corte posterior de protocolos concentra lectura/limpieza en el cursor y
añade iteración/context manager, cierre tras invalidate filtrado y cancelación.
Conserva su aceptación acotada en el mismo registro. El siguiente trabajo D04
se apoya en ese bucle y en el decoder ya entregados: checkpoint público,
apertura/avance atómicos, retención/gaps y precedencia de opciones. No repetir
las dos extracciones ni presentar esos cimientos como cierre de reanudación.

La entrega posterior de checkpoint registra getter owned, bootstrap/avance
atómicos, invalidación/reanudación y guardas de huecos/retención en el mismo
owner. Conserva los tokens de eventos ordinarios; la metadata opcional de
checkpoint/invalidate no exige migrar el journal. La aceptación distingue los
veinte escenarios nativos por major de las pruebas locales de intercalados y
reapertura. Quedan D04 primera tanda/backlog, opciones y precedencias restantes,
imágenes y fixture RS; no repetir la extracción del decoder, bucle o checkpoint.

### Refinamiento de la revisión 11: reducir el trabajo activo, conservar las fronteras

**Contraste del adjunto.** Es el mismo input de revisiones 3–10. Sus
retractaciones explican la caracterización histórica, no nuevos fallos del
runtime actual. Rango/vistas Binary, bytes/subtipo 0, SDK/distinct, fallback
eq/in SQL y clasificación de tipos tienen aceptación D03a/b; identidad y
fechas tienen sus cortes propios. La nueva traza conserva 18 observaciones de
documento/operando en seis pares motor/dialecto. Los 16 hashes oficiales
r8.0.0 coinciden otra vez. No convertir «retraso TTL irrelevante» en contrato:
el cutoff actual es entero; residual exacto y auxiliares previos conservan sus
garantías de D02. D00 sigue siendo transversal desde el primer corte.

**Un solo orden activo.** Esta tabla sustituye las tareas pendientes de las
revisiones 9/10, no sus resultados históricos. El registro de implementación
continúa siendo el único owner de aceptación; el árbol sin commitear no es
una entrega. No repetir decoder, lectura común, checkpoint o entrega DDL ya
aceptados, ni volver a extraer clasificación, identidad o fechas.

| Prioridad / corte | Trabajo restante mínimo | Dependencia y prueba de cierre |
| --- | --- | --- |
| D04, primera tanda y opciones — entregado funcionalmente | Inicialización/batches y validación por fase en el cursor existente; el registro de implementación posee su aceptación. No repetir la extracción. | Capturas/`--check` 7/8/9 y regresiones de backlog, precedencia, cantidad/bytes, filtros, anchor, overflow, imágenes prefetched y recursos. La integración de la fila siguiente conserva su pendiente. |
| P1 D04, integración | Interacción de imágenes con drop/rename/recreación y fixture replica set de CI. | Cortes aceptados y primera tanda validada. Barreras y continuidad al reanudar, errores/ausencia/null por modo y ámbito, cleanup del job, paquete instalado. Watch wire positivo permanece fuera del subset; no añadirlo para completar artificialmente una matriz. |
| P1 D06a/b, gramática y raw | Completar contextos/subpipelines DBRef y H55 String/Code sobre las autoridades existentes. | Primitivas D03a/raw entregadas; independiente de terminar D04. `$`/`$$` como String y Code con/sin scope, `$literal` control; orden y extras DBRef en directa/wire, vacío/poblado y fase/clase/código. Validar antes de perder representación; no excepción DBRef global en todo documento. |
| P1 D06c, consumidores | Completar group/sets/lookup/spill/Search y sus claves, retirando duplicación solo al sustituir también sus guardas. | Vistas/gramática aplicables de D06a/b. Igualdad implica misma clave dentro de cada política; resultados con/sin hash/spill y conservación de candidatos Search. Conservar claves persistidas y desempates; unificar átomos, no todas las políticas. |
| P2 H53 y conversión | Disponer observabilidad en C03/B08; completar contraste/regresión sNaN 7/8/9 en conversión. | Diferenciar permiso, evaluación y warning. No afirmar que H53 rechaza mal escrituras ni crear un canal de logs implícito. Un diagnóstico local no sustituye al testigo nativo. |
| D07 / D05 | Decidir continuar/descartar el piloto opcional; cerrar gates integrados de entregas obligatorias. | D07 no bloquea fixes ni fuerza migración schema. D05 conserva wheel inmutable, matrices Python/SDK/pytest, SPI, suite/cobertura, persistencia previa y costes; un replay focal no los sustituye. |

**Tres precisiones que evitan errores espejo de aceptación.**

- La misma invalidación necesita testigos distintos en aggregate inicial y
  getMore. Las capturas nuevas del candidato 7/8/9 registran `260`, fase lectura
  y `alive=True` para invalidación filtrada en primera tanda vacía; no exigir
  cierre universal basándose en el caso de lectura posterior. Fijar también
  clase/código/nombre y continuidad, conservando la versión del driver.
- Los helpers de pruebas deben recibir el cliente async real explícitamente.
  `collection.database.client` puede resolver una colección llamada `client`
  en la API local y acreditar un ámbito falso. Corregir/repetir los escenarios
  afectados; no cambiar la API para satisfacer el helper ni invalidar las
  superficies cuya inyección de cliente ya era correcta. Las capturas antiguas
  conservan su alcance y no se reescriben.
- Las transformaciones de una respuesta y su materialización pública tienen
  momentos distintos. Un error en un documento posterior puede impedir toda
  la tanda; el límite puede requerir evaluar un documento aún no entregado.
  Probar que no se decodifica dos veces, no cambia la imagen ya prefetched y
  no se publica checkpoint sobre filas aún no consumidas. Los casos de 6 MiB
  separan tandas, pero no acreditan el byte exacto del límite con otro envelope
  o token opaco: medir esa frontera antes de prometerla.

**Simplificación conjunta.** Conservar `bson_values` para vistas/clasificación,
`bson_identity` para modificación física, `bson_scalars` para fechas,
gramática/raw para interpretación y preservación, y preparación/contexto para
ejecución. Watch solo añade coordinación de respuesta en su cursor, sobre el
hub existente; los errores aritméticos pertenecen al evaluador compartido,
no a un traductor especial de watch. No añadir otro normalizador, intérprete,
jerarquía de valores ni infraestructura diferencial. Antes de extraer otro
helper, explicitar el invariante común y los testigos de sus callers;
compartir apariencia no demuestra compartir semántica.

Esta revisión ejecuta regresiones sobre el candidato identificado, reutiliza
referencias nativas y verifica fuentes; no realiza nuevas sondas físicas,
no modifica producción/tests ni cierra D04/D05/B07. La aceptación del trabajo
en curso requiere sus gates y commit propios, incluido el lint pendiente
observado en `hub.py`; documentación verde no lo subsana.

### Refinamiento de la revisión 12: procedencia de resultados y cierre del candidato

**El informe no es evidencia de un defecto todavía presente.** Es el mismo
adjunto de revisiones 3–11. Sus retractaciones sobre H48, `$in`, distinct,
D00 y TTL se conservan. Parser DBRef básico, reconocimiento Binary y eq/in SQL
ya tienen correcciones aceptadas; no repetirlas. D06 conserva contextos y
consumidores pendientes. Las 18 trazas nuevas del matcher en seis pares locales
confirman eq/in/subtipos; son diagnóstico de representación, sin convertir
nombres internos en API. Los 16 hashes oficiales r8.0.0 vuelven a coincidir.
TTL/H53 no se han vuelto a sondear: no reinterpretar «retraso irrelevante» ni
167 pruebas históricas como garantías nuevas.

**Causa común, precisada.** Reconocer una familia BSON, conservar identidad de
escritura y materializar una respuesta son tres decisiones distintas. La
materialización también necesita conocer la procedencia: PyMongo conserva el
`Binary(..., 0)` asignado a `inserted_id`, mientras una lectura o un ID de upsert
recibido del servidor se decodifica como `bytes`. Ambos codifican el mismo BSON;
la igualdad semántica, un JSON Extended decodificado o la coincidencia entre
motores pueden ocultar la diferencia pública.

| Contraste actual | Decisión y garantía pendiente |
| --- | --- |
| El candidato comparte materialización de lecturas/streams y convierte Binary 0 a bytes. El corpus nuevo separa IDs asignados y devueltos por el servidor. | Completar el censo de salidas, incluido metadatos y errores. Reutilizar el materializador de respuestas existente; preservar las conversiones de IDs asignados. No activar un booleano global en todos los callers de `to_pymongo`, ni introducir otra jerarquía de tipos. Conservar dos fases frente a decoders del usuario, opciones UUID y perfil sin SDK. |
| **Hueco reproducido:** `collection.options()` devuelve SDK Binary 0 en ambos motores/dialectos 7/8/9; los tres servidores devuelven bytes para el mismo validator. | Incluir esta salida antes de aceptar el corte de materialización. Añadir testigo permanente sync/async y wire en las superficies soportadas, y control Binary 5. Verificar por separado codec_options antes de extender esa política a metadatos. La sonda física actual es sync: no acredita esas otras superficies. |
| El caller de `BulkWriteError` parece usar conversión de IDs asignados, pero el ID de upsert ya llega convertido. | La sonda pública nativa/local coincide: ID de upsert es bytes y el ID del `op` original conserva Binary. Mantener ambos como controles en error parcial ordered/unordered; no corregir un caller basándose solo en su texto. |
| La guarda SQL histórica reconocía tag bytes, mientras sort podía seguir usando payload JSON para tag binary. El candidato amplía una detección compartida a bytes/binary/uuid. | Verificar todas las entradas de ordenación antes de skip/limit, índice, ruta punteada, caché negativa antes de mutar y reapertura antigua. Un filtro canónico correcto no acredita el orden/window del fast path. No reintroducir pushdown por tag; mantener una consulta de detección y la invalidación de caché actuales. Es ampliación con aceptación propia, no retirada del cierre D03a/D03b. |
| El candidato valida combinaciones `$merge` en preparación y ejecución mediante una regla compartida; analiza contenedores antes de optimizar expresiones. | Incorporar sus códigos 51181/51185 y estado tras error al mismo corte de preparación. La sonda nueva contrasta 8 escenarios por servidor: modo inválido prevalece frente a división constante, expresión desconocida, aridad y variable indefinida, vacío/poblado; local coincide. No inferir toda precedencia de errores ni mover cada validación a una pasada global. Conservar placement, registro de extensiones, dirección lógica y scopes anidados. |

Las dos sondas físicas usan bases UUID con limpieza en `finally`, MongoDB
7.0.43/8.0.32/9.0.2 y FCV correspondiente. Observan clases Python antes de su
serialización y bytes BSON con el encoder SDK independiente. El replay verde
no subsana el hueco de options: ninguno de sus testigos nuevos lo ejercita.
Las sondas deben incorporarse al capturador/replay existentes antes de aceptar
sus correcciones; scripts de diagnóstico no son otro runner permanente.

**Orden activo, con dependencias explícitas.** Sustituye la tabla activa de
revisión 11; las garantías no repetidas allí ni aquí conservan sus owners.

| Prioridad / bloque | Entrega mínima y dependencia | Condición de salida |
| --- | --- | --- |
| P1, candidato actual de D04 e integración BSON alcanzada | Cerrar imágenes × drop/rename/dropDatabase × recreación × ámbitos, materialización de respuestas (incluido options), orden SQL y preparación `$merge`. Usar los owners existentes; revisar los cambios ya escritos antes de añadir abstracciones. | Capturas y `--check` 7/8/9 sobre corpus definitivo; controles de ausencia/null, imagen capturada frente a updateLookup y continuidad tras resume/start. Identidad pública y BSON de lecturas/IDs/metadatos/errores; sort antes de ventana; precedencia y destino intacto. Baseline aislada, revisión, persistencia previa, perfil sin SDK, coste medido, wheel y gates del corte. No aceptar todo el candidato porque un subconjunto pase. |
| P1, fixture RS de D04 | Integrar la topología necesaria en el job existente, reutilizando matriz de imágenes fijadas, runner y capturador. Expresar capacidades requeridas en el registro de casos; hay consumidores de watch fuera del prefijo watch. | Selección canónica compartida entre listado, suite y reporte; ningún caso perdido entre particiones standalone/RS, ningún run vacío presentado como éxito físico. Verificar primary/buildInfo/FCV y wheel real; timeout y cleanup de recursos propios ante fallo/cancelación. Si se conservan dos fixtures, no duplicar matriz ni runner. El workflow actual sigue siendo standalone y no acredita esta garantía. |
| P1, D06a/b y D06c | Completar contextos DBRef/raw y H55 String/Code; después consumidores group/sets/lookup/spill/Search según política. No depende de cerrar el fixture de D04. | Diferenciales por contexto/superficie y con/sin hash/spill; igualdad/claves y candidatos Search, sin cambiar formatos persistidos. Mantener literales/parser básicos como controles. |
| P2, H53 y conversión | Disposición explícita C03/B08 y regresión sNaN 7/8/9. | Separar evaluación, permiso y warning; ningún canal nuevo de logging por implicación. Conservar límites de lo observado. |
| D07 y D05 | Decidir continuar/descartar piloto opcional; aceptación integrada sobre candidato congelado. | Matrices, wheel, SPI, cobertura, persistencia y costes del owner D05. Reutilizar evidencia con inputs idénticos; repetir gates alcanzados y global final. Commits semánticos y árbol revisado, sin confundir commit con aceptación. |

**Simplificaciones aplicables sin perder garantías.** Una sola frontera de
materialización para respuestas, con adaptadores pequeños para resultados
escalares solo donde hagan falta; una sola detección de familia binaria para
las guardas SQL; una regla de modos `$merge` compartida por preparación y
entrada directa. Mantener la política propia de cada consumidor y el reloj de
ejecución existente. El estado de los bloques pertenece al registro de
implementación; esta tabla posee prioridades y dependencias. Los receipts
poseen comandos/resultados y los corpus poseen regresiones: no copiar una
segunda matriz de aceptación en cada documento ni crear otro ID por cada
relectura del mismo informe.

Esta revisión no cambia producción/tests ni acepta el candidato sin commitear.
Pasan 172 pruebas focales y 486 de replay, y el lint del candidato pasa ahora;
no se repite la suite global ni se acredita CI, wheel o rendimiento. Los hashes
del candidato constan en el receipt. El hueco de options permanece reproducido
y debe corregirse durante la implementación, antes de aceptar su corte.

### Refinamiento de la revisión 13: un solo orden activo y aceptación por frontera

La autocrítica no añade un informe distinto. Sus retractaciones son correctas:
H48 ya describía el operando normalizado, `$in` y distinct ya estaban en las
familias, y las 167 pruebas eran regresión histórica. La observación TTL no
acredita un retraso universal de un milisegundo: había preselección y residual
diferentes; D02 ya entrega cálculo entero y migración de auxiliares. Conservar
sus controles, sin reabrirlo ni retirar la obligación de reapertura antigua.

**Contraste actual.** Las 18 trazas nuevas en seis pares motor/dialecto observan
por separado documento y operando: Binary local, SDK e interno tienen rango 6;
bytes/Binary 0 seleccionan 1/2 y Binary 5 selecciona 3. Distinct conserva los dos
subtipos, y comparación/clave distinguen 0 de 5. La API directa acepta el literal
DBRef ordenado y no selecciona el documento invertido. La guarda SQL rechaza
la traducción binaria por tags; no se propone sustituirla por otro pushdown.
Son diagnósticos locales actuales, no otra captura física ni cierre global.
La evidencia nativa versionada y la aceptación de los cortes conservan sus
receipts propios; este contraste no vuelve a ejecutarlas ni cambia sus hashes.

**Diagnóstico histórico de revisión 13, anterior a los cortes D06 actuales.**
H55 seguía reproducible: `Code("$v")` producía el valor del campo y
`Code("$$missing")` lanzaba 17276 tanto en preparación como en ejecución; `$literal`
conservaba Code. Era un error compartido de clasificación gramatical: que ambas
rutas coincidan no las convierte en oráculo. El diagnóstico usa los tres
dialectos; las referencias nativas previas mantienen su alcance.

**Orden activo.** Sustituye las instrucciones de ejecución de revisión 12.
Los estados de entrega y gates siguen perteneciendo al registro de implementación.
D04 ya tiene cierre funcional registrado con el fixture `0b5ac53`; conservar
sus controles y límites de ejecución local. El siguiente corte es D06a/b.

### Contraste actual y orden mínimo de ejecución

La revisión 20 se realizó sobre HEAD `79d5a62` y el entonces candidato de
mutaciones sin commitear, identificado en la [revisión 20](evidence/bson-consolidation-review/review20/README.md).
El adjunto mantiene SHA-256 `a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`:
no introduce hallazgos nuevos respecto de las revisiones anteriores. Sus cuatro
huecos ya están incorporados y tienen aceptación o pendientes por consumidor.
En este árbol pasan 186 regresiones Binary/SQL/TTL/watch/codificador, 49 controles
del candidato de mutaciones y 18 trazas del matcher en seis pares motor/dialecto.
Son comprobaciones sobre checkout, no aceptación instalada ni cierre de B07.
La revisión 19 conserva sus 16 descargas y hashes r8.0.0; esta revisión verifica
la integridad de ese receipt y relee el parser oficial, sin atribuirse otra
verificación completa por descarga ni nuevas ejecuciones MongoDB.

Conversión, lectura, inserciones, mutaciones y el corte bulk tienen
aceptación propia; no repetir su
extracción. El registro de implementación posee el estado aceptado de cada
corte. Este apartado mantiene el único orden actual; las caracterizaciones y
receipts anteriores conservan sus límites históricos.

**Recontraste vigente sobre `fe81fcf8`, con runtime/tests limpios.** El adjunto
conserva SHA-256 `a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`:
es la misma autocrítica. El [diagnóstico actual](evidence/bson-consolidation-review/current-recheck.json)
registra 153 regresiones focales correctas y la sonda existente sin cambiar
aserciones: seis pares motor/dialecto, 18 trazas, eq/in con IDs completos y
distinct con payload/subtipo. Las 16 fuentes oficiales r8.0.0 vuelven a
descargarse y coinciden con sus hashes; incluyen las 14 del manifiesto original.
El parser comprueba nombres y contexto, no los tipos del decoder SDK. Estas
comprobaciones sobre checkout no son nuevas capturas MongoDB, aceptación
instalada ni cierre global.

Los cuatro huecos conservan sus disposiciones: Binary SDK y representación
del matcher en D03a/D00; eq/in SQL con fallback binario en D03a/D03b; literal
DBRef básico aceptado, con contextos restantes en D06; clase/código/fase de
watch en D04. No se reabren por repetir el informe. H53 conserva su disposición
pendiente y las 167 pruebas citadas son históricas. La entrega posterior `2723fb28` acepta las cinco entradas Search después de
los cortes administrativos y namespace. `d5bdadb8` entrega después UUID coordinado;
el siguiente corte es positivos/respuestas, no repetir admisión. Los 24 nodos restantes de admission_options y dos de batching
siguen visibles allí: no hay cierre global.

**Simplificación de la aceptación pendiente.** Usar los owners y el runner
existentes, con una ficha por corte: frontera/consumidor, invariante, testigo
nativo cuando corresponda, fallo en la base, resultado candidato y alcance
pendiente. No crear otra matriz paralela ni otro recibo de aceptación por cada
relectura del mismo informe. La ficha distingue aceptación existente, hipótesis
por inspección y fallo reproducido; especifica también la disposición de cada
entrada delegada. Una delegación solo acredita reuse si conserva el original,
la fase y las opciones del dueño, codifica una vez y no repite callbacks. No
exigir una implementación diferente por cada método del censo. En Search,
separar admisión cliente contrastable con el driver de capacidades de servidor:
un standalone sin Atlas no acredita resultados positivos de Search.

Para las cinco entradas Search (`create_search_index`, `create_search_indexes`,
`list_search_indexes`, `update_search_index`, `drop_search_index`), el corte
comparte admisión de solicitudes y ejecución preparada donde la construcción
real del driver lo permita; no fuerza un único sobre para métodos diferentes.
Determinar dueño de codecs, construcción del modelo, precedencia y apertura
antes de extraer el helper. Observar callbacks/efectos y delegación sin segunda
codificación. Distinguir rechazo cliente antes del envío, rechazo de comando en
standalone y resultado positivo local: solo los dos primeros tienen aquí
oráculo nativo disponible. No normalizar el error de capacidad para fingir
paridad Atlas. Un positivo Atlas exige referencia propia o límite explícito;
no bloquea resolver la frontera cliente. El default UUID cambia después de
disponer las cinco entradas, incluyendo delegadas, con aceptación coordinada.

Conservar cinco obligaciones independientes:

- **Representación:** observar documento y operando en la frontera real; las
  clases privadas son diagnóstico, mientras rango, subtipo, bytes y resultado
  público son las garantías. Reusar `binary_parts`/`binary_order_key`.
- **Claves:** igualdad implica misma clave dentro de la política del consumidor;
  añadir controles negativos de subtipo/orden/scope. Si la clave solo selecciona
  candidatos, exigir inclusión y residual, no ausencia de colisiones. Si define
  grupos sin residual, exigir también que no fusione valores distintos según
  esa política. No imponer una igualdad universal entre consulta/grupo/Search.
- **Especialización:** acreditar la ruta SQL/hash efectivamente ejecutada y
  compararla con su fallback antes de skip/limit/top-k. Conservar fallback
  binario; una mejora de rendimiento no es dependencia del arreglo semántico.
- **Fases y contexto:** watch y comandos comparan apertura/consumo, clase y
  código de error, además del resultado. Las entradas públicas codifican una
  vez; las internas ya decodificadas reutilizan el ejecutor preparado. Admitir
  un comando DDL crudo no acepta por asociación create_collection/create_index/
  create_indexes ni su precedencia de validación.
- **Tiempo y efectos:** TTL separa candidatos, residual y cadencia; escrituras
  separan admisión, confirmación y publicación. Un residual correcto no recupera
  candidatos omitidos. No aceptar «un milisegundo irrelevante» ni ausencia de
  eventos basada únicamente en un timeout.

El orden activo es positivos/respuestas; contextos D06;
disposición H53/D07; integración D05. La admisión entregada queda como control
previo, sin repetir su implementación. La historia y sus receipts conservan
sus límites; el registro de implementación posee la evidencia de aceptación.

| Afirmación del informe | Contraste actual y decisión |
| --- | --- |
| H48 cruza decodificación, normalización y materialización; interno/local y SDK son mecanismos diferentes. | Correcto como diagnóstico histórico. `binary_parts` reconoce bytes y sus wrappers, y comparación/orden usan `binary_order_key`, incluida la longitud física del subtipo 2. Conservar pruebas separadas de rango, igualdad, subtipo y contenido público; no añadir otra tabla de clases. |
| Distinct necesita dos Binary SDK de subtipos distintos, y eq/in ya figuraban en el plan. | Adoptado en D03a. Conservar payload idéntico con subtipos 0/5 y bytes, variando orden y representaciones. No deducir corrección por cardinalidad. Los DBRef que colapsan al decodificar son otro testigo: comparar/deduplicar antes de materializar cuando lo exija el contrato del consumidor. |
| Eq/in SQL por tag omite Binary 0 equivalente a bytes. | La guarda de igualdad y `_translate_membership` ya declinan formas binarias mediante `binary_parts`. Conservar controles de SQL realmente ejecutado frente a matcher y fallback. El comentario de sort no era garantía de igualdad; tampoco hay motivo para reintroducir pushdown ahora. |
| El literal DBRef se rechaza y el decoder wire puede ocultarlo. | El parser común actual reconoce el literal por primer campo/contexto y presencia de campos. El parser oficial r8.0.0 comprueba nombres, no tipos. Conservar controles de orden, extras, incompletos y tipos de `$ref`/`$db`; no sustituir gramática por el predicado del decoder SDK ni aceptar todos los documentos con claves `$`. |
| La frontera del matcher necesita observar ambas representaciones. | Mantener trazas separadas de documento y operando, además del resultado público. Las propiedades de `test_bson_binary_coherence.py` ya cubren igualdad⇒clave entre formas. D00 sigue acompañando cada corte; las clases privadas observadas no se convierten en contrato público. |
| TTL solo puede retrasarse un milisegundo y es irrelevante. | No adoptar esa cota ni esa tolerancia. SQLite actual calcula auxiliares/cutoff con milisegundos enteros y conserva residual exacto; D02 prueba migración/reapertura. El residual impide ciertos borrados indebidos, pero nunca recupera un candidato omitido. Separar inclusión de candidatos, decisión de expiración y cadencia del monitor. |
| Watch exige clase/código/fase; H53 y fuentes no estaban totalmente verificados. | Clase/código/fase y contexto ya forman parte de la aceptación D04. H53 mantiene disposición pendiente C03/B08. Revisión 15 registra 16 hashes oficiales r8.0.0 coincidentes: los 14 del manifiesto original más parser y pipeline_executor. La revisión 19 vuelve a descargar y verificar las 16 fuentes: coinciden con los hashes fijados, incluidos los 14 del manifiesto original. Las fuentes explican mecanismos; no sustituyen diferenciales 7/8/9. |

**Caracterización histórica de revisión 14.** Se repiten las 78 pruebas focales de coherencia
Binary, gramática auxiliar, traductor SQL, migración TTL y contexto watch: pasan.
Las sondas locales nuevas observan en ambos motores que un `pull(1)` interno
evalúa cinco filas antes de devolver una; el pipeline de entrada pequeña/salida
grande con error de filtro en la quinta fila falla al abrir sin entregar filas.
También observan una asimetría del candidato: `$concat` rechaza una String de
16.777.216 bytes con 16493 en el dialecto predeterminado, mientras `$replaceAll`
la devuelve; `$concat` con cola null devuelve null. Son caracterizaciones locales,
no aceptación ni nueva divergencia nativa demostrada. La causa de la anticipación
se deduce de `_CursorPageSource → __aiter__ → _fetch_batch`: pedir una fila al
buffer no limita la evaluación que lo llena. Falta su diferencial exacto 7/8/9.
No se ejecutan servidores MongoDB ni nuevas capturas en esta revisión. Las 167
pruebas citadas por el informe son históricas. El corte de sufijos por cantidad
aceptado conserva su receipt y no acredita bytes/callbacks.

**Resultado de revisión 15, conservado como historia.** Las 78 regresiones focales siguen pasando.
El corpus `bson_dispatch` preexistente, con seis acciones y referencias 7/8/9,
da 111 casos correctos y 36 fallos en el candidato: 12 por familia de dispatch
escalar, pase único y orden de callbacks DBRef, todos en wire. Las comparaciones
de los archivos de captura/`--check` preexistentes coinciden; no se ejecuta un
servidor nuevo ni se acepta por ello el corpus o el candidato. Las unitarias
de codec/materialización/configuración dan 101 correctas, 21 subtests y un fallo
por el cambio del default UUID. La sonda SDK-free confirma que el fallback omite
callbacks dict/list anidados; un control con el mismo Python 3.13 y el SDK añadido
recupera esos callbacks. El helper portable UUID aún transforma Binary 4 canónico
a subtipo 3 bajo modos legacy, frente a Binary 4 conservado por el SDK. Ambos
motores aceptan UUID crudo con el nuevo default, mientras la codificación SDK
predeterminada lo rechaza con ValueError. Estos son pendientes del corte actual,
sin retirar la aceptación acotada de `381aa10`.

**Actualización de ejecución.** El corte raw de contenido/apertura/cantidades
`30c8ae2` ya tiene su aceptación acotada en el [registro](bson-consolidation-implementation.md#d06ab--lecturas-raw-apertura-y-cantidades).
El corte posterior `381aa10` acepta bytes/callbacks, capacidad String calculada
y sus controles de preparación/cierre en el
[registro de fronteras](bson-consolidation-implementation.md#d06ab--fronteras-por-bytes-capacidad-y-efectos-de-codec).
Conservar ambos conjuntos de controles; no volver a extraer el lector ni
reabrir sus fallos corregidos por repetir el adjunto. Siguen los demás contextos
D06a/b y los consumidores D06c, según sus dependencias. Ni el replay raw anterior
ni este corte acotado declaran D06/D05 completos.

El corte posterior `a53824d` entrega dispatch/consumo de respuestas con
[aceptación propia](evidence/bson-consolidation-implementation/codec-dispatch/README.md):
15 acciones nativas 7/8/9, 363 focales y 7.810 casos de replay instalado.
Los fallos de revisión 15 quedan como caracterización anterior a ese arreglo.
La entrega mantiene STANDARD como default UUID y coordina su cambio futuro
con B07. Los contextos D06a/b restantes, incluidas fábricas documentales con
efectos propios y otras combinaciones de consumo/estado y representación
portable, conservan aceptación pendiente; D06c y los demás owners no se cierran.

El corte posterior `d4caba5` entrega construcción y asignación de documentos de
respuesta con [aceptación acotada](evidence/bson-consolidation-implementation/document-factories/README.md):
cuatro acciones nativas standalone 7/8/9, 7.909 checks sobre wheel instalado,
362 unitarias/contratos y 72 escenarios nuevos realmente sin SDK. Preserva la
raíz del comando, selección intermedia y errores de constructor/setter; el
fixture/preflight distingue la topología comparable sin alterar los manifiestos
históricos. Conservar estos controles, sin repetir la extracción del mecanismo.
Siguen otros contextos/representaciones y consumo/estado público D06a/b, además
de D06c y los owners restantes; no se declara equivalencia de todos los efectos
de un mapping ni cierre completo de D06/D05.

El corte `72edfa3` añade posición/estado y propiedad de cursores sync con
[aceptación propia](evidence/bson-consolidation-implementation/cursor-consumption/README.md):
cinco acciones nativas 7/8/9, 8.032 checks sobre wheel instalado sin avisos y
30 comparaciones/1.278 trazas realmente sin SDK. Se retiran cachés y finalizadores
prestados; el cierre llega al delegado real y el runner dispone de forma común
los awaitables rechazados. Conservar controles de reanudación y de abandono del
propietario, sin reintroducir el estado suspended. La interfaz del fake y la
limpieza del propio test también necesitan oráculos independientes. Los demás
contextos/representaciones D06a/b y los otros bloques mantienen sus owners;
D05 conserva el coste observado de find(limit=1), sin declararlo irrelevante.

**Contraste nuevo de revisión 16.** Pasan 85 regresiones focales. Las seis
combinaciones locales motor/dialecto conservan selección bytes/Binary 0 frente
a 5, contenido/subtipo de distinct y 18 trazas separadas de documento/operando.
No se vuelven a ejecutar MongoDB ni capturas. Un control independiente con la
API pública BSON del SDK construye cuatro documentos al decodificar una
envoltura completa cursor/ok, frente a tres del materializador local, aunque
las filas son iguales. Esto identifica una frontera aún pendiente, no demuestra
por sí solo el protocolo nativo ni la decodificación selectiva del driver.
Conservar las fábricas con efectos como criterio propio; no ampliar por ello
el soporte público a clases/opciones actualmente no admitidas.

**Causa conjunta y refactorización.** Los defectos comparten pérdida de información
antes de que su consumidor decida: subtipo al comparar, orden al decodificar,
precisión al convertir o fase/contexto al ejecutar. La solución común consiste
en conservar la representación hasta la frontera que posee la decisión. Reusar
vistas BSON, identidad física, gramática y contexto existentes; cada consumidor
conserva su equivalencia. No crear un comparador universal ni un `mode` que mezcle
consulta, grupo, Search e identidad de modificación.

La respuesta del cursor ya comparte `ResponseBatchReader` y pospone la
materialización hasta completar la respuesta en el corte aceptado. Reutilizar
ese componente y el cálculo BSON existente, sin otra abstracción equivalente.
Demanda interna, formación de respuesta y decodificación pública conservan
dueños distintos, incluidas las rutas legacy. Raw y el servidor wire consumen
BSON sin codecs del usuario; el cliente wire aplica sus propios decoders SDK.
Los adaptadores mantienen sus reglas de apertura/getMore;
no duplicar evaluador, contexto ni almacén de cursores. Las vistas de tipos,
identidad física, gramática y límites de valores siguen siendo primitivas
separadas: una clase universal no reduce estas obligaciones. La pérdida de
PreparedPipeline se corrige conservando slices; no exige otra caché.

| Orden / prioridad | Trabajo y dependencia | Aceptación / owner |
| --- | --- | --- |
| Control previo cerrado — B07, admisión | `2723fb28` acepta las [cinco entradas Search](evidence/bson-consolidation-implementation/bson-input-search/README.md), opciones propias y creación preparada compartida. Completa las 17 disposiciones del censo administrativo histórico tras comandos, catálogo, índices, lectores y namespace, con sus aceptaciones en el registro. Lecturas, inserciones, mutaciones y bulk conservan sus cortes propios. No implica positivos Atlas ni una API universal ya censada. | Conservar originales, codificar una vez, errores cliente/precedencia y efectos. El wire ya decodificado no vuelve a codificarse; unidad 5 conserva positivos y batching. |
| Control previo cerrado — B07, UUID | `d5bdadb8` acepta [default UNSPECIFIED, dueños y lectura independiente](evidence/bson-consolidation-implementation/bson-uuid-policy/README.md), sobre la admisión ya entregada. `b29e9bf1` corrige ausencia en distinct, compartida por API/comando. | Mantener STANDARD/legacy explícitos, bytes/subtipos y lectura/reapertura de archivos previos. No cambiar otra vez el default ni reextraer el codec; no implica cierre D06 ni positivos completos. |
| Control funcional cerrado — B07, unidad 5 | `8e261c7a` acepta proyecciones, `dbe8257f` batching, `05b3f655` DTOs/sobres y `a8a81f0e` [constructores/setters y efectos confirmados](evidence/bson-consolidation-implementation/write-factory-failures/README.md). API y comando reutilizan el DTO atómico; se retiran las dos copias/capas privadas posteriores al codec. | 10.825 replay y 2.457/4.709 subtests sin fallos; 12.068 de identidad; standalone para callbacks y RS para eventos. Subset portable real y costes acotados en el receipt. La unidad 6 conserva integración D05; no reabrir fixes ni acreditar otras transformaciones de respuesta por asociación. |
| Cierre vigente — O1–O8 | La [lista única de obligaciones](bson-consolidation-implementation.md#obligaciones-de-cierre-vigentes) sustituye la secuencia pendiente de esta tabla; O1–O8 están cerradas. | D06, disposición H53, descarte D07 e integración D05 aceptados en el [receipt final](evidence/bson-consolidation-implementation/bson-grammar-context/d05-final-acceptance.json). Se mantienen los límites y seguimientos expresos; no queda otro backlog activo en las revisiones históricas. |

**Relectura de la autocrítica sobre el árbol actual.** El adjunto conserva el
SHA-256 ya identificado desde revisión 3. El [contraste reproducible actual](evidence/bson-consolidation-review/review21/README.md)
no abre nuevos IDs: distingue los mecanismos históricos de H48/H50/H51 de sus
arreglos aceptados y del candidato de proyecciones todavía sin aceptación.
Las retractaciones sobre causa, `$in`, distinct y las 167 pruebas se mantienen;
no se heredan sus resultados como gates actuales. La sonda de representación
repetida observa seis pares motor/dialecto y 18 filas; pasan 78 regresiones
focales. Las 16 fuentes oficiales descargadas coinciden con sus hashes fijados.
No son una nueva ejecución de MongoDB ni aceptación de la wheel candidata.

**Precisiones de aceptación, sin añadir bloques ni componentes:**

- D00 acompaña al consumidor que cambia. Para igualdad/claves, comprobar las
  formas SDK/local/interna y el resultado público; las trazas de clases sirven
  al diagnóstico y no fijan nombres privados como contrato. Una clave exclusiva
  exige equivalencia; una clave de candidatos admite colisiones con residual,
  pero debe conservar todos los matches antes de límites y ventanas.
- B07 unidad 5 reutiliza preparación y evaluación de expresiones existentes.
  Separar disponibilidad declarada de metadatos, valor por documento y salida
  pública: cambios de representación y planes optimizados deben conservarlos
  hasta su consumidor, sin persistirlos ni exponerlos como campos del usuario.
  Evaluar cada expresión una vez por fila. Cubrir ruta directa/comando/wire
  soportado, vacío/poblado, subpipeline, `let`, collation, reloj y callbacks.
  Conservar la fase nativa de los errores de proyección en findAndModify junto
  con estado y barrera de eventos; no presuponer rollback ante todo error.
- D06 conserva orden/extras DBRef antes del decoder SDK, gramática por contexto
  y controles negativos. Un parser correcto o una forma wire válida no acredita
  todos los contextos; las especializaciones SQL/hash deben demostrar su ruta
  real y su fallback. Mantener el fallback binario de D03a, sin obligar a crear
  un pushdown nuevo. No ampliar la gramática mediante un normalizador universal.
- D02 conserva inclusión de candidatos TTL, residual exacto y migración de
  auxiliares como garantías separadas; no introducir tolerancia de un milisegundo.
  D04 conserva apertura/consumo y clase/código de error. Sus controles siguen
  vigentes sin reabrir los cortes aceptados. H53 sigue requiriendo disposición
  explícita C03/B08; aceptación de escritura y warning son observables distintos.
- D05 acredita la fuente final instalada. Las pruebas locales de esta relectura
  no aceptan los cambios preexistentes de runtime ni sustituyen baseline fallida,
  referencias 7/8/9, matrices, typing/SPI, persistencia o recursos. No regenerar
  referencias históricas para ajustar expectativas a la implementación.

**Precisiones de esta relectura, incorporadas al orden anterior.** El adjunto
es idéntico al ya contrastado; la [evidencia actual](evidence/bson-consolidation-review/current-autocritique/receipt.json)
registra 78 regresiones, seis pares motor/dialecto, 18 trazas y los 16 hashes
oficiales coincidentes. Son controles del checkout, no aceptación del candidato
preexistente ni nuevas capturas MongoDB. El corte posterior `dbe8257f` tiene
aceptación independiente. Mantener estas tres obligaciones dentro
de los bloques existentes:

- **D06, artefacto preparado y ruta fuente.** Revisar las rutas que aceptan un
  pipeline/prefijo explícito y reconstruyen después el find desde el pipeline
  completo del cursor. En particular, `_materialize_pipeline(pipeline)` pasa el
  sufijo a `_load_pushdown_source_documents`, pero este crea la fuente mediante
  `_pushdown_find_operation()` sobre `_effective_pipeline()`. Esta relación está
  comprobada por lectura; su efecto incorrecto sigue sin demostrar. Caracterizar
  prefijo/sufijo con filtros, sort/skip/limit, proyección, contexto y subpipeline,
  activando la ruta materializada real. Solo si diverge, pasar el pushdown ya
  resuelto a su consumidor; no otro compilador, caché o servicio de contexto.
- **B07 unidad 5, finalización y efectos observables.** La igualdad de documentos
  no acepta batching: observar initial/getMore, cursor abierto/cerrado, EOF,
  callbacks y error diferido. Contrastar fuente ordenada por índice frente a
  sort/group bloqueantes y fuente más corta que el límite, por versión. No
  identificar prelectura agotada con filas ya entregadas ni cambiar el límite
  público local por analogía con PyMongo sin caracterizar el adaptador afectado.
  El cierre debe liberar resultados spill incluso antes de su primera lectura,
  conservando el error primario y el dueño de cada recurso. Reutilizar lector,
  ejecutores y spool existentes. `dbe8257f` acredita ese corte en su receipt;
  las demás combinaciones conservan aceptación pendiente.
- **D05 y cada captura, evidencia conservada.** Reservar un directorio nuevo por
  ejecución y verificar su ownership; un rerun no sobrescribe el manifest de una
  captura anterior. Separar controles del checkout, aceptación instalada y
  cierre integrado. Si falta un ledger histórico, registrarlo como limitación;
  una nueva ejecución o un inventario global no reconstruyen sus IDs perdidos.
  Esta garantía utiliza el fixture/capturador existente, sin otro runner.

Los criterios detallados de las seis unidades B07 se mantienen en su apartado;
esta tabla solo ordena sus dependencias con D06 y D05. No es necesario cerrar
todos los consumidores D06 para corregir la admisión cliente. Las unidades de
escritura, UUID y respuesta conservan aceptaciones separadas aunque compartan
un commit lógico; lectura y conversión ya entregadas no se reimplementan.

**Garantías D06 conservadas al simplificar el orden.** La tabla anterior no
reduce la aceptación de las familias pendientes:

- Representación/raw: diferenciales 7/8/9 de find/aggregate, sync/async y wire
  soportado; BSON exacto con observador independiente, orden/extras/`$db:null`,
  Binary 0/5 y Code scope anidados. Codec mutante con control decodificado
  activo y cero callbacks raw. Batch ausente/0/1/2 y ejecución de `$merge` al
  abrir se observan por superficie; raw no hereda sin prueba la primera tanda
  de aggregate normal. Congelar nuevas referencias y ejecutar `--check` sin
  regenerar goldens históricos para ocultar fallos.
- Demanda/bytes/callbacks: conservar prelectura y salida pendiente sin reevaluar;
  distinguir String calculada, documento devuelto, tanda y mensaje. Verificar
  cantidades junto con expansión/materialización, fase/clase/código/codeName/
  errmsg, orden y callbacks. El fallo servidor invalida la respuesta antes de
  decoders; el error del decoder cliente conserva los efectos previos que
  observe el SDK. Cada contexto/plan nuevo exige testigo nativo propio.
- Claves/consumidores: mismo BSON con/sin hash/spill y bajo collation. Las claves
  exclusivas exigen equivalencia en ambos sentidos; los hashes admiten colisiones
  solo con comparación posterior. Prefiltros Search conservan candidatos antes
  de residual/ventana; subpipeline y persistencia tienen testigos propios.
  Conservar storage keys y desempates persistidos.

H53 conserva strict/moderate/off × error/warn/bypass y antiguo/candidato,
separando evaluación, permiso y warning; su disposición no altera por sorpresa
la aceptación de escrituras. D05 conserva baseline que falla, candidato final
instalado que pasa y recursos sin fugas; ningún gate previo al último cambio
acredita la fuente posterior.

**Conversión: criterios del corte, sin otro componente semántico.** `51ff167`
tiene [aceptación propia](bson-consolidation-implementation.md#conversión-escalar--gramática-tipos-y-preservación-bson).
El candidato presente durante revisión 18 todavía no estaba aceptado. El corte
no se limita a una expresión regular para sNaN: preparación, evaluación y salida
pública pueden coincidir en valor y divergir en tipo o fase. Reusar sus owners,
sin cambiar la política numérica de consulta para corregir conversiones.

- Contrastar `$toDecimal` y `$convert` (alias, `onError` y `onNull`), literal y
  campo, vacío y poblado. Distinguir plegado constante de ejecución por fila;
  incluir ramas no seleccionadas para evitar que una validación eager nueva
  evalúe indiscriminadamente expresiones que el servidor no ejecuta.
- Separar String inválida, Code con/sin scope y Decimal128 ya almacenado. Los
  controles NaN/sNaN almacenados, signos, infinitos, cero negativo, double de
  15 cifras y extremos de exponente/precisión evitan prohibiciones o redondeos
  generales. Observar representación física donde importe, sin normalizar
  Decimal Python a Decimal128 dentro del oráculo para esconder una fuga.
- Exigir tipo público BSON y fase/clase/código por directa sync/async y wire,
  ambos motores y dialectos; preparación y subpipelines deben conservar el
  contexto tipado existente. Las conveniencias internas de evaluación no son
  por sí solas el contrato del API público. Verificar pipeline de escritura si
  comparte el productor, con identidad física y no-op como controles.
- Congelar el corpus final antes del `--check` nativo; proteger las capturas
  históricas. Baseline que falla, candidato instalado que pasa y revisión del
  alcance real preceden a la aceptación. El replay intermedio fallido solo
  identifica trabajo restante; no sustituye al gate final ni reabre B07.

**Entrega posterior `51ff167`.** El corte de conversión supera 8.601 checks
instalados y 1.144 pruebas con 181 subtests, con seis acciones/548 observaciones
nativas por versión 7/8/9, baseline que falla y 228 controles SDK-free. El
[receipt](evidence/bson-consolidation-implementation/decimal-conversion/receipt.json)
fija fuente final, checks nativos y alcance. Se conservan las garantías de
representación/tipos, fase, contexto y almacenamiento; no se añade otro
comparador ni se amplía admisión BSON. H53, decisión D07, resto D06, D05 y B07
siguen abiertos; el coste escalar diagnóstico 1,06 no cierra D05.

### B07 — Admisión en la fase y el lote correctos

Corte activo P1 tras la conversión, independiente de terminar todos los
consumidores D06 y necesario para el default UUID coordinado. El
[preflight](evidence/bson-consolidation-implementation/bson-admission-preflight/README.md)
contrasta seis acciones/676 observaciones por versión contra 7.0.43/8.0.32/9.0.2.
No implementa ni acepta B07: el corpus obligatorio nuevo tiene 96 fallos y 51
checks correctos en el baseline `2ae4b56`, diferenciados de los 97 nodos
históricos de D05. No se ocultan con xfail ni normalización del resultado.

**Entrega de lectura `0b01a62`.** El
[registro](bson-consolidation-implementation.md#b07--codificación-de-lecturas-y-solicitudes-find-diferidas)
y el [receipt](evidence/bson-consolidation-implementation/bson-input-reads/receipt.json)
aceptan el corte de codificación común de lecturas y find diferido, sin cerrar
B07. Las 75 comprobaciones del corpus suplementario pasan frente a 48 fallos
del baseline. Se conservan 96 fallos del corpus de admisión completo en el gate
amplio (8.727 correctas), junto con 1.199 pruebas/181 subtests y 748 controles
SDK-free correctos. La tabla siguiente conserva criterios; no vuelve a pedir
crear el componente, otra caché o reemplazar el cursor entregados.

**Entrega de inserciones `b713e52`.** El
[registro](bson-consolidation-implementation.md#b07--inserciones-ids-y-errores-públicos)
y su [evidencia](evidence/bson-consolidation-implementation/bson-input-inserts/README.md)
aceptan insert_one/insert_many, preparación común de IDs, copias después de la
admisión y materialización de sus errores públicos. Se conservan 123 checks nuevos
frente a 60 fallos de la base, con 169 observaciones nativas por versión. El gate
amplio mantiene 96 fallos B07 y 8.856 correctos. No se reextraen estos mecanismos
ni se acepta por ello todo bulk, todas las escrituras o el default UUID.

**Entrega de mutaciones `c83c95c`.** El
[registro](bson-consolidation-implementation.md#b07--mutaciones-públicas-y-respuestas-builtin)
y la [evidencia](evidence/bson-consolidation-implementation/bson-input-mutations/README.md)
aceptan el corte de update/delete/findAndModify, incluyendo pipelines, sesiones,
precedencias y respuestas builtin. Pasan 171 checks nuevos frente a 72 fallos
previos; el gate amplio mantiene 96 fallos B07 y 9027 correctos.
No se reextrae esa frontera ni se infiere cierre general de B07.

**Entrega de bulk `5a89dcc`.** El
[registro](bson-consolidation-implementation.md#b07--preparación-y-frames-bulk-clásicos)
y su [evidencia](evidence/bson-consolidation-implementation/bson-input-bulk/README.md)
aceptan preparación, frames clásicos, lookahead y efectos parciales, con 219
checks nuevos y 518 observaciones nativas por versión. El replay amplio mantiene
96 fallos B07 y 9246 correctos. La propiedad bulk_api_result
cierra ese testigo de unidad 5, sin cerrar todos los efectos de respuestas.

**Entrega Search `2723fb28` y siguiente corte.** Las cinco entradas del censo
tienen [aceptación de admisión](evidence/bson-consolidation-implementation/bson-input-search/README.md),
opciones propias y ejecución compartida; no acreditan positivos Atlas. Sigue
`d5bdadb8` entrega UUID coordinado; sigue la unidad 5 de positivos/respuestas,
con los 24 nodos admission_options y dos tests de batching aún rojos. Comandos, creación de catálogo,
cuatro admisiones de índices y seis entradas de listados/validate conservan
aceptación acotada; no se reimplementan por repetir el censo. Reutilizar encoder,
parsing y ejecutores. La
entrada interna wire ya recibió BSON: conservar orden/undefined y contexto,
sin imponer otra codificación cliente ni endurecer DocumentCodec.to_internal.
Mantener errores simultáneos, clase/fase/código, originales, IDs, estado, índices
y eventos con barrera. No reextraer preparación, agrupador o frame encoder bulk.

Las reglas servidor compartidas conservan la clasificación por primer campo
(vacío es reemplazo) y los códigos de ordered inválido. Driver y servidor no
comparten el default de null para su orden: conservar ambas decisiones y sus
testigos. Los documentos antiguos con _id no inicial se siembran por SPI; no
migrar almacenamiento ni retirar sus controles para normalizar escrituras nuevas.

`d5bdadb8` entrega la unidad 4 con default UNSPECIFIED; STANDARD/legacy requieren
selección explícita. La unidad 5 conserva positivos de proyección/sobres y otros
efectos de respuesta. D06,
H53, decisión D07 y cierre integrado D05 siguen abiertos. D05 conserva costes
de lectura, mutaciones y bulk (hasta aproximadamente 1,28 en Memory update lote
1), ligados al runtime final; no se acepta presupuesto por otras rutas verdes.

**Causa compartida.** La API mezcla normalización interna con codificación de
solicitudes: puede perder el tipo original o compilar antes del momento en que
el SDK codifica. La frontera pertenece a la solicitud cliente y a su lote,
no al motor ni a `DocumentCodec.to_internal` global. Los consumidores de valores
internos deben seguir aceptando sus representaciones válidas. Wire ya recibió
BSON: no volver a imponerle la codificación cliente ni perder undefined/orden
al reconstruir un DBRef con el decoder SDK.

| Unidad / dependencia | Implementación y simplificación | Garantía de cierre |
| --- | --- | --- |
| 1. Solicitud find sin codificar | Reutilizar `AsyncCursor._as_operation`, caché/contexto actuales y validadores de forma. Retirar la compilación/normalización anticipada de la entrada pública find. El camino interno con FindOperation preparado conserva su artefacto; no otra clase de cursor o caché. | Find difiere InvalidDocument/OverflowError hasta consumir; cerrar no codifica. Filtro inválido→válido y válido→inválido antes del primer I/O, clone/rewind/explain, contexto temporal, sesión, codecs y cursores raw. No guardar un error al construir: el filtro todavía puede cambiar. Preservar validaciones de forma que el SDK sí hace eager. |
| 2. Codificación pública común | Separar admisión estricta y codec interno reversible. Integrar en el límite cliente de documentos/filtros/proyecciones/updates/replacements/pipelines/let/array_filters/comments y comandos/índices/validators. Reusar vistas/normalización y capacidades SDK/portable existentes; no dependencia obligatoria PyMongo ni marca/flag público de «validado». | Clase y fase nativas para Decimal crudo/object/set/frozenset/bytearray/date, claves NUL/no String, entero fuera de int64, UTF-8 inválido, Code.scope y DBRef anidados. Controles Decimal128/Binary/datetime/documentos/tuplas siguen válidos. Distinguir codificación inválida del error semántico servidor; comparar estado/aux_exists y no inventar rollback de efectos semánticos nativos. Un solo chequeo de admisión por solicitud, sin repetirlo en fachada/comando/motor. |
| 3. Lotes de escritura | Entregado en `5a89dcc` para bulk clásico, sobre el agrupador y encoder existentes. Conservar la admisión del frame que se va a enviar y el orden entre frames/familias; no admitir todo el bulk anticipadamente. | Un segundo insert/update inválido impide el primer write válido de su mismo frame. Un lookahead inválido aborta el frame pendiente; uno válido que demuestra desbordamiento permite confirmar el prefijo. Los frames/familias anteriores permanecen, también unordered. Clase cliente conserva InvalidDocument/OverflowError/etc.; no envolver como BulkWriteError/WriteError servidor. IDs asignados, contadores, estado, índices y ausencia/entrega de eventos tienen controles de frontera y barrera. |
| 4. UUID coordinado D06a/b | Entregado en `d5bdadb8`: opciones de escritura antes del almacenamiento, lectura independiente y default UNSPECIFIED coordinado. STANDARD/legacy siguen explícitos. Reusar estas políticas; CodecOptions del dueño de la solicitud no se sustituye por el de otra fachada ni por un nuevo codec. | Default/UNSPECIFIED rechazan UUID con ValueError en fase SDK, sin mutación/eventos. STANDARD y tres legacy conservan bytes/subtipos y lectura correctos, también wire y reapertura, con/sin SDK. No reescribir Binary 4 almacenado a 3 al leer; no retocar el snapshot para ocultar la diferencia. |
| 5. Consumidores positivos D06/B08 | La proyección de find debe reutilizar la preparación/evaluación de proyección existente para valores calculados. Sobres raw y resultado bulk reutilizan materialización y contratos DTO: tipos, orden de campos y callbacks una vez. | Preservar proyecciones Decimal128/Binary/datetime/arrays/documentos y el error 9 de la constante Binary 5; no reducirlas a flags Python. Raw find conserva firstBatch/id/ns e id BSON int64; update conserva n/nModified/ok; bulk expone el resultado público nativo. No ordenar claves ni borrar metadata del oráculo. Decoders/fábricas activas detectan doble pase y diferencias de fase. |
| 6. Aceptación conjunta | Baseline/candidato, referencias nuevas congeladas/check, wheel instalado y gates alcanzados; D05 conserva matrices/cobertura/costes integrados. | Todos los testigos anteriores por directa sync/async y wire × Memory/SQLite × 7/8/9, subset portable real, persistencia/reapertura y recursos limpios. Referencias/observadores históricos intactos y commits semánticos. El preflight rojo no acepta ninguna unidad productiva. |

**Aceptación posterior de unidad 5.** `a8a81f0e` completa los controles de
constructor/setter y errores de respuesta previstos aquí, sobre los cortes
anteriores de proyecciones/batching/DTOs. La sonda replica set acredita entrega
tras commit y ausencia tras rechazo mediante barrera; standalone conserva su
traza de factories. El perfil sin SDK no promete la clase RawBSONDocument que
no tiene disponible. D06 conserva contextos y transformaciones aún sin aceptar;
B07 unidad 6/D05 conservan matrices y cierre integrado. No añadir otra capa
privada de resultado para volver a copiar un documento ya decodificado.

**Precisiones tras contrastar la autocrítica con el código actual.**

- Conservar la entrada pública sin normalizar hasta la admisión del lote,
  sin copiar objetos arbitrarios antes de que BSON los admita. La preparación
  bulk anterior convertía valores con `to_internal`; `5a89dcc` retira esa
  normalización anticipada. Conservar el testigo que impedía admitir un tipo
  cliente originalmente inválido tras transformarlo. Reutilizar la preparación y los modelos existentes, sin introducir
  otro normalizador, journal ni flag público para saltarse la frontera.
- Separar validación de parámetros cliente, codificación del lote y validación
  semántica servidor. Inserciones y bulk ya corrigen la comprobación del `_id`
  raíz previa a la codificación. Conservar sus diferenciales de errores
  simultáneos (`_id` inválido y valor no codificable), y añadirlos para cada
  nueva entrada antes de fijar su precedencia; el éxito de errores aislados
  no acredita la combinación. Observar también asignación de
  IDs y efectos ya confirmados en familias anteriores.
- Un error de codificación impide enviar su lote; no revierte lotes anteriores
  ni constituye un fallo servidor dentro de BulkWriteError. Los errores de
  validación/duplicados servidor conservan sus propios efectos ordered/unordered.
  La barrera de eventos debe comprobar ambas situaciones, además del estado.
- Las trazas de representación son diagnóstico de frontera. Mantener las leyes
  entre formas y el BSON público como garantías permanentes; no fijar nombres
  de wrappers privados ni multiplicar tablas de clases. El fallback SQL probado
  sigue siendo la alternativa mínima correcta mientras no haya un pushdown
  exacto acreditado.

Las unidades 1 y 3 tienen aceptación acotada; 2 comparte el encoder entregado
e integra comandos en `3bfe5fb`, creación de catálogo en `7524e39` y las cuatro
admisiones de índices en `c715ae7`; `d9c9c80` entrega las seis entradas de
listados/validate y `b938556d` entrega drop/rename. `2723fb28` dispone las cinco
entradas Search del censo. `d5bdadb8` entrega la unidad 4 y el default UNSPECIFIED,
con [lecturas, bytes y persistencia](evidence/bson-consolidation-implementation/bson-uuid-policy/README.md). Los consumidores positivos tienen owner propio: una codificación
correcta no acepta su proyección/sobre. No se crea otro comparador, planificador,
contexto o formato de almacenamiento para resolver esta frontera. Los demás
contextos D06, H53, D07 y el cierre integrado D05 conservan sus obligaciones.

**Entrega posterior `332d07a`.** El corte de claves/consumidores tiene
[aceptación propia](evidence/bson-consolidation-implementation/bson-consumers/README.md):
grupo DBRef/collation, átomos del índice de conjuntos y representantes BSON
según ejecución preparada. La decisión antes numérica se conserva en
`group_execution.py`/`group_uses_slot_semantics`; gobierna average e ID sin otra
regla por dialecto. `_aggregation_key` y el parámetro redundante de dialecto en
finalización se retiran conservando la guarda escalar y la gramática existente.
Cinco acciones nativas 7/8/9, 8.221 checks instalados y 675 unitarias/propiedades/
contratos acreditan ese alcance. Conservar los controles de hash/spill y
representantes raw; no rehacer la extracción ni declarar D06c/D05 completos.
Search completa las comprobaciones públicas de persistencia/reapertura en el
corte posterior `fbc7cde`; los demás contextos/representaciones mantienen sus
propios pendientes. La matriz siguiente
conserva el criterio del corte, ya satisfecho donde registra el receipt.

**Entrega posterior `fbc7cde`.** El corte Search tiene
[aceptación propia](evidence/bson-consolidation-implementation/search-bson-persistence/README.md):
persistencia/reapertura, mutaciones de dominios BSON, filtro pre-top-k frente a
match posterior y contexto de candidatos. La misma guarda de elegibilidad sirve
para ejecución/explain y ambos motores; los dialectos propios conservan residual.
El ranking examina la tanda ANN recibida completa y materializa solo ganadores,
con un orden común; no se cambia el formato persistido ni el desempate. Explain
transmite su verbosity explícita y el oráculo de pruebas se configura antes de
la apertura sync, con prueba del modo real en el ejecutor. El corpus nativo posee
solo selección MQL: ranking y overrides siguen su contrato local, sin afirmar
paridad Atlas. Pasan 8.454 checks de replay y 1.144 pruebas instaladas con 181
subtests. D05 conserva el coste Binary residual/limit=2 (ratio CPU diagnóstico
1,47), el find(limit=1) anterior, matrices/cobertura y los demás pendientes.
El nodo histórico de errores Search se acredita con la referencia bien
configurada; no se aceptan por asociación los otros nodos del listado de 97.

**D06c: diseño del corte concretado en revisión 17, conservado como historia.**
Grupo/conjuntos y el alcance Search tienen las aceptaciones posteriores citadas
arriba; esta tabla conserva sus garantías, no otra lista de fallos presentes.
Las sondas de revisión 17 no aceptaban el corpus ni sustituían su contraste nativo. Identificaban
estos mecanismos dentro del bloque existente, sin abrir otro programa:

| Unidad / prioridad | Corrección o refactorización mínima | Garantía antes de aceptar |
| --- | --- | --- |
| Grupo / P1 | `aggregation_equality_key` recorre dict, pero no la vista documental de DBRef. El DBRef decodificado cae en la guarda de hashabilidad y falla; el SDK evita el recorrido con collation. Reutilizar `document_mapping` antes de la recursión, conservando orden/extras y política de missing/null del grupo. Retirar `_aggregation_key` y sus exports privados solo tras censar consumidores y conservar la guarda efectiva; no sustituirla por un validador BSON permisivo ni ampliar admisión B07 por accidente. | MongoDB 7/8/9: partición de miembros y representante físico observado por separado; DBRef SDK/local/documento, orden inverso, extras, `$db:null`, arrays/anidados y collation. Propiedad de equivalencia en ambos sentidos para la clave exclusiva en su dominio; un test del helper común no acredita este consumidor. |
| Conjuntos / P1 | `_hashable_set_lookup_key` usa igualdad Python sobre bytes/Binary y admite Code por herencia de str. Delegar los átomos elegibles a las vistas/claves existentes; excluir Code del atajo String o darle una política acreditada, conservando residual. No ampliar dialectos, tipos elegibles ni habilitar el atajo bajo collation para simplificarlo. | bytes/Binary 0 equivalentes, 0/5 distintos, Code/String, scope ausente/vacío/numérico/reordenado y collation; setUnion/intersection/difference/equals/subset y addToSet por sus rutas reales. Comparar pertenencia/contenido, no solo tamaños; controles 8/9 y dialecto propio deben demostrar el fallback. |
| Spill y lookup / P1 de garantía | Reutilizar claves y candidato/residual actuales; no crear un servicio universal. En spill, igualdad de claves Python no basta: `_group_digest` serializa la clave. En lookup, mantener admisión conservadora, presupuesto y fallback de valores no representados. | Equivalentes producen la misma entrada serializada de partición; grupos iguales con/sin spill forzado y en subpipeline. Lookup hash activado/desactivado conserva miembros y orden, con contadores/spy que prueben la ruta. Presupuesto agotado, valores no elegibles como DBRef, collation y dialecto propio prueban fallback; Code tiene controles propios sin presuponer su exclusión. Un umbral mínimo que desactive hash no cuenta como test de hash. |
| Search / P1 de garantía, sin ampliación funcional | Conservar su dominio y contrato propios. No sustituir filter_value_key por la clave general si admite más valores o cambia equivalencia/orden. Ampliar propiedades de conservación existentes para las representaciones alcanzadas. | Verdad del filtro ⊆ candidatos ⊆ universo elegible, antes de residual y ventana, también materializado/persistido/reabierto. Valores no representados fuerzan fallback; exact implica igualdad. MongoDB puede acreditar el filtro MQL usado como oráculo, no la semántica Atlas Search/vector completa: esa parte mantiene su contrato local. |

D00 acompaña estas unidades y D05 integra sus gates. Grupo y conjuntos pueden
corregirse juntos por compartir primitivas, sin imponer dependencia entre sus
arreglos. Spill depende del contrato de clave del grupo; lookup y Search pueden
revisarse en paralelo lógico sin ampliar elegibilidad. Una prueba nativa del
grupo no acepta addToSet, un set correcto no acepta el índice de lookup y un
roundtrip de producción no acredita la serialización física. Mantener controles
negativos con subtipo/orden/scope distintos para detectar fusiones indebidas.

Este corte comparte **vistas y átomos**, no políticas: identidad de modificación,
igualdad de consulta, grupo y Search siguen teniendo owners diferentes. Las
claves persistidas, storage keys y tie-breakers permanecen fuera de esta
refactorización; cualquier cambio requeriría contrato de migración y pruebas
propias. Las entradas no BSON hashables admitidas hoy por la guarda no se
convierten en un nuevo contrato de soporte; su rechazo pertenece a B07.

**Garantías del paso 2, conservadas tras su corte aceptado.**

1. Congelar las funciones del corpus y capturar/`--check` contra 7/8/9. Añadir
   el testigo de entrada pequeña y salida expandida antes de corregir la demanda;
   incluir filtro fallido después del primer corte por bytes y controles de
   batch ausente/0/1/2. El baseline debe fallar por el contrato observado, no
   por importación, dependencia o fixture ausente.
2. Corregir la demanda interna solo cuando lo exija el diferencial, conservando
   ownership, snapshot, sesión y cierre. Las capturas posteriores 7/8/9 del
   control de entrada pequeña/salida grande también fallan al abrir sin filas:
   la prelectura local, por sí sola, no justifica una fuente unitaria distinta.
   Conservar evaluación bloqueante cuando sort/group o una extensión la
   requieran; no forzar streaming por el nombre de la etapa. Comprobar residual
   antes de ventana y acceso ordenado frente a sort bloqueante con controles
   de hint/índice. El registro del corte conserva la evidencia de ese contraste.
3. Compartir una comprobación estrecha de capacidad String entre los productores
   alcanzados, después de contrastar `$concat` y `$replaceAll` en el servidor.
   La ubicación en el dispatcher solo se adopta si respeta sus cortocircuitos
   y valores intermedios. Incluir cola null, UTF-8 multibyte y String frente a
   Code; no aplicar `isinstance(str)` como clasificación BSON suficiente.
   Retirar la guarda aislada de `$concat` si queda reemplazada. Un documento
   grande compuesto por dos Strings válidas distingue este límite del límite
   documental; no imponer 16 MiB a todas las fronteras por analogía.
4. Resolver diagnóstico y codecs en la frontera que realmente entrega la
   respuesta. El find interno no debe atribuir a find un fallo de aggregate.
   Capturar `errmsg` desde details, sin metadatos volátiles ni el `str(error)`
   completo. Incluir primer campo String/Binary/documento grande para contrastar
   truncación del diagnóstico; reutilizar el renderer existente antes de añadir
   otro. Raw y metadatos de protocolo conservan controles propios de codecs.
5. Revisar cierre/rewind con documento pendiente, error de cleanup, cancelación
   repetida y fallo de decoder. Medir coste de demanda unitaria en sort, group,
   expansión y spill: no introducir otro ejecutor o caché para compensarlo sin
   evidencia. Con fuente final congelada, ejecutar focales, replay, wheel y
   gates alcanzados; D05 conserva sus matrices completas. Referencias nuevas
   preparadas o gates de una fuente anterior no aceptan el candidato final.

Las comprobaciones de representación del matcher siguen siendo instrumentación
de tests por frontera, no un API de depuración en producción. Clasificación,
comparación, igualdad física y claves tienen propiedades enlazadas por política;
solo se comparte el átomo que conserve esa política. Así se reduce duplicación
sin esconder diferencias de consulta, agregación, schema o Search.

**Garantías transversales del cursor.** Mantener reloj/bindings/dialecto por
operación, ownership de sesión/principal/namespace, exclusión de getMore y cierre
idempotente en EOF/error/kill/cancelación/teardown. Probar el documento pendiente
al cerrar, la precedencia de error original frente a cleanup y las extensiones
que sustituyen built-ins: el nombre de una etapa no garantiza cardinalidad.
Reutilizar el ciclo de vida existente; no otro journal ni otra política de dueño.
No ampliar soporte wire no implementado para completar artificialmente la matriz.

**Precisiones para el siguiente corte D06a/b y su frontera B07.** No son nuevos
bloques ni obligan a rehacer los cortes aceptados:

1. **Una respuesta, políticas de decodificación explícitas.** Reutilizar
   `ResponseBatchReader` y un constructor de la envoltura cursor para find/aggregate
   directa y wire, conservando orden y tipos de metadatos. El orden batch/id/ns
   es observable por callbacks del SDK, aunque se descarten sus valores
   transformados. No añadir un registro de cursores o un ciclo de vida paralelo.
   Watch conserva sus metadatos y aceptación propios; no extenderle la regla sin
   testigo. Un cambio en la envoltura debe probar initial/getMore, EOF y error.
2. **Tipo exacto y contexto, sin cascada.** El decoder se selecciona por el tipo
   público decodificado exacto, una vez; su salida no vuelve a despacharse por
   otro decoder. Cubrir bool/int/Int64, str/Code y bytes/Binary, dict/SON/list y
   UUID. Distinguir raíz BSON, fila embebida en batch, contenedor batch y
   Code.scope: no todas las raíces documentales tienen los mismos hooks. El
   DBRef se reconoce después de recorrer sus campos en orden; reconocerlo antes
   puede cambiar efectos aunque el valor final parezca idéntico. Capturar también
   decoder que transforma un contenedor, falla o devuelve un valor no documental;
   observar efectos previos, fase/clase del error y cierre, sin normalizar su salida.
3. **SDK opcional, una garantía portable.** Puede reutilizarse el codec público
   instalado al materializar la respuesta completa si reduce semántica duplicada.
   No adoptar APIs privadas, modificar globals del SDK ni importar su dependencia
   desde el núcleo semántico. La alternativa sin SDK debe conservar los contratos
   soportados usando los conversores existentes; un branch SDK correcto no acepta
   el fallback. Las sondas de checkout de esta revisión son feedback. La entrega
   exige wheel instalado con/sin SDK y cobertura de ambos caminos, además de
   comparar bytes/orden/callbacks contra un observador independiente. Medir coste
   con opciones explícitas STANDARD y codecs activos, además del default rápido.
4. **UUID: separar escritura de lectura.** La representación de lectura decide
   cómo interpretar subtipo 3/4; nunca debe reescribir el subtipo almacenado 4 a
   3 por seleccionar un modo legacy. Probar los bytes y bits UUID bajo STANDARD,
   UNSPECIFIED, PYTHON/JAVA/CSHARP_LEGACY, con y sin decoder; conservarlos tras
   persistencia/reapertura. El cambio de default a UNSPECIFIED es un cambio público
   y se coordina con el corte UUID de B07: filtros y escrituras UUID predeterminados
   deben rechazar con la clase/fase del SDK, sin mutación ni eventos; modos
   explícitos válidos conservan controles positivos. No diferir esta incoherencia
   al B07 general ni actualizar un snapshot para ocultarla. Puede entregarse el
   dispatch antes conservando el default anterior hasta completar ese corte.
   Object/Decimal/set y demás admisión B07 siguen independientes; no asumir que
   todo error de codificación es InvalidDocument: UUID puede producir ValueError.
5. **Opciones equivalentes, planes comprobados.** Reutilizar `EngineFindSemantics`
   normalizado y el owner de orden/residual ya extraído. Conservar testigos de
   `_id_`, hint por lista/mapa, collation ausente/simple, asc/desc, y controles de
   índice secundario/collation no simple que requieren otra ruta. La equivalencia
   sintáctica no autoriza ignorar la validación del hint ni adelantar ventana al
   residual. Observar primer resultado y fase del error con batch 1; extender a
   getMore y plan físico alcanzado, no solo explicar el helper.
6. **Fábricas documentales y topología comparable.** El corte `d4caba5` acepta
   construcción/asignación en las respuestas probadas. Conservar el criterio
   para cualquier ampliación: capturar con/sin TypeRegistry la construcción y asignación de campos de la
   respuesta completa: raíz del comando, cursor, fila, anidados y Code.scope.
   Incluir respuesta vacía, initial/getMore y fábricas que fallen en cada nivel;
   observar fase, clase y efectos previos, sin envolver todo fallo como InvalidBSON.
   La decodificación selectiva puede construir contenedores intermedios aun si
   descarta sus valores: una única llamada a decode no prueba equivalencia.
   Reutilizar APIs públicas del SDK y el materializador portable; no depender
   de su implementación privada ni replicar un driver completo.

   Elegir topología según el contrato observado. Para la superficie local que
   anuncia standalone, usar un oráculo standalone explícito; los metadatos
   adicionales de replica set pueden activar constructores distintos. No
   normalizar esos efectos ni inventar relojes distribuidos para igualarlos.
   Extender el fixture existente si hace falta, conservando RS por defecto,
   ownership, cancelación, limpieza y verificación efectiva de hello/build/FCV.
   Watch sigue exigiendo RS y no hereda la referencia standalone. Registrar
   topología y versión del SDK con las capturas; validar las opciones del
   capturador/replay existentes, sin otro runner.

   El corte `72edfa3` acepta las combinaciones de consumo/estado y propiedad
   registradas en su receipt. Para ampliarlas, conservar el mismo criterio:
   next/try_next donde
   exista/to_list intercalados, límite exacto, tanda transformada vacía o
   expandida, alive/cursor_id y cierre. Contrastar las superficies sync/async
   soportadas; no deducir su estado del contador interno ni forzar el mismo
   tratamiento de None o iterables sin longitud en query y command cursors.

Aceptar conjuntamente estos mecanismos cuando simplifique la entrega, pero
mantener las garantías por superficie. Los 36 fallos wire y los fallos portables
de revisión 15 están corregidos en el corte aceptado; conservar sus regresiones,
sin presentarlos como pendientes actuales. Los efectos probados de fábricas y
estado público tienen la aceptación acotada de `d4caba5` y `72edfa3`; otras
combinaciones conservan aceptación pendiente. No crear otro runner: incorporar los
testigos al registro/capturador/replay y a las propiedades existentes. Antes de
aceptar, congelar el corpus final, comprobar las nuevas referencias contra el
servidor correspondiente y acreditar baseline/candidato con esa misma fuente.
Un test que aún exige la política UUID errónea necesita corregirse con el oráculo
independiente; su estado verde previo no era garantía de corrección.

**Detección y mantenimiento.** Ampliar registro/capturador/replay y propiedades
existentes, sin otro runner. Por frontera modificada, un testigo que active el
camino y un control que lo distinga del vecino: local/SDK/interno,
SQL/fallback, raw/decodificado, apertura/iteración, error servidor/codec cliente.
La coincidencia Memory/SQLite o interpretado/compilado no es oráculo cuando
comparten helper. Mantener oráculo nativo e inspección independiente de BSON;
ningún roundtrip que use el mismo helper a ambos lados acredita preservación.
Los ejes solo se reducen si no cambia el contrato y se documenta esa equivalencia.

Esta relectura modifica solo plan/evidencia. No acepta el candidato preexistente,
no altera referencias históricas ni declara cierre D06/D05. La documentación
histórica permanece como trazabilidad; solo esta secuencia dirige lo pendiente.

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
| Watch | Apertura sin eventos y pipeline inválido, replaceWith válido, filtro de evento, undefined tipado, NOW entre etapas/eventos/batches, now_factory, getMore/reanudación/invalidate. Incluir los modos de imagen y ciclo de vida por ámbito concretados en revisión 8, con barrera y controles de ausencia frente a null. Capturar clase de excepción además de código y fase: TypeError local de replaceWith frente a apertura válida física, y OperationFailure 31325 diferido frente a apertura fallida. Colección/DB/cliente, sync/async y wire soportados, cursor/offset/token/cierre; sin ampliar semántica distribuida. |
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
- Cerrar B07 por normalizar Binary/DBRef. La admisión general ya tiene cortes
  aceptados en el registro; sus controles de tipos arbitrarios y Decimal crudo
  no aceptan por asociación positivos, sobres o batching aún pendientes. Nombrar
  las garantías de cada corte sin volver a presentar sus fixes como ausentes.
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


<a id="incremento-propuesto-20261101"></a>

## 9. Propuesta de incremento posterior: correcciones y ampliaciones

Estado: propuesta de planificación, sin implementación ni aceptación nueva.
Base congelada: 2fa637f6254a09acd6c81aeee5a36384fdbdbfa7.
Refinamiento 1 sobre c9620e23 (solo documentación; mismo runtime).
Informe de revisión SHA-256: d88c5971320918ffed6adb7260e6611ec3f2e2f3fc5687492f20f81b4905849d.
Owner de este plan: este apartado. Los apartados anteriores conservan su
alcance histórico; el estado aceptado lo posee
[bson-consolidation-implementation.md](bson-consolidation-implementation.md).
Las dos partes siguientes tienen cierre independiente. La parte II no bloquea
el cierre de la parte I por el mero hecho de permanecer sin ejecutar.

Entrada: informe de regeneración aportado el 1 de noviembre; ocho baterías
y 22 sondas adicionales. El contraste anterior reejecutó 96 casos locales,
192 observaciones Memory/SQLite con dialecto 9.0; 67 casos cuentan con registro
nativo aportado de la discrepancia. No hubo una nueva ejecución física en ese
contraste. Son caracterización, no aceptación ni cobertura completa de MongoDB.
Al ejecutar F00 se incorporarán los estímulos necesarios al inventario existente,
con hash/procedencia del informe y de sus sondas, sin depender del scratchpad.

### 9.1. Decisiones de alcance y simplificación

Se propone ejecutar la parte I: defectos concretos del contrato existente,
normalización de capacidades y cierre sobre candidato instalado. La parte II
es una cartera de decisiones independientes: implementar capacidades ausentes,
cambiar representación persistida, backend o garantías transversales.

Un cambio es acotado si conserva SPI v2 y formato SQLite, emplea los owners y
runners existentes, tiene un conjunto finito de testigos y consumidores, y no
requiere una capacidad nueva. Su aceptación no significa paridad universal.
Si al reproducirlo aparece una dependencia mayor, se registra el caso y el
contrato incumplido, se propone su corte en la parte II y se explicita el límite.
No se puede borrar un criterio aprobado de la parte I mediante ese traslado:
haría falta una disposición explícita antes de aceptar un alcance reducido.

No se propone un Value universal, otro OperationContext, un resolver único con
flags para todas las rutas, otro runner ni una reescritura general del evaluador.
Se reutilizan las primitivas BSON, el contexto de operación, las tablas de
gramática, document_with_id_first y el registro de corpus actuales.
Cada corrección elimina la duplicación responsable del fallo; no migra todos
los consumidores por similitud ni agrega una caché global.

Disposiciones previas a la lista de defectos:

- limit(0) es una regresión de Memory respecto al corte anterior, aunque
  precede a los commits P2: snapshots propios de 30c8ae24 devuelven cinco
  documentos y de 381aa10e/2fa637f6 devuelven cero para setter y keyword.
  SQLite ya devolvía cero en 30c8ae24. La antigüedad de un guard aislado no
  prueba que la ruta pública anterior lo alcanzara. F00/F01 amplían el corpus
  semantic_response_frontier de 381aa10e sin reabrir su aceptación histórica.
- La indexación numérica de rutas de expresión sí existe desde marzo.
  Su mayor exposición no prueba por sí sola una regresión nueva.
- to_list(0) no se conserva como vacío sin consumir por suposición local.
  PyMongo 4.9.2 y 4.19.0 comprobados lanzan ValueError antes de abrir la
  consulta. F01 fija método/perfil/estado, distinguiéndolo de limit(0).
- natural pasa a F08: sort/hint de escaneo físico +/-1 se corrigen dentro
  de los motores incluidos, sin exponer recordId ni migrar formato/SPI.
  El ascendente actual es accidental: con inserciones 30,10,20, Memory
  conserva ese orden, pero SQLite devuelve 10,20,30; ambos ignoran -1.
  No se considera ya implementado ni se rechaza globalmente por F10.
- Las dos diferencias aportadas de orden de grupos no son defectos de runtime.
  Se comparan grupos sin exigir orden exterior, conservando multiplicidad,
  tipos y orden de campos. Lo mismo se aplica a empates cuyo orden no se garantiza.
- No se exige imitar el casing Unicode de toLower/toUpper fuera de ASCII.
  Trim y conversión numérica a texto tienen contratos propios y sí entran.
- La ausencia de un literal de error 52 no prueba qué operaciones fallan.
  Los errores se capturan por contexto, versión, fase y efectos.
- Views, capped y otras capacidades pendientes no se convierten en aceptadas
  por almacenar sus opciones. Su implementación pertenece a la parte II;
  la parte I debe dejar explícita su disposición, sin silencios ni falsos
  diagnósticos nativos. No se rebaja una capacidad públicamente garantizada
  a unsupported para ocultar un fallo: F00 debe identificar ese conflicto.
- Los mensajes solo diferentes se priorizan después de resultados, integridad,
  familias/códigos y fases, salvo garantías de diagnóstico completo ya exigidas.

### 9.2. Parte I — Entrega acotada del contrato existente

Prioridades: P1, pérdida/alteración de filas o valores, selección/aceptación
de escrituras y fallos públicos no tipados; P2, firmas de conveniencia,
metadatos y diagnósticos que no alteren esas garantías. F09b/F09c son P1
por resultados incorrectos; los detalles solo diagnósticos pueden ser P2.
Estado inicial del incremento: todos los bloques empiezan pendientes. F00 fija
los casos antes de editar. El estado vigente y la evidencia de cierre de los
12 bloques se mantienen en el receipt de implementación.
F11 es el cierre integrado; un bloque focal verde no acepta toda la entrega.

| Bloque | Prioridad | Entregable | Dependencias |
| --- | --- | --- | --- |
| F00 | P1 | Inventario cerrado, oráculos y mapa de impacto | Base 2fa637f6 |
| F01 | P1 | Límites de cursor sin confundir cero con agotamiento | F00 |
| F02 | P1 | Salidas de ventanas y objetos BSON válidos | F00 |
| F03 | P1 | Rutas por contexto y consumidores de arrays | F00 |
| F04 | P1 | Gramática de predicados Regex y size BSON | F00 |
| F05 | P1 | Collation efectiva y validación del subset | F00 |
| F06 | P1 | Integridad y reglas de catálogo de índices | F00; F05 para herencia/collation |
| F07 | P1/P2 | Candidatos de escritura, campos e identidad física | F00; integrar F03/F04/F06 al seleccionar/validar |
| F08 | P1/P2 | Claves de sort, natural sort/hint y acumuladores/buckets | F00; F03 para rutas, F05 para collation; integrar F01 |
| F09 | P1/P2 | Argumentos BSON y gramática; calendario/texto P1 | F00 |
| F10 | P2 | Fachadas, metadata y capacidades explícitas | F00; F06 para índices |
| F11 | P1 | Regresión, coste, packaging y entrega integrada | F01–F10 dispuestos y aceptados |

Las dependencias de integración no obligan a serializar las reproducciones
independientes. Orden recomendado: F00; F01/F02/F04/F05 y subunidades P1
de F09 (incluido range); F03/F06; F07/F08; diagnósticos P2 de F09 y F10; F11.
Las correcciones P1 no esperan al final de los P2 por pertenecer a F09.
No se añade una dependencia de F01 hacia F02 ni una cadena entre todas las
familias numéricas: compartir un archivo no demuestra dependencia semántica.

#### F00 — Inventario y aceptación antes del arreglo

- Fijar estímulo mínimo, seed y orden BSON, superficie, consumidor, versiones,
  efectos observables, diagnóstico, prioridad y bloque para cada recomendación.
  Clasificar: defecto reproducido, comprobación obligatoria o hipótesis/capacidad.
- Cubrir las familias explícitas de F01–F10 y la tabla de disposición siguiente.
  Las 1.688 son sondas, no 1.688 defectos; las 724 discrepancias aportadas
  tampoco son causas independientes. No importar todas como obligaciones
  ni lanzar otra auditoría general. No dejar una familia de la revisión
  omitida: cada caso queda en F, X o seguimiento con razón contractual.
- Priorizar el testigo limit(0) de Memory y añadirlo a semantic_response_frontier:
  la reproducción de snapshots identifica su regresión; el corpus existente
  no la protegía. No crear un corpus/runner exclusivo para este caso.
- Capturar los contraejemplos con el inventario/capturador/replay existentes.
  MongoDB 7.0.43, 8.0.32 y 9.0.2 fijados por imagen/buildInfo/FCV conforme a CI;
  las versiones que difieran conservan expectativas distintas.
- Caracterizar namespace ausente/vacío/poblado cuando afecte a validación,
  lectura inicial, optimización o efectos. Registrar apertura y consumo.
- Congelar base y workloads funcionales/coste antes de editar; observar el
  artefacto instalado y mantener el observador independiente de helpers productivos.
- Cierre: cada recomendación tiene un testigo reproducido o una disposición
  explícita razonada; cada prueba obligatoria tiene oráculo y ruta productiva.
  Ninguna hipótesis se convierte en defecto o en garantía aceptada por inspección.

#### Disposición de las familias señaladas en la revisión

Esta tabla fija destinos, no afirma que todos los casos sean defectos
reproducidos. F00 conserva estímulo, perfil, versión, resultado y evidencia de
cada entrada. La igualdad entre motores o una descripción de familia no cierra
el caso. Un control que ya pasa se conserva como positivo, sin inventar un fix.

| Familia retenida | Destino y alcance | Evidencia/criterio de disposición |
| --- | --- | --- |
| all mezclado con elemMatch | F04: gramática de consulta. | Arrays mixtos y permutaciones; rechazo/aceptación, código y fase nativos, no vacío silencioso para una forma inválida. |
| Orden de push/firstN en SQLite | F08, con F03 si interviene traversal. | Reproducir con sort previo total/desempate o fuente ordenada exigida. Conservar el orden de entrada en el acumulador; no ordenar el array resultante para hacer coincidir. Una sonda sin orden de entrada garantizado queda como seguimiento no concluyente, no como obligación de imitar un plan físico nativo. |
| Range que materializa un array enorme | F09a, P1: límite de recursos y error antes de reservar memoria. | Calcular cardinalidad/tamaño o comprobar el límite de la referencia antes de list(range); nativos fijan límite/código/fase. Pruebas acotadas o en el aislamiento de procesos existente, sin intentar agotar el host; comprobar rechazo previo a materializar, continuidad y ausencia de hijos vivos. No crear otro framework de ejecución. |
| Null/missing en ambos lados de operadores | F09a y F02 donde hay materialización. | Matriz por operador retenido, izquierda/derecha/ambos, literal/campo/constante, tipo inválido y error alcanzado/no alcanzado. No imponer conmutatividad ni una política global de null; no cambiar la semántica MQL de consulta. |
| Timestamp/ObjectId como fechas | F09b. | Casos de entrada por operador soportado, incluyendo extracción/formato y suma/diferencia; valor UTC, tipo de resultado, null/error y fase según versión. No se amplía el dominio de fecha por asociación. |
| dateDiff/startOfWeek y formatos desconocidos | F09b. | week frente a otras unidades, nombres válidos/inválidos y validación de formato dateToString: constante/dinámico, % desconocido y % final, con valor/error/fase nativos. |
| toString de Decimal128/UUID | F09c, conectado a F02 por representación. | Conservar cohort/escala decimal y formato Binary UUID por versión. UUID Python solo con CodecOptions que lo admitan; no relajar UNSPECIFIED ni extrapolar novedades de versiones posteriores. |
| Coerción toUpper/toLower/strcasecmp | F09c: dominio de operandos y formato, independiente del casing Unicode. | Números/long/decimal/fecha/string, bool/ObjectId y null/missing, también ambos órdenes de strcasecmp; comprobar resultados y error 16007 donde corresponda. Compartir formato solo después de validar el dominio propio de cada operador, no una coerción universal. |
| Nombres y contexto de stages/let | F09a; F10 consume sus reglas de namespace. | group con _id punteado, nombres de facet, count "_id", sort vacío, documents en colección frente a DB y variables de let/nombre/scope. Cada forma conserva aceptación o diagnóstico/fase nativos; no usar reglas de nombres Python o de otro stage por analogía. |
| Familia geo | Seguimiento del subset declarado; ampliación geodésica/planner en X04d (X05 si cambia formato). | COMPATIBILITY 7.2 declara distancias planares, nearSphere local y ausencia de planner geoespacial. Diferencias derivadas de esos límites no obligan a paridad esférica en F. F00 registra cada testigo y motivo; un fallo que incumpla el subset ya prometido requiere disposición explícita F04/F09, no se excusa por el límite de geodesia. |
| Int64 público materializado como int | F02 para frontera/codec; F09a si el productor pierde el ancho. | Hace falta el estímulo y perfil exactos. Los casos propios básicos de long almacenado y toLong conservan Int64/BSON 0x12 en ambos motores y son controles positivos. Exigir tipo/bytes en el caso que falle, diferenciando SDK-free y decoder intencional; no envolver todos los int en Int64. |
| Índice sobre a. y otros nombres inválidos | F06. | Vacío/poblado, a ausente/presente, create e insert posterior, raw/método: rechazo nativo (67 para el caso aportado), sin OperationalError público ni índice/datos parcialmente aceptados. El mínimo reejecutado acepta a. en ambos motores; no se afirma que toda población produzca el error SQLite descrito. |
| sortArray con escalares y documentos | F08. | El TypeError local está reproducido; comprobar elementos sin campo, null, arrays y empates conforme al servidor, sin convertir escalares en documentos ni imponer orden entre empates no garantizados. |
| Reglas de catálogo y vista _id_ | F06 valida; F10 proyecta. | 32/33 campos, opciones de _id, campos con $, TTL compuesto/_id, códigos/fases y estado; index_information omite unique intrínseco si el servidor lo omite y expone v según la referencia (v:2 en el caso retenido), sin modificar la unicidad interna. |

La disposición geo o de un orden no garantizado es un seguimiento razonado,
no una corrección implementada ni una exclusión silenciosa. El caso Int64
queda como comprobación obligatoria de representación hasta identificar una
reproducción negativa; no obliga a reescribir materialización que ya es correcta.

#### F01 — Contrato de límites y consumo del cursor

- Distinguir límite público cero sin límite, ausencia de límite, ventana
  interna vacía, cantidad cero de to_list y agotamiento. Normalizar en la
  frontera adecuada; no convertir globalmente todos los ceros a None.
- Tratar límite negativo según el perfil PyMongo/contrato raw correspondiente,
  incluido su efecto de singleBatch, sin sustituirlo por abs indiscriminado.
- Diferenciales: límite omitido, 0, 1, -1 y varios lotes; limit(1).limit(0);
  keyword y setter; skip, sort, primera página, first, iteración, to_list,
  agotamiento y cierre. to_list(0) debe conservar la validación del perfil:
  ValueError en los SDK 4.9.2/4.19.0 comprobados, antes de consulta/decoder.
  Capturar la precedencia con cursor agotado/cerrado y otras formas de length;
  no extrapolar la validación de Cursor a CommandCursor u otro método.
- Cubrir directa sync/async y driver/wire, ambos motores; observar IDs, orden
  garantizado, offsets y estado de cursor, getMore y errores en su fase.
- Cierre: todos los accesos al mismo cursor conservan la secuencia esperada,
  no pierden filas ni materializan fuera de la demanda; cero interno sigue
  significando vacío donde ya tenía ese contrato. Presupuestos y codecs intactos.

#### F02 — Ventanas, ausencia y construcción de objetos

- Corregir el productor de valores de shift/linearFill y los bordes retenidos
  de locf que comparten su ruta: faltante, null, default y filas fuera de rango.
  Respetar la semántica de cada operador al asignar una salida.
- Rechazar claves con NUL de arrayToObject con su diagnóstico contextual,
  incluida evaluación constante frente a dependiente de una fila.
- Reutilizar materialización y errores existentes. No volver permisivo
  bson_size ni capturar todo TypeError/ValueError como un error de expresión.
- Diferenciales: faltante/null al inicio, centro y final; defaults; entrada
  vacía y sort inválido retenido; objetos anidados/arrays; lotes separados
  por cantidad y bytes, codecs que transforman sin lanzar y codecs que fallan.
- Cierre: ningún sentinel interno alcanza una respuesta BSON; resultados,
  familias/códigos y fases coinciden con el servidor para los casos exigidos,
  conservando capacidad, efectos del decoder, continuidad y limpieza.
  No se amplía aquí la familia completa de interpolación.

#### F03 — Rutas de expresiones y traversal por consumidor

- Corregir rutas numéricas en expresiones, ROOT/CURRENT/variables y proyección
  calculada de find, incluida la rama de RuntimeDocumentState. Conservar
  claves documentales llamadas "0" y posiciones MQL en su contexto.
- Sustituir heurísticas de distinct por extracción con contrato explícito:
  arrays terminales sobre ramas punteadas, ausencia/null, vacíos y anidados.
  Deduplicar con la igualdad/colación ya establecida, sin aplanado recursivo.
- Cubrir elemMatch objeto sobre elementos array, unset de agregación a través
  de arrays, includeArrayIndex punteado de unwind y lookup con array local vacío.
  Compartir piezas de traversal/proyección apropiadas, no borrado clásico para
  simular exclusión de agregación ni extracción MQL para toda expresión.
- Diferenciales: a.0, a.0.b, {"0": valor}, arrays de documentos/arrays, ramas
  ausentes/null/vacías, hermanos que no se modifican; lookup con extranjero
  null/ausente/array vacío. Repetir con fuente materializada y subpipeline.
- Cierre: cada contexto produce los valores/IDs/BSON nativos y conserva las
  ramas no alcanzadas. Las rutas MQL numéricas ya correctas y la selección
  SQL/residual/Search no pierden coincidencias por el cambio compartido.

#### F04 — Predicados y operandos BSON de consulta

- Separar eq explícito con Regex almacenado de regex implícito; admitir not
  con Regex según su gramática. Reutilizar los nodos de consulta existentes.
- Size acepta/rechaza representaciones numéricas BSON conforme al servidor,
  sin depender de isinstance(int) ni confundir bool, long, double o Decimal128.
- Diferenciales: Regex literal, eq, in, not y regex; strings y Regex almacenados,
  flags, arrays, valor nulo/ausente. Size con long, integral/fraccionario,
  negativos, límites, NaN/infinito y bool, en vacío y con datos.
- Observar find/match, count/distinct y selección update/delete; compilado,
  canónico y SQL/fallback, incluidas representaciones tras codec/persistencia.
- Cierre: misma selección y misma aceptación/error/fase; no se convierte eq
  en ejecución de patrón ni se altera la gramática de expresiones de agregación.
  PCRE completo queda en X01, no es requisito para corregir estos nodos.

#### F05 — Collation efectiva del subconjunto existente

- Resolver explícita de operación, default de colección y simple, preservando
  la diferencia entre ausencia, null/forma inválida y override simple según
  los contratos Python/raw. Integrar en OperationContext, sin otro contexto.
- Propagar a find/sort/distinct/group/lookup, selección de update/delete,
  validators e índices heredados. No olvidar comandos raw, subpipelines,
  fuentes materializadas, índices parciales y residual Search alcanzados.
- Validar locale requerido y opciones desconocidas; para caseFirst y otras
  opciones fuera del subset, dar una disposición local explícita y coherente
  con el backend disponible. No prometer tailoring adicional.
- Diferenciales: simple/en, strengths 1/2/3 y opciones ya soportadas;
  default/override/ausencia, igualdad/rangos/orden/grupo, cachés frías/calientes
  y namespaces recreados/renombrados, lectura tras reapertura e índices nuevos
  y existentes. No se presupone que collMod permita cambiar la collation.
- Cierre: una misma operación usa una única collation efectiva en todos sus
  consumidores; estado y errores coinciden dentro del subset. Una petición
  fuera del subset no pasa silenciosamente como si se hubiera aplicado.

#### F06 — Integridad y semántica de índices

- Corregir la representación de claves unique Memory para ausente/null
  no sparse, manteniendo vacíos, sparse, partial, compuestos y multikey.
  Unificar construcción, mantenimiento y consulta del índice; no añadir una
  excepción de insert_one ni cambiar la igualdad BSON global.
- Aplicar herencia de F05 y caracterizar conflictos por patrón/opciones/nombre:
  emitir 85 solo donde lo exige la referencia por versión.
- Rechazar segundo text y arrays en hashed en construcción y escritura según
  el contrato retenido. Validar nombres de índice/rutas (incluido a. y campos
  con $), límite de 32 campos, opciones de _id y TTL compuesto/sobre _id:
  aceptación, código y fase por forma y dialecto, antes de usar SQL inválido.
  No limitarse a envolver OperationalError ni implementar recuperación por
  text/hashed nueva.
- Diferenciales: crear antes/después de datos; insert/update/unset/replacement/
  upsert/bulk; colisión/no colisión y mismo documento; perfiles sparse/partial;
  mismo patrón con opciones/colación distintas. Capturar contadores, índices,
  documentos y estado parcial de bulk ordered/unordered tras error.
- SQLite con archivo anterior y reapertura; Memory con índice materializado y
  fallback cuando sea pertinente. La coincidencia entre motores no es oráculo.
- Cierre: no se aceptan estados inválidos del conjunto; no se rechazan claves
  válidas por una simplificación de null. Se conserva formato persistido,
  selección, diagnóstico de duplicado y publicación de escrituras confirmadas.
  No reetiquetar índices históricos como si se conociera que su collation
  original fue omitida: la ambigüedad de procedencia exige disposición o X05.

#### F07 — Finalización de escrituras e identidad física

- Completar document_with_id_first en replace_one/bulk replacement sin upsert,
  upserts por operadores y findOneAndUpdate/findAndModify. Aplicarlo a la raíz
  tras resolver el ID y antes del almacenamiento en la fase adecuada.
- Validar nombres con $ y documentos de _id por contexto: insert, replacement,
  modificador, upsert con/sin match y pipeline. No prohibir valores documentales
  anidados válidos ni los operadores posicionales soportados.
- Bit conserva/promueve ancho int32/int64 según operandos; pull por ruta que
  atraviesa una estructura inválida produce su error, no traversal implícito.
- Caracterizar y corregir Timestamp(0,0) raíz donde el servidor lo sustituye;
  conservar timestamps explícitos, anidados y en arrays. Reutilizar reloj/contexto
  de escritura existente; no implementar un reloj de replicación distribuido.
- Diferenciales: clásicos/pipeline/replacement/upsert, bulk ordered/unordered,
  métodos y comandos, ambos motores; guardar y reabrir. Observar BSON con orden/
  anchos, matched/modified/upserted, errores, estado parcial y eventos/preimágenes.
  Para timestamps, relaciones de no-cero/rango/consistencia, no igualdad de reloj
  entre procesos; los valores intencionalmente fijos mantienen sus bytes.
- Cierre: candidato almacenado, respuesta, counters y eventos describen la
  misma escritura; no-op/identidad siguen correctos y no se reordenan documentos
  embebidos. Validators strict/moderate/off y error/warn/bypass conservan O3.

#### F08 — Ordenación y acumuladores afectados

- Corregir clave de array vacío y rutas numéricas/anidadas en sort documental;
  detectar arrays paralelos según la referencia, también antes de una ventana.
- Implementar/corregir natural sort/hint +/-1 como escaneo físico de Memory y
  SQLite: forward/reverse coherentes, sin compararlo como campo ausente ni
  ordenar por _id. Reutilizar almacenamiento/lectores (diccionario/rowid)
  sin migración/SPI nuevo; SQLite debe especificar el orden físico, no confiar
  en un SELECT sin ORDER BY. Capturar mezclas con otras claves, dirección/tipo
  inválidos, sort+hint, filtros, skip/limit, reapertura y reemplazo/reinserción.
  Se valida el orden local del escaneo; no se promete que MongoDB y los motores
  mantengan idéntico orden físico tras cualquier relocalización/concurrencia.
  No extender natural de find a un stage de agregación por analogía.
- Separar extracción de claves documentales para sort/top/bottom de sortArray,
  que no tiene el mismo traversal. Mantener comparación BSON/collation común.
- Evaluar n de topN/bottomN en su contexto de inicialización de grupo, no
  inferir el contrato de una variación por fila. Retener testigos de clave de
  grupo escalar/documental y n constante/dinámico.
- BucketAuto no divide valores iguales entre buckets incompatibles; stdDev
  acepta Decimal128 en los contextos retenidos con su tipo y reglas propios.
  Reutilizar estado numérico donde corresponda sin convertir todo a float
  prematuramente ni extrapolar las reglas de sum/avg.
- Diferenciales: asc/desc, empates con/sin desempate contractual, arrays vacíos/
  anidados y mezcla escalar/documento en sortArray (sin TypeError crudo),
  missing/null, collation, límites y grupos/tamaños extremos retenidos.
  Push/firstN conservan la secuencia de entrada ordenada, incluido SQLite:
  comprobar antes/después de materializar o de spill, sin ordenar su resultado.
  Top-level, facet/lookup, ventanas/spill cuando la ruta afectada los consume.
- Cierre: claves y resultados nativos sin exigir órdenes no garantizados;
  selección antes de limit/top-k; mismos resultados en fast path/fallback y
  con/sin spill. La optimización de enteros de P2 mantiene su guard contextual.

#### F09 — Argumentos, calendario y formato: tres unidades de corrección

- Prioridad F09: las subunidades b/c son P1 por valor erróneo; a es P1 para
  selección/valores, coerciones y recursos, P2 para detalles solo diagnósticos.
- F09a: extender tablas de aridad existentes para los variádicos retenidos;
  incorporar primitivas BSON a limit/sample.size/slice/range/round.place/
  dateAdd.amount/dateTrunc.binSize/maxN.n. Cada argumento conserva aceptación,
  truncamiento o entero exacto según su referencia, rango y fase propios.
  Añadir las formas de null/missing, nombres de stages/variables y contexts
  enumeradas en la tabla de disposición. Range debe rechazar tamaños fuera
  del contrato antes de materializar el array, no después de un OOM.
  Un aborto del host previo es evidencia aportada, no una ejecución a repetir.
- F09b: corregir anclas/bins de dateTrunc, duración frente a calendario/DST de
  dateAdd/dateSubtract, acarreo de dateFromParts y formatos j/U/w/Z/%% de
  dateToString, el diagnóstico de formatos desconocidos, startOfWeek de
  dateDiff y entradas Timestamp/ObjectId de los operadores retenidos.
  No deshacer los helpers enteros de milisegundos ya aceptados.
- F09c: formato double/Decimal128/UUID de toString, coerción de operandos
  toUpper/toLower/strcasecmp y conjunto explícito de trim por defecto;
  preservar chars explícitos, null, tipos y errores. Validar tipos antes
  de reutilizar stringify: bool/ObjectId admitidos por toString no quedan
  admitidos por toUpper por esa sola razón. No imitar casing Unicode
  indefinido ni sustituir el backend regex.
- Diferenciales de a: cero/uno/varios argumentos, tipos SDK/local/internos,
  long/double/decimal, bool, NaN/infinito, rangos y vacío/poblado. Preparación,
  evaluación constante/dinámica y ramas no alcanzadas con su fase nativa.
- Diferenciales de b: pre/post epoch, bins que cruzan día/mes/año, dos cambios DST
  y ambos signos, UTC/naive/aware, offsets y zonas IANA retenidas, acarreos
  positivos/negativos. Fechas representables en el dominio público vigente;
  fuera de rango, error o valor conforme al contrato ya declarado.
- Diferenciales de c: ceros con signo, NaN/infinito, notación científica,
  fronteras de exponente y caracteres de trim incluidos/excluidos, NUL y chars
  vacío/multicodepoint. No normalizar texto diferente en el observador.
- Cierre por unidad: selección/valor/tipo/error/fase nativos del inventario,
  aislados del decimal context y defaults Python. Se requieren los tres
  cierres para aceptar F09; ampliar a todo BSON Date o todo formato pasa a X04.

#### F10 — Fachadas, metadatos y límites de capacidad

- Database.client devuelve el cliente propietario, conservando identidad sync/
  async y clones; command(nombre, valor) y rename(dropTarget) respetan su
  contrato PyMongo, aliases y guardas Python sin alterar parsers raw P2.
- Index_information de namespace ausente devuelve la vista nativa sin crearlo;
  proyectar metadata pública, incluido _id_: unique:true intrínseco se omite
  donde lo omite el servidor y v se conserva (v:2 en el testigo retenido).
  La eliminación previa de unique:false no resolvía ese caso; no debilitar
  la unicidad del índice intrínseco ni imponer v:2 a índices con otra versión.
  Conservar la diferencia raw listIndexes/error frente a método de listado.
- Distinguir stage reconocido no implementado (out/graphLookup) de desconocido,
  usando catálogo y diagnóstico locales existentes. No falsificar paridad de
  error con MongoDB, donde el stage válido puede ejecutarse.
- Natural sort/hint se acepta y corrige por F08, con su gramática compartida
  en métodos/raw. No introducir aquí un rechazo general de esa opción.
- Para viewOn, capped, return_key/show_record_id y demás capacidades
  ausentes: documentar soporte/rechazo explícito coherente, sin ejecutar una
  interpretación silenciosamente incorrecta ni declararlas implementadas.
  El soporte efectivo de esas capacidades pertenece a X03/X04.
- Diferenciales de API/metadata: SDK soportados, llamadas posicionales/keyword,
  identidad del cliente, rename con destino existente/ausente, dropTarget y
  errores sin efectos; namespaces ausentes/vacíos/poblados e índices explícitos.
  Para capacidades ausentes, testigo nativo de capacidad y test local de la
  limitación declarada: no se exige que ambos devuelvan el mismo resultado.
- Cierre: fachadas del inventario coinciden con su oráculo; los límites locales
  quedan visibles en runtime, catálogo/docs/tests. No hay ghost namespace ni
  cambios de datos al rechazar una capacidad; extensiones registradas conservadas.

#### F11 — Gate final de la parte I

- Integrar el mapa requisito → caso → consumidor → evidencia → artefacto,
  conservando las aceptaciones de bloque y los rojos intermedios.
- Un wheel inmutable ligado por hashes/rutas a las ejecuciones; unittest y
  pytest completos en Python 3.13/3.14/3.15, cobertura instalada real >=99,00%,
  propiedades deep; matriz Python × pytest 8.4.2/9.1.1 × SDK 4.9.2/4.19.0
  y perfil realmente sin bson/pymongo, conforme a los runners actuales.
- Tipado/API pública/manifiestos/SPI v2, snapshots, lint contra ancestro de
  release, smokes wheel/sdist/pytest-kit, dos builds reproducibles y checks
  de distribución; archivos SQLite anteriores, reapertura, índices y escritura.
  Reutilizar perfiles SDK independientes cuando sus entradas sigan idénticas.
- Diferenciales nuevos 7/8/9 del alcance y regresiones afectadas O1–O3/P2 sobre
  las superficies soportadas. Replica set solo para efectos/watch que lo exijan;
  usar fixture propia existente y limpiar bases, procesos y contenedores.
- Conservar presupuestos deterministas. Comparar coste contra 2fa637f6 con
  datasets y resultados contractualmente equivalentes en los controles de
  paths modificados (índices, collation, sort, consumo, matching, ventanas y
  acumuladores según impacto). Si el bug cambiaba el resultado de la base,
  esa salida errónea no es oráculo ni control de rendimiento equivalente:
  validar la salida nueva contra MongoDB y medir el coste correcto por separado.
  No exigir otra mejora sobre el P2 ya aceptado ni sumar ratios con bases distintas.
  Reformulación del gate de coste autorizada el 2026-11-02: se separan
  presupuesto, precisión de medida y decisión. No reabre O1–O8/D05/P2 ni
  modifica los demás gates de F11 o la parte II.
  Presupuesto explícito: aumento máximo de CPU del 5% respecto a 2fa637f6
  en cada uno de los 44 controles retenidos, sin compensar casos entre sí.
  Sustituye expresamente la tolerancia variable `1 + ruido A/A`; conserva
  su techo anterior de 5%, pero ahora es un presupuesto de regresión decidido,
  no una afirmación de ausencia de cualquier incremento detectable.
  Identidad BSON, datasets, tipos, contadores y presupuestos deterministas
  siguen siendo requisitos duros. Una divergencia funcional no es ruido.

  Reutilizar los dos workers existentes sin modificarlos, incluidos sus
  calentamientos y agregación interna: CPU por operación en consumo y mediana
  de tres muestras en búsqueda. Python 3.14.5, GC activo, PYTHONHASHSEED=0 y
  taskpolicy -a idénticos para A/base y B/candidato. Fijar 500 operaciones
  de consumo y 1.000 de búsqueda por muestra antes de observar A/B. Los tres
  intentos A/A ya conservados son diagnóstico de inestabilidad; no se ejecutan
  más calibraciones ni se usan sus tiempos para aceptar al candidato. Cada
  worker ya calienta su lectura antes de medir; no añadir calentamientos externos.

  Máximo dos intentos por candidato material y configuración fijada, cada uno
  con tres bloques ABBA completos y secuenciales. El segundo añade tres bloques
  al primero: se conservan y usan todos los seis, sin escoger rondas, descartar
  valores ni elevar el presupuesto. No reiniciar el contador sin una corrección
  material causal o una nueva disposición explícita del owner. Los datos anteriores
  al cambio de protocolo nunca se mezclan con esta comparación.

  Para cada caso y bloque i, x_i = (log B1 + log B2 - log A1 - log A2)/2,
  con la agregación propia del worker aplicada antes de los logs. ABBA cancela
  deriva temporal lineal en esa escala; no garantiza eliminar interferencias.
  La razón estimada es exp(media(x)). Tras n=3 bloques, y sólo si hace falta
  tras n=6, calcular límites exp(media(x) +/- t*s(x)/sqrt(n)), con desviación
  muestral y t=4,303 (n=3) o 2,571 (n=6), redondeados conservadoramente desde
  la tabla de Student de NIST (https://itl.nist.gov/div898/handbook/eda/section3/eda3672.htm).
  Son límites unilaterales del 97,5% en cada una de las dos evaluaciones,
  repartiendo alfa 0,05 entre ambas. La aceptación exige que todos los casos
  cumplan, una prueba de intersección-unión; no se promete cobertura simultánea
  de todos los intervalos ni una garantía universal fuera de este experimento.
  La inferencia es condicional a bloques independientes y log-razones
  aproximadamente normales; no es una cota libre de supuestos con tres muestras.
  Valores no finitos, tiempos no positivos, varianza cero por resolución,
  bloques incompletos o interferencias que invaliden esos supuestos dejan
  el caso inconcluyente, nunca verde. Conservar datos y rangos para revisión.

  Decisiones observables: aceptado sólo si el límite superior <=1,05 en los
  44 casos; regresión observada si algún límite inferior >1,05 (investigar
  causalmente ese caso); en los demás casos, inconcluyente. Una regresión
  observada es evidencia por caso, no una afirmación familiar de significación.
  Si la primera evaluación es inconcluyente, ejecutar el único intento adicional
  y evaluar los seis bloques acumulados. Si la segunda sigue inconcluyente,
  detener medición automática y conservar F11 pendiente: falta otro entorno
  adecuado o disposición explícita del owner; no es aceptación de rendimiento
  ni cierre de entrega. Un fallo técnico/funcional se conserva y se corrige
  según su causa, sin convertirlo en inconcluyente para ocultarlo. Los demás
  gates ya aceptados no se repiten salvo invalidación causal.
- Cierre: todos los criterios exigidos de F00–F10 comprobados, gates aplicables
  terminales verdes, revisión final, docs de comportamiento/migración alineadas,
  commits semánticos por corrección/familia y árbol limpio. No basta el
  redondeo de coverage, un skip físico, un smoke o una prueba focal.
  Este gate no implica tag, publicación, despliegue ni aceptar la parte II.

#### Evaluación autorizada de b93681b7 como candidato separado

Autorizada en conversación tras el plan acotado B→A. Payload productivo
b93681b74a284ed09138ef3d647bedf6ff7f78da; e4eb17cc y su wheel/evidencia
permanecen congelados. No reabre O1–O8/D05/P2 ni amplía parte II.

Obligaciones finitas, por orden y dependencia:

1. G1: identidad y reutilización. Fijar hashes/inputs y conservar focales B→A,
   revisión, oráculos nativos, fixtures históricas y controles estáticos con
   inputs transitivos idénticos. No atribuir resultados del wheel anterior al nuevo.
2. G2: dos builds reproducibles, checks de distribución, instalación aislada
   y guard de procedencia sobre wheel inmutable; no sobrescribir artefactos previos.
3. G3, después de G2: coste aislado contra 2fa637f6, los 44 controles completos,
   dos workers inalterados y protocolo F11 anterior. Contador propio 0/2 para
   esta corrección material: máximo dos intentos de tres ABBA; segunda evaluación
   acumula los seis bloques. Sin mezclar muestras ni heredar los 32 verdes de
   e4eb17cc. A/base también se mide en cada ABBA nuevo. Topes operativos añadidos:
   45 minutos de pared por intento y 90 acumulados; al agotarlos, detener y
   conservar datos incompletos como inconcluyentes, sin ampliar ni reiniciar.
4. G4: integración final sobre el artefacto nuevo: unittest/pytest completos
   Python 3.13/3.14/3.15, cobertura instalada >=99,00% y propiedades deep. Replay
   alcanzado incluido en esa suite; no duplicar focales ni recapturar oráculos
   sin invalidación. Una regresión de coste demostrada detiene promoción y
   difiere globales hasta resolver su causa; un coste inconcluyente permite
   completar los gates independientes para dejar explícito el bloqueo restante.
5. G5: matriz de doce celdas Python/pytest/SDK y consumidores alcanzados: SPI,
   perfil realmente sin SDK, conformance, reapertura/lectura SQLite histórica,
   smokes wheel/sdist/pytest-kit. Reutilizar controles no alcanzados con
   justificación por hashes; repetir sólo lo invalidado o exigido al artefacto final.
6. G6: receipt final con todos los gates exigidos, evidencias y límites ligados
   a la nueva identidad, revisión, commits semánticos y árbol limpio. No hay
   aceptación de F11 sin coste aceptado y demás criterios obligatorios verdes.

Se reutilizan los runners, scripts, capturas y documentos existentes. No hay
nuevas optimizaciones, workloads, calibraciones A/A ni ampliaciones de muestra.
Coste sin otras cargas propias; gates compatibles agrupados después. El intento
segundo sólo procede por primera evaluación inconcluyente, no por regresión
confirmada o fallo técnico que deba conservarse y resolverse causalmente.

#### Corrección acotada del viaje de metadatos SQLite

Autorización posterior del owner: «Corrígelo», sobre el coste residual de
resolución de contexto. Se implementa en rama perf/sqlite-empty-options desde
afcf3990; e4eb17cc y b93681b7/artefactos permanecen congelados. No se reinicia
ninguna campaña ni se alteran límites, muestras o decisiones de coste.

S1: mantener el contrato de collection_options y su autoridad de sesión y
namespace. S2: evitar sólo el viaje redundante de opciones vacías ya observadas
en SQLite :memory:, sin SQL ni espera de locks en el event loop. Reutilizar el
owner SQLiteConnection: una observación acotada (total_changes, db, collection),
fuera de transacciones y ligada a la conexión primaria. Una escritura invalida
conservadoramente por contador monotónico, incluso tras rollback; close libera
la observación. Archivo, opciones no vacías, perfil virtual, conexión no estándar,
conexión/contexto diferente o lock ocupado conservan el executor. No hay caché
por colección en API, invalidaciones DDL dispersas, nuevo SPI ni formato persistido.

S3: pruebas focales de ausencia de job/SQL en caliente, frescura en cambios,
sesiones/rollback/reconexión/contención y archivo externo; tres Python y replay
7/8/9 alcanzado, lint, revisión y commits semánticos. No repetir los gates F11
globales ni ejecutar perfiles/coste dentro de esta corrección. El resultado es
una corrección comprobada focalmente; no acredita el +5% ni aceptación de entrega.

#### Evaluación propuesta de b83789bc tras la corrección SQLite

Estado: ejecución autorizada por el owner mediante «Impleméntalo», después de
preparar este plan. Se conserva el protocolo F11: no se sustituyen sus garantías
por la sonda diagnóstica ni se reinician campañas anteriores. La autorización
alcanza G1–G6 y los intentos acotados de este candidato material.

Payload productivo propuesto: b83789bc36bc1bebe693d09b782e5e242be24db4,
rama perf/sqlite-empty-options. Los commits posteriores exclusivamente de
plan/evidencia no cambian esta identidad. Base de coste inmutable:
2fa637f6254a09acd6c81aeee5a36384fdbdbfa7. Permanecen congelados e4eb17cc,
b93681b7, sus wheels/manifests y los registros de sus dos intentos agotados.
O1–O8, D05 y P2 siguen cerrados; parte II excluida.

Causa material que justifica evaluar otro candidato: ce2ca71c elimina el job
redundante de opciones vacías en SQLite en memoria; b83789bc conserva identidad
SQL ante subclases str. Frente a b93681b7 sólo cambian tres módulos productivos:
_sqlite_admin_runtime.py, _sqlite_connection.py y sqlite.py. Cambian elegibilidad,
observación y lectura de metadata, con alcance en contexto/collation de lecturas,
agregación y búsqueda, sesiones y lifecycle SQLite. No hay nuevo formato ni SPI.

La sonda ya ejecutada mantiene GC activo, 600 lecturas y 21,376081 segundos;
compara b93681b7 instalado con b83789bc fuente usando el mismo worker. En cada
caso elimina 99 jobs de opciones y 297 iteraciones del event loop por 99 lecturas
perfiladas; conserva BSON, fetch/cierre y trabajo vectorial observado. Es evidencia
causal de trabajo eliminado, no aceptación de coste: una ejecución C→D con
profiling no ofrece los intervalos F11 ni equivalencia de packaging. Registro
canónico de resultados en el receipt y evidencia b837-attribution-*.

Lista cerrada de obligaciones; los IDs G1–G6 se califican aquí por b83789bc y no
reabren las disposiciones históricas de b93681b7:

| Obligación / dependencia | Evidencia faltante y ejecución mínima | Criterio observable de cierre |
| --- | --- | --- |
| G1 identidad y reutilización | Comparar manifest de producto b83789bc con b93681b7; fijar intérpretes, dependencias, scripts, corpus y entradas transitivas de cada control reutilizado. | Ledger enlaza hashes, comando/corpus y evidencia terminal; tres módulos productivos cambiados identificados, ninguna reutilización sólo por nombre del gate. |
| G2 artefacto, después de G1 | Dos builds reproducibles y distribución con mecanismos existentes; instalación aislada y guards de imports. | Wheels/sdists reproducibles, checks verdes y hashes completos; todas las pruebas posteriores identifican el payload instalado, sin importar src por accidente. |
| G3 coste, después de G2 | Nueva comparación de los 44 controles completos contra 2fa637f6, sólo tras autorización de inicio y bajo el presupuesto de abajo. | Los 44 límites superiores <=1,05 y requisitos duros de equivalencia/supuestos satisfechos; inconcluyente no cierra. |
| G4 integración, después de G2 | Unittest/pytest completos del candidato instalado en Python 3.13/3.14/3.15, cobertura instalada y deep. Son requisitos expresos de artefacto final, aunque gran parte del producto no cambie. | Procesos terminales verdes, cobertura >=99,00% sin redondeo y deep verde; replay alcanzado dentro de la suite, sin recaptura ni tanda focal duplicada. |
| G5 compatibilidad, después de G2 | Matriz vigente de doce celdas Python/pytest/SDK y consumidores invalidados por SQLite/artefacto: conformance, SPI, SDK-free, smokes wheel/sdist/pytest-kit, archivos SQLite históricos, sesiones y reapertura. | Celdas/gates exigidos verdes sobre el artefacto o controles independientes reutilizados mediante G1; sin skips nuevos que sustituyan una obligación. |
| G6 entrega, después de G1–G5 | Revisar diff final, registrar aceptación y límites en receipt, recursos liberados, commits semánticos y limpieza. | Todos los criterios anteriores aceptados, producto idéntico al evaluado y árbol limpio; sin tag/publicación implícitos. |

Reutilización e invalidación, sin otro runner ni ledger documental paralelo:

- Se conservan focales/revisión de b83789bc: 306 casos por Python y 840 replays,
  más evidencia anterior de consumo no alcanzado. No repetirlos por separado;
  los globales finales los incluyen cuando pertenezcan a su corpus.
- Capturas nativas 7/8/9, expectativas, fixtures SQLite históricas, base de coste
  y workers permanecen oráculos/inputs válidos. Verificar hashes antes del uso;
  no volver a capturar ni reconstruir una base antigua.
- La aceptación funcional/compatibilidad de b93681b7 no se transfiere al nuevo
  artefacto. El camino metadata/contexto SQLite invalida sus resultados alcanzados;
  G4 y la matriz G5 exigen además la ejecución del candidato final expresamente.
- Controles estáticos puros (p. ej. snapshots o tipado con targets no alcanzados)
  sólo se reutilizan con inputs transitivos idénticos y sin requisito de ejecutarse
  sobre la distribución nueva. Packaging, inventario, procedencia y cualquier
  control que lea uno de los tres módulos se comprueban de nuevo. Resolver el
  ledger antes de lanzar cada grupo; no presumir reutilización del tipado engine.
- Ningún tiempo antiguo ni los 30/32 controles aceptados se heredan. Los registros
  históricos sirven de diagnóstico. Los 14 inconcluyentes no son una selección
  suficiente para aceptar este candidato: se mantienen los 44 controles.

Presupuesto de coste propuesto, sin cambiar el protocolo vigente:

- Contador separado de evaluación del producto material b83789bc: 0/2 intentos
  ejecutados; no es un reset de e4eb17cc ni b93681b7. No comienza con este plan.
- Máximo dos intentos, tres ABBA cada uno, 45 minutos de pared por intento y
  90 acumulados. El reloj incluye lanzamiento/calentamiento/medición de workers;
  preparación de artefactos va en G2, no se usa para esconder mediciones adicionales.
- Mismos workers y hashes acreditados, Python 3.14.5, GC activo con thresholds
  (2000,10,10), PYTHONHASHSEED=0 y taskpolicy -a; 500 operaciones de consumo y
  1.000 de búsqueda por muestra, agregación y calentamientos existentes intactos.
  No calibraciones A/A, perfiles adicionales ni otras cargas propias concurrentes.
- A/B siempre base 2fa637f6 / wheel nuevo. Primer análisis tras tres bloques;
  sólo si es inconcluyente procede el segundo, acumulando los seis. Fórmula,
  límites t=4,303/2,571, alfa y supuestos exactamente los del protocolo F11.
- Los 44 casos deben pasar el límite superior 1,05 sin compensación entre ellos.
  Límite inferior >1,05 detiene promoción para investigar una regresión observada.
  Fallo técnico/funcional queda registrado como fallo; no se disfraza de ruido.
- Timeout, bloques incompletos o supuestos no defendibles impiden aceptación;
  conservar todo lo ejecutado y el intento consumido, sin reemplazar muestras,
  reintentar automáticamente o extender presupuesto. A los dos intentos o al
  límite operativo, detener: F11 pendiente si queda un caso inconcluyente.
  Otra configuración/entorno exige disposición previa del owner, no un tercer
  intento encubierto. No se promete que este diseño decida cerca del umbral.

Orden de ejecución futura: G1→G2→G3 aislado; después agrupar G4/G5 compatibles.
Si G3 demuestra regresión, detener promoción y diferir globales hasta resolver
su causa. Si queda inconcluyente, completar los gates independientes autorizados
sin repetirlos después por calendario; G6 sigue pendiente de aceptación de coste.
Antes de lanzar: comprobar intérprete/dependencias/rutas/configuración y colección
de selecciones complejas; usar resultados terminales. Sólo una regresión demostrada
o una garantía obligatoria incumplida permite cambiar el payload; cualquier cambio
posterior exige identificar qué evidencia invalida, sin reiniciar automáticamente
coste. No hay nuevas auditorías, optimizaciones opcionales, infraestructuras,
publicación ni ampliación del alcance.

Estado inicial: 0/6 obligaciones de evaluación aceptadas para este artefacto;
preparación del plan y sondas/focales sí comprobadas. Siguiente gate al autorizar
la ejecución: G1, seguido de G2; ningún gate se presenta como ya ejecutado aquí.

#### Revisión de los tres informes: salida acotada del inconcluyente b83789bc

Disposición: análisis y preparación solicitados por el owner tras ef578283.
El owner autoriza mediante «Continúa con la implementación» la continuación
confirmatoria con datos nuevos descrita abajo. Se reanuda G4/G5 y después esta
única etapa final; no se reinicia la campaña agotada ni se reinterpretan como
verdes los resultados anteriores. Presupuesto productivo
+5%, payload b83789bc/wheel 176a9857, base 2fa637f6, cierres previos y exclusión
de parte II permanecen intactos. Los dos intentos originales siguen agotados.

**Contraste y decisión sobre las ideas de los informes**

| Idea agrupada | Resultado del contraste | Decisión |
| --- | --- | --- |
| Fallo aritmético del gate o de identidad | Los 88 intervalos/clasificaciones se reproducen; 287 módulos por lado coinciden con Git/instalación. El pendiente conserva razón 1,01834, cota 1,05521. | No corregir workers ni artefactos. Mantener G3 inconcluyente. |
| «No hay sobrecoste real», por media <5% o menor CPU entre campañas | No se deduce de esas observaciones; cambian las sesiones y la base medida. | No declarar ausencia de coste ni introducir una optimización especulativa. |
| Ruido compartido y carga externa | La dispersión mediana pasa de 0,0163/0,0148 a 0,0266; correlación del pendiente con los otros 39 controles 0,9267. Explica covariación, no identifica temperatura, frecuencia, scheduler o carga causante. | Conservar datos crudos; no sustraer modo común ni descartar bloques. Tratar mecanismo externo como hipótesis. |
| Prefetch, codec y GC | Hay 101 documentos públicos por lectura, no uno; ambos lados leen 200 filas. Ninguna recolección cíclica en la sonda exacta. | No convertir volumen lógico de strings en bytes de basura cíclica ni atribuir la varianza histórica al GC. No modificar batching. |
| Sonda exacta base→candidato | Ejecutada: 200 lecturas/12,698536 s, GC activo, artefactos instalados, mismo BSON y contadores seleccionados. | Reutilizarla; no repetir atribución antes de continuar. No descubre un defecto productivo removible. |
| Otro test nuevo al 97,5% basta gracias a intersección-unión | Confunde la intersección de controles con repetir decisiones sobre el mismo control. Simulación en frontera eleva falsa aceptación a ≈0,069. | Rechazar esta regla y sus promesas de estabilización/potencia. |
| Pooling 6+6 con t elegido entre 2,49 y 2,85 | 2,49 puede superar 0,05 incluso bajo normalidad iid; 2,85 tiene apoyo por simulación, no una cota demostrada para todos los escenarios reclamados. Mezclar sesiones exige justificar varianza común. | No adoptar pooling ni un calibrador permanente. Reservarlo como alternativa con decisión y prueba propias, fuera de esta salida. |
| Datos nuevos con alfa residual acotado | Se demuestra una reserva conservadora de 0,005 bajo el modelo del protocolo, sin igualar la varianza de la sesión vieja y la nueva. | Adoptar esta alternativa, con seis bloques nuevos y una sola decisión final. |
| Repetir todo o medir en otro hardware | No se autoriza como retry automático; tampoco conserva automáticamente los 43 verdes ni el presupuesto de error al ofrecer más oportunidades. | No usar ahora. Sólo nueva disposición si esta salida no se autoriza o no decide. |

También se corrige la tabla histórica del primer informe: los doce inconcluyentes
de e4eb17cc eran nueve de consumo SQLite, dos vectoriales y uno Memory, no doce
de SQLite. Los catorce inconcluyentes de b93681b7 están aceptados en b83789bc;
el único pendiente actual estaba aceptado en b93681b7. No son «13 de 14 resueltos».
Esto es historia descriptiva, nunca transferencia de aceptación entre payloads.

Los informes fueron escritos antes del checkpoint ef578283: sus referencias a
G4 todavía ejecutándose son históricas. Hoy hay seis procesos completos verdes
y cobertura instalada 48288/48770 = 99,0116875128%; sólo deep falta en G4.
G5 tiene perfiles/selección prevalidados, no matriz/consumidores ejecutados.
Ni la resolución de coste ni esta revisión aceptan esos gates pendientes.

**Garantía estadística de la alternativa elegida**

La intersección-unión no exige dividir alfa entre 44 para aceptar que todos
cumplen, pero sí exige controlar el procedimiento completo de cada control,
incluidas miradas adicionales. Se conserva un error de falsa aceptación <=0,05
por procedimiento, condicional al modelo ya usado por F11; no se promete
cobertura simultánea de 44 intervalos ni normalidad/independencia demostradas
por seis observaciones. Referencia: Berger y Hsu, Statistical Science 1996,
[registro de los autores](https://asu.elsevierpure.com/en/publications/bioequivalence-trials-intersectiounion-tests-and-equivalence-conf/).
Cambiar un diseño después de observar datos exige controlar su error, no sólo
preinscribir el cambio antes de la siguiente medición; véase
[Müller y Schäfer 2004](https://doi.org/10.1002/sim.1852).

Sea E3/E6 el evento de falsa aceptación en las miradas de tres/seis bloques,
con críticos 4,303/2,571, para un control en la frontera log(1,05).
Bajo bloques normales iid de la campaña original, cada marginal <=0,025.
Su intersección tiene probabilidad >0,0053: por tanto
P(E3 unión E6) <0,0447. Esta envolvente también cubre el stop global del runner.
No se estima el alfa a partir de la varianza observada del control pendiente.

Derivación reproducible de la cota: separar los seis normales estandarizados
en dos triples. Sus medias escaladas Z1/Z2 son normales estándar independientes;
sus sumas residuales U/V son chi-cuadrado(2) independientes. En coordenadas
Z1=r cos(theta), Z2=r sin(theta), definir a=4,303, b=2,571,
phi=theta-pi/4, h=5 cos(phi)^2/b^2-sin(phi)^2,
q=2 cos(theta)^2/a^2 y m=min(q,h). Para h>0, integrar U/V y r da

P(E3 intersección E6) = integral [m/(1+m)-m/(1+h)^2] dtheta/(2 pi),

en theta=pi/4 +/- atan(sqrt(5)/b). El integrando crece con m y h cuando
0<=m<=h. Acotar q por el extremo derecho de cada subintervalo y h por el
extremo más distante de pi/4 produce sumas inferiores: 0,0053693 con 1000
subintervalos, 0,0053810 con 10000. Se resta margen numérico en cada cota;
se usa sólo la cota holgada >0,0053, no un cuantil ajustado por Monte Carlo.
Código/resultados conservados en b837-report-review.py/json.gz. Simulación
independiente de cuatro millones de réplicas respalda el cálculo, pero no es
la justificación de nivel ni una prueba del comportamiento de la máquina.

Una única prueba nueva con alfa <=0,005 da, por unión de eventos,
P(falsa aceptación antigua o nueva) <0,0447+0,005 <0,05.
La prueba futura debe ser de nivel 0,005 condicional a la información antigua:
bloques nuevos independientes de ella, mismo parámetro objetivo y tamaño/regla
fijos antes de medir. Puede tener otra varianza que la campaña antigua, sin
mezclar ambas varianzas. Dentro de cada etapa rigen normalidad iid/independencia;
no se afirma validez uniforme con dependencia, deriva o colas arbitrarias.
La selección del único pendiente no elimina este requisito ni añade alfa;
los otros 43 no reciben oportunidades nuevas de aceptación.

**Ejecución propuesta: una etapa final de G3, no un reinicio**

- Primero, al reanudar la ejecución, completar deep y G5 con el artefacto actual.
  Reutilizar las seis suites/cobertura ya aceptadas y las preselecciones válidas;
  no lanzar globales nuevos ni recapturar oráculos por esta revisión documental.
  Ninguna enmienda de coste dispensa G4/G5/G6. Si aparece una regresión, resolver
  su causa e identificar invalidaciones antes de medir otro payload.
- Sólo después de autorización explícita de esta enmienda: congelar antes del
  lanzamiento hashes A/B, intérprete/dependencias, worker y reglas que siguen.
  G1/G2 y los 43 controles aceptados de look6 siguen válidos para el mismo payload,
  hardware y configuración; otro hardware o cambio material requiere otra decisión.
- Un único intento adicional explícito, seis bloques ABBA nuevos (24 invocaciones)
  del worker de consumo existente, sin tocar sus bytes, orden, calentamientos,
  500 repeticiones, relojes ni GC. Se ejecutan y archivan sus 40 controles;
  sólo se decide SQLiteEngine-standard-aggregate-first-page. Los otros 39 son
  diagnóstico/fidelidad, no nuevas decisiones; los cuatro vectoriales ya aceptados
  no requieren ejecutar otra vez el worker de búsqueda. Fallos de equivalencia
  en cualquier salida ejecutada son fallos duros, no se ignoran por ser diagnóstica.
- Python 3.14.5, GC activo (2000,10,10), PYTHONHASHSEED=0 y taskpolicy -a;
  mismos wheels instalados, codecs/datos y APIs. Sin cargas propias concurrentes.
  Evitar carga intensa ajena conocida al iniciar, sin exigir una reserva de
  60–90 minutos ni prometer fijación de núcleos. Si se observa interferencia que
  invalida supuestos, conservar todos los bloques y no aceptar; nunca filtrarlos.
- Tope de 40 minutos de pared desde el primer lanzamiento; preparación y checks
  sin ejecutar lecturas de benchmark antes del reloj. Estimación histórica:
  2024 s (33,7 min). Sumado a 2866 s ya gastados, el máximo es 5266 s (87,8 min),
  dentro del techo acumulado original de 90 min. Cambia sólo el máximo de intentos
  mediante esta excepción explícita, no se ponen a cero sus dos intentos agotados.
  Ocho bloques completos con el mismo worker estiman 2698 s (45,0 min) y 2833 s
  usando el brazo más lento: no caben con margen en este presupuesto, se descartan.
- Sin análisis intermedio ni cambio adaptativo de n. Tras seis bloques nuevos
  completos, computar exclusivamente sus seis log-razones con la fórmula ABBA
  vigente. Límites exp(media +/- 4,033*s/sqrt(6)); 4,033 es conservador para
  t(0,995;5), límite unilateral de 99,5%, según [NIST](https://itl.nist.gov/div898/handbook/eda/section3/eda3672.htm).
  No agrupar con los seis antiguos ni retirar muestras. Superior <=1,05 acepta
  el pendiente; inferior >1,05 señala regresión; en otro caso, inconcluyente.
- Comprobar hash BSON/count/repeticiones de todas las salidas contra referencia,
  procedencia del artefacto, valores finitos/positivos, varianza no nula y bloques
  completos. Un fallo técnico se conserva como fallo; timeout/incompletitud o
  supuestos no defendibles impiden aceptación. No reemplazar ni repetir bloques.
- Una única decisión final; sin cuarto look ni continuación automática. Si no
  acepta, F11 continúa pendiente para b83789bc. Registrar las tres disposiciones
  de etapa sin sobrescribir look3/look6 ni la campaña agotada. Sólo si acepta se
  compone G3 con los 43 aceptados de look6, con procedencia distinta explícita.
  G6 exige además G4/G5 verdes, revisión, commits semánticos y árbol limpio.

Esta alternativa conserva garantías, pero sacrifica potencia frente al pooling:
con seis bloques nuevos y crítico 4,033, simulaciones al efecto puntual observado
estiman aceptación 0,15 si sigma=0,034 y 0,59 si sigma=0,018. Son escenarios
normales hipotéticos, no probabilidades de éxito verificadas del equipo. No se
adoptan las promesas 80–95% de los informes. Si no se autoriza este coste para
esa capacidad de decisión, la salida vigente es mantener G3 inconcluyente;
no cambiar umbral, alfa o producto para fabricar el cierre.

No se añade runner, monitor permanente, selector de casos ni capa de validación.
La futura orquestación adapta la existente; sólo debe fijar antes de lanzar
esta única etapa, sus paths separados, límites y composición. Evidencia de esta
revisión y sonda en la carpeta existente; decisiones aquí, ejecuciones/estado
en el receipt. La implementación se reanuda desde ese checkpoint, conservando
las evidencias y pendientes allí registrados. Esta sección no declara F11 cerrado.

#### Disposición final de la evaluación b83789bc

Evaluación ejecutada según la enmienda autorizada, con payload/wheel intactos.
G1/G2/G4/G5 aceptados y G3 resuelto: 43 controles conservados de look6 más el
pendiente aceptado por la única etapa nueva. Sus seis ABBA completos dan razón
0,9960111770, cota superior 1,0453725368 con crítico 4,033 y alfa 0,005.
No se agrupan con bloques antiguos ni se ofrecen más oportunidades de decisión.
Etapa 2027,880412 s; acumulado 4893,895488 s, inferior a 90 min. Se conserva
la evidencia histórica inconcluyente y la procedencia de cada aceptación.

El receipt registra todos los gates, identidades y límites; G6 se materializa
con su commit semántico y la comprobación final del árbol limpio. No hay otro
gate técnico pendiente de parte I. La aceptación de F11 corresponde al producto
b83789bc/wheel 176a9857; no cambia retrospectivamente la disposición de e4eb17cc
ni b93681b7, no publica una release ni autoriza parte II. La inferencia de coste
conserva los supuestos del protocolo y sólo alcanza sus 44 controles.

### 9.3. Parte II — Capacidades o cambios de contrato con decisión propia

No es una segunda tanda implícitamente autorizada. Cada bloque requiere una
decisión de alcance antes de implementar: alternativa mínima, contrato público,
coste, dependencia y criterio de aceptación. Puede aprobarse, reducirse o
descartarse por separado, sin condicionar el cierre acotado.
Prioridad aquí es de evaluación, no autorización de ejecución.

| Bloque | Prioridad | Qué requiere algo más | Dependencias |
| --- | --- | --- | --- |
| X01 | P2 | Backend y contrato PCRE compatibles | Límites de F04/F09; decisión de dependencia |
| X02 | P2 | Collation adicional y tailoring | F05; backend/índices persistidos |
| X03 | P2/P3 | Views, capped e identidad pública/opciones avanzadas de registros | F05/F06/F08/F10; contrato de almacenamiento |
| X04 | P2/P3 | Stages, operadores y dominios adicionales | F02/F03/F08/F09/F10 según capacidad |
| X05 | P3 | Migración persistida o refactorización transversal | Perfil medido de un bloque que lo necesite |
| X06 | P2/P3 | Ampliación sistemática de garantías O1–O8 y corpus P2 | Decisión de cobertura finita y owners |

#### X01 — Regex/PCRE más allá de la gramática ya corregida

- Decidir subset PCRE2, dependencia obligatoria/opcional y comportamiento sin
  backend. No basta cambiar re por otra librería con sintaxis parecida.
- Censar matching, not/in/all, expresiones regexFind/regexMatch, schema pattern,
  índices/optimizaciones y consumidores que anuncien esa semántica.
- Diferenciales por versión PCRE de referencia: Unicode/opciones, capturas e
  índices, grupos, escapes, lookarounds, patrones inválidos y casos costosos;
  preservar perfil SDK-free y soporte de plataformas. Search-v1 mantiene su
  contrato propio: no se amplía a Atlas por asociación.
- Cierre: subset/degradación explícitos en todas las rutas, mismo valor/error
  de los casos aprobados y presupuesto de recursos comprobado; no quedan dos
  motores dando respuestas incompatibles al mismo contrato sin una distinción.

#### X02 — Collation ampliada

- Decidir caseFirst, strengths/locales adicionales y tailoring ICU, backend
  requerido y fallback/rechazo explícito cuando no exista.
- Diferenciales de igualdad, orden, grupos, índices/unique/partial y operaciones
  cruzadas tras reapertura; datos Unicode y empates, sin prometer reglas de
  casing no garantizadas por los operadores de MongoDB.
- Si exige regenerar claves persistidas, depende de X05; si solo usa el
  subset básico actual, no necesita este bloque y se resuelve en F05.
- Cierre: una matriz pública concreta de backend/opción/dialecto, resultados
  y fallos aprobados, coste y compatibilidad del almacenamiento demostrados.

#### X03 — Capacidades de colección y almacenamiento

- X03a, views: decidir catálogo, viewOn/pipeline, resolución de namespaces,
  herencia de collation, comandos permitidos, ciclos/errores y persistencia.
- X03b, capped: decidir unidades de tamaño, límites/max y expulsión,
  restricciones de updates/delete, atomicidad, recuperación y eventos.
- X03c, recordId/return_key/show_record_id: fijar identidad física pública
  y relación con índices, cursores, borrados, updates y reapertura.
  Natural sort/hint básico pertenece a F08, no depende de exponer recordId.
  No inventar un recordId público inestable ni extrapolar un rowid interno
  a un identificador persistente garantizado tras mantenimiento/reconstrucción.
- Cada subbloque puede aceptarse independientemente. Diferenciales nativos
  por operación/topología necesaria, estado antes/después y reapertura;
  atomicidad/concurrencia solo con pruebas de intercalación si se prometen.
- Cierre: las opciones producen los efectos contratados, no solo metadata;
  las capacidades no elegidas mantienen rechazo explícito de F10.
  Un cambio de archivo o SPI requiere X05 o su decisión equivalente.

#### X04 — Ampliaciones de operadores, dominios y planificación

- X04a, out: decidir reemplazo de colección, índices/opciones, validadores,
  atomicidad/fallo, permisos y efectos de watch, no implementarlo como merge.
- X04b, graphLookup: fijar recorrido, ciclos, profundidad, filtros, arrays,
  collation, memoria y restricciones del subset/topología.
- X04c, capacidades escalares: granularity de bucketAuto, formatos/dominios
  de fecha no anunciados, fechas fuera del dominio público representable y
  otras familias ausentes. No exigir calendario completo para aceptar F09.
- X04d, geo ampliado: geodesia/nearSphere real y planner geo especializado
  fuera del subset planar declarado. Fijar geometrías, distancias/unidades,
  índices/requisitos, degradación, precisión y recursos; no convertir la
  comparación con MongoDB en una promesa esférica implícita.
  Si cambia el almacenamiento de índices, requiere X05.
- Diferenciales nuevos por capacidad; native replica set cuando existan efectos
  o límites transaccionales requeridos. Usar control de datos, índices y eventos
  en out; grafos cíclicos y presupuestos en graphLookup; tipos/rangos en fechas;
  geometrías/límites y precisión/distancia/planner en X04d.
- Cierre: solo se anuncia la capacidad tras sus pruebas funcionales, efectos,
  fallos y límites; no queda una implementación parcial presentada como paridad.

#### X05 — Migración o consolidación estructural

- Solo se abre con evidencia de que una corrección no cabe sin cambiar claves/
  archivos, autoridad semántica o SPI, o con una mejora de mantenimiento medida.
  Mantener el formato/SPI actual es una alternativa explícita.
- Preparar lectura/escritura de versiones, detección de formato, migración/
  recuperación, colisiones/unique, índices y snapshots. Cambiar representación
  no puede aceptarse comparando dos consumidores que comparten el mismo helper.
- El piloto de schema D07 descartado en O4 no revive por este plan. Si se
  reconsidera, exige propuesta separada y demostrar menos puntos de coherencia
  sin otro intérprete permanente ni pérdida de diagnóstico/subset.
- No adoptar Value/KeyString, sidecar universal o SPI v3 como prerrequisito
  implícito de los arreglos. Requieren una decisión arquitectónica propia.
- Cierre: alternativa elegida motivada, migración/compatibilidad y costes
  comprobados, autoridad reemplazada retirada y consumers actualizados.
  Si el piloto no mejora o añade duplicación, se descarta con evidencia.

#### X06 — Ampliación de garantías a familias no cubiertas

- Es la opción para extender exhaustivamente O1/O2 a todas las fuentes,
  expresiones, productores BSON, respuestas, codecs y consumidores; la gramática
  raw P2 a otros comandos/opciones; o la garantía de coste a otros workloads.
- Fijar primero una lista finita de familias y superficies. No usar "paridad
  completa con MongoDB" como criterio de cierre ni convertir todo el informe
  de sondas en una obligación indivisible.
- Un canal nuevo de warnings O3 exige definir API, privacidad, nivel, ciclo de
  vida, sincronía y costes. No se deduce de comprobar strict/moderate/warn.
- Diferenciales: cobertura matricial de esa lista, versiones/topologías y
  contextos definidos, observación independiente y propiedades complementarias.
- Cierre: mapa de requisitos sin entradas exigidas sin prueba; cada incremento
  ejecuta su propio gate tipo F11. No amplía por retroactividad los receipts
  históricos ni exige mejoras porcentuales universales.

### 9.4. Alcance ampliado de O1–O8 y de los dos P2

Recomendación: incluir en la parte I las garantías transversales afectadas y
los gates de entrega; reservar en la parte II las capacidades o la ampliación
general de cobertura. No reabrir los estados ni cambiar los receipts de O1–O8
o B1–B4. Las nuevas aceptaciones usarán los IDs F/X y su candidato propio.

| Antecedente | Parte I: ampliación necesaria | Parte II: trabajo adicional |
| --- | --- | --- |
| O1, fuente materializada | Extender testigos a rutas, collation, sort y selección nuevos; mismo resultado/contexto antes y después de materializar. F03/F05/F08/F11. | Inventariar todas las fuentes/backends o rediseñar sus contratos: X05/X06. |
| O2, BSON/gramática/codecs/contexto | Incluir salidas window/objetos, Regex/size, nuevos argumentos y candidatos de escritura en la matriz de consumidores; batching/decoders previos como regresión. F01–F04/F07/F09/F11; tipos int64 según productor/codec y perfiles. | Garantía general sobre todos los productores/respuestas/tipos o nuevo dominio de representación: X04–X06. |
| O3, validadores/permiso/warn | Repetir los casos afectados por collation, índices y candidato de escritura; conservar niveles/acciones/bypass y ausencia de canal nuevo de logs. F05–F07/F11. | Ampliación de cobertura o canal de warnings: X06; validators de capacidades nuevas: X04; rediseño de schema: X05, con decisión. |
| O4, piloto D07 | Conservar descarte; ninguna obligación de migrar CompiledJsonSchema. | Solo piloto independiente con criterio de beneficio/rechazo: X05. |
| O5, suite/cobertura/deep | Gate obligatorio del nuevo candidato, sin reabrir aceptación anterior ni rebajar 99,00%. F11. | Cada incremento X vuelve a cerrar su candidato con el gate aplicable. |
| O6, Python/SDK/pytest/SDK-free | Matriz y perfiles vigentes sobre candidato exacto; reusar solo evidencia independiente con entradas idénticas. F11. | Nuevos runtimes, dependencias o perfiles tienen aceptación propia: X01/X02/X05/X06. |
| O7, packaging/API/SPI/archivos/coste | Builds reproducibles, compatibilidad SQLite, manifests/typing/SPI y coste de rutas afectadas. F11. | Nuevo formato/SPI/backend o garantía de rendimiento general: X01–X03/X05/X06. |
| O8, trazabilidad/revisión/limpieza | Receipt del incremento, revisión por bloque, recursos liberados, commits semánticos y árbol limpio. F11. | Mismo mecanismo por incremento X; no exige publicación ni otra plataforma de validación. |
| P2 gramática raw, B1/B2 | Conservar 32 formas y sus controles nativos aceptados. Añadir solo campos/operadores/diagnósticos tocados por F01/F04–F07/F09/F10. No delegar validación Python al parser raw indiscriminadamente. | Otros comandos/opciones, catálogo general de gramática o capacidades nuevas: X03/X04/X06. |
| P2 coste, B3 | Conservar los dos workloads y 44 controles como regresión causal; verificar contexto, claves, residual y exact fallback. Nuevos datos/coste solo para paths cambiados. F03/F05/F08/F11. | Nuevas optimizaciones, workloads, SLA o backend: X01/X02/X05/X06. No son requisito para cerrar los arreglos. |
| P2 cierre, B4 | Reutilizar el mecanismo de artefacto/evidencia dentro de F11, sin crear otro gate paralelo. | Aplicar el mismo cierre a cada incremento X aprobado. |

O1–O3 no significan probar de nuevo cada combinación histórica manualmente.
Se reutilizan capturas aceptadas cuyos estímulos/observador siguen válidos;
se ejecutan sus regresiones sobre el código nuevo cuando el camino cambia.
O5–O8 tampoco permiten usar un global antiguo como global del candidato nuevo.

La aceptación raw de las 32 formas y sus controles se conserva como baseline;
el agregado nuevo se identifica por casos y versiones. Para unsupported local
se separa el límite del resultado nativo, no se introduce una falsa divergencia
"aceptada" dentro del corpus que exige paridad.

### 9.5. Evidencia, ejecución y control de alcance

- Mantener el capturador/replay y los owners de corpus en tests/differential,
  semantic_corpora.py y las fixtures versionadas; ampliar cuerpos de casos y
  registro solo donde haga falta. Los workflows ya derivan el inventario:
  no añadir un segundo runner ni otra tabla independiente de expectativas.
- Disposición de este refinamiento: sustituye las afirmaciones sobre la
  regresión limit(0), to_list(0), natural y prioridad F09 del borrador c9620e23;
  mantiene F00–F11/X01–X06 y dependencias salvo natural en F08. Se usaron
  snapshots fuente aislados y SDK reales sin red; no hubo nueva captura
  mongod, tests masivos ni implementación. Las sondas negativas nuevas
  siguen siendo caracterización para F00, no aceptación física 7/8/9.
- Referencias del contraste: [Cursor PyMongo 4.19](https://pymongo.readthedocs.io/en/4.19.0/api/pymongo/cursor.html),
  [hint natural](https://www.mongodb.com/docs/manual/reference/method/cursor.hint/),
  [orden de push](https://www.mongodb.com/docs/manual/reference/operator/aggregation/push/)
  y [toString](https://www.mongodb.com/docs/manual/reference/operator/aggregation/toString/).
  Las tablas actuales de documentación no sustituyen capturas 7/8/9: pueden
  incluir formatos o tipos incorporados en versiones posteriores.
- Estados de F/X en el owner vigente de implementación cuando comience el
  trabajo; propuesta/requisito aquí, ejecuciones/aceptación allí y resultados
  derivados en la estructura de evidencia existente. No copiar historia ni
  editar goldens anteriores para adaptar una corrección.
- Por bloque: caracterizar → reproducir rojo → corregir causa/consumidores →
  diferencial verde → regresión causal → revisión y commit semántico.
  Mensajes/códigos, orden y tipos se normalizan solo donde el contrato lo permita.
- El gate final se hace sobre candidato congelado. Repetir por cambios,
  fallos o requisitos del gate, no por un calendario de auditorías. Agrupar
  verificaciones compatibles; separar mediciones de coste de otros gates CPU.
- Si hay un nuevo hallazgo, entra solo por regresión del candidato o garantía
  obligatoria de la lista F aprobada. El resto se registra como seguimiento.
  Un hallazgo X no amplía de forma implícita el cierre de la parte I.
- Antes de aceptar, lista única de obligaciones pendientes con contrato,
  evidencia faltante y criterio observable; progreso por cerradas/total,
  bloqueos y próximo gate, sin porcentajes estimados.
- Documentar migraciones de comportamiento y decidir la versión en el cierre
  de release correspondiente. Este plan no cambia versión ni publica nada.

Resultado esperado: una entrega acotada F00–F11 demostrada y mantenible,
más una cartera X01–X06 con decisiones explícitas, no una obligación de
implementar todo el catálogo antes de poder entregar una corrección.

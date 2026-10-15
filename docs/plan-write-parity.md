# Plan de la siguiente entrega: escrituras fiables para testkits

Estado: objetivo acordado; corpus y criterios preparados para revisión. **La
implementación de este plan no ha comenzado.** Base inspeccionada: Mongoeco
`c0746ab`, que ya incluye PyMongo 4.19 en `Unreleased`. No se reserva número de
versión, ni se autoriza aquí un tag o una publicación. Fecha: 15 de octubre de 2026.

## 1. Resultado y perímetro

Para el corpus finito de esta entrega, la misma petición debe conservar el
resultado público, el error y el estado persistido que admite MongoDB real.
La unidad de atomicidad es el documento; un lote puede conservar éxitos
anteriores. El objetivo práctico es que un testkit delegue sus escrituras en
Mongoeco sin un intérprete que repare su semántica.

El [manifiesto del corpus](write-parity-corpus.json) es el inventario autoritativo
de IDs, entradas, peticiones, invariantes y referencias de cobertura. Es una
especificación de planificación, **no un runner, una fixture ya capturada ni
un nuevo lenguaje de ejecución**. Los códigos y resultados completos pendientes
se obtendrán de MongoDB, sin rellenarlos con la salida de Mongoeco.

| Parte | Tamaño | Disposición |
| --- | ---: | --- |
| P: paths, arrays y operaciones posicionales | 114 | Reutilizar entradas y ampliar observación. |
| I: inserciones | 8 | ID explícito/generado, duplicados, forma inválida, lote y propiedad BSON. |
| M: modificadores y pipelines | 8 | Éxito, no-op, errores de tipo/path, orden BSON, identidad y update_many. |
| R: reemplazos, retornos y borrados | 8 | Identidad, BEFORE/AFTER, proyección, selección y contadores. |
| U: upserts e idempotencia | 8 | Seed, ID, setOnInsert, pipelines y modelos bulk. |
| N: índices únicos | 8 | Clave secundaria/compuesta, sparse, partial, collation y equivalencia numérica. |
| V: validación de colección | 8 | Insert/update/replace, lotes, bypass, warn y moderate. |
| B: lotes y fallos parciales | 8 | Modelos mixtos, ordered/unordered, varios errores y límite de bloque. |
| **Escenarios secuenciales** | **170** | P + I/M/R/U/N/V/B; sin productos cartesianos adicionales ocultos. |
| Q: historias competitivas | 6 | Diez repeticiones fijas por celda. |
| C: flujos de consumidor | 8 | Canario aislado, sin migrar el repositorio consumidor. |
| Vistas por database.command / wire | 8 + 8 | Son vistas de ocho IDs anteriores, no escenarios semánticos nuevos. |

Las 56 especificaciones adicionales están enumeradas como I01–I08, M01–M08,
R01–R08, U01–U08, N01–N08, V01–V08 y B01–B08 en el manifiesto. P001–P114
referencian por nombre y hash la fixture posicional existente; no duplican sus
inputs ni reescriben sus expectativas históricas.

Quedan fuera: transacciones distribuidas, retries de red y respuestas perdidas,
write concern no reconocido (`w=0`), Search, `$merge`/`$out`, DDL como producto,
nuevos operadores, backends, cambios del formato SQLite y expansión del SPI.
Los índices y validators del corpus son setup de escrituras ya soportadas.
Los contratos de transacciones, publicación local y limpieza existentes siguen
siendo gates de regresión, aunque no se anuncie paridad distribuida.

## 2. Qué cubrimos hoy

El inventario se basa en lectura de código y en la evidencia terminal de
[PyMongo 4.19](evidence/pymongo419/validation.json). No se ejecutaron suites de
runtime para este trabajo documental. Ese cierre registró 8.955 pruebas por
versión de Python, cobertura del 99,01 % y las comprobaciones nativas descritas
abajo; no acredita por anticipado las nuevas observaciones del corpus.

| Zona inspeccionada | Cobertura existente | Hueco para este objetivo |
| --- | --- | --- |
| [Posicionales](../tests/fixtures/positional_update_mongodb8.json) | 114 inputs: 78 errores y 36 éxitos. API sync/async en ambos motores; diferencial sync con perfiles 4.9/4.19 y servidores 7/8/9. | El [observador](../tests/support_positional_updates.py) conserva status, código, índices/códigos bulk y documentos; descarta el resultado de la operación y sus contadores/retornos. El golden serializado sigue siendo de 8.0. |
| [Suite nativa general](../tests/differential/cases.py) | De sus 29 escenarios, cinco son CRUD relevantes: addToSet con orden BSON, setOnInsert ID, pipeline project/ID, pipeline ID inválido e ID raíz array. | No son una matriz de 29 escrituras. Algunos errores solo devuelven código o un flag; no todos conservan estado y forma pública completos. |
| [Retornos y reemplazos](../tests/unit/api/test_collection_find_modify.py) | ID, no-op, BEFORE/AFTER, proyección y opciones; hay pruebas de ambas fachadas en integration. | Referencias locales relacionadas, no oráculos completos por dialecto/caso nuevo. |
| [Índices](../tests/integration/api/test_partial_index_semantic_safety.py) | Unicidad pública, índices compuestos/multikey, collation y pertenencia partial, además de pruebas de engine. | Falta mapear N01–N08 a resultados/error/estado completos del servidor real. Las capturas de recreación de índices son DDL y no cuentan como cobertura de estas escrituras. |
| [Validación pública async](../tests/integration/api/test_async_api.py) y [sync](../tests/integration/api/test_sync_api.py) | Validator, bypass, validación de lotes, duplicados y bulk ordered/unordered. | Hay una tensión contractual concreta en insert_many; ver D1. Tener prueba local no prueba paridad nativa. |
| [Preparación SQLite](../tests/integration/api/test_sqlite_bulk_preparation_contract.py) | Trabajo por bloques, progreso de lectores, cancelación, precedencia de validación y error global tras límite de bloque. | Son garantías del engine/rendimiento, no un oráculo nativo completo del API público. Deben mantenerse al corregir la frontera pública. |
| [Mutaciones seleccionadas](../tests/integration/api/test_atomic_selected_mutations.py) y [snapshots](../tests/integration/api/test_selected_mutation_snapshot_contract.py) | Barreras controladas para CAS/findAndModify, retornos coherentes y CAS SQLite entre instancias. | La matriz competitiva no es simétrica en todas las fachadas/rutas y carece del contraste de historias nativas Q01–Q06. |
| [PyMongo 4.19 wire](../tests/integration/wire/test_pymongo419_compatibility.py) | Bulk, getMore, sesiones y rechazo de BSON malformado con clientes reales. | No es cobertura de todos los fallos y resultados seleccionados para las ocho vistas write. |
| Consumidor mochuelo-testkit | Tests de upsert, arrays, borrado, pipeline y entrada Storage. | Actualmente pasan por lowering de array_filters, reparación de filtros y ejecución propia de modelos bulk. No acreditan delegación directa. |

Las 284 capturas generales por versión del cierre anterior se conservan como
regresión. No se suman como 284 escenarios CRUD: sus corpus principales son
agregación, variables, conversiones e índices administrativos.

Los `related_existing_tests` del manifiesto son referencias **relacionadas**
inspeccionadas; no significan que esa función ya pruebe toda la fila nueva.
Cinco filas tienen un antecedente nativo parcial explícito; V04/V05 identifican
una tensión local y B08 requiere todavía mapear su preflight específico.

## 3. Primer hallazgo a disponer: D1

Hechos locales inspeccionados:

- `Collection.insert_many` y `AsyncCollection.insert_many` no exponen `ordered`.
- Ambas suites públicas contienen
  `test_insert_many_validation_failure_writes_no_partial_batch`: una inserción
  válida seguida de una inválida por schema debe dejar la colección vacía.
- En cambio, las pruebas de duplicidad ya conservan éxitos anteriores.
- El SPI de batch insert y la preparación SQLite mantienen garantías propias de
  validación y publicación; no deben cambiar silenciosamente para resolver el API.

La [documentación nativa de insertMany](https://www.mongodb.com/docs/manual/reference/method/db.collection.insertMany/)
describe conservación de documentos válidos anteriores a un error de schema y
continuación de los posteriores según `ordered`. La
[atomicidad de MongoDB](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/)
es por documento. Esto identifica una diferencia entre expectativas locales y
norma nativa; **la reproducción con errores/contadores exactos sigue pendiente**.

Disposición propuesta para el bloque B0: capturar I05/I06/I07, V04/V05, B04 y B07;
separar forma/serialización del driver de validación de colección del servidor;
documentar el antes/después de la corrección y añadir `ordered` de forma
compatible en las dos fachadas. Revisar la clasificación SemVer y la migración
de aserciones antes de fijar el número de la entrega. El objetivo aprobado
prioriza fidelidad nativa; este plan no autoriza ocultar la diferencia bajo un
perfil nuevo, un modo estricto ni una alteración del SPI v2.

No se conserva una expectativa local contraria al golden solo para mantener
la suite verde. Tampoco se cambia una garantía de bajo nivel sin demostrar qué
consumidores alcanza: si la corrección exige romper el SPI o reabrir una ADR,
se detiene esa corrección y se eleva una decisión acotada al propietario.

## 4. Matrices y fronteras

**API directa:** 170 escenarios × tres dialectos (7.0/8.0/9.0) × dos perfiles
(4.9/4.19) × dos motores × sync/async = **4.080 observaciones locales**. El
oráculo necesita 170 × tres servidores = **510 observaciones de referencia**.
Los inputs comunes no usan `update_one(sort=...)` en el perfil 4.9; las diferencias
de firma de perfiles siguen protegidas por la matriz SDK, sin recortar celdas.

**Concurrencia:** Q01–Q06 cubren find_one_and_update CAS, update_one con hint,
find_one_and_replace CAS, replace_one CAS, colisión unique entre inserts y
find_one_and_delete. Se ejecutan diez historias por cada celda directa:
**1.440 historias locales y 180 nativas**. El ganador no se fija. Q04 utiliza
conexiones SQLite independientes al mismo fichero, además de la autoridad común
de Memory. Las barreras coordinan inicio; no se usa sleep como oráculo ni se
reintenta un fallo hasta obtener verde.

**database.command y wire:** I01, I03, I06, V02, N02, R05, B02 y B03 tienen ocho
vistas en cada ruta, con tres dialectos, dos motores y dos fachadas:
**96 observaciones raw y 96 wire**. La referencia raw son 24 respuestas nativas.
El mapeo usa comandos clásicos insert/update/findAndModify y sus lotes; no añade
Client.bulk_write multi-namespace. Una respuesta raw con `ok:1` y `writeErrors`
no se compara como si fuera la excepción BulkWriteError del helper de colección:
cada ruta se contrasta con la misma ruta nativa y luego se verifica su proyección
común de efectos. Se mantiene la versión wire anunciada.

**Consumidor:** ocho flujos × Memory/SQLite = **16 ejecuciones aisladas**, con
perfil 4.19 y dialecto 7.0 del consumidor actual. Los payloads de los ocho flujos
se contrastan además con las tres versiones nativas (24 referencias), sin
atribuir al wrapper legado cobertura directa de dialectos 8/9.

Los servidores de admisión serán los tres de la matriz fijada de CI:
7.0.43 / 8.0.32 / 9.0.2, con su FCV exacta y el SDK 4.19.0. Un ensayo local
con otro patch se conserva identificado como tal y no se hace pasar por la
matriz alojada. Python 3.13 y 3.14 ejecutan la suite completa contra el wheel.

## 5. Criterios de aceptación

1. **Inventario cerrado y oráculo independiente.** Todos los IDs anteriores
   existen y están seleccionados; no hay casos vacíos, duplicados, skips
   inesperados ni celdas omitidas. Las expectativas completas proceden de las
   capturas nativas con inputs/corpus, buildInfo, FCV, SDK y hashes. El capturador
   no importa engines ni usa la salida candidata para rellenar el golden.

2. **Resultado público.** Comparar `acknowledged`, inserted_id(s), matched_count,
   modified_count, deleted_count, upserted_id(s) y contadores bulk cuando apliquen.
   Para findOneAnd*, comparar None o el documento BEFORE/AFTER y su proyección.
   Los índices de upsert y de error son los de la petición original, también
   cuando cambie el bloque o la familia de modelos ejecutada.

3. **Error público.** Comparar éxito/error, familia pública de excepción, código,
   codeName cuando exista como dato estable nativo, errorLabels aplicables y
   writeErrors[index, code]. Conservar resultados parciales en BulkWriteError.
   No basta que ambos lados fallen: un TypeError previo al envío no equivale a
   un WriteError de schema. Un error sin código tampoco equivale a éxito.
   No se exige igualdad literal de errmsg, namespaces temporales, stack traces,
   logs ni el árbol explicativo completo de errInfo de JSON Schema.

4. **Estado BSON y caller.** Comparar todos los documentos después de cada paso,
   no solo el documento elegido. Distinguir ausente/null, Int32/Int64/double y
   tipos BSON admitidos; preservar orden de arrays y de campos de documentos
   anidados cuando afecta a igualdad. El orden raíz solo es una aserción en
   escenarios que lo declaran; no se promete orden de scan sin sort. Los IDs
   generados se comparan por una biyección de tokens de igual tipo, manteniendo
   igualdad entre resultado/caller/storage y sin normalizar IDs explícitos.
   No se ordenan recursivamente claves BSON para esconder diferencias.

5. **Fallos y continuación.** Un documento fallido conserva sus valores
   anteriores; los éxitos previos de un lote ordered se conservan, se detiene
   lo posterior y unordered puede completar operaciones independientes.
   Los errores de forma que el driver detecta antes del envío no crean
   escrituras. M08 admite únicamente las dos secuencias finales enumeradas:
   el update_many no garantiza qué documento se visita primero. Ese margen
   no permite cambiar el documento inválido, devolver éxito o inventar filas.
   Las sondas posteriores de unicidad/lectura comprueban que no queden índices
   dañados y que el motor siga utilizable. La atomicidad de todo el lote solo
   se exige si otro contrato vigente de transacción la establece.

6. **Concurrencia.** Todas las historias cumplen el predicado Q correspondiente:
   un único ganador CAS o unique, contadores/retornos coherentes con el ganador,
   ausencia de efectos del perdedor y estado final admisible. Se conservan
   historial de invocación/respuesta, operación de cada actor y resultado por
   repetición. Diez repeticiones son una cobertura finita, no una prueba de
   todos los schedules ni una garantía de throughput.

7. **Delegación consumidora.** C01–C08 alcanzan las entradas reales del wrapper
   y C08 la adaptación Storage. En copia aislada, retirar solo las correcciones
   semánticas y hacer fallar cualquier llamada a `_lower_array_filter_update`,
   `_lower_unsupported_aggregation_expressions`, `_order_filter_fields_before_operators`,
   `_rewrite_dotted_array_filter` o al ejecutor manual de bulk. Conservar la
   adaptación async, los codecs y la autoridad de clock/lifecycle del owner.
   Trazar que las peticiones originales y array_filters llegan a Mongoeco.
   Resolver la restricción actual `mongoeco>=4.8.1,<4.9` mediante overlay del
   ensayo y verificar hashes/imports del wheel candidato. No modificar el pin
   propietario ni afirmar adopción permanente. Si la composición real no carga,
   el canario queda pendiente; un stub no lo sustituye.

8. **Regresión y admisión.** Mantener los 114 inputs y las garantías de cada
   nivel que no contradigan una disposición explícita. Pasan la suite completa
   en Python 3.13/3.14, unittest, perfiles Hypothesis ci/deep, cobertura >=99 %,
   matriz de seis SDK, typing, manifests, snapshots actuales, conformance de
   ambos motores/canario y compatibilidad SQLite vigente. Los cambios de paths
   de SQLite añaden prueba de equivalencia y, si cambian planificación/coste,
   baseline y candidato del harness existente. No se aumentan budgets para
   lograr paridad. Antes de publicar se completa el checklist de release sobre
   los mismos artefactos inmutables, con autorización de publicación aparte.

Infraestructura ausente, timeout de conexión, FCV incorrecta, fallo de limpieza,
captura indeterminada o cero casos son fallos del gate nativo. No se almacenan
como el error esperado de una escritura. Las diferencias intencionales por
versión se conservan separadas y justificadas con sus capturas.

## 6. Plan de implementación por bloques

| Bloque | Trabajo y frontera | Salida y puerta de cierre |
| --- | --- | --- |
| **B0 — Disposición y baseline** | Capturar primero D1, revisar firmas y garantías API/SPI; fijar las entradas concretas B07 y C08 con las fuentes del manifiesto. | Diferencia normativa reproducida o descartada, antes/después decidido, clasificación de compatibilidad y número de entrega aún separado del código. No cambiar tests para anticipar el resultado. |
| **B1 — Observación completa** | Derivar un observador común del existente, con adaptadores sync/async y proyecciones raw/wire; conservar el estado/error actual como proyección de compatibilidad. | Autoridad independiente de normalización BSON; resultado/retorno/errores parciales y caller capturados. Pruebas focales con salidas nativas, errores sin código, códigos distintos y diferencias BSON que el comparador debe detectar. |
| **B2 — Captura del corpus** | Materializar los 56 especímenes con el mecanismo actual RealParityCase/corpus de captura; importar los 114 inputs, sin otro intérprete JSON. Capturar 170×3, las vistas raw y los payloads C. | Goldens nativos completos y hashes del inventario; cada fila se clasifica como paridad, defecto reproducido o diferencia contractual a disponer. El catálogo de inputs no importa Mongoeco. |
| **B3 — Correcciones acotadas** | Corregir únicamente divergencias del corpus en normalización/API, update core o persistencia Memory/SQLite. Añadir ordered en insert_many y conservar caller IDs; reutilizar rutas bulk existentes cuando preserven sus garantías. | Regresión del caso que fallaba, mismo resultado/error/estado en la matriz directa y continuación correcta. No convertir preparación por bloques en N envíos de executor ni cambiar SPI como atajo. Un requisito nuevo fuera del corpus vuelve al propietario. |
| **B4 — Historias y rutas** | Completar Q01–Q06 con barreras públicas y controles existentes; ejercitar las ocho vistas raw y wire sobre el mismo candidato. | 1.440 historias locales/180 nativas y 96+96 vistas completas, sin seleccionar ganador ni relajar aserciones por engine. Cancelación, publicación y scopes existentes siguen pasando. |
| **B5 — Canario consumidor** | Copia efímera de la composición observada; overlay de dependencia, retirada vigilada de correctores y ejecución de C01–C08 por las entradas reales. | 16 flujos, peticiones intactas, resultados/códigos/estado nativos y ausencia de intérprete en la ruta ensayada. Registrar la adopción del owner como trabajo separado. |
| **B6 — Cierre de implementación** | Revisar diff y límites; actualizar docs/compat/manifests solo si cambia su contrato; ejecutar los gates globales y conservar la evidencia del candidato final. | Cero divergencias sin resolver en el corpus; matriz y consumidores completos; PyMongo 4.19 incluido y regresión histórica conservada. Preparar release sin etiquetar/publicar hasta la petición correspondiente. |

Orden de dependencia: B0 → B1 → B2 → B3 → B4 → B5 → B6. Los casos que ya
coincidan en B2 se cierran con evidencia reutilizada: no requieren cambios de
producción ni una refactorización por completar casillas. B3 se organiza por
causa compartida; no implementa 170 soluciones independientes.

Límite de ampliación: una divergencia alcanzada por uno de estos IDs puede
añadir su caso mínimo reducido de regresión, enlazado al ID padre, sin ampliar
la capacidad prometida. Una familia nueva, operator nuevo, SPI sucesor, modo de
compatibilidad, infraestructura distribuida o migración permanente consumidora
requiere decisión nueva; no entra por descubrimiento durante B3.

## 7. Reutilización y propietarios

| Pieza | Disposición | Garantía que protege y qué se perdería al retirarla |
| --- | --- | --- |
| Diferencial, capturador y goldens actuales | **reuse / derive** | Oráculo ajeno al candidato y diferencias por servidor; sustituirlo por igualdad Memory/SQLite perdería independencia. |
| Observador posicional | **consolidate** en observación completa, conservando su proyección | Preservar las 114 regresiones y sumar retornos/contadores; mantener dos normalizadores independientes permitiría discrepancias ocultas. |
| Contexto de update y publicación atómica actuales | **reuse** | Snapshot del documento, validación y publicación única; solo se tocan ante reproducción que identifique su invariante roto. |
| Batch insert SPI / preparación SQLite | **reuse**, con D1 en la frontera pública | Validación/encoding, trabajo por bloques, cleanup y publicación existentes; no se amplía el SPI por comodidad del API. |
| Barreras CAS y canario externo | **reuse / derive** | Exponer selección obsoleta y consumo real; probar solo helpers perdería ambas fronteras. |
| Intérpretes del testkit | **remove en el canario aislado**, disposición permanente del owner | Acreditar que Mongoeco recibe la petición original. Su retirada global todavía no está autorizada ni acreditada. |
| Nuevo runtime de escritura, modo estricto o normalizador por motor | **no introducir** | El arnés/catálogos y runtime comunes ya cubren el mecanismo; no aportan una garantía diferencial al objetivo acordado. |

Mongoeco es owner del API, semántica y persistencia de esta entrega. El testkit
es owner de sus dependencias y wrappers; Mochuelo conserva Storage, clock y
lifecycle. Las fuentes consumidoras inspeccionadas y sus hashes/revisión están
en `coverage_basis` del manifiesto. Se inspecciona su material actual sin suponer
que el nombre de rama sea una garantía de frescura.

Este plan aplica [ADR-002](architecture/decisions/ADR-002-dialecto-y-perfil-como-ejes-distintos.md),
[ADR-009](architecture/decisions/ADR-009-parity-tests-como-politica-de-aceptacion.md)
y [ADR-015](architecture/decisions/ADR-015-pushdown-sqlite-sujeto-a-prueba-de-equivalencia.md),
y mantiene el SPI v2. No crea un sistema RFC nuevo.
D1 y cualquier impacto que no quepa en esas decisiones se revisan antes
de escribir la corrección afectada. El objetivo no concede por sí solo una
transición de lifecycle o una publicación.

## 8. Runners y evidencia que debe producirse

Existen `tests.support_positional_updates`, `tests.differential.test_positional_update_parity`,
`tests.differential.cases`, `scripts/capture_differential_replay_golden.py`,
`scripts/run_mongodb_real_differential.py` y el workflow MongoDB differential.
Se amplían esos mecanismos; sus comandos actuales de 29 casos y posicionales
no se presentan como ejecución de los 170 casos.

Durante B1/B2 se elegirán módulos de tests dentro de esas familias y se
registrarán las selecciones reales descubiertas. Sus nodeids nuevos **todavía
no existen**; este documento no proporciona comandos inventados como si fueran
ejecutables. CI deberá contar escenarios/celdas contra el manifiesto y comprobar
que cada proyección tiene consumidores efectivos. Los globales finales siguen
siendo los declarados por el repositorio, sin sustituirlos por una suite focal.

Cada informe final liga commit/workset, SHA del wheel, hash de corpus, selección,
Python, SDK, motor, fachada, dialecto/FCV, éxitos/errores/skips, referencias de
captura, historial competitivo y limpieza. La evidencia de 4.9.1 y PyMongo 4.19
permanece histórica; no se reescribe para aparentar el cierre de este plan.

# Consultas, validadores y prefiltros: análisis causal y plan

Fecha: 31 de octubre de 2026. Base inspeccionada:
`79529a2424f2ba16b454c559bf2202bd0bf9b055`.

Este documento posee la ampliación H37–H47 del
[plan de paridad](plan-semantic-parity-refactor.md). H01–H36 y B00–B10 conservan
sus identidades, estados y receipts. Los cortes C00–C06 son unidades de ejecución
que cruzan esos bloques, no otro roadmap de plataforma.

Se ha recibido un informe con cuatro áreas. El mensaje anuncia dos informes,
pero el segundo no está incluido; su integración queda pendiente. Este plan
cubre completamente los hallazgos del informe disponible y los contraejemplos
adicionales comprobados aquí. No se han aplicado correcciones productivas.

## 1. Base, evidencia y límites

El árbol estaba limpio al comenzar. La identidad de las 276 fuentes Python se
conserva en [la evidencia de análisis](evidence/predicate-authorities/analysis-validation.json).
La reproducción distingue tres niveles:

1. [65 estímulos nativos](evidence/predicate-authorities/native-9.json) contra
   MongoDB **9.0.2**, FCV **9.0**, mediante PyMongo **4.19.0**. Son comandos
   `aggregate: 1` con `$documents` literal y `$match`: no se crean ni escriben
   colecciones. Se conservan documentos, filtros, opciones, resultados y errores.
2. [Rutas públicas locales](evidence/predicate-authorities/local-routes.json):
   ocho escenarios en Memory y SQLite `:memory:`, consultas con/sin índice,
   búsqueda vectorial y aceptación de una escritura inválida por `enum`.
   Solo se usan datos sintéticos propios en motores privados.
3. [Pruebas existentes](evidence/predicate-authorities/baseline-tests.txt):
   38 pruebas y 307 subpruebas pasan. No constituyen aceptación de los nuevos
   casos. Varias comparan evaluadores locales y no un resultado nativo.

En 42 de los 65 estímulos difiere el éxito/error o el resultado de al menos un
evaluador local con dialecto 9. No son 42 defectos independientes. Ese conteo
no incluye diferencias entre clases, códigos o mensajes cuando ambos fallan;
estas permanecen en la observación. Los dialectos locales 7/8 también se
ejercitan, pero **no se han ejecutado servidores 7/8 en este análisis**.

Los reproductores están en
[characterize_predicates.py](evidence/predicate-authorities/characterize_predicates.py)
y [characterize_routes.py](evidence/predicate-authorities/characterize_routes.py).
Son caracterización; C00 debe integrar los testigos en los runners mantenidos.
El contraste de escrituras contra validadores nativos, wire, sesiones,
concurrencia y Atlas Search queda pendiente. No se acredita ausencia de otros
defectos a partir de este corpus finito.

## 2. Hallazgos y causas raíz

### Consultas y composición compilada

**H37 — P1, observado nativamente: comparación sin restricción por familia BSON.**
`{"a":"z"}` con `{"a":{"$gt":5}}`, `true > 0` y `null < 5` coinciden en ambos
evaluadores locales y no coinciden en el servidor. El primer invariante roto
está en `_comparison_matches_candidate`: tras las especializaciones escalares,
usa el orden total de `compare_with_collation` sin comprobar si la pareja puede
compararse en una consulta. El orden BSON general es correcto para otros
consumidores; cambiarlo globalmente rompería agregación y ordenación.

La [distinción normativa entre consulta y agregación](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
exige elegibilidad por familia antes de comparar; los tipos numéricos comparten
familia. No equivale a imponer igualdad de clases Python. Arrays, sentinelas,
MinKey/MaxKey, NaN y rutas requieren sus reglas concretas y testigos nativos.
El control `$expr: {$gt:["$a",5]}` sobre `"z"` coincide correctamente.

**H38 — P1, observado: las rutas punteadas compiladas tienen otra semántica.**
`{"o":{"a":[]}}` con `{"o.a":{"$gt":5}}` da falso interpretado y verdadero
compilado; MongoDB da falso. También ocurre con arrays anidados. La causa está
en `CompiledQuery._comparison_code`, que llama directamente al comparador y
omite la selección y elegibilidad del matcher. Memory `find` devuelve el
documento; SQLite `find` no lo devuelve en el escenario reproducido. SQLite
desactiva `compiled_query` en el scan/residual Python; esa bifurcación explica
por qué la divergencia interna llega a la API pública.

**H39 — P1, observado con inyección de fallo: excepciones convertidas en no-match.**
El `try/except` generado captura `KeyError`, `TypeError` y `AttributeError` de
toda la ejecución, incluidas extensiones y helpers. Una extracción que lanza
`TypeError` termina en `False`. Hay un caso natural combinado: un límite de
schema sobre `+Infinity` provoca `TypeError` interpretado y se transforma en
no-match compilado, mientras MongoDB acepta ese documento para `minimum: 0`.
La ausencia de un campo debe ser un resultado explícito del traversal; no debe
deducirse de cualquier excepción producida durante la operación.

**Control de caché:** dos filtros de la misma forma con valores 1 y 2 comparten
plantilla y mantienen listas de valores distintas y resultados correctos. No
se ha demostrado un fallo de identidad como el de `CompiledGroup`; no se
justifica sustituir esa caché por una nueva ni cambiar su clave por analogía.

### JSON Schema y validación de colecciones

**H40 — P1, observado: `enum` usa igualdad Python.**
`enum: [1]` acepta `true`, también anidado, y rechaza Decimal128 `1.0`, que el
servidor acepta. Una colección local con ese validator acepta la escritura
`a: true` en ambos motores. La pertenencia Python es la primera autoridad
equivocada, antes del transporte de errores.

Tres controles impiden una refactorización espejo: el servidor acepta objetos
equivalentes con claves reordenadas en `enum`; un array `[1]` no satisface
`enum: [1]`; `"A"` no satisface `enum: ["a"]` aunque el aggregate tenga collation
de fuerza 2. Por tanto, ni la igualdad de consulta con traversal de arrays,
ni una clave BSON sensible al orden de campos, ni la collation de consulta
pueden sustituir directamente la regla de igualdad de schema.

**H41 — P1, observado: otra conversión numérica y un dominio incompleto.**
`_as_decimal` convierte doubles mediante `Decimal(str(x))`. El double `0.1`
supera un `maximum` Decimal128 `0.1` en MongoDB, pero pasa localmente; un
`minimum` Decimal128 `0.100000000000000002` produce el error inverso. El servidor
acepta `+Infinity` con `minimum: 0`; el evaluador local falla y el compilado lo
oculta. NaN y Decimal128 NaN muestran además excepciones Python distintas.

La sonda separa Decimal128 local y Decimal128 del driver. Este último es
reconocido por `bson_numeric_alias`, pero `_as_decimal` no lo consume y puede
devolver `None`. Esa inconsistencia de helpers está observada; su alcance por
adaptador público debe medirse sin asumir que todas las entradas llegan con la
misma representación. No debe resolverse aceptando `Decimal` Python sin pasar
por la frontera BSON prevista en B07.

**H42 — P1, observado: preparación del schema incompleta o tardía.**
MongoDB rechaza `type` junto con `bsonType`, `type: "int"`, `enum: []`, valores
duplicados en `enum` y `required`, keywords desconocidas y patrones inválidos.
Localmente hay aceptación, una regla distinta o errores dependientes de filas.
Con `$documents: []`, por ejemplo, un patrón inválido y `enum: []` no fallan
localmente y sí fallan en el servidor. La gramática se recorre parcialmente en
`_validate_schema_definition` y se vuelve a interpretar durante la evaluación.

**H43 — P1, observado: restricciones aceptadas sin ejecutarse.**
`exclusiveMinimum`, `uniqueItems` y `multipleOf` dejan pasar contraejemplos que
el servidor rechaza. `additionalProperties` como schema es válido en MongoDB
y se rechaza localmente. Una lista de keywords soportadas debe distinguir
restricciones, anotaciones sin efecto y funcionalidades todavía no soportadas.
La aceptación silenciosa de una restricción no implementada no es un subset
seguro. El [contrato MongoDB de JSON Schema](https://www.mongodb.com/docs/manual/reference/operator/query/jsonschema/)
sirve para censar la gramática; los errores y valores concretos se capturan del
servidor. No se propone implementar todo JSON Schema en este incremento.

**H44 — P1, observado localmente: `moderate` pierde el dialecto del documento previo.**
`should_validate` llama recursivamente a `validate_document` sin dialecto;
este vuelve a 7. Con validator `{a: {$ne: null}}`, un original con `UNDEFINED`
es válido en 9 e inválido en 7. Cambiarlo a `null` debería validar y fallar;
la transición local se acepta porque el original se considera inválido bajo 7
y se omite la comprobación. Los controles validan por separado original y
nuevo: descartan que el resultado se deba a un original inválido en 9.
La escritura nativa equivalente con BSON undefined sigue pendiente de C03.

Además hay trabajo duplicado comprobable en código: el enforcer prepara el
validator por invocación; el error vuelve a construir `CompiledJsonSchema` y a
validar en `schema_failure_details`. No se ha demostrado un error adicional
por ese coste, pero separar decisión y diagnóstico permite futuras divergencias.

### Prefiltros y autoridad de candidatos

**H45 — P1, observado: claves numéricas con pérdida.**
`filter_value_key` convierte enteros a `float`; `2**53` y `2**53+1` tienen la
misma clave. El segundo satisface `$gt: 2**53` en MongoDB y en `find` local,
pero la búsqueda vectorial lo descarta en **ambos motores**, con un único vector
válido y presupuesto superior al tamaño del conjunto. No es un efecto de recall
ANN ni de ranking. La misma conversión interviene en índices materializados y
en el filtrado documental que debería actuar como referencia.

**H46 — P1, observado: el prefiltro cambia traversal, ausencia y correlación.**
También se pierden resultados vectoriales válidos con:

- `a: [1,20]`, filtro `{a: {$gt:5, $lt:10}}`: MongoDB y `find` local coinciden;
  vector no. La forma `$and` con ambos predicados separados sí devuelve el vector.
  El helper exige que un mismo elemento satisfaga ambos límites.
- Campo ausente con filtro `{a: null}`: `find` y servidor coinciden; vector no.
- `items: [{x:7}]`, filtro `{"items.x": {$gt:5}}`: el acceso escalar y la
  materialización de claves no conservan el traversal de consulta.
- `a: [7]`, filtro `{"a.0":7}`: el helper documental acepta, pero la ruta
  vectorial no. El índice materializado no representa esa ruta numérica.

El contrato local ya promete `vectorSearch.filter` mediante `QueryEngine`.
La primera ruptura sucede al reemplazar esa semántica por un filtro
candidateable que se considera definitivo. Un filtro residual no recupera
documentos descartados. Tampoco basta degradar a una ruta llamada «exacta»
si esa ruta comparte el helper defectuoso.

Search introduce dos consumidores diferentes: `vectorSearch.filter` y un
`$match` downstream tienen contrato MQL; un operador `search-v1` conserva su
propio contrato, incluida la correlación de sus rangos. No deben intercambiarse
solo porque sus representaciones tengan `$gt`/`$lt`. Los fallos en las rutas
vectoriales están reproducidos; el alcance adicional en Search textual,
collectors y writeback está deducido de los callers y requiere sus testigos.

### `$mod` de consulta

**H47 — P2, observado nativamente: faltan preparación y coerción enteras.**
MongoDB acepta `8` con `$mod: [4.5,0]`, `5` con `[2,1.9]`, `5.5` con `[2,1]`
y Decimal128 `5` con `[2,1]`; los evaluadores locales discrepan. Rechaza restos
NaN/Infinity, argumentos fuera de int64 y divisor `0.5`, que tras truncar es
cero; varias de esas consultas se aceptan localmente. El parser solo comprueba
parte del dominio, y el evaluador implementa división flotante y tolerancias.

La [regla de consulta documentada](https://www.mongodb.com/docs/manual/reference/operator/query/mod/)
incluye truncamiento hacia cero y dominio int64 de argumentos. El control de
resto próximo `1 + 1e-13` coincide en ambos sistemas: por sí solo no demuestra
que retirar `isclose` arregle nada. La corrección debe partir de coerción,
dominio y resto entero. El `$mod` de agregación tiene otro contrato y conserva
su implementación y regresiones específicas.

## 3. Causa sistémica y diseño objetivo

Hay cuatro fallos de organización recurrentes:

1. **La política vive en el consumidor.** Un comparador total se usa como
   matcher, una pertenencia Python como igualdad BSON y una clave aproximada
   como decisión definitiva.
2. **La optimización añade otra autoridad.** Compilador, SQL, índices y Search
   implementan reglas en vez de especializar una regla ya preparada.
3. **Se reconstruye contexto o significado.** El original de `moderate`, los
   detalles del error y el filtro residual reciben defaults o árboles rehechos.
4. **El oráculo comparte el defecto.** Comparar Memory/SQLite, interpretado/
   compilado o Search/reference puede validar el mismo error dos veces.

La solución comparte primitivas exactas y conserva adaptadores explícitos:

| Contexto | Autoridad que se conserva o prepara | Lo que no debe absorber |
| --- | --- | --- |
| Ordenación y agregación | Comparación total BSON y collation existentes. | Restricción por familia de una consulta. |
| Matching MQL | `QueryNode` y matcher hoja: traversal, elegibilidad, comparación, negación y contexto. | Semántica `enum` o correlación de rangos Search. |
| JSON Schema | Árbol preparado; igualdad de schema y restricciones propias, sobre primitivas numéricas exactas. | Traversal implícito del matcher o igualdad Python. |
| Selección de candidatos | Plan existente con candidatos, exactitud y residual reales. | Decidir verdad semántica con una codificación no demostrada. |
| Escrituras | Contexto de operación existente y una validación del original/nuevo coherente. | Defaults internos de dialecto o revalidación para construir errores. |

No se crea un comparador universal con una colección de flags, otro planner,
un segundo sistema de observación ni una caché global de schemas.

## 4. Plan por cortes

| Corte | Prioridad | Bloques existentes / hallazgos | Dependencias y salida |
| --- | --- | --- | --- |
| C00 — Testigos y oráculos | P1 | B00; H37–H47 | Primero. Integrar casos, registrar fronteras y capturar 7/8/9 antes del arreglo. |
| C01 — Matcher único | P1 | B03/B04/B05/B08; H37–H39 | C00. Toda ejecución MQL usa la misma regla hoja y conserva contexto/errores. |
| C02 — Schema preparado | P1 | B03/B04/B07; H40–H43 | C00; primitivas existentes B03. Gramática cerrada y semántica de schema explícita. |
| C03 — Validación de escritura | P1 | B04/B08; H44 y efectos de H37/H40–H43 | C00; contrato hoja C01 y árbol C02 para cierre conjunto. Original/nuevo comparten contexto y diagnóstico. |
| C04 — Candidatos conservadores | P1 | B03/B05/B06/B08; H45/H46 | C00 y regla MQL C01 para cierre; contención segura puede avanzar antes. Sin pérdida antes de residual/ranking/ventana. |
| C05 — Mod de consulta | P2 | B03/B04/B08; H47 | C00 y coerción escalar existente. Argumentos preparados y resto entero compartido por rutas. |
| C06 — Integración y mantenimiento | P1 para cierre | B09/B10; todos | C01–C05. Distribución, regresión, garantías de optimización, documentación y commits verificables. |

C01 y C02 pueden avanzar tras C00 sin esperar al cierre completo del otro.
La corrección de propagación de dialecto de C03 tampoco necesita esperar a
terminar todas las keywords. C04 puede dejar de podar los dominios no seguros
inmediatamente; recuperar la aceleración requiere demostrar sus precondiciones.
C05 no necesita esperar a todo Search ni a la frontera BSON B07 completa.

### C00. Integrar las pruebas sin construir otro testkit

- Incorporar testigos reducidos en `tests/differential/semantic_*_cases.py`, el
  registro `semantic_corpora.py`, el capturador y el replay existentes. Cada
  caso identifica H, entrada, frontera y oráculo; los scripts de este análisis
  se conservan como evidencia y no se convierten en un runner paralelo.
- Capturar MongoDB 7/8/9 con buildInfo, FCV estable, PyMongo, semillas y hashes.
  Separar el oráculo del servidor del motor que se prueba; no importar helpers
  productivos para calcular el resultado esperado.
- Registrar resultado/tipo BSON/orden cuando aplique, clase y código de error,
  fase, estado posterior, efectos previos y recuperación. Los nombres de
  namespace pueden normalizarse de forma acotada; no los datos ni el código.
- Conservar controles que ya coinciden: `$expr` entre tipos, números exactos,
  enum con objetos reordenados y arrays atómicos, enum binario con collation,
  negativos de mod y aislamiento de valores en plantillas cacheadas.
- No promover los receipts antiguos a la nueva base ni regenerar fixtures con
  el resultado de mongoeco. Una celda nativa ausente queda pendiente.

### C01. Compartir matching; acotar el generador

1. Hacer explícita la elegibilidad de comparación MQL antes del comparador BSON.
   Reutilizar clasificación y comparación numérica exactas. Mantener traversal
   de consulta separado de acceso escalar y de paths de expresión/update.
2. Usar esa regla en rutas planas y punteadas. Conservar arrays completos y sus
   candidatos con suficiente información para no imponer correlación donde
   no existe; `$elemMatch` continúa siendo una operación distinta.
3. Simplificar `CompiledQuery` a composición booleana y llamadas a hojas del
   matcher común. Retirar reglas propias de igualdad, negación, pertenencia,
   existencia y comparación. Conservar la separación plantilla/valores ya
   comprobada; no añadir otro plan preparado o una segunda tabla de handlers.
4. Retirar el catch global. La preparación presenta errores de entrada y el
   traversal devuelve ausencia explícita; los fallos internos se propagan con
   cleanup y causa. Mantener la presentación pública en su frontera existente.
5. Adaptar `semantic_core`, `compiled_pipeline` y rutas SQLite exactas/residuales.
   Una ruta SQL solo será exacta con prueba de familia, representación y
   traversal; si produce un superset, ejecutará residual antes de limit/skip.
   No reactivar índices numéricos antiguos por haber corregido el matcher.
6. Censar los consumidores de selección, incluidos `$pull`, array filters e
   implicación/pertenencia de índices parciales. Un índice persistido puede
   contener pertenencias calculadas con el matcher anterior aunque su formato
   de claves no cambie. Probar fixture anterior y reapertura, lecturas e índices
   unique; si se demuestra estado derivado obsoleto, reconstruirlo o invalidarlo
   atómicamente antes de usarlo. Es un riesgo deducido que debe resolverse con
   evidencia, no una migración ya declarada necesaria para todos los índices.

Aceptación: nativo = interpretado = compilado = scan/índice para cada contrato
MQL; controles de `$expr` sin cambios; fallos inyectados no se convierten en
no-match. Medir los workloads existentes de predicados e índices antes/después.
No retirar todo `compiled_pipeline` ni perseguir una eliminación general de
codegen ajena al fallo. La retirada completa de `CompiledQuery` solo sería una
simplificación posterior si el benchmark y los callers la justifican.

### C02. Preparar y evaluar JSON Schema una sola vez por programa

1. Concentrar gramática de keywords, tipos y anotaciones en la preparación de
   schema. Distinguir `type` de `bsonType`, exclusión mutua, arrays no vacíos,
   duplicados semánticos y patrones compilados antes de filas.
   `JsonSchemaCondition.compiled_schema` ya existe: completar y reutilizar esa
   preparación, sin añadir otro nodo ni otra caché. El kernel de schema no debe
   depender de matching de colección; si se separa en un módulo vecino, la
   composición MQL/schema y `moderate` permanecen en `schema_validation`.
2. Introducir una igualdad de schema pequeña y recursiva que reutilice números
   exactos y preserve sus controles específicos. La misma igualdad comprueba
   `enum`, sus duplicados y `uniqueItems`; los arrays siguen siendo atómicos
   para enum y ordenados para su propia igualdad.
3. Sustituir `_as_decimal` como autoridad de comparación por las primitivas
   numéricas existentes, con política nativa explícita para NaN/Infinity y
   wrappers internos/locales/driver. No usar el contexto decimal ambiental.
4. Implementar en cortes acotados `exclusiveMinimum/Maximum`, `uniqueItems`,
   `multipleOf` y `additionalProperties` como schema para cerrar H43. Sus
   semánticas necesitan captura propia: `multipleOf` de schema no se implementa
   delegando al mod de consulta ni al de agregación.
5. Censar las demás keywords: las anotaciones reconocidas se conservan; una
   restricción conocida todavía no implementada se rechaza explícitamente,
   y un nombre desconocido sigue el error nativo. Nunca devolver aceptación
   omitiendo la restricción. Esta contención puede preceder al soporte completo
   de las cuatro familias; no equivale a declararlas soportadas.
6. Mantener un árbol preparado inmutable y un resultado estructurado suficiente
   para renderizar el diagnóstico. Retirar los recorridos paralelos de
   `schema_failure_details` al migrar sus consumidores.

Aceptación: `$jsonSchema` y validator de colección comparten semántica; errores
de definición aparecen con entrada vacía y no dependen de que el documento
contenga el campo. El resultado estructurado explica la misma decisión que
rechaza la escritura. Se conserva el catálogo explícito de capacidades.

### C03. Un contexto para original, candidato y error

1. Propagar el dialecto efectivo hasta `should_validate` y la evaluación del
   original; los helpers internos no eligen 7 por ausencia de un argumento.
   Reutilizar `OperationContext`/semántica de operación existentes, sin otra
   jerarquía. Caracterizar collation de validator MQL frente a schema antes
   de trasladarla indiscriminadamente.
2. Preparar el validator una vez por operación con las opciones efectivas de
   la colección. Reutilizarlo en documentos y en original/nuevo. No añadir una
   caché global por schema; una futura caché exigiría revisión de opciones,
   dialecto y vida de colección en la identidad, con un beneficio medido.
3. Integrar insert, replace, update clásico/pipeline, upsert, findAndModify,
   bulk y writeback de `$merge`/`$out` por sus callers reales de Memory/SQLite.
   Conservar `off/strict/moderate`, `warn/error` y bypass según su contrato.
4. Validar antes de persistir o actualizar índices/outbox del documento
   rechazado. Probar atomicidad por documento, efectos previos permitidos en
   escrituras múltiples y diagnóstico 121 por API/comando/bulk.
5. Renderizar detalles desde el resultado de C02, sin revalidar el documento
   ni reconstruir un árbol con otros defaults. Añadir pruebas de `collMod`,
   cambio de validator, bypass, reapertura y restauración de opciones.

Aceptación: transición válida→inválida falla bajo `moderate`, inválida→inválida
puede omitirse según el servidor, y los inserts/upserts insertados conservan
su regla. El testigo BSON undefined se captura físicamente en 7/8/9 sin convertirlo
en null en la sonda. No basta el test directo de `CompiledCollectionValidator`.

### C04. Una selección conservadora y un residual independiente

Para cada universo de documentos `U`, matches semánticos `T` y candidatos `C`:

- la poda segura exige `T ⊆ C ⊆ U`;
- `exact=True` exige `C = T`;
- no poder resolver una cláusula significa «sin reducción demostrada», nunca
  el conjunto vacío;
- `$and` puede aprovechar partes seguras; `$or` necesita cubrir todas las
  ramas; no se puede complementar un superset para implementar negación.

Aplicar estas garantías usando `CandidateFilterPlan/Result` y el planner Search
actuales, no nuevos frameworks ni un segundo conjunto de flags contradictorios.

1. Separar codificación de índice y matching. Usar claves exactas solo para
   dominios que representen íntegramente sus valores y familias; conservar
   residual cuando la cobertura no sea exacta. Las claves float dejan de ser
   prueba de igualdad/rango de enteros. La cuantización de vectores y sus scores
   sigue siendo otra política y no se cambia para arreglar metadatos BSON.
2. Reutilizar la semántica de candidatos de rutas MQL de C01. La materialización
   debe representar existencia, ausencia/null, arrays, rutas numéricas y arrays
   de documentos o declarar que no puede podar esos shapes. No inventar un
   flatten que elimine información de correlación.
3. Retirar `matches_candidateable_filter` como autoridad final de verdad. El
   residual MQL usa el matcher común; los operadores de `search-v1` usan su
   propio contrato preparado. Una ruta de referencia no consume las claves
   aproximadas que debe comprobar.
4. Probar Memory materializado, SQLite usearch, fallback exacto y cache frío/
   caliente, cambios de documentos/índices y reconstrucción. Los índices de
   filtros vectoriales observados viven en el estado materializado; no se
   prescribe una migración de disco sin demostrar un formato persistido afectado.
5. Verificar `$match` downstream antes/después de barreras del planner, límites,
   orden/ties, metadata, collectors, writeback/eventos y explain. No extender el
   pushdown a través de una transformación por corregir sus primitivas.

Aceptación: propiedades de conservación de candidatos y equivalencia completa
para cada regla, incluidos los testigos H45/H46. No se acepta «Memory = SQLite»
ni «ANN = fallback» como único oráculo. El contrato de referencia existente
puede reutilizarse una vez comprobado que no comparte el helper defectuoso.

### C05. Preparar `$mod` de consulta

1. Preparar divisor y resto una vez: tipo numérico admitido, truncamiento hacia
   cero, int64, NaN/Infinity y cero después de convertir. Separar error de
   argumento de documento no elegible. Capturar límites y código/fase nativos.
2. Aplicar la coerción observada al candidato numérico, incluidos Decimal128 y
   arrays, y calcular resto entero con signo de dividendo. Retirar la aritmética
   flotante propia e `isclose`; mantener `$mod` de agregación separado.
3. Verificar la ruta SQL `%` en `_sqlite_read_execution`, scans y selectores de
   escritura. No usar un `CAST` permisivo si no reproduce dominio y truncamiento.
   SQL puede degradar al matcher común cuando no se demuestre equivalencia.

Aceptación: negativos, fracciones, valores cercanos, wrappers, Decimal128,
int64 extremos y operandos inválidos, con entrada vacía y poblada; error de
consulta antes de modificar documentos. Los tests de aritmética de agregación
existentes siguen pasando.

### C06. Cierre conjunto, trazabilidad y commits

- Cerrar cada corte con código, casos mantenidos, capturas nativas y alcance
  explícito; no basta cambiar helpers sin verificar las rutas públicas.
- Reejecutar los nuevos casos y las regresiones afectadas contra la identidad
  final. Reutilizar evidencia inalterada de H01–H36; no rehacer el kernel decimal,
  la acumulación ni sus benchmarks si no cambia una entrada causal.
- Ejecutar los gates del owner en [.github/workflows/ci.yml](../.github/workflows/ci.yml):
  lint ratchet, typing/manifiesto público, pytest/unittest completos y propiedades,
  SPI, coverage `>=99%` en 3.14, wheel/sdist y smokes; Python 3.13/3.14/3.15 y
  extremos SDK de la matriz vigente. Distinguir skips opcionales de celdas
  obligatorias no ejecutadas. No considerar este análisis como ese cierre.
- Medir predicados planos/punteados y booleanos, scan/índice, validator por
  documento/operación y Search/vector selectivo/no selectivo. Usar garantías de
  plan/coste como gate y benchmarks como tendencia; no fijar un umbral nuevo
  de CPU arbitrario. Cualquier nueva regla SQL conserva el benchmark obligatorio.
- Actualizar compatibilidad, catálogo vigente, `search-v1` si procede e inventario
  H/frontera/caso. El registro de implementación posee estados; este documento
  posee el alcance de la ampliación. Conservar receipts históricos.
- Materializar commits semánticos por corte integrado con sus pruebas y docs;
  separar benchmarks/evidencias cuando resulte coherente. La identidad del
  workset validado debe coincidir con la entregada.

## 5. Matriz diferencial mínima

| Familia | Valores / shapes obligatorios | Fronteras que deben observarse |
| --- | --- | --- |
| Matching | Familias BSON cruzadas; 0/false/null/ausencia/undefined; NaN/Infinity; int32/int64/double/decimal; arrays vacíos/anidados; documentos y extremos MinKey/MaxKey. | Interpretado/compilado, find/findOne/count/distinct, `$match`, joins que reutilicen el matcher y selección de escrituras. |
| Paths y booleanos | Plano, punteado, array de documentos, posiciones numéricas, multioperadores, `$and/$or/$not/$nor`, `$elemMatch`, ramas inválidas y sin filas. | Scan/índice/hint, SQLite SQL exacto/prefiltro/residual, `compiled_pipeline`, sync/async/comando/wire cuando aplique. |
| Schema | Enum bool/número, anidados, objetos reordenados, arrays atómicos, collation; límites exactos/no finitos; exclusividad, uniqueItems, multipleOf; grammar y anotaciones. | `$jsonSchema`, create/collMod, insert/replace/update/upsert/findAndModify/bulk/writeback; estado, índices, outbox y diagnóstico. |
| Moderate | Original válido/inválido × candidato válido/inválido; missing/null/undefined; strict/off/warn/bypass; schema y validator MQL. | Dialectos 7/8/9 físicos, perfiles, por documento y múltiples escrituras, reapertura. |
| Candidatos | 2**53 y siguiente, 2**63−1, double/decimal, ausencia/null, arrays correlacionados/no correlacionados, rutas no representadas, mixtos/unsupported en booleanos. | MQL nativo para elegibilidad; referencia normativa `search-v1` para Search; poda, residual, ranking, ventana, collector y writeback. |
| Mod | Divisor/resto/candidato fraccionario, signos, cero tras truncar, tipos y rangos, no finitos; arrays y errores sin filas. | Nativo 7/8/9, matcher común, SQL, selectores de update/delete y errores de comando. |

No se exige el producto cartesiano de todas las columnas. Cada regla nueva
tiene un positivo, un negativo y la frontera que puede cambiar su resultado;
las primitivas compartidas se prueban exhaustivamente una vez y las fronteras
aportan testigos discriminantes. En propiedades, reutilizar Hypothesis y su
shrinking existentes: un fallo reducido se incorpora al corpus nativo.

El oráculo de elegibilidad vectorial puede componerse de MQL nativo y un
dataset pequeño con orden/score esperado independiente. Esto prueba el contrato
local; no acredita recall ANN arbitrario ni paridad Atlas. Los controles de
`search-v1` siguen su autoridad normativa local, como prescribe la política del
repositorio; un servidor Community sin el servicio Search no puede sustituirla.

## 6. Primer incremento ejecutable y condiciones de cierre

El siguiente incremento recomendado es C00 + la regla de comparación H37/H38
de C01, con los controles que preservan `$expr`. En paralelo puede corregirse
la propagación de dialecto H44 y desactivarse la poda no demostrada de C04;
ninguno exige terminar todas las familias de schema. Después, C02/C03 y C04
se cierran con sus consumidores; C05 tiene una preparación y aceptación propias.

No se declara cerrado un H por la mera coincidencia entre rutas locales. Cada
H requiere testigo previo rojo, referencia independiente, corrección, pruebas
de sus consumidores y evidencia sobre el candidato final. El cierre de esta
ampliación no cierra automáticamente los pendientes anteriores B00–B10.
Antes de ampliar el alcance con otro conjunto de hallazgos debe incorporarse
el segundo informe anunciado, conservando estos IDs y evitando duplicados.

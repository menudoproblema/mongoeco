# Plan de corrección y refactorización de la paridad semántica

Fecha: 30 de octubre de 2026. Base inspeccionada:
`b38ef96764ce4a976883005ade83f82f7b3ef547` (4.11.0).
Estado y evidencia de ejecución: [registro de implementación](semantic-parity-implementation.md).
Este plan conserva las obligaciones; el registro posee los estados y resultados.

Revisión de mantenibilidad: 31 de octubre de 2026, contrastada con el árbol de
trabajo en curso y sus receipts. Se conservan H01–H36, B00–B10, las celdas
instaladas y los gates. Esta revisión ajusta el trabajo restante; no declara
nuevos resultados ni cambia estados de implementación.

Este documento posee el alcance, la secuencia y los criterios de cierre de los
análisis de esta conversación y su verificación inicial. No acredita una
implementación, no reserva una versión ni autoriza publicar. Los números de
sondas comunicados durante el análisis son caracterización de sesión, no un
corpus instalado ni una prueba de paridad completa. Los scripts temporales y
sus resultados deben reducirse e incorporarse a testigos mantenidos en B00;
este plan no depende de que sobrevivan archivos de `/tmp`.

La política vigente sigue en [testing y compatibilidad](architecture/testing-and-compatibility.md),
los contratos públicos en [COMPATIBILITY](../COMPATIBILITY.md) y
[DIALECTS](../DIALECTS.md), y los gates de entrega en el
[checklist](release-checklist.md). Las evidencias históricas conservan su base;
no se reescriben para hacerlas pasar por validación de este candidato.

## 1. Objetivo, alcance y reglas de implementación

Para cada testigo de la sección 3, conservar el valor, tipo BSON, forma del
documento, errores, fase observable y efectos que produce MongoDB real en el
dialecto seleccionado. La igualdad entre Memory y SQLite es necesaria, pero
no sustituye al oráculo nativo. Una limitación de producto aceptada conserva
su diferencia explícita y no cuenta como paridad.

Owners por bloque son módulos, no personas asignadas. Las correcciones viven
en mongoeco; el testkit externo solo se modifica en B09, dentro de su propio
repositorio y tras refrescar su revisión. No se arreglan en mongoeco los
errores lógicos del compilador de joins de Mochuelo: un pipeline válido pero
lógicamente incorrecto debe seguir produciendo el mismo resultado incorrecto
que el servidor. Un pipeline inválido debe rechazarse como en el servidor.

- Mantener SPI v2, formato SQLite y APIs existentes salvo los deltas públicos
  explícitos de este plan. No crear otro backend, lenguaje de fixtures, runner
  general o capa de reparaciones para consumidores.
- Separar corrección de una regla y optimización de esa regla. Una optimización
  sin equivalencia demostrada delega en el ejecutor común; no se conserva para
  mantener un benchmark incorrecto.
- Distinguir identidad de programas/cachés, identidad física de almacenamiento,
  igualdad de valores y matching de consultas. No fusionarlas en una función
  universal por compartir algunos testigos.
- Las diferencias de versión se capturan en MongoDB 7/8/9; no extrapolar de
  9.0.2. El perfil PyMongo y el dialecto MongoDB siguen siendo ejes separados.
- Cada bloque añade sus regresiones y corrige documentación/catálogo vigentes
  de su delta. B09 reconcilia el conjunto, sin aplazar toda la documentación
  hasta el final ni reescribir snapshots históricos.
- No introducir migración de archivos ni limpieza automática de datos locales
  antiguos. Si aparecen objetos/sentinelas ya persistidos, caracterizar su
  lectura y rechazo; cualquier reparación de datos requiere un alcance propio.

## 2. Bloques, prioridades y dependencias

P1: resultados, datos o garantías de ejecución incorrectos; trabajo prioritario.
P2: completar superficies y cerrar la integración. P3: límite de producto que
se dispone explícitamente. B10 es el gate conjunto, no una corrección adicional.

| Bloque | Prioridad | Owner principal | Depende de | Entregable verificable |
| --- | --- | --- | --- | --- |
| B00 | P1 | `tests/differential`, capturas y observadores | — | Observación independiente y mecanismo de captura incremental 7/8/9. |
| B01 | P1 | grouping y compilación de agregación | B00 | Retirada preferente de la ruta compilada divergente y su caché. |
| B02 | P1 | evaluación de expresiones y entornos | B00 | Ausencia/eliminación preservadas y materialización contextual. |
| B03 | P1 | `core/bson_scalars.py`, ordering y collation | B00 | Comparación, claves, precisión y promoción numéricas coherentes. |
| B04 | P1 | preparación, catálogos y contextos de ejecución | B02 | Gramáticas separadas, validación completa y `let` de comando. |
| B05 | P1 | paths, proyecciones y transform stages | B02 | Semánticas de ruta y proyección por contexto. |
| B06 | P1 | acumuladores, grouping, joins y distinct | B01, B02, B03, B05 | Un núcleo de acumulación y adaptadores correctos por consumidor. |
| B07 | P1 | codec, frontera pública y adaptadores de errores | B02, B03 | Validación BSON pública estricta y salida sin valores internos. |
| B08 | P2 | fachadas sync/async, comandos y wire | B04, B05, B06, B07 | Superficies completas y errores coherentes; aggregate de base. |
| B09 | P2/P3 | documentación y consumidor testkit | B08 | Contrato local conciliado y migración externa con estado independiente. |
| B10 | gate | CI, distribución y regresión transversal | B01–B08, parte local de B09 | Candidato mongoeco con evidencia completa; cierre coordinado exige además B09 externo. |

Tras B00, B01, B02 y B03 pueden avanzar por separado. B04 y B05 no necesitan
esperarse entre sí: integrar sus deltas de proyección/expresiones sin exigir que
termine toda la otra familia. B06 reúne sus consumidores y B08 cierra las
superficies. La validación BSON de entradas de B07 también puede prepararse
desde B00; su salida y aceptación conjunta requieren B02/B03. Cada cambio lleva
sus errores públicos y pruebas de transporte cuando estén afectados; B08 no es
una espera para añadirlos al final.

Las dependencias son de aceptación de las garantías compartidas, no una
obligación de completar todos los testigos de otro bloque antes de comenzar.
Para cerrar una unidad, exigir la captura previa y la corrección de sus reglas
dependientes; documentar el resto del bloque como pendiente. Los IDs de la tabla
son paquetes de alcance y trazabilidad, no once refactors indivisibles.

En particular, separar en B08 la incorporación de `Database.aggregate` de la
verificación transversal H35. La primera requiere la preparación collectionless,
el contexto y las fronteras que realmente use su pipeline; no espera al cierre
de distinct, buckets o todas las proyecciones. H35 sí reúne los deltas de B04–B07.
Un pipeline con un operador pendiente conserva esa condición: añadir la fachada
no acredita su semántica. La misma separación permite retirar en B09 el aggregate
manual cuando ese contrato concreto esté validado, sin esperar a otros wrappers.

Separar commits de captura nativa, corrección y refactor cuando ayude al review;
la unidad entregable debe incluir regresión y contrato actualizado. Un bloque
puede entregarse por separado con su alcance explícito, sin afirmar cerrado el
programa. Los P1 conocidos pendientes bloquean el cierre conjunto.

### Contratos mínimos para reducir mantenimiento

Reutilizar `PreparedPipeline`/`PreparedStage`, `EvaluationEnvironment`,
`ExpressionExecutionContext`, `_IncrementalGroup` y la preparación de
acumuladores existentes. No introducir un segundo árbol de operaciones,
otra VM de expresiones, un framework de políticas o un nuevo contexto que
duplique dialecto, collation y reloj. Ampliar un tipo solo con el dato que una
regla capturada necesita. La frontera de preparación conserva la fase de error;
unificar representación no autoriza adelantar validaciones dependientes de datos.

| Regla | Owner único | Adaptaciones que siguen siendo explícitas |
| --- | --- | --- |
| Resultado de expresión | Evaluador común y entorno existente | Omisión/eliminación, posición de array y consumidor booleano/acumulador. |
| Números y orden BSON | Funciones escalares y de comparación actuales | Matching de consulta, promoción de escritura/agregación e identidad física. |
| Sintaxis | Preparador/handler de cada gramática | Filtro, expresión, acumulador, stage y bindings de comando. |
| Paths/proyección | Helpers existentes separados por contexto | Traversal de expresión, broadcast de stage y selección/update clásico. |
| Acumulación | Preparación, transición y finalización existentes | Group, ventana, bucket, spill y forma/orden de salida. |
| Frontera pública | Conversión/validación y adaptadores de error existentes | API directa, comando, elemento bulk y transporte wire. |

Cada unidad elimina o delega el camino anterior al introducir su sustituto;
no deja dos implementaciones seleccionables, flags permanentes de transición
ni caches nuevas para compensar duplicación. Si una garantía necesita todavía
el camino antiguo, registrar el caller y la condición de retirada concreta.

### Unidades de trabajo y límites de refactorización

Los bloques conservan sus IDs, pero la siguiente unidad se elige por la regla
compartida pendiente y sus consumidores, no por terminar un archivo o todo B03
antes de empezar B04. En el árbol revisado ya hay un kernel Decimal128 avanzado;
conservar sus regresiones y procedencia sin exigir ampliar esa familia para
desbloquear gramática, rutas o frontera BSON. Corregir nuevos contraejemplos
confirmados dentro de su regla; una ampliación exploratoria de cobertura puede
avanzar aparte, sin rebajar las obligaciones de cierre.

| Unidad de ejecución | Alcance de aceptación | Simplificación |
| --- | --- | --- |
| Suma, promedio y promoción (B03 + parte de B06) | H26/H27 y consumidores pendientes; overflow de escritura con su contrato propio. | Adaptar consumidores al estado y finalización compartidos existentes; selección de operandos por contexto. No rehacer ventanas/spill ni extender ese estado a toda la aritmética. |
| Gramática y bindings (B04, integración B08) | H09–H14/H30, incluidos errores sin filas, ramas inactivas y ciclo de vida del cursor. | Migrar una familia cubierta cada vez. Una preparación de `let`, invocada por los adaptadores en su fase; distinguir programa de petición, bindings evaluados y reloj sin otra jerarquía de contextos. |
| Escritura de rutas y proyección (B05) | H05/H28/H29 con efectos, retornos y contadores. | Reutilizar clasificación/tokenización; mantener traversal y asignación específicos de cada contexto. |
| Frontera pública (B07, integración B08) | H16 y fugas restantes H06/H26/H35 por cada entrada real. | Validar en los puntos comunes de entrada, materializar en salida y adaptar el error en su frontera. |
| Trazabilidad (B00/H35) | Relación H/caso/frontera e inventario ejecutado. | Extender el soporte de casos existente; un caso puede cubrir varios H. No copiar la matriz en documentación. |
| Evidencia y cierre (B09/B10) | Receipt aceptado por unidad y cierre del mismo artefacto. | Registro con enlaces y pendientes; conservar fuentes y fallos relevantes, sin transcribir los reportes. |

Las correcciones son P1 y admiten intercalado según sus dependencias; evidencia
y cierre conservan las prioridades B09/B10. La tabla no crea bloques ni estados
adicionales. Describe cómo agrupar trabajo, no qué sigue pendiente: consultar
el registro vivo y conservar
los cortes ya aceptados. Completar los consumidores restantes de B03/B06 sin
rehacer sus núcleos. H15 puede entregarse por separado con sus dependencias
concretas; B05 y la entrada BSON de B07 no esperan al cierre completo de H14.

La duplicación inicial de `_sum_accumulator_operand` ya dio paso a
`numeric_accumulation.py` como owner del estado aritmético de sum/avg. Conservar
sus adaptadores de operandos: las reglas de arrays dependen del contexto.
La media añade conteo/finalización al mismo estado. No extenderlo a `$add`,
`$inc` o desviación estándar por semejanza de implementación. Spill y ventanas
deben conservar/restaurar todo ese estado, no solo el total visible; capturar
su contrato antes de decidir si necesitan merge o recomputación.

Una unidad termina cuando sus contraejemplos conocidos y consumidores declarados
están cubiertos, se retira la autoridad duplicada y pasa su regresión causal.
No necesita explorar exhaustivamente todos los operadores vecinos para permitir
el siguiente P1. Un contraejemplo nuevo confirmado mantiene su obligación y se
vincula a su H; no se descarta para dar por cerrada la unidad. Separar esa
obligación de una ampliación exploratoria todavía sin fallo evita que B03
absorba indefinidamente gramática, paths y BSON. El registro vivo decide la
siguiente unidad con esa distinción, sin otro backlog o estado paralelo.

En particular, no añadir ahora una representación preparada de `let`, una caché
de validación o una jerarquía de contextos solo para evitar un segundo recorrido
estático. En el árbol actual la compilación valida dependencias y la ejecución
prepara/evalúa de nuevo: ese coste es distinto de evaluar dos veces. Primero
conservar las fases con testigos de errores simultáneos y ejecución única;
eliminar recorridos únicamente cuando el mismo dato y la misma frontera ya
estén cubiertos. La procedencia de la petición no debe cambiar lo que significa
superponer bindings internos ya evaluados. Los testigos deben conservar también
el alcance por sentencia bulk y los contratos distintos de clone/rewind/getMore.

## 3. Inventario completo y trazabilidad

Los IDs H01–H36 son el inventario canónico de este plan. La columna de prueba
da una familia semántica, no impone un prefijo ni un módulo nuevo a los casos.
Los IDs ejecutables incorporados se consultan en el catálogo y el registro de
implementación; las familias pendientes aún requieren sus testigos.
Cada familia tendrá IDs estables para sus variantes mínimas. Una fila solo se
cierra con referencias reales de sus testigos y consumidores efectivos. Las
familias no obligan a crear 36 módulos: agrupar casos por regla y reutilizar
setup y acciones. Esta tabla posee el alcance; los casos ejecutables poseen
las entradas/expectativas y el reporte genera su matriz de ejecución. No
mantener a mano otra copia de esos resultados ni un lenguaje adicional de fixtures.

| ID | Hallazgo y testigos que se deben conservar | Bloque owner | Familia diferencial |
| --- | --- | --- | --- |
| H01 | Ausente frente a null en `$eq`, `$ne`, `$in`, `$cmp`, `$gte`, `$lt`; null explícito frente a ausente; dos ausentes siguen siendo iguales entre sí. | B02 | `missing_compare` |
| H02 | `$type`/ausencia a través de `$field`, `$$ROOT.field`, `$$CURRENT.field`, `$let` y `$getField` con input ausente. | B02 | `missing_reference` |
| H03 | Fuera de rango de `$arrayElemAt` y `$first` sobre array vacío producen ausencia, no null en todos los contextos. | B02 | `missing_producer` |
| H04 | Omisión frente a null en objetos, `$map`, `$cond`/`$switch`, `$setField`, `$mergeObjects`, `$replaceWith` y proyección compuesta. | B02 | `missing_composition` |
| H05 | `$set`/`$addFields` y updates por pipeline: destino existente con fuente ausente debe eliminarse; no conservar valor previo ni escribir null. | B05 | `missing_write` |
| H06 | Fugas de `_MISSING`/`_REMOVE` en objetos y arrays, copias, resultados, escritura Memory, serialización SQLite y respuesta wire. | B02 | `sentinel_boundary` |
| H07 | Booleanos, `$filter`, comparación y equivalencia entre distintas expresiones ausentes; `$getField` ausente no es verdadero ni ordena después de null. | B02 | `missing_consumers` |
| H08 | `$push`/`$addToSet` con ausencia; variantes compuestas y ventanas; `$sortByCount` no puede devolver un sentinel como `_id`. | B06 | `accumulator_missing` |
| H09 | `$exists`, `$elemMatch`, `$all`, `$nin` aceptados como expresiones de agregación; limitar la gramática de consulta a su contexto. | B04 | `expression_grammar` |
| H10 | Formas field-bound de `$and`/`$or`: aceptan expresiones inválidas y reinterpretan otras válidas; mantener objetos literales verdaderos. | B04 | `logical_grammar` |
| H11 | `$not` como expresión, con array y sin él, y `$last` como expresión faltantes. | B04 | `expression_surface` |
| H12 | `$and`/`$or` con operando escalar e índice double integral de `$arrayElemAt`: formas válidas rechazadas. | B04 | `expression_forms` |
| H13 | `$in` con segundo argumento null/ausente debe fallar, no devolver null ni un resultado vacío. | B04 | `in_type` |
| H14 | Aridades/estructura inválidas en colecciones vacías (`$cmp`, `$ifNull`, etc.), acumuladores unarios con arrays inválidos, ramas inactivas y códigos por contexto (`$match/$expr` frente a `$project`). | B04 | `validation_phase` |
| H15 | `Database.aggregate` inexistente en fachadas y `aggregate: 1` rechazado en wire; `$documents` y `$$NOW`. | B08 | `database_aggregate` |
| H16 | Frontera BSON: Decimal crudo, object arbitrario, set/frozenset, UUID por defecto, claves NUL y excepciones públicas. | B07 | `bson_boundary` |
| H17 | `$where`: subconjunto Python local, sin JavaScript; errores directos y wire distintos. Disposición explícita sin simular paridad JS. | B09 | `where_limit` |
| H18 | Reasignar `CURRENT` debe afectar `$field` igual que `$$CURRENT.field`, preservando ROOT y alcance léxico. | B02 | `current_scope` |
| H19 | Workarounds `$max`, dotted arrays y orden campo/operador: retirar solo tras demostrar sus callers; no llamarlos código muerto. | B09 | `consumer_workaround` |
| H20 | Datetimes naive UTC/milisegundos en ambos backends; distinguir normalización de entrada redundante y restauración aware exigida por la aplicación. | B09 | `datetime_boundary` |
| H21 | Caché compilada: `True`/`1`/`1.0` y `False`/`0`/`0.0`, orden inverso, clientes/motores distintos y caché fría/caliente. | B01 | `cache_identity` |
| H22 | `$sum: True` cambia entre grupo principal/subpipeline, con deadline no agotado o acumulador independiente. | B01 | `group_route` |
| H23 | Codegen por repr de Decimal128/ObjectId y otros literales BSON; NameError en `$facet`, `$lookup`, `$unionWith`; evitar interpretar arrays de expresiones como literales Python. | B01 | `compiled_literal` |
| H24 | Double 0.1 frente a Decimal128 0.1: comparación, filtro, grupo, joins e índice unique; control positivo exacto con 0.5. | B03 | `numeric_equivalence` |
| H25 | `distinct` de campo ausente y de tipos numéricos equivalentes; valores públicos usados en comparadores internos. | B06 | `distinct_semantics` |
| H26 | Precisión decimal ambiental, `$divide`/`$add`/`$multiply`/`$avg` y tipo público Decimal frente a Decimal128. | B03 | `decimal_arithmetic` |
| H27 | Overflow int64: agregación promueve a double; `$inc` rechaza con error de escritura, sin filtrar excepción interna ni mutar datos. | B03 | `numeric_overflow` |
| H28 | Paths numéricos en expresiones y broadcast en `$set` de agregación/update pipeline; distinguir updates clásicos y verificar estructura escrita. | B05 | `path_context` |
| H29 | Flags de proyección 2/0.0/1.0, constantes mediante `$literal`, expresiones de `find`, rutas en arrays y combinación inclusión/exclusión. | B05 | `projection_context` |
| H30 | `let` de comando: evaluar expresiones antes de filas, rechazar acceso a campos y conservar `$literal`; valores erróneos llegan a persistirse por varias rutas. | B04 | `command_let` |
| H31 | `$lookup` directo con array local vacío, null, ausente y `[null]`; no confundir sus reglas con `$expr`. | B06 | `lookup_keys` |
| H32 | `$bucket`: buckets/default vacíos, límites de tipos incompatibles y validación de `groupBy`; `$sortByCount` con forma inválida. | B06 | `bucket_contract` |
| H33 | Orden BSON de documentos: tipo del valor frente a nombre de clave, orden de claves y recursión; ejemplo `{z: null}` frente a `{a: 1}`. | B03 | `document_order` |
| H34 | Tests espejo que comparan CompiledGroup con apply_pipeline que vuelve a elegir CompiledGroup; aserciones que no distinguen bool/int/double. | B00 | `oracle_independence` |
| H35 | Mismo defecto por API directa, comandos y proxy: familias/códigos/fases, recuperación, contadores y estado persistido. | B08 | `surface_conservation` |
| H36 | Contratos/documentos/catálogos/tests que consolidan la divergencia; MISSING_FEATURES no inventariaba estas carencias; separación entre limitación y defecto. | B09 | `contract_inventory` |

Los hallazgos no son 36 defectos independientes ni 36 tests: hay causas
compartidas y múltiples consumidores. No sumar wrappers como nuevos escenarios
semánticos. Conservar la matriz caso × ruta y el estado de cada celda.

## 4. Trabajo y aceptación por bloque

### B00 — Oráculos, observación y cobertura independiente

Reutilizar `tests/differential/cases.py`, las familias de casos existentes,
`tests/support_write_parity.py`, `tests/support_write_histories.py` y los
capturadores de `scripts/`. Añadir familias a los módulos de casos; no
otro runner. Extender el observador existente si un dato necesario se pierde.

El crecimiento actual ya repite el inventario de corpus en `cases.py`, el
capturador, el replay y la matriz pública. Consolidar exclusivamente esos datos
de registro en un módulo de soporte de tests: ID de corpus, casos, módulo fuente
y ruta de fixture. Los consumidores derivan selecciones e inventarios desde
él. Conservar `RealParityCase` y los formatos existentes; no introducir plugins,
descubrimiento automático, otra jerarquía de casos ni un motor de ejecución.
Comprobar IDs únicos, casos aplicables, fixtures completas y selección no vacía.
El soporte de registro no importa el evaluador de mongoeco en las acciones
nativas; el observador conserva sus controles independientes H34.

La independencia alcanza también al comparador. La dependencia inicial de
`_real_parity_base._normalize` respecto a `core.identity.canonical_document_id`
ya se retiró en el corte H27/H34: conservar esa separación. El owner actual es
`tests/support_bson_observation.py`. Comprobarlo con
pares construidos independientemente: tipos numéricos, cero con signo, cohortes
decimales, orden de claves, ausencia y errores. Conservar cada distinción donde
su contrato la observe; no imponer identidad binaria a toda comparación MongoDB.
Los formatos actuales BSON y observaciones JSON canónicas pueden permanecer;
un único owner de comparación no exige migrar todos los goldens. No compartir
con producción las reglas que el diferencial pretende verificar.

Completar la trazabilidad usando los IDs de casos existentes y una relación
mínima con H01–H36 y las fronteras cubiertas. El registro de corpus actual no
contiene esa relación: contar casos o ejecuciones no demuestra cierre de un H.
Generar el inventario ejecutado desde esos datos; el registro de implementación
conserva valoración, pendientes y enlaces, sin copiar otra matriz de resultados.
No añadir una DSL de selección ni dependencias automáticas entre funciones.

Conservar el hash del módulo como procedencia, sin interpretarlo como identidad
completa del testigo: acciones, seeds y observadores pueden depender de helpers
importados. Registrar también los helpers relevantes mediante una lista explícita
pequeña en el soporte existente; no construir un grafo automático de imports.
Un cambio de esos helpers invalida las referencias afectadas hasta comprobarlas
contra el servidor. Un cambio de formato del módulo requiere revisión/check,
pero no justifica sustituir automáticamente los resultados históricos. El hash
del candidato identifica su ejecución y no obliga a recapturar el oráculo.

1. Preparar el esquema de observación y demostrarlo con testigos representativos
   de valor/tipo, ausencia, error y efecto persistido. Reducir y capturar los
   ejemplos de H01–H36 en el bloque que los corrige, antes de editar su semántica:
   setup, petición, versión mínima, rutas y dato observado. Usar PyMongo contra
   el servidor real, sin importar el evaluador de mongoeco en el oráculo.
2. Capturar éxito, error y estado posterior, incluidos códigos raw ausentes,
   retornos BEFORE/AFTER, modified/matched/upserted, índices bulk y recuperación.
3. Comparar tipos BSON y estructura, conservando orden de claves y distinción
   ausente/null. No usar `str`, `repr`, JSON ordinario ni igualdad Python como
   observación suficiente de números, sentinelas, Decimal128 o errores.
4. Normalizar únicamente datos incidentales documentados: namespaces únicos,
   IDs generados correlacionados, duración y reloj cuando corresponda. Ordenar
   conjuntos solo donde el contrato no promete orden; no ordenar arrays de datos
   ni claves de documentos. No elevar selección de un tipo entre extremos
   numéricos empatados a defecto sin contrato nativo que lo exija.
5. Contrastar el ejecutor común con el servidor real. Cuando sobreviva otra
   estrategia, forzar e identificar ambas implementaciones; si se retira el
   compilador, conservar las secuencias públicas de regresión y comprobar su
   ausencia productiva, sin mantener una implementación defectuosa como oráculo.
   Añadir mutaciones del observador: bool por int, ausencia por null, código
   omitido, escritura distinta y bucket vacío deben causar fallo.
6. Conservar casos pendientes como no capturados. Una referencia ausente,
   selección sin tests o skip inesperado no cuenta como éxito de una celda.

Aceptación inicial: observador comprobado, capturas representativas 7/8/9 y
conexión al runner/replay existentes. No exige capturar los 36 hallazgos antes
del primer arreglo. Cada bloque incorpora sus casos y referencias inmutables;
al cerrar B10, todo ID aplicable debe estar vinculado y ejecutado. Los casos aún
pendientes se conservan como caracterización explícita, fuera del gate de fixes
ya cerrados; no usar xfails permanentes ni tolerancias silenciosas. Retirar esa
disposición de pendiente al incorporar cada regresión al gate ordinario.

### B01 — Retirada de la ruta compilada divergente

- Opción preferida: hacer que `_apply_group` use `_IncrementalGroup` y retirar
  `CompiledGroup`, su caché y generación por repr tras migrar sus callers/tests
  internos. La búsqueda de la base encuentra un caller productivo en
  `grouping_stages.py`; refrescar ese censo antes de editar. No es necesario
  reparar una caché ni un compilador que dejan de ejecutarse.
  Comprobar exports/manifiesto público antes de borrar el símbolo: si existe un
  contrato de importación conservado, usar una fachada fina al mismo núcleo,
  sin caché ni codegen alternativos. La retirada no alcanza `CompiledQuery`.
- Conservar tests públicos de secuencias True/1/1.0, literal BSON, subpipelines y
  deadline; sustituir tests ligados a detalles de la caché por estas garantías.
  No alterar el SPI ni las fachadas públicas para realizar la retirada.
- Solo si una medición demuestra necesario mantener esta optimización, tratar
  la alternativa como decisión explícita: claves tipadas, literales en slots y
  operaciones delegadas al mismo núcleo; nunca `repr` ejecutable ni semántica
  alternativa de sum. No ejecutar ambos proyectos de reparación y retirada.
- Eliminar la captura de errores de ejecución seguida de reintento genérico;
  no repetir una evaluación después de consumir parcialmente una fuente.

Aceptación: H21–H23 y metamorfismos de la sección 5 verdes; orden de consultas,
otro cliente, deadline y acumulador independiente no cambian valores ni tipos.
Registrar coste de los workloads afectados antes/después. Una regresión de
rendimiento se hace visible, pero no justifica conservar resultados incorrectos.
Reintroducir codegen más adelante exige un beneficio medido y una garantía
adicional de equivalencia; no es requisito para cerrar este programa.

### B02 — Valores de expresión y entornos

- Sustituir la dualidad «evaluar normal»/«evaluar con missing solo si la raíz es
  una referencia» por una evaluación que conserve ausencia/eliminación en toda
  composición. Preferir los valores BSON actuales y sentinelas internos de copia
  estable; no envolver cada escalar/documento en un nuevo árbol de resultados.
  Distinguir solo estados que los testigos requieran, con reglas explícitas de
  copia/pickle/materialización; ningún estado interno es BSON serializable.
- Definir adaptadores consumidores: omisión de campos, null en posiciones de
  arrays, eliminación, veracidad, comparación y reglas de cada acumulador. No
  tratar todo ausente como null ni eliminar todos los ausentes indiscriminadamente
  (`$first` y `$push` no comparten necesariamente esa regla).
- Unificar ROOT/CURRENT, referencias implícitas y variables léxicas; conservar
  su alcance al cambiar de documento/subpipeline. CURRENT reasignado cambia
  `$field`; ROOT permanece estable según el contexto nativo correspondiente.
- Preservar el contexto de collation, dialecto y reloj. Ningún sentinel puede
  salir por cursor, metadata, writeback, raw batch o snapshot de almacenamiento.

Aceptación: H01–H04, H06–H07 y H18 verdes en composiciones y contextos; incluir
variables de lookup, `$getField`, `$map`, `$cond`, `$switch` y copia anidada. Las
escrituras H05/H06 se cierran también en B05/B07, no solo en el helper.

### B03 — Álgebra BSON contextual

- Asignar una implementación propietaria a comparación, igualdad y generación
  de claves semánticas, con adaptadores de collation. Corregir double/decimal y
  orden de objetos. Mantener identidad física y matching de consultas separados.
- Separar conversión para comparar y generar claves de conversión para operar.
  `_numeric_to_decimal` participa hoy en ambas rutas: cambiarla globalmente para
  H24 puede alterar redondeo/promoción en H26. Compartir la representación de
  comparación solo con sus claves semánticas; capturar por separado la conversión
  aritmética antes de cambiarla. No crear una política numérica universal.
- Verificar la ley `igual(a,b) => clave(a)==clave(b)` en cada contexto que use
  hash; no exigir el recíproco para un índice de candidatos con residual.
  Toda poda debe conservar candidatos válidos antes del chequeo canónico.
- Encapsular precisión, redondeo, exponentes y traps del contexto Decimal128 en
  todas las operaciones y finalizaciones; no depender de `decimal.getcontext()`
  ni de si la API ejecuta en otro hilo.
- Declarar política de promoción/overflow por operación: expresiones y
  acumuladores frente a modificadores de escritura. Adaptar errores de escritura
  sin mutaciones parciales del documento y conservar la presentación pública.

Aceptación: H24, H26–H27 y H33 verdes, más consumidores de índices, `_id`,
multikey, partial/sparse, collation, group y lookup. Incluir 0.1/0.5, tipos
bool/int32/int64/double/decimal, enteros alrededor de 2**53, límites int64,
NaN/infinidades válidas, cero con signo y subdocumentos. Capturar las
expectativas nativas; no deducir reglas para todos estos tipos de un solo par.

### B04 — Preparación de expresiones, gramáticas y let

- Concentrar aridad, validador y handler en las familias de operadores actuales;
  enlazar soporte por dialecto y pureza solo donde cambien. Derivar sus listas
  de operadores en lugar de añadir una tercera whitelist. Los validadores de
  objetos complejos siguen siendo funciones normales; no crear una DSL ni un
  metarregistro universal de todas las gramáticas. Comprobar la alineación de
  preparación/runtime/catálogo y no anunciar soporte por una lista aislada.
  Migrar primero las familias afectadas y cubiertas por el corpus, manteniendo
  los dispatchers existentes donde no duplican una regla. No convertir B04 en
  una reescritura previa de todos los operadores para poder corregir uno.
  Incluir en el censo `group_average._CLASSIC_EXPRESSIONS`: describe elegibilidad
  nativa que afecta al resultado de avg, no soporte de sintaxis. Mantener ambos
  conceptos distintos y comprobar su alineación al añadir operadores. Si el
  catálogo familiar puede poseer ese dato por versión sin duplicarlo, derivar
  la selección desde allí; de lo contrario conservar el selector especializado
  como único owner con sus testigos. No reconstruir un planificador SBE ni
  inferir elegibilidad a partir de que el operador esté soportado.
- Mantener gramáticas de filtro, expresión, acumulador y stage diferenciadas.
  Retirar de la gramática MongoDB las extensiones field-bound que interceptan
  entradas. Si se decide conservar una extensión pública local, aislarla en un
  contrato explícito que no cambie el dialecto MongoDB por defecto.
- Completar aridades y validación sin filas; distinguir preparación, optimización
  de constantes y evaluación de datos. Validar ramas inactivas sintácticamente
  sin forzar indiscriminadamente sus errores de ejecución.
- Implementar `$not`, `$last` y formas válidas H12 con pruebas por contexto.
  Corregir `$in` y los códigos/metadata según operación y versión capturadas.
- Introducir preparación específica de `let` de comando: evaluar antes de filas,
  validar referencias y reservar bindings internos para valores ya evaluados.
  No reevaluar valores heredados ni confundir `$literal`, `$let` y `$lookup.let`.
  Reutilizar el visitante de alcance de la gramática: buscar recursivamente
  cadenas que empiecen por `$` no distingue literales, CURRENT cerrado ni
  variables locales. Conservar un único owner de esas decisiones.

Aceptación: H09–H14/H30; mismas reglas en find/count, agregación, update y
subpipelines. Ninguna whitelist de preparación puede aceptar una forma que el
runtime interpreta de otra manera. Revisar tests que exigían extensiones.

### B05 — Paths y proyecciones por contexto

- Compartir tokenización donde ya se repite, con helpers de contexto explícitos:
  selección de consulta, lectura de expresión, proyección, update clásico y
  escritura de stage. No trasladar índices numéricos de updates a expresiones.
- Implementar broadcast de stages sobre arrays y reglas de segmentos numéricos;
  resolver destino existente/ausente, null, escalar, array vacío y arrays anidados.
- Hacer que missing elimine el destino cuando corresponde en `$set/$addFields`
  y updates; conservar atomicidad y contadores/retornos del resultado real.
- Compartir clasificación de flags, detección de conflictos y operaciones de
  selección que tengan igual contrato entre `find` y `$project`. Conservar
  preparadores de contexto para expresiones calculadas, `$slice`, `$elemMatch`,
  posicional y `$meta`; no exigir un ejecutor universal con flags de modo.
  Extender `_ProjectionExecutor` y el stage existentes donde corresponda, sin
  construir una segunda representación completa de proyección. Clasificar flags
  numéricos como inclusión/exclusión; constante explícita mediante `$literal`.

Aceptación: H05/H28/H29 verdes; mismo testigo en update clásico y pipeline con
expectativas nativas distintas cuando proceda. Proyecciones calculadas funcionan
por find/find_one y retornos findAndModify donde el servidor las admita. No
reinterpretar un operador específico de find como expresión de agregación.

### B06 — Un núcleo de acumulación y consumidores explícitos

- Hacer que grupo principal, subpipelines, ventanas y spill compartan las reglas
  de transición/finalización ya presentes en `accumulators.py` y la preparación
  existente. Tras B01 no se construye otro motor genérico: B06 adapta consumidores
  y elimina atajos semánticos. Si subsiste una estrategia rápida, solo acelera
  esas mismas reglas sin otro intérprete.
- Compartir reglas del valor y del acumulador no obliga a unificar los
  algoritmos de ventana, spill y agrupación. Conservar sus estrategias de
  retención/recomputación cuando la cardinalidad u orden lo exijan; delegar la
  regla duplicada concreta y demostrar equivalencia de las estrategias activas.
- Mantener en cada consumidor sus reglas de cardinalidad, orden y claves:
  missing en group IDs, `$sortByCount`, particiones, `$bucket` y valores de salida.
- Corregir buckets vacíos, default, límites y groupBy. Validarlos también sin
  filas; no inicializar buckets visibles que nunca recibieron documentos.
- Preparar las claves de `$lookup` directo con su semántica de arrays/null;
  mantenerla distinta de `$expr`. Comprobar hash y nested-loop, con/sin collation,
  índices y umbrales. En 9.0 no afirmar probado un hash que el catálogo no activa.
- Ejecutar `distinct` con valores internos hasta terminar la deduplicación;
  ignorar campos ausentes y materializar una vez al entregar el resultado.

Aceptación: H08/H25/H31/H32 y regresiones de B01/B03; group/top/facet/lookup/union,
ventanas crecientes/deslizantes, bucket/bucketAuto y spill/no-spill aplicables.
No ampliar una excepción de un consumidor a todos por compartir un acumulador.

### B07 — Entrada BSON pública y salida materializada

- Separar codec interno reversible y validación pública BSON. Reutilizar tipos y
  adaptadores existentes, sin obligar al runtime local básico a instalar PyMongo.
  Con extras oficiales, ajustar clases y codec_options al perfil soportado.
- Validar recursivamente documentos, filtros, proyecciones, updates, replacements,
  pipelines, let, array_filters y modelos bulk. Incluir formas equivalentes por
  comandos raw y por índices/validators donde ya se normalicen documentos.
- Localizar los puntos de entrada compartidos antes de colocar validaciones:
  una solicitud no debe recorrer el mismo árbol BSON completo en cada fachada,
  comando y motor. Delegar desde esas rutas hacia el validador común existente;
  validar de nuevo solo ante datos nuevos o un contrato distinto. No introducir
  un flag público o una marca de «validado» que permita saltarse la frontera.
  El rechazo de un objeto Python no codificable por PyMongo ocurre antes del
  transporte: observarlo con el driver real y no exigir al servidor wire recibir
  ese objeto imposible. La API directa debe conservar su rechazo equivalente;
  documentos BSON válidos con expresiones inválidas sí alcanzan el servidor.
- Distinguir BSON inválido enviado por el cliente de una fuga interna producida
  por una expresión válida: esta última se corrige antes, no convirtiéndola en
  InvalidDocument tardío ni ocultándola con serialización permisiva.
- Centralizar materialización de resultados, tipos numéricos e IDs públicos;
  cubrir cursores, respuestas raw, detalles de error y eventos locales afectados.

Aceptación: H16, salida de H06/H26/H35; Memory/SQLite aceptan y rechazan las
mismas entradas en la misma frontera. UUID por defecto/con representación
explícita, Decimal crudo frente a Decimal128, claves NUL, nested object y set
conservan las clases/efectos del driver real. Datetime no retrocede.

### B08 — API, comandos y transporte

- Implementar aggregate de base sobre la preparación común, con namespace
  collectionless explícito, `$documents`, contexto temporal y validación nativa.
  Añadir fachadas sync/async, `database.command` y `aggregate: 1` wire; no un
  caso especial que devuelva manualmente documentos con NOW.
- Adaptar errores estructurados en sus fronteras: driver/BSON, comando,
  elemento de escritura y consumo del cursor. No reducir todos los rechazos a
  un mensaje sin código ni convertir un error de servidor en TypeError interno.
  Reutilizar catálogo y adaptadores existentes; cada bloque corrige y prueba sus
  errores al introducir el delta. B08 verifica la integración, no reimplementa
  todos los formateadores ni pospone su corrección.
- Comprobar alias/opciones heredadas, cloning, getMore, batches y recuperación.
  Conservar error raw, bulk ordered/unordered y prefijos válidos del lote.
- El proxy aplica el mismo contrato semántico preparado; BSON wire no sustituye
  la validación de la API directa ni la materialización correcta de sentinelas.

Aceptación: H15/H35 y todas las vistas aplicables de la sección 5; un fallo no
rompe la solicitud válida posterior. La recuperación de pool/conexión se prueba
con observación física, no solo con un ping que pueda abrir otra conexión.

### B09 — Consumidor, contratos y limitaciones

Dos entregables con estados separados: el contrato/documentación de mongoeco y
la migración del repositorio consumidor. El primero es requisito de B10; el
segundo es requisito del cierre coordinado H19/H20, no del build del paquete.
No marcar completo B09 global ni todo el plan mientras falte el segundo.

- Refrescar revisión/estado del testkit antes de proponer cambios en
  `mochuelo_testkit/mongodb_mock.py`; el checkout inspeccionado estaba bajo
  `/Users/uve/Proyectos/gdynamics-pre/gdynamics-testing/`. Respetar sus normas y
  entorno; no tocar compilador de Mochuelo ni aplicaciones como arreglo del mock.
- Retirar por unidades los lowerings de `$max`, reescrituras dotted-array y
  reordenación campo/operador tras probar todos sus callers reales sin wrapper.
  Comparar con/sin wrapper sobre arrays vacíos, documentos heterogéneos y arrays
  poblados por pipeline, sin convertir coincidencias limitadas en prueba universal.
  Sustituir aggregate manual solo cuando B08 conserve el reloj autoritativo.
- Mantener la frontera aware de aplicación; quitar una normalización de entrada
  duplicada únicamente con prueba de equivalencia. Naive UTC y truncado BSON no
  son defectos de mongoeco ni del driver que haya que invertir globalmente.
- Para `$where`, mantener JavaScript como capacidad no implementada y documentar
  el subconjunto local por separado. Conservar/revisar su contrato Python sin
  romperlo silenciosamente; la superficie compatible rechaza capacidades no
  implementadas mediante error público explícito. La sonda JS nativa queda como
  diferencia conocida, no como prueba verde de paridad ni obligación de añadir
  un motor JavaScript en este programa.
- Corregir COMPATIBILITY/DIALECTS, catálogo actual, docs de errores, TODO y
  MISSING_FEATURES según sus owners: carencias de producto allí; decisiones de
  refactor aquí/arquitectura. Actualizar manifiesto público y typing para APIs
  añadidas. No sustituir fixtures históricas con resultados del candidato.

Aceptación local: H17/H36 dispuestos y contrato BSON/datetime de H20 verificado
en mongoeco. Aceptación externa: H19/H20, testkit sin reparaciones que enmascaren
los casos C1/C2/S2c/S2d/S3d del análisis inicial y regresiones físicas del
consumidor. No inferir el estado de datos de producción. Si el consumidor no
está disponible, conservar explícitamente esos casos/celdas pendientes: se puede
cerrar el candidato mongoeco, pero no declarar cerrado el programa coordinado.

### B10 — Integración y cierre del candidato

Ejecutar los gates existentes contra un wheel inmutable, sin confundir pytest de
source con validación de distribución. La sección 6 concreta el mínimo. Publicar
un mapa H → caso → captura nativa → consumidores/rutas → resultado del candidato,
incluyendo excepciones declaradas y evidencia de no regresión.

Aceptación del candidato mongoeco: no hay P1 pendiente, cero celdas locales
obligatorias sin captura/ejecución, sin skips inesperados ni xfails de los fixes,
sin divergencias no dispuestas y sin dependencia de archivos temporales. La
única diferencia semántica conocida que se dispone como límite es JS en H17.
Las celdas externas pendientes de B09 se publican como tales; no se convierten
en exclusiones ni garantías de consumidor. El cierre completo de H01–H36 exige
también esas celdas. Cualquier otra exclusión requiere revisar el alcance, no
ocultarla en el observador. No tag, merge o publicación implícitos.

## 5. Matriz diferencial y propiedades contra errores espejo

### Referencias y superficies

Usar las imágenes con digest del workflow
[mongodb-differential.yml](../.github/workflows/mongodb-differential.yml): MongoDB
7.0.43, 8.0.32 y 9.0.2 en la base actual. Registrar buildInfo y FCV estable real,
SDK, Python, plataforma, SHA del candidato, hash de corpus/observador y seed.
Un cambio de patch/digest obliga a refrescar la referencia explícitamente.

| Eje | Celdas obligatorias según testigo |
| --- | --- |
| Servidor | 7.0, 8.0, 9.0; diferencias por versión capturadas, nunca copias del golden 9. |
| Motores | Memory y SQLite; SQLite nuevo y reapertura cuando hay persistencia; fixtures antiguas como regresión. |
| APIs | Nativas mongoeco sync/async y PyMongo sync/async real contra el proxy; cliente PyMongo equivalente contra mongod. |
| Lecturas | find/find_one, count_documents, distinct, aggregate y proyección/retornos de findAndModify cuando admita la forma. |
| Escrituras | update_one/update_many/find_one_and_update, bulk ordered/unordered y comando update, más inserts/replacements para BSON. |
| Agregación | Principal, facet, lookup y union; consumidores ventana/bucket/sortByCount y writeback donde corresponda. |
| Cursores | Materialización e iteración; first/clone, batches/getMore y error inicial frente a consumo posterior en testigos de frontera. |
| Plan | Estrategias que sigan existiendo: ejecución común/rápida, caché fría/caliente, deadline no agotado, índice/scan, hash/nested-loop y spill/no-spill efectivos. Tras retirar CompiledGroup, conservar regresión pública y prueba de retirada; no mantenerlo solo para completar una celda. |
| Python/SDK | Gates de wheel en 3.13/3.14/3.15; testigos de contexto decimal en los tres; extremos PyMongo 4.9.2/4.19.0 y matriz de perfiles existente. |

No ejecutar el producto cartesiano completo indiscriminadamente. Los casos
canónicos cubren cada regla con MongoDB 7/8/9; cada ruta compartida añade sus
testigos de frontera y fallos parciales. El manifiesto debe declarar N/A con
motivo observable (capacidad/versión/gramática), nunca por ahorro de tiempo.
Una ruta anunciada pero inaccesible no acredita que se haya probado su plan.

Para H35 distinguir tres observaciones: operación de la API pública, comando
que el SDK realmente envía y respuesta/efectos del servidor. La misma acción
PyMongo contra mongod y proxy prueba las dos últimas solo para la petición
efectivamente enviada; no demuestra por sí sola el contrato directo de mongoeco.
En opciones heredadas, clone y codec, añadir un testigo de petición equivalente
explícita y, si hace falta, observación focal del comando enviado con el soporte
existente. No duplicar todos los corpus ni añadir otro proxy. Una diferencia
del SDK se atribuye a su perfil y una del servidor a su dialecto; no introducir
un flag de dialecto para reproducir un fallo del cliente.

### Cobertura por regla, frontera y estrategia

Mantener una sola definición del testigo semántico y expandir sus vistas con
adaptadores pequeños del arnés existente. No copiar inputs ni expectativas por
motor, API o bloque. La reducción de ejecuciones se justifica por la cobertura
observable, nunca por suponer que ambos lados llaman al mismo helper.

La matriz completa ya instalada se conserva hasta justificar cualquier reducción.
Para nuevos casos, declarar la regla y las fronteras alcanzadas en los registros
de casos existentes; seleccionar al menos un testigo que distinga cada contrato
de adaptador y todas las variantes que cambien su comportamiento. No sustituir
los negativos de una escritura o de BSON por un positivo de agregación. Los
metamorfismos que buscan selección accidental de rutas siguen cruzando las rutas
que puedan seleccionar estrategias diferentes.

Antes de reducir esa matriz, demostrar que falla al mutar cada contrato del
adaptador que se pretende cubrir (tipo, opción, error o efecto). La selección
reducida debe detectar las mismas mutaciones relevantes. Reutilizar los tests y
adaptadores actuales; no crear un framework de mutation testing para cumplir
este requisito. Si no hay esa evidencia, mantener las celdas instaladas.

| Tipo de garantía | Cobertura necesaria | Reutilización permitida |
| --- | --- | --- |
| Regla semántica | Testigos válidos, inválidos y frontera capturados en MongoDB 7/8/9; ambos motores locales. | Una captura por testigo/servidor e identidad de entorno, compartida por consumidores equivalentes. |
| Adaptador público | Cada ruta modificada o alcanzada por H35 con testigos positivos/negativos que observen tipos, opciones, fase/error y efectos aplicables; sync/async y wire incluidos. | Compartir acción/observador y la referencia de la misma petición nativa; no ejecutar todo el dominio escalar en cada fachada. |
| Estrategia física | Testigos que fuercen e identifiquen los planes existentes, incluidos cache/deadline/spill y cardinalidades frontera. | Metamorfismos locales con referencia nativa común cuando la petición es equivalente; capturar también cada forma nativa distinta de subpipeline/contexto cuya equivalencia se afirme. |
| Entorno de distribución | Matriz de Python/SDK/wheel establecida y testigos específicos del entorno, como precisión decimal y codecs. | No multiplicar todas las capturas semánticas por cada intérprete si el entorno no altera petición/observación; comprobarlo con las pruebas de frontera. |

El reporte genera la cobertura desde los casos y adaptadores registrados. Cada
H debe señalar la regla y sus consumidores efectivos, además del testigo de
frontera de cada ruta afectada. Una omisión/N/A necesita razón comprobable; una
celda no declarada no pasa por compartir implementación. Esta descomposición
conserva los mismos ejes de la matriz sin imponer su producto cartesiano.

Capturar de nuevo cuando cambien inputs, SDK/codec relevante, observador o
servidor/FCV; no cuando solo cambia el SHA de mongoeco. Una captura conserva el
entorno nativo original y cada candidato conserva su propia ejecución contra
ella. Los gates vivos 7/8/9 del candidato final se mantienen: reutilizar un
golden no los sustituye. Los cambios históricos se añaden con identidad nueva.

### Metamorfismos y testigos mínimos

- Repetir consulta aislada, después de otra con literal equivalente en Python
  pero distinto BSON, en orden inverso, en otro cliente y después de expulsar
  entradas de caché. Comparar también tipos, no solo `==`.
- Comparar la misma semántica en grupo principal y subpipeline; añadir deadline
  no agotado o acumulador independiente sin alterar los campos anteriores.
  Forzar/instrumentar las estrategias que existan para no compararlas consigo
  mismas; si el compilador se retira, conservar estas comparaciones de resultados
  públicos contra la referencia nativa y comprobar que no quedan callers activos.
  Cuando las formas nativas produzcan resultados distintos (por ejemplo, la
  finalización de avg int64 en SBE/clásico), capturar y conservar cada regla de
  contexto. No exigir una igualdad entre formas que el propio servidor incumple;
  la anotación preparada debe sobrevivir al cambio de estrategia física local.
- Insertar/remover índice, habilitar/deshabilitar optimización o variar umbral
  de spill preserva el resultado semántico. Conservar métricas/plan para probar
  que hubo realmente un cambio de ejecución; no usar tiempos como prueba.
- Cruzar ausente, null, false, int 0, double 0.0, Decimal128 0, cadena vacía,
  array vacío, objeto vacío y valores positivos. Aplicar referencias directas,
  variables y composición; mantener expectativas diferentes donde MongoDB las
  tiene. No asumir que lookup directo y `$expr` son intercambiables.
- Cruzar colección vacía, cero matches y filas presentes; distinguir errores
  estáticos, constantes y dependientes de datos. Para una escritura fallida,
  observar también el estado posterior y ejecutar una recuperación válida.
  Cuando se mueva una validación, añadir pares de defectos simultáneos para
  comprobar cuál prevalece, por ejemplo `let` inválido y cuerpo inválido.
  Un caso negativo por separado no conserva la precedencia. Seleccionar los
  pares de las fronteras modificadas, sin cruzar todos los errores posibles.
- Comparar API directa y wire; un valor no puede aparecer como objeto interno
  en una y fallar al codificarse en otra. Las diferencias propias del driver se
  observan en su frontera, no se borran para igualar los reportes.

Controles positivos del informe inicial: objetos literales en `$and/$or`;
`$nor/$regex` rechazados en contexto correcto; igualdad e `$in` de arrays;
find con null/ne null; `$gt/$lte/$ifNull` en sus testigos; `$max` escalar,
lista y array; dotted find tras update pipeline; orden campo/operador;
lookup válido let/pipeline; datetime UTC naive y milisegundos. Conservar
validación de ramas inactivas y el aislamiento de ROOT/CURRENT/collation/NOW.

## 6. Capturas, comandos y gates

B00 incorpora las familias al catálogo existente y a capturas/replay. Hay casos
registrados bajo `semantic_fix_*`, `semantic_missing_*` y `semantic_absence_*`;
esto no implica que estén incorporados todos los hallazgos. El listado del runner
es la autoridad para seleccionar los casos disponibles, con una URI aislada:

```bash
python scripts/run_mongodb_real_differential.py 9.0 --list-cases
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
python scripts/run_mongodb_real_differential.py 9.0 'semantic_*' \
  --seed 42 --json-report /tmp/semantic-fix-9.json
```

Repetir para 7.0 y 8.0 contra instancias de esa versión con FCV estable. No
ejecutar sobre bases compartidas de aplicación ni producción. El arnés usa
namespaces únicos y limpieza en finally. Para capturas, extender/reutilizar
`capture_differential_replay_golden.py` y los observadores/capturadores de writes
y wire ya existentes; un cambio en opciones del capturador debe implementarse
en B00 antes de documentarlo como comando disponible.

| Momento | Gate y evidencia |
| --- | --- |
| Cada corrección | Testigos focales, referencia real de la regla, ambas APIs/motores aplicables; estado posterior y regresión del consumidor compartido. |
| Cambio de núcleo | Familias dependientes completas, gramática negativa sin filas, propiedades de comparación/materialización y rutas optimizada/genérica. |
| Cambio de plan | Semántica + plan/coste observables y benchmarks focales reproducibles con dataset/hash iguales. No aceptar resultados distintos por ser más rápidos. |
| Integración | Suite differential 7/8/9, capturas nuevas consumidas por replay, testigos wire y documentos/errores completos. |
| Candidato final | Gates CI actuales de wheel/sdist reproducibles, pytest y unittest 3.13/3.14/3.15, cobertura >=99 % en 3.14, propiedades deep, tipado/PEP 561, SPI v2, lint ratchet y manifiestos/snapshots actuales. |
| Compatibilidad conservada | Fixtures SQLite 4.5/4.10, identidad/CAS/atomicidad, índices partial/multikey, cancelación/budgets, sesiones y change events locales existentes; no afirmar nueva paridad distribuida. |
| Consumidor | Canario anónimo y suite física/mock sin workarounds retirados; pytest kit y extremos SDK contra el mismo wheel. |

Reutilizar los comandos de [development](development.md),
[testing real](testing-real-mongodb.md) y [release checklist](release-checklist.md),
además de la matriz actual de CI. No ejecutar ni declarar estos gates al preparar
este documento. Los tests focales pasados durante los análisis anteriores no
acreditan el wheel futuro ni la corrección de sus fallos.

Las filas «cada corrección» y «cambio de núcleo/plan» seleccionan el alcance
incremental; B10 ejecuta el conjunto final una vez sobre la pareja inmutable
wheel/sdist. No repetir el cierre completo tras cada commit sin cambios causales
en los inputs. El gate del consumidor externo completa B09; pytest kit y canario
del propio paquete siguen siendo gates locales de B10.

Para nuevas unidades, conservar un receipt aceptado que enlace capturas, hashes
y reportes terminales, más el antes/después que demuestre la corrección. El
registro vivo enlaza ese receipt y explica pendientes; no vuelve a transcribir
todos los contadores, hashes y matrices. Los intentos fallidos de diagnóstico
solo se incorporan si explican una causa o decisión que deba conservarse.
Esto no elimina ni reescribe evidencias históricas ya incorporadas. La selección
de casos y fronteras reside en el soporte ejecutable existente, sin un segundo
registro documental de selección.

Identificar por separado la referencia nativa, el código candidato y la ejecución.
En un árbol sin commit, el SHA base no identifica el candidato: usar los hashes
de fuentes y el parche conservado en el receipt; en B10, el hash del wheel/sdist.
Reutilizar los campos de evidencia existentes, sin crear un sistema adicional
de procedencia. Un cambio posterior conserva el resultado como histórico y
requiere la regresión causal correspondiente, no vuelve vigente todo el receipt.
Un cambio exclusivamente de formato del corpus se verifica contra la referencia
conservada antes de actualizar su procedencia; no permite sustituir expectativas
ni obliga a publicar otra copia idéntica del golden. El mecanismo actual de hash
estricto debe seguir rechazando una referencia desalineada hasta esa verificación.

## 7. Riesgos y decisiones de mantenimiento

- **Contrato local incorrecto:** tests y documentación actuales pueden exigir
  normalizar ausencia a null o aceptar extensiones. Cambiarlos con referencia
  nativa y nota de migración; un test verde no invalida el hallazgo.
- **Rendimiento:** delegar código compilado puede aumentar coste. Medir group,
  subpipelines, lookup, índices, cursor y spill. Conservar la implementación común
  si cubre la necesidad medida; una optimización futura debe justificar coste
  adicional de mantenimiento y equivalencia, no solo una mejora de microbenchmark.
- **Identidad/persistencia:** una nueva clave semántica no cambia claves físicas
  ni formato SQLite implícitamente. Si el plan de un índice persistido depende
  de ellas, detectar y resolver compatibilidad/reconstrucción antes de usarlo.
- **Precisión/tipos:** corregir una comparación sin corregir keys/hash/unique o
  materialización deja errores espejo. Los consumidores H24/H25 son gate conjunto.
- **Flujo de errores:** endurecer BSON antes de arreglar sentinelas convierte una
  consulta válida en un rechazo; no cuenta como corrección del productor.
- **Alcance acotado:** Search/ANN, transacciones distribuidas, change streams
  remotos, retries de red y un motor JavaScript nuevo no se amplían en este plan.
  Sus contratos locales existentes sí siguen siendo regresiones exigibles.

La refactorización se considera útil cuando una regla tiene owner único, los
adaptadores de contexto son explícitos y añadir un testigo nativo alcanza a sus
consumidores sin duplicar intérpretes. Dividir archivos sin reducir esa duplicación
no constituye por sí solo el cierre de ningún bloque.

Para cada unidad de refactor, revisar la regla antes/después, los callers que
delegan en ella y los caminos/cachés/whitelists retirados. Una nueva abstracción
debe reemplazar una duplicación concreta o hacer verificable una frontera
observada. Si solo cambia nombres o añade configuración sin eliminar una segunda
autoridad, no es requisito de este plan. Los tipos/registros internos se pueden
ajustar durante la implementación sin perder los IDs, obligaciones y testigos.

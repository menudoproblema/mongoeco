# Optimización previa a 5.0.0: análisis y plan acotado

Estado: propuesta, 2 de noviembre de 2026. No implementada ni medida como mejora.
Este documento posee las decisiones propuestas para este incremento; la
[preparación de release](release-5.0.0.md) conserva sus resultados históricos.
Normas vigentes: [testing y compatibilidad](architecture/testing-and-compatibility.md)
y [checklist de release](release-checklist.md).

## Decisión recomendada y límites

Empezar por el tamaño BSON y la preparación de filtros residuales; después,
reutilizar materialización textual en SQLite Search. Son oportunidades concretas
que permiten quitar trabajo y duplicación sin retirar las fronteras semánticas.
Precalcular configuración SDK inmutable cabe en el mismo incremento. Reducir
tránsitos sync sólo entra cuando se demuestra que son vacíos y que la solución
conserva el ownership y el momento de los errores. No se promete un tiempo final.

La mantenibilidad y los contratos aceptados son condiciones de admisión, no
variables para negociar una mejora temporal. No se reabren O1–O8, D05, P2 ni
F00–F11; tampoco se incorpora el resto de la parte II histórica. Los controles
sobre código modificado comprueban su conservación, no una nueva auditoría.
No cambian Search v1, SPI v2, el formato SQLite, candidatos ANN, scores,
representación pública BSON, fases de error ni las reglas de contexto.

Referencias inmutables:

- Base del incremento: `cc2d07d7836a567617d5601b2ef74190d55a3203`.
- Runtime empaquetado: `7dcb9df2087c4e8d0cad091bfdce1ad9d45c3699`.
- Wheel: `2ed1bd93385d759d7a7a61ebc7409ab74c544ba7d8cf58590f56405325974d2f`.
- Referencia temporal histórica v4.11.0: `bb0597c1b42d4228b547a8eec87ce704489bcb7b`.
- `e4eb17cc`, `b93681b7`, `b83789bc` y `perf/f11-context-startup` permanecen congelados.

La implementación futura usa rama propia. La base de coste será el candidato
actual correcto, no 4.11.0: aquella versión omitía trabajo contractual. No se
reinicia F11 ni se sobrescriben sus intentos o la evidencia de preparación.

## Qué se ha contrastado

Se leyeron los cuatro informes, código, harness, datos crudos de la tanda válida,
perfiles conservados y contratos de testing. La
[evidencia de esta revisión](evidence/mongoeco-5.0.0/performance-study.json)
contiene los 104 cocientes recalculados, contadores, contraejemplos y una sonda
propia de seis lecturas, límite 30 s, GC activo, sobre el wheel instalado.
Sus 310 archivos coinciden con el código actual. No se han ejecutado benchmarks,
builds, suites globales ni modificaciones productivas durante esta revisión.
`benchmarks/execution.json` ya estaba sin seguimiento y se conserva intacto.

Los perfiles y la ablación aportados son evidencia orientativa de trabajo,
no una atribución exacta de CPU del candidato sin instrumentación. Se conservan
[los perfiles seleccionados](evidence/mongoeco-5.0.0/performance-study-profiles.tar.gz).
La sonda propia y su código también están en ese archivo. La ablación que anula
el tamaño rompe deliberadamente un contrato: indica dónde estudiar, no cuánto
recuperará un atajo válido. No se ha repetido esa ablación.

| Motor | Mediana de los 26 cocientes históricos B/A1 | Delta de Search dentro de la suma de tareas |
| --- | ---: | ---: |
| Memory sync | 1,350× | +0,8345 s; 49,56 % |
| Memory async | 1,286× | +0,6031 s |
| SQLite sync | 1,461× | +12,3664 s; 92,27 % |
| SQLite async | 1,336× | +11,8492 s; A2 no disponible |

Las sumas ponderan cada tarea tal como está construida, no la frecuencia de uso
de una aplicación. No prueban que Search consuma el 92 % de una aplicación.
Los informes originales guardan resúmenes de cinco repeticiones, sin sus cinco
muestras individuales: no permiten reconstruir intervalos de confianza. Los
hashes ANN verifican forma/cantidad y unicidad, no identidad completa ni recall.

### Disposición de los cuatro informes

| Informe, por orden de adjunto | Qué se retiene | Qué se corrige o no se acepta |
| --- | --- | --- |
| I1: materialización, BSON y causas comunes | Reutilización textual, constructor único, tamaño exacto; separación entre llamadas y preparación efectiva | Los conteos no atribuyen por sí solos porcentajes de CPU; la elegibilidad de la caché necesita prueba |
| I2: estudio por bloques | Prioridad del caso Search y la idea general de quitar trabajo repetido | Mezcla el artefacto de b83789bc con el wheel 5.0; sus tiempos absolutos no coinciden con la tanda válida; propone heaps ya existentes y una unión `should` insegura; los objetivos 0,55 s/1,30×/1,15× carecen de justificación |
| I3: perfiles y ablación | Coste BSON, recompilación Memory, materialización Search y contraejemplos de presencia | Dos entradas al método async de opciones no son dos trabajos de executor. La magnitud async de las ablaciones no está medida |
| I4: puente sync y frontera | Tránsitos y configuración SDK merecen estudio; no truncar candidatos | No son tres loops nuevos por find: el Runner mantiene un loop estable. Sus microbancos no equivalen a las tareas originales; su sonda Search de unos 230 s excedió el límite declarado y no acredita el protocolo de coste |

### Hallazgos demostrados y sus límites

1. **BSON por documento.** `ResponseBatchReader` mide antes de los decoders,
   conserva un documento pendiente y delimita respuestas/10334. Es obligatorio.
   `bson_value_size` visita familias genéricas antes de tipos nativos habituales.
   El workload devuelve 187 documentos por consulta materializada, 37.400 en
   200 consultas, no 33.200 ni 125 por consulta. El perfil contiene 1.957 visitas
   a valores por consulta; no son 1.957 documentos distintos.
2. **Filtro residual.** Memory llama a `QueryEngine.match`, que compila cada vez,
   desde los bucles de Search/vector. El perfil de diez consultas textuales con
   filtro posterior muestra 10.000 llamadas al predicado y 10.120 a
   `compile_filter`. SQLite ya tiene `_operation_filter_predicate`, perezoso y
   local, en algunas rutas vectoriales, pero su fallback textual todavía usa
   llamadas sin plan preparado. No hace falta una caché global ni otro matcher.
3. **SQLite Search.** El fallback reconstruye y tokeniza el contenido por
   candidato. El perfil de diez consultas contiene 10.000 materializaciones.
   Existe una caché de entradas/materialización por namespace, nombre físico y
   versión del backend, utilizada en otras rutas. Su constructor duplica el de
   `core/search.py`. El fallback **ya pasa la misma materialización** a matching
   y ranking; sí se repiten evaluaciones de cláusulas. No son el mismo problema.
4. **Opciones SQLite.** En el perfil de 1.000 lookups hay 1.000 aperturas y 1.000
   `_collection_options_sync`, aunque el método async aparece 2.000 veces por
   suspensión/reanudación. La sonda propia confirma una resolución y una apertura
   en cada lectura observada. El lookup caliente SQLite trazado ejecuta siete
   sentencias, incluidos control de versión y savepoint. No acredita lecturas
   redundantes ni que cualquier once sentencias observadas sean eliminables.
5. **Sync.** La sonda distingue `list(find.limit(1))`: lectura y `aclose`, dos
   tránsitos; `to_list()` sobre ese shape: uno; `list(aggregate(...))`: inicializar,
   respuesta, respuesta terminal y cierre, cuatro. Son shapes concretos; otros
   agregados/perfiles incluyen más llamadas. No se extrapola un coste fijo por
   tránsito ni se exige uno por toda agregación: el comando y su error temprano
   pueden necesitar ejecución antes de devolver el cursor.
6. **Configuración BSON.** `_encode_bson_document` construye el mapa de las
   **cinco** representaciones UUID, incluida UNSPECIFIED, y `DriverCodecOptions`
   en cada llamada. Esa configuración privada es inmutable y finita. El
   roundtrip, la admisión y la lectura raw BSON siguen siendo obligatorios.
7. **Trabajo existente.** `sort_documents_window` ya usa heap; varias rutas de
   ranking SQLite también. `prepare_pipeline` devuelve el mismo preparado si
   coinciden dialecto/contexto/registro. El explain indexed de `sort_limit` se
   toma antes de crear el índice: se corregirá el diagnóstico, sin invalidar por
   ello todos los tiempos. No hay evidencia para introducir otra caché de planes.
8. **Propiedad.** `RuntimeDocumentState` copia al derivar metadata, pero `frozen`
   no hace inmutable su `dict`. El [ADR-011](architecture/decisions/ADR-011-provenance-runtime-fuera-del-documento-bson.md)
   exige ownership defensivo. Quitar copias requiere una decisión distinta del
   ahorro de tokenización o de dispatch de tamaño.

### Contraejemplo a la poda propuesta

Con `should=[text, exists(a)]`, mínimo 1, un documento `{a: null}` puede coincidir
sólo por `exists` y carecer de posting. La unión de las listas conocidas lo
eliminaría. El evaluador posterior no puede recuperar lo descartado.

El plan actual ya exige al menos `max(0, mínimo - cláusulas_desconocidas)` aciertos
en listas conocidas. En el contraejemplo ese número es cero y no se poda; con
mínimo 2 es uno y sí se usa la lista conocida. La sonda comprueba ambos casos.
Tener un superconjunto seguro tampoco basta para ranking/top-k exacto: faltan
scores y empates de los candidatos no evaluados. No se elimina ese guard.
Un posting puede servir como evidencia positiva sólo tras acreditar path,
versión y mantenimiento; su ausencia nunca demuestra ausencia del campo.

## Incremento recomendado, por bloques

Cada bloque termina con diff revisado, evidencia focal y commit semántico.
«Implementado y comprobado focalmente» no significa «aceptado en la entrega».
Se agrupan verificaciones después de un cambio coherente, no tras cada edición.
No se añaden runners, almacenes, jerarquías genéricas ni interfaces públicas.

| Bloque | Prioridad | Depende de | Resultado exigible |
| --- | --- | --- | --- |
| Q00: base y diagnóstico del harness | P1 | Ninguno | Identidades y selección verificadas; explain fiel al índice real |
| Q01: tamaño BSON nativo | P1 | Q00 | Menos dispatch genérico, mismo tamaño y errores |
| Q02: predicado residual por operación | P1 | Q00 | Preparación perezosa reutilizada, mismo evaluador semántico |
| Q03: configuración SDK finita | P2 | Q00 | Configuración construida una vez, admisión BSON conservada |
| Q04: materialización Search única y reutilizada | P1 | Q02 | Constructor único y ausencia de retokenización caliente elegible |
| Q05: tránsitos de cierre sin trabajo | P2, condicionado | Q00; medir junto al resto | Reducir sólo tránsitos vacíos demostrados o documentar descarte sin cambio |
| Q06: aceptación del nuevo candidato | Obligatorio | Disposición de Q00–Q05 | Artefacto nuevo probado y coste caracterizado con límites explícitos |

Orden de ejecución: **Q00 → Q01 → Q02 → Q03 → Q04 → Q05 → Q06**.
Q01/Q02/Q03 son independientes funcionalmente. No exige ejecuciones paralelas:
la secuencia permite atribución y evita cargas propias durante mediciones.

### Q00 — Fijar la base y corregir el explain del harness

Congelar cc2d07d7 y su wheel, comprobar intérprete, dependencias constrained,
USearch >=2.26.4, SDK, imports, GC, hashes de harness/dataset y archivo de salida.
Preservar los datos inválidos anteriores como diagnóstico, separados.
Crear el índice antes de obtener el explain indexed de `sort_limit`; obtener el
unindexed en su colección sin índice. No afirmar uso físico de índice Memory
por la etiqueta `scan`: comprobar la selección de candidatos efectiva.

Prueba focal: el adaptador de benchmark observa el índice creado al pedir el
explain indexed; los hashes del resultado medido no cambian. Usar los tests de
contratos de benchmarks existentes. Si cambia el hash del harness, aplicar ese
mismo harness nuevo a ambos sujetos; no comparar planes de harness distintos.
**Cierre:** comandos/selección válidos y diagnóstico veraz; no se exige una
mejora de tiempo por corregir metadata.

### Q01 — Abaratar la autoridad existente de tamaño

Atajos por tipo **exacto** para los nativos frecuentes en `core/bson_size.py`.
Subclases, SON, Code, Int64, Binary y wrappers internos conservan el fallback.
Preservar UTF-8, NUL en claves/cstrings, rangos int32/int64, overflow y todos los
consumidores de la función. La clasificación numérica sigue teniendo owner en
`bson_scalars`: reutilizarla o extraer allí una primitiva local compartida si
hace falta, sin duplicar reglas numéricas en un segundo clasificador.

No aproximar tamaños, dimensionar sólo cerca del límite, usar la longitud del
JSON ni sustituir por serializar/copiar documentos con el SDK en producción.

Pruebas focales y diferenciales:

- Ampliar `tests/property/test_bson_size.py`: bytes del encoder PyMongo como
  oráculo independiente, vistas pública/interna/almacenada, Unicode, claves y
  enteros frontera. Casos inválidos y subclases conservan el error anterior;
  el helper interno no amplía su admisión sólo porque el SDK admita otro tipo.
- Replay `semantic_bson_size` y respuestas frontier/command/aggregation/raw/
  transform de MongoDB 7/8/9, más `test_response_boundaries.py`. Mantener tamaño
  exacto, lotes, pendiente, 10334, `batch_size`, callbacks y fase del decoder.

**Cierre:** mismas observaciones, menos visitas a helpers genéricos para los
nativos elegibles; sin documentos copiados ni codificados adicionales. Tiempos
de `cursor_consumption` y lookup se caracterizan al final, en los cuatro motores.

### Q02 — Compilar el residual una vez, sin una caché nueva

Generalizar mínimamente el patrón perezoso SQLite existente a un helper interno
en el owner de filtrado. Recibe dialecto/collation/variables explícitos, usa
`compile_filter` y `QueryEngine.match_plan`; el core no importa `OperationContext`
de la API. Memory y los fallbacks SQLite lo consumen. Eliminar el helper local
sustituido, sin crear una segunda autoridad de matching.

Una instancia por filtro y operación; query-filter y downstream-filter conservan
sus dominios/fases separados. Compilar al primer documento que antes lo
necesitaba, nunca anticipar el fallo en cero candidatos ni evaluar filtros que
el plan declara exactos y no evaluaba. No cachear por igualdad, ID global,
namespace ni proceso. No modificar las garantías de snapshots de argumentos.

Pruebas: Search textual/near/compound y residual vectorial, filtros válidos e
inválidos con cero/uno/varios candidatos, collation, `let`, reloj por operación,
llamadas sucesivas con distintos contextos y error posterior. Contrastar con
`QueryEngine` y capturas nativas MQL de `semantic_search_eligibility`, además del
oráculo Search completo independiente. Cubrir ejecución y explain cuando este
ejecuta trabajo, más `test_search_candidate_context.py`.

**Cierre:** cero compilaciones si no se invoca el residual y una preparación
raíz por residual realmente usado; no exigir que desaparezca toda llamada
recursiva del compilador. Mismos matches, errores/fases, candidatos y metadata
semántica. Ningún plan se comparte entre operaciones.

### Q03 — Precalcular sólo configuración SDK inmutable

Mover a ámbito privado de módulo la correspondencia de las cinco representaciones
y sus `DriverCodecOptions`, únicamente cuando el SDK existe. No cachear opciones
arbitrarias del usuario, peticiones ni documentos. Mantener la ruta sin SDK,
type registries/decoders y los errores de UNSPECIFIED.

Pruebas: frontera BSON de entrada y UUID, las cinco representaciones contra
bytes PyMongo, DBRef/undefined raw, objetos inválidos y orden de claves, además
de imports mínimos sin BSON/SDK. No eliminar encode/decode ni reutilizar un
contexto de ejecución por este motivo.

**Cierre:** mismo roundtrip y errores; desaparece la construcción por petición
del mapa/configuración finita. Es una simplificación aceptable sin atribuirle
los 38 µs íntegros que un informe asigna a toda la frontera BSON.

### Q04 — Una materialización textual, reutilizada donde es válida

Dos commits revisables dentro del bloque, con verificaciones agrupadas:

1. Una función pura en `core/search.py` construye `MaterializedSearchDocument`
   desde entradas. La materialización desde documento y el loader SQLite la
   reutilizan; retirar el constructor duplicado del engine.
2. Usar el loader/caché existente también en el fallback textual **elegible**.
   Comprobar que las entradas representan el mismo documento y definición bajo
   la misma vista. Validar nombre físico, versión del backend, cambios de
   definición/recreación del índice, sesión y snapshot. Si esa identidad no
   basta, conservar evaluación documental en ese dominio; no ampliar la caché
   con claves ad hoc. El documento original sigue gobernando `exists` y MQL.

No cambia la selección de candidatos, la exactitud declarada ni la poda top-k.
No crea un índice persistente de presencia. La caché fría puede construir una
vez por versión; la caliente elegible no retokeniza contenido sin cambios.
No se confunden los tokens de la consulta con retokenización del documento.

Pruebas: comparar entradas y todos sus contadores/campos con expectativas
literales y con el artefacto congelado, no sólo dos rutas que compartan el nuevo
constructor. Incluir mappings dynamic/explicit, paths/arrays, Unicode,
analizadores soportados, null, ausente, vacíos y valores mixtos. Diferencial
optimizador/oráculo completo Search para resultados, scores, highlights, count,
facets, empates/orden y límites. Warm/cold, insert/update/delete, rollback,
commit, restore/reopen, reemplazo de índice y otra conexión que modifica el
fichero deben evitar materialización obsoleta. Mantener deadlines y fallbacks.

**Cierre:** un constructor propietario; en la consulta caliente elegible,
ninguna tokenización de contenido por documento sin cambios; resultados y
observables equivalentes. No se exige bajar candidatos a O(limit), ni se promete
recuperar los 11,64×: aún puede ser necesario leer 1.000 documentos por presencia.
Los campos de explain que describen una estrategia física pueden cambiar sólo
si son veraces; su schema y los flags de exactitud no se falsean para comparar.

### Q05 — Evitar tránsitos vacíos, conservando el dueño del cierre

Examinar los puntos de EOF/cierre de find y aggregate junto con sus owners async.
Reutilizar los mecanismos existentes de cierre idle, EOF conocido y retired
sources. No fundir todo el ciclo en una superclase de cursor ni mantener un
segundo protocolo de estado. No adelantar inicialización de find, retrasar el
comando aggregate, agrandar lotes ni consumir documentos antes de tiempo.

Pruebas: `test_read_lifecycle.py`, cursores/respuestas/ownership en sync/async,
replay nativo 7/8/9; EOF, cero resultados, cancelación, close que falla/reintenta,
fuentes retiradas, iteración parcial, mezcla next/first/to_list, rewind/clone,
sesión implícita y custom providers SPI. Una falla de cleanup que hoy aparece
tras entregar un documento no puede adelantarse y ocultar ese documento.

**Cierre por implementación:** menos tránsitos **vacíos** en shapes acreditados,
conservando documentos entregados, eventos, errores y momento de cleanup.
No exigir un único tránsito para toda operación. **Cierre por descarte:** si el
ahorro exige ownership nuevo, diferirlo con el contraejemplo y retirar el bloque
del workset; esa disposición no acredita una mejora. Se toma una decisión,
no se abre una investigación indefinida antes de la release.

## Propuestas reservadas: no forman parte de la implementación inicial

| Idea | Valor posible | Evidencia necesaria para admitirla después |
| --- | --- | --- |
| Una evaluación compound para matching/ranking | Menos evaluaciones y una autoridad por cláusula | Perfil residual tras Q04; diseño local que conserva short-circuit, orden de errores y ranking, sin memo global; mismo oráculo completo |
| Presencia exacta/poda Search más potente | Puede ahorrar cargas de documentos | Prueba positiva de cobertura/transacciones/path y complemento residual, propiedad de conservación textual y ranking con scores/empates; el universo sin postings sigue necesitando resolución |
| Derivar metadata sin copiar documento | Menos copias en Search/vector y agregación | Contrato de ownership del ADR-011, ausencia de mutaciones compartidas en fan-out/joins/facets/snapshots/codec/persistencia, bytes/copias evitados medidos |
| Coalescer opciones y lectura SQLite | Menos cambios de executor, quizá coherencia de vista | Coste material residual; un trabajo realmente evitable y una integración compatible con resolución API/SPI de collation, sesiones, TTL, otra conexión y rollback |
| Mover el guard de undefined tras el fast path | Posible dispatch más barato | Perfil residual, prueba de que el camino nativo declina todos los undefined y sus combinaciones/dialectos; casos nativos de ordenación |

Estas ideas no se convierten en obligaciones pendientes de Q06. Cada una se
admite sólo mediante un cambio causal y acotado del alcance. No basta una
expectativa de ahorro. No se incorpora una tabla de presencia, caché general,
representación global inmutable, índice ordenado Memory ni cambio ANN ahora.

Se descartan: contexto singleton por defecto (`$$NOW`, `let`, sesión y collation
son de la operación); caché file-SQLite basada sólo en `total_changes`; tamaños
aproximados; copias superficiales de documentos públicos anidados; omitir outbox
sin contrato; restaurar generación de código/atajos semánticos; reducir
`numCandidates`; heap adicional donde ya existe; umbrales inventados frente a
4.11.0; y la banda A1–A2 presentada como criterio estadístico de aceptación.

## Q06 — Validación y presupuesto de evaluación

### Oráculos y selección

- BSON: bytes PyMongo, capturas Community Server 7/8/9 y fronteras de respuesta.
- Filtros/cursores: capturas MQL/lifecycle y observación de errores, callbacks,
  recursos, sesiones y resultados en ambas APIs y engines.
- Search textual: oráculo normativo interno `search-v1`, expectativas literales,
  propiedades/metamorfismos y ejecución completa del artefacto congelado.
  Community Server no es un oráculo de Atlas Search. El corpus nativo
  `semantic_search_eligibility` prueba MQL/prefiltros vectoriales; no acredita por
  sí solo `SearchExistsQuery` textual ni su ranking. Ampliar los tests textuales
  existentes si se necesitara poda textual; no reetiquetar aquel corpus.
- Vector: mismos parámetros, presupuesto de candidatos y backend. Corpus exacto
  controlado para IDs/scores/filtros; los hashes ANN de forma no acreditan recall.
  El incremento no toca algoritmo ni selección ANN, por lo que no se crea un
  gate de recall nuevo como sustituto de las pruebas de contexto/respuesta.

Reutilizar capturas aceptadas cuando el contrato no cambia; recapturar MongoDB
real sólo para una observación exigida nueva o una discrepancia no resuelta.
Seleccionar primero con collect-only los nodos complejos del archivo de replay;
registrar selectores, número de casos y skips, evitando falsos verdes vacíos.
No se copian como comandos los nombres de tests inexistentes de los informes.

### Trabajo durante bloques

Sólo pruebas focales y conteos deterministas tras el diff coherente. Para resolver
una hipótesis pendiente, máximo **una sonda por bloque, 60 s**, GC activo, sin
cargas propias simultáneas; presupuesto conjunto **300 s** para Q01–Q05.
Una sonda fallida consume presupuesto; no se reinicia por no favorecer la idea.
No repetir ablaciones que omiten garantías. Sin necesidad causal, no medir en
cada bloque: llevar los tiempos a la evaluación integrada.

La revisión actual consumió una única sonda de seis lecturas en menos de un
segundo; no pertenece al futuro gate ni inicia un intento de medición temporal.

### Candidato final y gates afectados

Congelar el runtime optimizado y construir/verificar una nueva pareja wheel/sdist
reproducible con los mecanismos existentes. El wheel 2ed1bd93 sigue acreditando
la base anterior; nunca se atribuyen sus gates al artefacto nuevo.

| Evidencia anterior | Qué exige el incremento final |
| --- | --- |
| Contratos, capturas y cierres históricos | Se conservan; no recaptura/auditoría general |
| R1 narrativa/migración | Revisar que sólo se afirman optimizaciones comprobadas; no nuevo contrato |
| R2 artefacto reproducible | Repetir sobre los bytes nuevos, hashes/imports reales y fuentes/recursos |
| R3 tiempos | Comparación nueva frente a cc2d07d7; los resultados antiguos son contexto |
| R4 globales | Una ejecución final exigida por política: pytest/unittest 3.13/3.14/3.15, cobertura >=99 % en 3.14, deep y matriz de doce celdas |
| R5 consumidores | Gates de typing/SPI/conformance/smokes por perfiles requeridos; UUID/BSON sin SDK/PCRE2 y canario sustitutivo conservado contra el wheel nuevo |
| R6 revisión/integración | Diff y documentación alineados, commits semánticos, árbol limpio; integración al cerrar, sin tags/publicación |

No reconstruir o ejecutar globales tras cada bloque. Un fallo final invalida sólo
la evidencia afectada por su corrección, salvo requisito explícito de ejecución
global del candidato final. Las tareas compatibles se agrupan; los tiempos se
miden cuando hayan acabado tests, builds y otras cargas propias.

### Protocolo temporal propuesto, distinto de F11

Un solo intento sobre candidato final. Presupuesto **1.800 s de pared** para la
campaña completa, máximo **180 s por invocación**, sin reposición automática.
Dos sujetos: A=cc2d07d7 congelado, B=runtime optimizado. Mismo harness posterior a
Q00 para ambos, dataset/seed 1000, warmup 1, **repetitions 1**. Conservar cada
JSON v2 por separado: así hay observaciones individuales usando el runner
existente, sin otro schema/runner ni reconstruir muestras desde medianas.

En cada motor, en orden fijo memory-sync, memory-async, sqlite-sync, sqlite-async:
**A, A (control), A, B, B, A (contraste ABBA)**. Repetir los seis workloads
históricos, 26 tareas/motor: lookup, cursor, simple aggregation, sort-limit,
Search y vector. Máximo 24 informes; no una familia de intentos por cada una de
las 104 tareas. Una invocación agotada queda incompleta; conservarla y continuar
sólo mientras quede presupuesto. Nada permite completar A2 histórico ni F11.

Registrar wall, CPU user/sys y RSS disponibles, hashes y trabajo, configuración,
versiones, orden y tiempos totales. Mantener todos los datos, incluidos fallos;
los resúmenes redondeados del harness limitan especialmente tareas pequeñas.
Separar los conteos de caché fría/caliente de la medición normal con warmup.
No derivar porcentajes de cProfile ni sumar sus tiempos acumulados superpuestos.

Disposición, fijada antes de medir:

- **Aceptación funcional/estructural:** oráculos verdes, fronteras conservadas y
  el trabajo objetivo realmente eliminado; una simplificación debe retirar una
  duplicación concreta y no añadir estados/cachés sin necesidad.
- **Mejora temporal observada:** las dos observaciones B son más rápidas que las
  cuatro A para la tarea, con identidades/oráculos iguales y sin una incidencia de
  entorno registrada que invalide el contraste. Es evidencia descriptiva del
  entorno, no intervalo de confianza ni garantía de
  un porcentaje universal. Informar además las diferencias absolutas y controles.
- **Empeoramiento observado:** las dos B son más lentas que las cuatro A. Revisar
  causalmente la tarea afectada antes de aceptar esa optimización; no compensar
  un empeoramiento ocultándolo en la mediana de los 104 cocientes.
- **Fallo obligatorio:** cualquier divergencia contractual, oráculo rojo o
  evidencia funcional exigida ausente bloquea la aceptación; no se clasifica
  como ruido ni se compensa con tiempos menores.
- **Inconcluyente en tiempo:** rangos solapados, control con diferencias de
  equivalencia o incidencias de entorno, resolución insuficiente, timeout o
  presupuesto agotado. No convertir
  «sin regresión confirmada» en aceptación de rendimiento. Una simplificación
  puede aceptarse por mantenimiento y trabajo reducido con tiempo inconcluyente,
  si no se afirma una mejora temporal ni queda una discrepancia obligatoria.

La amplitud de los cuatro A se informa por tarea; no se impone al control A/A
un porcentaje máximo de ruido ni se convierte su rango en intervalo de confianza.
No hay umbral global nuevo de +5 %, ni objetivo 0,55 s, ni calibración indefinida.
Si hace falta otro intento, proponer la tarea mínima, causa, coste y criterio de
resolución; requiere autorización adicional. Una corrección material delimita
qué evidencia deja de ser válida, pero no repone automáticamente el presupuesto.
Los efectos temporales pequeños que el diseño no distingue se reconocen así.

**Cierre de Q06:** todos los gates reales del artefacto final comprobados,
disposición de Q05 explícita y cada medición clasificada con sus límites. Lista
breve de garantías verificadas y seguimientos reservados; ningún hallazgo nuevo
amplía el trabajo salvo regresión del incremento o garantía obligatoria rota.
No declarar una optimización temporal cerrada con evidencia inconcluyente ni
publicar/etiquetar la release como consecuencia automática de este plan.

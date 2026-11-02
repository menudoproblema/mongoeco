# Optimización previa a 5.0.0: análisis y plan acotado

Estado: entrega local aceptada e integrada en main, 3 de noviembre de 2026.
Q06 conserva dos tradeoffs temporales autorizados y treinta inconcluyentes.
Este documento posee las decisiones y criterios vigentes para este incremento; la
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

La implementación usa rama propia. La base de coste será el candidato
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

## Simplificaciones de esta revisión

- Q04 no depende funcionalmente de Q02: constructor/caché textual y residual
  MQL resuelven trabajos distintos. Se conserva el orden propuesto para revisar
  cambios pequeños, sin convertirlo en una dependencia artificial.
- Q01 mantiene intacta la clasificación de enteros de `bson_scalars`; Q02 usa
  una función/cierre en el módulo existente de filtrado; Q03 usa una tabla finita,
  sin LRU ni factory. Ningún bloque crea una autoridad, protocolo o capa nuevos.
- Q04 se limita inicialmente a documentos originales del namespace y una vista
  corriente sin sesión. Los demás dominios conservan el fallback. Ampliar la
  caché a transacciones/snapshots no es requisito para optimizar el caso habitual.
- Q05 es una decisión acotada, no una obligación de cambiar el lifecycle. Si la
  reducción exige otro estado/owner o desplaza errores, se difiere sin código.
- Cada bloque tiene una verificación agrupada y un commit coherente por defecto;
  no se exigen dos commits ni dos tandas sólo por separar extracción y consumo.
- El control A/A y ABBA comparten una observación: `A0,A1,B1,B2,A2` sustituye
  `A,A,A,B,B,A`. Son veinte invocaciones máximas en vez de veinticuatro, con ambos
  contrastes presentes. El control compartido no es una calibración independiente.

## Incremento recomendado, por bloques

Cada bloque termina con diff revisado, evidencia focal y commit semántico.
«Implementado y comprobado focalmente» no significa «aceptado en la entrega».
Se agrupan verificaciones después de un cambio coherente, no tras cada edición.
No se añaden runners, almacenes, jerarquías genéricas ni interfaces públicas.
El único registro de avance es este plan y su evidencia existente: por bloque,
revisión/diff, selección focal, resultado/oráculo y contador objetivo antes/después.
No se crean fichas o documentos adicionales por criterio.

**Condición común de cierre focal:** todos los casos exigidos del bloque verdes
(o un skip permitido y justificado por el contrato, nunca por falta de setup),
sin cambio en observables salvo metadata física veraz declarada, y sin dependencia
productiva de los contadores de prueba. Se comparan valores/tipos BSON, orden,
metadata semántica, errores/fases y recursos con los oráculos ya establecidos;
no se introduce un normalizador que borre diferencias. El diff debe retirar
trabajo o duplicación concretos sin añadir otra autoridad semántica. Las
comprobaciones globales se reservan a Q06. Sólo Q05 permite descarte sin cambio
como disposición prevista; los objetivos incumplidos de Q01–Q04 siguen abiertos.

| Bloque | Prioridad | Depende de | Resultado exigible |
| --- | --- | --- | --- |
| Q00: base y diagnóstico del harness | P1 | Ninguno | Identidades y selección verificadas; explain fiel al índice real |
| Q01: tamaño BSON nativo | P1 | Q00 | Menos dispatch genérico, mismo tamaño y errores |
| Q02: predicado residual por operación | P1 | Q00 | Preparación perezosa reutilizada, mismo evaluador semántico |
| Q03: configuración SDK finita | P2 | Q00 | Configuración construida una vez, admisión BSON conservada |
| Q04: materialización Search única y reutilizada | P1 | Q00 | Constructor único y ausencia de retokenización caliente elegible |
| Q05: tránsitos de cierre sin trabajo | P2, condicionado | Q00; medir junto al resto | Reducir sólo tránsitos vacíos demostrados o documentar descarte sin cambio |
| Q06: aceptación del nuevo candidato | Obligatorio | Disposición de Q00–Q05 | Artefacto nuevo probado y coste caracterizado con límites explícitos |

Orden de ejecución: **Q00 → Q01 → Q02 → Q03 → Q04 → Q05 → Q06**.
Q01–Q04 son independientes funcionalmente después de Q00. No exige ejecuciones
paralelas: la secuencia permite atribuir cambios de trabajo por diff/conteos.
La medición final sólo atribuye tiempos al incremento conjunto, no un porcentaje
a cada commit.

### Q00 — Fijar la base y corregir el explain del harness

Congelar cc2d07d7 y su wheel, comprobar intérprete, dependencias constrained,
USearch 2.26.4, SDK, imports, GC, hashes de harness/dataset y archivo de salida.
Preservar los datos inválidos anteriores como diagnóstico, separados.
Crear el índice antes de obtener el explain indexed de `sort_limit`; obtener el
unindexed en su colección sin índice. No afirmar uso físico de índice Memory
por la etiqueta `scan`: comprobar la selección de candidatos efectiva.

Prueba focal: el adaptador de benchmark observa el índice creado al pedir el
explain indexed; los hashes del resultado medido no cambian. Usar los tests de
contratos de benchmarks existentes. Si cambia el hash del harness, aplicar ese
mismo harness nuevo a ambos sujetos; no comparar planes de harness distintos.
**Cierre:** `tests/unit/core/test_benchmark_harness.py` y el test de orden
create-index/explain verdes; mismos resultados medidos antes/después de la
corrección; inventario fijo de seis workloads y 26 tareas por motor derivado de
la tanda válida anterior. Preflight documentado, comandos válidos y destino nuevo
sin sobrescribir evidencia. No se exige una mejora de tiempo por corregir metadata.
Este inventario se fija antes de ver resultados B; un skip simétrico no lo reduce.

### Q01 — Abaratar la autoridad existente de tamaño

Atajos por tipo **exacto** para los nativos frecuentes en `core/bson_size.py`.
Subclases, SON, Code, Int64, Binary y wrappers internos conservan el fallback.
Preservar UTF-8, NUL en claves/cstrings, rangos int32/int64, overflow y todos los
consumidores de la función. El dispatch temprano se limita a `str`, `dict`,
`list`, `bool`, `None` y `float` exactos. Los enteros siguen pasando por
`bson_numeric_alias`; no se extrae otro helper numérico ni se duplican rangos.
Optimizar también ese clasificador queda fuera de Q01.

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

**Cierre:** propiedad de bytes y replays verdes en las vistas indicadas; cada
familia nativa optimizada evita el dispatch genérico anterior, mientras wrappers,
subclases e enteros mantienen su tratamiento. El contador focal distingue raíz,
recursión y tipos; no cuenta una visita como un documento. Cero copias o encodes
adicionales en producción, sin modificar `ResponseBatchReader`. Tiempos de
`cursor_consumption` y lookup se caracterizan al final, en los cuatro motores.

### Q02 — Compilar el residual una vez, sin una caché nueva

Generalizar mínimamente el patrón perezoso SQLite existente a una función que
devuelve un cierre, en el módulo existente de filtrado; sin clase, módulo nuevo
ni interfaz adicional. Recibe dialecto/collation/variables explícitos, usa
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
semántica. Ningún plan se comparte entre operaciones. Un caso con N candidatos
acredita N decisiones MQL y una preparación raíz; cero candidatos acredita cero
preparaciones. El filtro que falla conserva la misma primera observación de
error que la base. Se comprueban dos operaciones sucesivas con contextos distintos;
los contadores van en pruebas/sonda, no en una nueva instrumentación productiva.

### Q03 — Precalcular sólo configuración SDK inmutable

Mover a ámbito privado de módulo la correspondencia de las cinco representaciones
y sus cinco `DriverCodecOptions`, únicamente cuando el SDK existe. Es una tabla
privada completa construida al importar, sin estado de misses ni invalidación.
No cachear opciones arbitrarias del usuario, peticiones ni documentos. Mantener la ruta sin SDK,
type registries/decoders y los errores de UNSPECIFIED.

Pruebas: frontera BSON de entrada y UUID, las cinco representaciones contra
bytes PyMongo, DBRef/undefined raw, objetos inválidos y orden de claves, además
de imports mínimos sin BSON/SDK. No eliminar encode/decode ni reutilizar un
contexto de ejecución por este motivo.

**Cierre:** mismo roundtrip y errores; desaparece la construcción por petición
del mapa/configuración finita. El contador agrupa las cinco modalidades y
comprueba cero construcciones adicionales tras el import; los tests prueban
bytes/errores, no sólo identidad de objetos. Es una simplificación aceptable
sin atribuirle los 38 µs íntegros que un informe asigna a toda la frontera BSON.

### Q04 — Una materialización textual, reutilizada donde es válida

Un cambio coherente en dos pasos lógicos, verificado conjuntamente. Separar
commits sólo si la extracción es útil para revisar o revertir de forma independiente:

1. Una función pura en `core/search.py` construye `MaterializedSearchDocument`
   desde entradas. La materialización desde documento y el loader SQLite la
   reutilizan; retirar el constructor duplicado del engine.
2. Reutilizar el bucket existente también en el fallback textual **elegible**.
   El loader SQLite sigue usando el constructor común. Comprobar que las entradas
   representan el mismo documento y definición bajo
   la misma vista. Validar nombre físico, versión del backend, cambios de
   definición/recreación del índice, sesión y snapshot. Si esa identidad no
   basta, conservar evaluación documental en ese dominio; no ampliar la caché
   con claves ad hoc. La nueva reutilización se habilita sólo para documentos
   originales del namespace, vista corriente y `session is None`, con identidad
   backend/índice acreditada. Sesiones, snapshots o fuentes transformadas conservan
   fallback, sin ampliar el ámbito de cachés de otras rutas. El documento original
   sigue gobernando `exists` y MQL; una entrada textual vacía no prueba ausencia.

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
observables equivalentes. El testigo obligatorio es el compound candidateable
should del benchmark, repetido sobre el mismo engine: la segunda consulta sin
cambios tiene cero retokenizaciones de contenido. Después de una mutación válida
no se conserva la versión obsoleta y el resultado equivale al oráculo. La primera
consulta de la nueva versión puede reconstruir el bucket completo: no se exige
invalidación por documento ni un mecanismo nuevo. Un miss no resuelto usa fallback; no se confunde con una materialización vacía válida.
Los dominios excluidos prueban equivalencia por fallback y no tienen objetivo de
hit. Si ni el caso corriente puede acreditar identidad, la reutilización sigue
pendiente: el constructor único no cierra por sí solo Q04.
No se exige bajar candidatos a O(limit), ni se promete
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

La decisión reutiliza primero la traza aceptada y el mecanismo idle existente;
como máximo usa la sonda de 60 s prevista para Q05. Si implementa, al menos un
shape EOF elegible reduce un tránsito vacío respecto a su base; los demás shapes
controlados no añaden tránsitos ni trabajo físico. Debe pasar el test negativo
con cleanup que falla y conserva retry, y el de entrega del último documento.
Si exige un estado nuevo, mover cleanup entre respuestas o cambiar SPI, se
registra «diferido sin cambio», motivo y superficie afectada; no se fuerza un fix
para alcanzar un número de tránsitos.

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

### Revisión del plan antes de Q06

Esta revisión documental conserva los cierres focales Q00–Q04 y la disposición
Q05; no vuelve a implementar bloques ni inicia builds, suites o mediciones.
El runtime actual es b5d9e803, registrado después en d1241e31 sin cambios
productivos. La identidad completa del futuro artefacto se fijará en R2;
ninguno de estos commits acredita todavía la entrega Q06.

Se mantiene una sola unidad de entrega y un solo intento temporal. No se añaden
subgates, runners ni documentos: la tabla de ejecución conserva el estado y el
directorio de evidencia existente recibe los resultados. Los controles focales
acreditan trabajo eliminado; el contraste conjunto sólo acredita tiempos del
conjunto, sin exigir un benchmark por commit. Q05 diferido no bloquea Q06 ni
cuenta como ahorro comprobado.

La ejecución restante es secuencial: fijar artefacto reproducible, completar los
gates funcionales/consumidores R1–R6 aplicables, comprobar el preflight temporal,
ejecutar el único intento y registrar su disposición antes de integrar. R6
termina después de interpretar los tiempos. La duración de los gates funcionales
no consume los 1.800 s de campaña; no se ejecutan en paralelo con ella. No hay
promesa de que veinte procesos quepan en ese presupuesto: prevalece el límite
total y el resto se declara no ejecutado, sin ampliar automáticamente el intento.

### Trabajo durante bloques

Sólo pruebas focales y conteos deterministas tras el diff coherente. Para resolver
una hipótesis pendiente, máximo **una sonda por bloque, 60 s**, GC activo, sin
cargas propias simultáneas; presupuesto conjunto **300 s** para Q01–Q05.
Una sonda fallida consume presupuesto; no se reinicia por no favorecer la idea.
No repetir ablaciones que omiten garantías. Sin necesidad causal, no medir en
cada bloque: llevar los tiempos a la evaluación integrada.

El estudio inicial consumió una única sonda de seis lecturas en menos de un
segundo. Esta revisión del plan reutiliza esa evidencia, sin otra sonda ni
medición; ninguna inicia el futuro intento temporal.

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

### Protocolo temporal revisado, distinto de F11

Este apartado sustituye la secuencia anterior de seis invocaciones por motor.
No se inicia ninguna medición al revisar/aprobar el plan.

**Preflight, antes del intento:**

1. Q00–Q05 tienen disposición y el runtime B está congelado. Las pruebas
   funcionales finales y builds han terminado; no hay otra carga propia en marcha.
2. A corresponde al wheel 2ed1bd93 y cc2d07d7; B al wheel nuevo con su SHA-256 y
   revisión. Verificar todos los archivos empaquetados de los sujetos importados,
   no sólo `__version__`. El harness posterior a Q00 es idéntico para ambos.
3. Un mismo intérprete CPython 3.14.5 y entorno constrained para todas las
   invocaciones: PyMongo 4.19.0 y USearch 2.26.4. Comprobar `pip check`, inventario
   de dependencias completo, paths e imports antes de lanzar. Si se reconstruye
   el entorno, usar las versiones registradas en la preparación, sin actualizarlas
   durante la campaña. Mismo host/SQLite/SDK/RSS sampler para A y B.
4. Fijar `MONGOECO_JSON_BACKEND=stdlib`, `MONGOECO_REGEX_BACKEND=python` y GC
   activo, sin profiler, monkeypatches ni variables de cobertura. Mantener la
   política de ejecución de la preparación; no añadir afinidad o GC-off para
   favorecer un sujeto. Los perfiles opcionales conservan sus tests funcionales.
5. Verificar los argumentos de la CLI existente por lectura/`--help`, las rutas
   de sujeto y salida, y el inventario Q00. Usar un destino nuevo bajo `.tmp/`
   identificado por candidato. No escribir `benchmarks/execution.json` ni
   sobrescribir informes anteriores. Un fallo aquí se corrige antes del intento.

**Comando de una invocación**, lanzado desde el checkout del harness fijado:

```sh
MONGOECO_JSON_BACKEND=stdlib MONGOECO_REGEX_BACKEND=python \
  "$PYTHON" -m benchmarks.report \
  --subject-root "$SUBJECT_ROOT" --engine "$ENGINE" \
  --size 1000 --warmup 1 --repetitions 1 \
  --workload secondary_lookup_indexed --workload cursor_consumption \
  --workload simple_aggregation --workload sort_limit \
  --workload search_diagnostics --workload vector_search_diagnostics \
  --output-json "$OUT/$ENGINE-$SLOT.json" \
  --output-markdown "$OUT/$ENGINE-$SLOT.md"
```

Las variables se resuelven y registran en el preflight, no se dejan al azar en
un shell anterior. `SUBJECT_ROOT` es un árbol congelado que contiene `src/mongoeco`,
como exige `_subject.py`, con bytes contrastados contra el wheel correspondiente.
Cada invocación es un proceso nuevo. El paquete medido procede de ese sujeto y
el harness del checkout común; no alternar instalaciones ni reutilizar un proceso
que ya haya importado el otro sujeto. Si el árbol exportado no tiene `.git`,
registrar la revisión y el SHA del wheel en la evidencia existente: no inventar
`gitRevision` ni atribuir al sujeto el HEAD del harness.

No pasar `--max-wall-regression-percent` ni introducir un umbral CLI. Validar y
comparar después los JSON mediante `validate_report`/`compare_reports`, sin
umbral temporal. No modificar el runner ni su schema.

**Tamaño y estado de la carga:** `size=1000`, semillas actuales del harness sin
cambios (usuarios: 42), seis workloads, 26 tareas/motor y cero workloads omitidos.
La única repetición da una observación individual por tarea en cada JSON v2;
no se reconstruyen muestras desde medias ni se aumenta la repetición tras ver B.
`warmup=1` se conserva para ambos sujetos, pero los workloads hacen setup/teardown
entre ejecuciones: no garantiza reutilizar la caché del warmup. Se mide la carga
actual, incluidas sus consultas/explain de preparación según el harness. La
propiedad fría/caliente de Q04 se prueba aparte, con consultas consecutivas en
el mismo engine. No requiere un segundo benchmark ni más knobs de warmup.

Los tiempos por tarea abarcan el callback que ya delimita el harness, no todos
los imports/setup/índices. La duración externa de cada proceso sirve para el
presupuesto; no se presenta como latencia de cada consulta ni mejora end-to-end.

**Orden, unidad y límites, fijados antes de ejecutar:**

- Orden de motores: memory-sync, memory-async, sqlite-sync, sqlite-async.
- En cada motor: **A0, A1, B1, B2, A2**. A/A = A0–A1; ABBA = A1–B1–B2–A2.
  A1 se reutiliza: el control no es independiente del contraste. No se promete
  independencia entre invocaciones ni entre las 26 tareas del mismo proceso.
- Máximo **20 invocaciones**, un solo intento. **1.800 s de pared** desde el
  lanzamiento de A0 del primer motor, incluyendo warmups, setup, controles,
  comprobaciones intermedias y cleanup; máximo **180 s por proceso**. El límite
  efectivo de cada proceso es el menor entre 180 s y el saldo de campaña.
- Ejecución secuencial, sin tests/builds/perfiles propios concurrentes. Usar los
  límites externos y la recogida terminal existentes; no un runner adicional.
  El proceso/sus recursos propios deben quedar terminados antes del siguiente.
- Tras cada invocación, conservar stdout/stderr, JSON/Markdown disponibles,
  duración monotónica externa, salida y configuración/identidades. Validar su
  inventario y oráculos antes de continuar. No usar una tarea truncada como muestra.
- **Primera salida no cero, timeout, equivalencia rota o presupuesto agotado:**
  detener nuevas invocaciones. Recoger/cerrar sólo recursos propios y conservar
  toda la evidencia parcial. No saltar a otro motor ni completar huecos con datos
  históricos. El resto queda «no ejecutado por parada del intento», sin convertirlo
  en éxito. No eliminar muestras por ruido, outlier o carga externa.

Si una incidencia de entorno se registra después de empezar, se conserva su
traza y se trata el contraste afectado como inconcluyente. No se inventa un
umbral de load average ni se añade un monitor; se registran las incidencias
conocidas y la amplitud A/A. La ausencia de incidencia registrada no demuestra
que el host estuviera libre de interferencias.

### Interpretación y criterios observables de Q06

Las observaciones son descriptivas. La unidad de repetición es la invocación;
las 104 tareas/motores no son 104 repeticiones independientes de una misma carga.
Por tarea completa se informan las cinco observaciones, rango A, diferencia
absoluta `mediana(B) - mediana(A)` y cociente `mediana(B) / mediana(A)` cuando
el denominador tiene resolución útil. CPU user/sys y RSS se conservan como
contexto; no son un gate adicional, ni se atribuyen porcentajes de cProfile.

El JSON redondea wall time a cuatro decimales. Fijar **q = 0,0001 s** como
margen de redondeo entre extremos: `A={a0,a1,a2}`, `B={b1,b2}`. Usar unidades
decimales; las diferencias iguales a q no bastan para separar observaciones.
Este margen cubre el redondeo, no el ruido estadístico. Valores cero no acreditan
coste cero ni un porcentaje de mejora.

| Disposición por tarea | Criterio observable | Consecuencia |
| --- | --- | --- |
| Mejora temporal observada | Cinco observaciones comparables y `max(B) + q < min(A)` | Afirmación limitada a esta carga/host; sin CI, p-valor o porcentaje universal |
| Empeoramiento observado | Cinco observaciones comparables y `min(B) > max(A) + q` | Discrepancia por resolver antes de aceptar el candidato; no se compensa con otra tarea |
| Inconcluyente en tiempo | Ninguna separación anterior, contraste incompleto o incidencia que invalida comparabilidad | No afirmar mejora/ausencia de regresión; no repetir automáticamente |
| Fallo funcional/estructural | Oráculo rojo, cambio contractual, falta de evidencia exigida, mayor trabajo incompatible con el objetivo | Bloquea aceptación, aunque el tiempo baje |

`compare_reports` debe devolver cero diferencias de configuración/oráculo/resultado
para comparaciones A/A y B/A. Comprobar además las 26 tareas esperadas y cero skips:
la validación permite skips simétricos, que aquí no satisfacen la cobertura fijada.
Para evitar comparaciones redundantes, en cada motor A0 es la referencia fija:
validar A0 y comparar A1, B1, B2 y A2 con A0 conforme llegan. La igualdad de
configuración y de oráculos/resultados con esa referencia cubre A/A y B/A sin
comparar todos los pares. Cada informe debe acreditar además el inventario Q00
exacto por workload, 26 tareas y cero skips; no basta contar 26 nombres en total.

`compare_reports` no compara toda la metadata: conserva explícitamente
`outcome_oracle`/`outcome_sha256` y los campos de configuración enumerados en
`benchmarks/contracts.py`. Los contratos de candidatos, exactitud, fases y
recursos se acreditan con las pruebas focales/globales exigidas y la revisión de
metadata física; un comparador verde no los sustituye. Conservar la metadata
íntegra, sin borrarla ni normalizarla para lograr equivalencia. No se amplía el
comparador genérico para este incremento.

Los hashes ANN no se convierten en prueba de recall. Las diferencias físicas de
plan admitidas por Q04 requieren el test específico, schema conservado y metadata
veraz; no se ignoran diferencias semánticas bajo esa excepción.

Informar también el rango A/A de A0–A1 como diferencia absoluta y cociente
cuando sea resoluble, sin convertirlo en un filtro de muestras o un gate de
calibración adicional. No se exige que A/A esté dentro de un porcentaje. La
separación por extremos es una descripción conservadora de estas observaciones,
no una prueba estadística de causalidad ni de ausencia de regresión poblacional.

Una parada por timeout/presupuesto produce «evaluación temporal incompleta»;
una parada por equivalencia rota produce además un fallo funcional obligatorio.
En ambos casos el intento está consumido, pero sus efectos de cierre son
distintos. Sólo cabe aceptación por mantenimiento con coste inconcluyente en
las condiciones que siguen: gates obligatorios comprobados y ninguna
discrepancia abierta. No se anuncia cobertura temporal completa de los cuatro
motores si faltan muestras, ni se usan muestras parciales para clasificarlas
como mejora o empeoramiento con la regla de cinco observaciones.

Una diferencia semántica no se reclasifica como ruido: tiene prioridad como fallo
obligatorio. Un empeoramiento temporal observado sigue abierto hasta resolver la
causa, retirar el cambio causante o autorizar expresamente el tradeoff; no basta
llamarlo ruido. Una tarea temporal inconcluyente puede acompañar un cambio
aceptado por mantenimiento/trabajo reducido, con esa etiqueta y sus límites,
si los gates obligatorios están verdes y no queda una discrepancia abierta.

**Cierre completo de Q06** exige simultáneamente:

1. Q00–Q04 implementados y comprobados focalmente, Q05 con disposición explícita,
   y revisión final sin nueva autoridad semántica, caché o protocolo innecesarios.
2. Gates de artefacto, globales y consumidores de la tabla R1–R6 comprobados sobre
   el nuevo runtime, usando la evidencia válida y los oráculos nativos vigentes.
   La selección de replay tiene casos efectivos; los golden no se regeneran para
   hacer coincidir la implementación. Ningún criterio funcional queda pendiente.
3. Intento temporal terminal y registrado: máximo veinte informes, cobertura
   completa o parada explícita con tareas afectadas/no ejecutadas. Cada tarea
   tiene una disposición; no quedan empeoramientos observados sin resolver.
   Un cierre por mantenimiento con coste inconcluyente no se anuncia como
   «optimización temporal verificada» ni «sin regresiones de rendimiento».
4. Diff y docs alineados, commits semánticos y limpieza de recursos/auxiliares
   propios. Conservar el archivo ajeno que ya estaba sin seguimiento; «árbol
   limpio» se refiere al workset propio, sin borrar trabajo preexistente.

No hay nuevo umbral de +5 %, objetivo 0,55 s ni calibración indefinida. Otro
intento necesita una propuesta focal con causa, coste y criterio de resolución,
y autorización adicional. Una corrección material invalida sólo evidencia
alcanzada por ella; no repone automáticamente presupuesto ni permite volver a
medir para seleccionar el resultado más favorable.

La entrega conserva las garantías previas y enumera las reservas fuera de
alcance. Ningún hallazgo nuevo amplía el trabajo salvo regresión del incremento
o incumplimiento obligatorio. El plan no autoriza tags ni publicación.

## Ejecución vigente

Rama de implementación `perf/q00-q06`, base 79337a11, integrada por fast-forward
en main hasta 5c4e0767 y retirada después. No se crearon worktrees auxiliares.
Se conserva `benchmarks/execution.json` preexistente, sin modificar.

| Bloque | Estado | Evidencia / siguiente gate |
| --- | --- | --- |
| Q00 | Aceptado en Q06 | 28 tests del harness; el nuevo test falla en la base y pasa tras mover el índice. Pip check y CLI conformes, 310 archivos del wheel y 26 tareas/motor verificados. [Registro](evidence/mongoeco-5.0.0/q00-preflight.json), logs comprimidos q00-* en el mismo directorio. Preflight del artefacto B completado en Q06 |
| Q01 | Aceptado en Q06 | 366 passed (300 casos seleccionados de replays BSON/respuestas 7/8/9, más propiedades y fronteras); Ruff y formato conformes. [Conteos](evidence/mongoeco-5.0.0/q01-counts.json): seis raíces nativas conservan tamaños y evitan seis llamadas al alias numérico; ninguna medición temporal. Logs q01-* en el mismo directorio |
| Q02 | Aceptado en Q06 | 30 tests unit/contexto y 234 replays verdes; lint ratchet conforme. [Conteos productivos](evidence/mongoeco-5.0.0/q02-counts.json): dos operaciones por engine, dos preparaciones raíz y veinte decisiones MQL. Los tests conservan preparación vacía/error perezosos, bindings, collation y relojes separados. Diff revisado, sin formatter ajeno; logs q02-* en el mismo directorio |
| Q03 | Aceptado en Q06 | 67 tests + 12 subtests y 99 replays UUID 7/8/9 verdes; lint conforme. [Conteos](evidence/mongoeco-5.0.0/q03-counts.json): cinco modalidades × dos requests, cero nuevas construcciones tras importar y bytes/tamaños equivalentes al SDK. Ruta sin SDK, rechazo UNSPECIFIED y raw BSON conservados; logs q03-* |
| Q04 | Aceptado en Q06 | [Registro](evidence/mongoeco-5.0.0/q04-counts.json): 285 casos únicos comprobados en tandas focales y 16 comparaciones del constructor congelado. Cero retokenizaciones calientes sobre 1.000 documentos con mappings explicit/dynamic; cambios externos, valores mixtos, commit/rollback, índice, reopen/backup restore conformes. Los doce mocks retirados se corrigieron en el addendum Q02; los otros 144 casos de esa tanda se reutilizan. Logs q04-* |
| Q05 | Diferido sin cambio; disposición aceptada | [Decisión](evidence/mongoeco-5.0.0/q05-decision.json): hook idle de cursor no equivale a cerrar iterador EOF; reusarlo rompe rewind. Requiere otro predicado/estado de lifecycle y tratamiento de retries/contexto, no una reutilización suficiente |
| Q06 | Aceptado e integrado con reservas | [Preflight](evidence/mongoeco-5.0.0/q06-preflight.json): builds wheel/sdist reproducibles de 95fa38c5, runtime b5d9e803; wheel 5ac014b1… y 310 archivos idénticos en instalaciones físicas 3.13/3.14/3.15. [Globales/cobertura](evidence/mongoeco-5.0.0/q06-globals.json) comprobados; [matriz 12/12](evidence/mongoeco-5.0.0/q06-r4.json) aceptada; [R5 consumidores](evidence/mongoeco-5.0.0/q06-consumers.json) aceptado, deep 74 y PCRE2 339 replays por perfil verdes. Intento temporal completo (20 invocaciones), R3 aceptado por tradeoff explícito del usuario; R1 y R6 conformes, integración y limpieza propias realizadas |

Q01 conserva la ruta genérica de subclases/wrappers y la clasificación de enteros.
El helper local de arrays evita duplicar la regla de framing entre lista nativa
y fallback. `ResponseBatchReader` no se modifica. El replay reutiliza las capturas
aceptadas; no se han recapturado ni reescrito golden. La sonda de conteos Q01
consume su única sonda focal (límite 60 s, GC activo); no es un intento temporal.
Q00 registra además los hashes efectivos del harness corregido y dataset.

Q02, control adicional durante Q04: la tanda ampliada encontró doce tests que
parcheaban el import privado SQLite del compilador retirado. Se adaptó su
observación a la autoridad compartida; siguen exigiendo una preparación raíz
por residual, reutilización con fallback exacto y los IDs del golden nativo.
Los doce casos pasan (q02-fallback.txt.gz). No hay alias de compatibilidad ni
cambio productivo adicional por esta adaptación de test.

Q04, simplificación aplicada: la versión local del backend no prueba ausencia de
escritores externos. Sin añadir otra clave, caché ni observer, el bucket existente
se consulta y sus entradas se comparan con la secuencia textual exacta extraída
del documento actual y su definición. Sólo si coinciden se reutilizan sus hechos
textuales; en caso contrario se reconstruyen con el constructor común. Se evita
retokenización, no la extracción de paths ni la lectura necesaria de documentos.
El loader FTS también usa ese constructor, sin cambiar sus otros dominios.

La comparación incluye orden/contenido de entradas, por lo que protege cambios
externos y de mappings sin confiar sólo en el contador local. No representa
presencia ni usa igualdad BSON relajada. Sesiones, transacciones, vistas no
acreditadas y backend ausente conservan el fallback. La función local no se
construye en la rama vectorial. Lint ratchet conforme; diff revisado sin cambios
funcionales de candidatos, scores, top-k o protocolos. No hay mediciones temporales.
La sonda única Q04 (16 comparaciones de campos con el constructor congelado)
usa GC activo y límite 60 s; fuente y resultado se conservan en q04-constructor-*.

Q05, disposición conforme al contrato: `AsyncCursor._close_nowait_if_idle` cierra
el cursor completo, mientras `_AsyncCursorIterator.close` conserva la posibilidad
de rewind y coordina fuentes/batches/retired sources. La sonda de tres lecturas
(límite 60 s, GC activo) conserva un documento tras rewind ordinario y devuelve
vacío si se intercala el hook de cursor. No es un reemplazo correcto del cierre
EOF. Saltar directamente el close duplicaría en sync las condiciones y estados
del owner async; avanzar el cleanup dentro de la respuesta podría adelantar un
fallo y ocultar el último documento. La vía inline existente del runner tampoco
concede una capacidad genérica de cierre: contexto, task factory y suspensión
siguen siendo condiciones del owner. Se difiere esta optimización, sin otro
estado/protocolo ni modificación productiva. No se acredita ahorro de tránsitos.

Q00–Q04 están aceptados en la entrega y Q05 tiene su disposición de diferir
aceptada. Q06 está cerrado con los límites documentados. El código productivo
permanece congelado desde b5d9e803; la evidencia reutilizada conserva sus inputs.


Q06, globales: 65.020 passed, 1.130 skips previstos y 14.946 subtests por
Python; unittest 3.685 casos y tres skips por perfil. El cálculo exacto detectó
98,99894 % de cobertura aunque pytest mostrara 99,00 redondeado. Se añadió un
contrato focal de bytearray: tamaño legado contra bytes SDK, rechazo público
contra la excepción SDK y entrada intacta, con tres payloads en cada Python.
No cambió el runtime ni se repitieron suites completas. La unión de esas trazas
con la ejecución global da 48.459/48.948 líneas, 99,0009806 %, en los mismos 289
módulos. Se preservan el informe anterior y el final, sin cambiar exclusiones,
umbral ni denominador. Deep conserva sus 74 casos aceptados; los tres controles
adicionales se registran aparte. La matriz terminal añade doce celdas verdes de 3.587 casos; R4 queda aceptado.


### Q06 — Resultado temporal y diagnóstico (historia del 3 de noviembre)

El único intento completó veinte invocaciones en 844,9208 s, dentro del límite
de 1.800 s y de 180 s por proceso. Cero skips y cero diferencias de equivalencia
contra A0 con el inventario de 104 tareas/motores intacto. Datos completos:
[análisis](evidence/mongoeco-5.0.0/q06-temporal-analysis.json),
[contexto de los empeoramientos](evidence/mongoeco-5.0.0/q06-temporal-worsening-context.json)
y [informes/logs](evidence/mongoeco-5.0.0/q06-temporal-logs.tar.gz).

| Motor | Mejora observada | Inconcluyente | Empeoramiento observado |
| --- | ---: | ---: | ---: |
| Memory sync | 17 | 9 | 0 |
| Memory async | 17 | 7 | 2 |
| SQLite sync | 21 | 5 | 0 |
| SQLite async | 17 | 9 | 0 |

Los dos empeoramientos son Memory async, Search compound: `msm2_topk_100`
(+5,82 % de mediana) y `tie_heavy_topk_100` (+6,26 %). Sus valores/resultados y
metadata completa coinciden entre los cinco informes. También crece CPU, por
lo que no se atribuyen sólo a esperas externas. No hay causa productiva o de
entorno demostrada; CPU no distingue por sí sola dispatch, GC o frecuencia del
host. Los controles sync equivalentes no presentan separación de empeoramiento.
Las 72 mejoras no compensan ni cierran por sí solas esos dos pendientes. En este
punto R3 seguía abierto; su disposición posterior figura al final.

El usuario autoriza una única sonda causal adicional, máximo 60 s y 44 consultas
con GC activo, sin reiniciar la campaña de aceptación ni modificar A/B. Se
reutilizan dataset/índice/pipelines del harness. Cuatro procesos (A/B por cada
shape), cada uno con una consulta fría y diez calientes. cProfile y estadísticas
GC aportan llamadas y contexto diagnóstico; sus tiempos instrumentados no son
muestras de aceptación. Se conserva el límite total incluyendo imports, setup y
cleanup, y se detiene ante el primer fallo/timeout. No se infiere causalidad de
una variación de tiempo del perfil sin mecanismo y evidencia de trabajo.

La sonda distingue trabajo añadido en dispatch BSON/configuración, preparación
de residuales sin filtro y materialización fría/caliente; GC queda como contexto
instrumentado, con sólo diez consultas calientes por proceso. Si no decide la
causa, se declara expresamente. Cualquier nueva campaña de aceptación requiere
propuesta y autorización adicionales; esta sonda no repone los intentos.


Resultado de la sonda causal autorizada: 44 consultas en 3,3717 s, GC activo,
cuatro procesos terminados sin errores y hashes equivalentes al benchmark.
[Registro](evidence/mongoeco-5.0.0/q06-causal-probe.json) y
[fuente, perfiles y logs](evidence/mongoeco-5.0.0/q06-causal-probe-logs.tar.gz).

En diez consultas calientes, A/B ejecutan las mismas 50.000 decisiones de
matching y 5.000 rankings para msm2; 27.500 y 2.500 para tie-heavy. Cero
materializaciones calientes en ambos; GC tiene los mismos deltas por generación
en consulta inicial y fase caliente. Las llamadas al alias numérico bajan de
2.060/2.080 a 500; la preparación raíz MQL es cero en estos shapes sin filtro.
B añade veinte llamadas a la fábrica de predicados (dos por consulta), pero
no hay evidencia de que ese trabajo explique los empeoramientos observados.

**No se ha identificado la causa.** Diez consultas instrumentadas no reproducen
las cien salidas retenidas ni el historial completo de workloads del intento
original. El perfil no corrige sus dos clasificaciones ni es otro gate de coste.
No se modifica el producto por una atribución especulativa y no se inicia otra
sonda con el saldo del límite anterior. En este punto R3 seguía pendiente de
resolución o aceptación explícita; no se integró antes de recibir esa disposición.


### Q06 — Aceptación expresa del coste y gate de integración

El 3 de noviembre el usuario autorizó: «Aceptar expresamente el tradeoff y
completar los gates restantes (recomendado)». La autorización alcanza los dos
casos Memory async descritos (+5,82 % y +6,26 % de mediana), manteniendo los 30
inconcluyentes. **R3 queda aceptado con esas reservas**, sin cambiar muestras,
umbrales, clasificaciones ni afirmar causa conocida, significación estadística
o ausencia de regresiones. No hubo otro intento de aceptación.

R1 conforme: no cambian contratos públicos, migración ni alcance de la versión;
la nota de release distingue la preparación anterior del incremento y conserva
los límites de los datos. R2 conserva la pareja reproducible de 95fa38c5, runtime
b5d9e803 y 310 archivos empaquetados. R4 y R5 están aceptados por sus registros.
El test adicional de bytearray cambia sólo tests; no está empaquetado en el
sdist ni invalida el runtime o la evidencia funcional restante.

R6: diff productivo y documentación revisados; whitespace conforme, sin cambios
de runtime posteriores al congelado ni nuevas autoridades semánticas. Se integró
en main mediante fast-forward hasta 5c4e0767 y se retiró perf/q00-q06 con `-d`.
Las referencias e4eb17cc,
b93681b7, b83789bc y perf/f11-context-startup se han comprobado intactas.
`benchmarks/execution.json` conserva el SHA-256 del preflight Q00. No se borran
worktrees o ramas ajenos ni se crean tags o publicaciones.


### Cierre integrado de Q06

| Gate | Disposición y evidencia |
| --- | --- |
| R1 | Narrativa y migración revisadas; nota de release alineada y sin nuevas promesas de rendimiento |
| R2 | Dos builds idénticos; wheel 5ac014b1… / sdist 3ab93460…; 310 archivos contrastados también tras integrar. [Identidades completas](evidence/mongoeco-5.0.0/q06-preflight.json) |
| R3 | Veinte invocaciones en 844,9 s; 72 mejoras, 30 inconcluyentes y dos empeoramientos aceptados expresamente por el usuario. Una sonda causal autorizada de 44 consultas, sin segundo intento de aceptación |
| R4 | Pytest/unittest en 3.13/3.14/3.15, cobertura exacta 99,0009806 %, deep y matriz 12/12. [Globales](evidence/mongoeco-5.0.0/q06-globals.json) y [matriz](evidence/mongoeco-5.0.0/q06-r4.json) |
| R5 | Typing/API/SPI/smokes en los tres Python; SDK/PCRE2, core sin SDK y canario autorizado conservados. [Consumidores](evidence/mongoeco-5.0.0/q06-consumers.json) |
| R6 | Diff y docs revisados; integración fast-forward en main, rama propia eliminada, workset propio limpio y archivo preexistente preservado |

Runtime final: `b5d9e8039bf685839c92dbd08f88ab9124f245b4`; fuente del artefacto
`95fa38c549051398225c6c90ce90c28989594c94`. La integración documental
no cambia sus bytes ni invalida los gates. No se han creado tags, hecho push
ni publicado la versión.

No queda una obligación exigida de Q00–Q06 sin disposición/comprobación. Las
reservas son explícitas: no se conoce la causa de los dos empeoramientos,
los treinta controles inconcluyentes no prueban ausencia de regresión y Q05 no
aporta ahorro. El canario sustituto no acredita las aserciones privadas perdidas.
La evidencia local es macOS arm64; CI alojado y Trusted Publisher siguen siendo
pasos posteriores antes de un tag/publicación, fuera de esta entrega.

# Revisión de C00–C06 / H37–H47

Base revisada: `2671229`. Correcciones: `d80bd24`, `76aa1cb` y `4fbf0e0`.
La revisión corresponde al plan `docs/predicate-authorities-analysis-and-plan.md`;
no cierra ni absorbe los pendientes históricos B00–B10.

## Hallazgos corregidos

| ID | Prioridad / bloques | Causa y efecto observado | Corrección y prueba |
| --- | --- | --- | --- |
| R01 | P1; C01/C02 | `Code` hereda de `str`: se seleccionaba como string, coincidía con regex y pasaba keywords de schema. Los argumentos Code de `$type` se aceptaban y los de `$regex` podían producir TypeError. | Clasificación BSON común para tipos, regex y schema; rechazos preparados de argumentos Code, con códigos/mensajes físicos y entrada vacía/poblada. |
| R02 | P1; C01/C02/C03 | DBRef tenía representaciones SDK/local que escapaban de la evaluación de propiedades y enum. SQLite excluía tags DBRef, SON y documentos escapados de `$type: object`. | Reutilizar `path_mapping` en clasificación/evaluación/igualdad, con adaptador SDK opcional; aceptar los tres tags de objetos en las dos formas SQL de `$type`, escalares y arrays. |
| R03 | P1; C02/C03 | La igualdad de Code con scope reutilizaba la clave BSON sensible al orden de campos; rechazaba enum válidos y admitía duplicados en uniqueItems. | Igualdad recursiva de schema dentro del scope, conservando el código y la distinción entre scope ausente y `{}`. MQL conserva su comparación independiente. |
| R04 | P2; C02 | min/maxItems y min/maxLength aceptaban valores que no caben en int64. Su conversión decimal y errores constituían otra regla propia. | Coerción int64 compartida más integridad/no negatividad propias de schema; captura de extremos, fracciones, negativos, tipos inválidos y no finitos, incluidos mensajes. |
| R05 | P2; C02/C03 | `compile_filter` preparaba dos veces el mismo schema. Los diagnósticos añadidos para R04 requieren conservar el dialecto, pues el formato observado difiere en 7/8 y 9. | Preparar una vez y conservar dialecto efectivo, también en el fallback de nodos manuales y los validators de colección. Spy de preparación y físicos 7/8/9. |
| R06 | P1; C01 | Había dos normalizadores de `$type`; además el código BSON 6 se asociaba a dbPointer y no a undefined. | Un solo parser, reutilizado por el matcher. Testigo BSON undefined físico, distinto de null/ausencia, y equivalencia de aliases numéricos/string. |
| R07 | P2; C06 | Un test de comparación dejaba una conexión SQLite abierta: el context manager de Connection gestionaba la transacción, no close(). El warning se recogía más tarde en otro test. | Context managers de transacción y cierre explícito combinados; atribución con tracemalloc/GC y regresiones de limpieza en los tres intérpretes. |

Todos estos hallazgos se comprobaron con herramientas sobre la base o su
artefacto instalado. No se infiere su aceptación de la coincidencia entre motores.
Los seis casos nuevos se integran en el capturador y replay existentes.
Los 144 tests de API de estos casos fallan con el wheel anterior y pasan con
la revisión. Las observaciones de los casos nativos anteriores permanecen
idénticas en las seis capturas ampliadas.

## Revisión por bloque

- **C00:** observadores y snapshots históricos protegidos intactos. Oráculos
  externos capturados en 7/8/9; códigos, mensajes, estado y recuperación de
  escrituras conservados. Los casos de estado usan DBRef canónico del SDK;
  la equivalencia BSON con documentos `$ref/$id` se prueba en decisiones por ID,
  evitando confundir una representación Python del decoder con otra semántica.
- **C01:** revisadas hojas comunes, composición/cache del generador, traversal,
  SQL y consumidores de selección. Corregidos R01/R02/R06; no se restaura el
  catch global ni comparaciones entre familias en consultas.
- **C02:** corregidos R01–R05. Retirada la tabla duplicada de clasificación de
  schema. Se mantienen igualdad atómica/recursiva, restricciones y rechazos
  explícitos del subset y la independencia de multipleOf respecto de mod.
- **C03:** revisadas preparación por operación, moderate original/nuevo,
  collMod, bulk y merge, errores y efectos. Clasificación de DBRef y dialecto
  preparados quedan cubiertos por escrituras directas y wire, además de lectura.
- **C04:** revisados conservación de candidatos, representabilidad, residual,
  exactitud, cachés, ventanas/collectors y backend vectorial. Nuevos controles
  de elegibilidad BSON usan IDs nativos en Search textual/vectorial. No se
  cambian cuantización, score ni garantías de recall ANN ajenas a este plan.
- **C05:** revisadas coerción, preparación, signo del resto, selección de
  escrituras y guarda de SQL. Sin nueva divergencia de mod. El helper pasa a
  llamarse `truncate_bson_int64`: schema añade sus requisitos de integridad y
  no negatividad, y mod conserva su truncamiento y dominio anteriores.
- **C06:** revisadas correspondencia fuente/distribución, gates, evidencia y
  límites. Distribuciones desde archive limpio, reproducidas dos veces. La
  primera sonda focal no reprodujo el warning; el barrido posterior con
  tracemalloc y GC lo atribuyó a `test_sqlite_query.py`. Se cierra el fixture
  sin cambiar el paquete productivo ni sus transacciones.

## Aceptación y límites

- Python 3.13: **50507 passed**, 220 skips, 0 warnings y 14471 subpruebas.
- Python 3.14: **50507 passed**, 220 skips, 1 warning y 14471 subpruebas.
- Python 3.15: **50507 passed**, 220 skips, 0 warnings y 14471 subpruebas.
- Cobertura 3.14: **99,00132%** (mínimo conservado: 99%).
- 40 acciones físicas por versión, ambos motores, sin fallos/errores/skips.
- 12 celdas del kit y seis replays semánticos SDK verdes; contratos, smokes, propiedades y lint verdes.
- Benchmarks secuenciales antes/después con el mismo entorno; resultados y límites en [el receipt](evidence/predicate-authorities/review-20261031/validation.json).

Los cocientes de medianas antes/después son 1,002–1,032× en predicados
Memory, 0,953–1,035× en predicados SQLite, 0,993–1,013× en Search Memory y
0,985–1,014× en Search SQLite. Vector queda entre 1,003× y 1,036×. Preparar
250 validators pasa de 0,0047 a 0,0050 s; validarlos ya preparados de 0,0028 a
0,0030 s; insert_many Memory se mantiene en 0,0117 s y SQLite pasa de 0,0247 a
0,0257 s. Son tendencias de tres repeticiones, sin warmup y con las mismas
dependencias; no se cambian umbrales de CPU ni se promete rendimiento general.

Los 220 skips son 219 comprobaciones físicas sin URI configurada en la suite
global y un backend ICU opcional; las 40 acciones nativas de esta revisión y
las 12 celdas del kit se ejecutaron expresamente sin skips.

Las suites globales conservaron un warning de SQLite antes del cierre del
fixture R07. Su runtime y las demás entradas permanecen idénticos; la limpieza
posterior se acredita con 431 tests SQLite instrumentados sin warning y controles
outbox sin instrumentación (42 en 3.14; 82 controles afectados en 3.13 y 3.15).
El primer barrido diagnóstico obtuvo dos fallos en tests de procesos/outbox;
no se admite como gate. Los controles outbox se ejecutaron separadamente y
pasaron, además de estar verdes en las suites globales de los tres intérpretes.

Se conserva la advertencia y cualquier skip del gate global en su log. Los
skips opcionales no sustituyen una celda física o SDK obligatoria. Las ejecuciones
interrumpidas sobre d80bd24 no acreditan el candidato final.

La revisión tiene una base exacta y workset identificable. El diff, los callers,
los testigos y los gates aportan garantías finitas; no permiten declarar ausencia
exhaustiva de otros defectos. B07 sigue pendiente para la frontera Python/BSON
general. No se acredita Atlas, recall arbitrario, nuevas fórmulas vectoriales,
concurrencia nativa, sesiones distribuidas ni change streams.

Diff final y documentación revisados; los cambios se entregan en commits
semánticos locales, sin push ni publicación.

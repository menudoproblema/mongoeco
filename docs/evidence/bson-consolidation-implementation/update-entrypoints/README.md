# D01b: aceptación permanente de comandos e imágenes

Base: `f8344e7`. El runtime permanece idéntico al incremento aceptado de
[contenido y entrega](../update-events/receipt.json); se incorporan pruebas,
referencias nativas y la aceptación restante del bloque. El owner de estado es
el [registro](../../../bson-consolidation-implementation.md), con las garantías
del [plan](../../../bson-semantics-consolidation-plan.md).

## Matriz discriminante

El nuevo corpus `update_entrypoint` usa registro, capturador y replay existentes.
No modifica los observers protegidos ni referencias previamente aceptadas.
Se añade únicamente find_one_and_replace al adaptador de superficie async.

- 45 escenarios de respuestas y eventos: update command, findAndModify command
  y APIs find_one_and_update/replace; classic/pipeline/replacement, documentos
  pequeños/grandes, cambios físicos/no-op y upsert con ID determinado.
- 18 escenarios de identidad: `_id` int→double e int→otro entero, por las tres
  formas de escritura y entradas; reply por elemento frente a excepción de
  comando, código 66, tipo físico del ID anterior y estado posterior.
- 36 escenarios de imágenes: BEFORE/AFTER y proyección completa/solo ID/vacía.
  `{}` es un resultado válido distinto de None; la imagen devuelta no decide
  el contenido del evento.
- Dos escenarios de prefijo confirmado: comando update con dos items y
  update_many con segundo documento inválido. Se exige la primera notificación
  después del error y antes de otra petición; después se inserta una barrera
  para comprobar que no hay duplicación. Fallar una escritura no revierte por
  asociación el prefijo ya confirmado de operaciones múltiples.

Las respuestas conservan BSON físico cuando contiene documentos o IDs. El API
find no expone modified_count: no se inventa a partir de la imagen. El comando
update observa sus contadores reales y códigos de errores por índice; findAndModify
observa value y lastErrorObject. Las excepciones conservan familia y código.
Los eventos y el estado usan BSON SDK, sin identidad/comparador de producción.
Se reutilizan los helpers independientes de barrera/observación del corpus de
eventos aceptado. No se normaliza update/replace ni el contenido de los cambios.

Cuatro acciones contienen 101 escenarios por major: MongoDB 7.0.43/8.0.32/9.0.2
con FCV correspondiente y replica sets propios en 49973/49974/49975. Capturas
verificadas mediante --check. El replay ejerce Memory/SQLite, dialectos 7/8/9 y
superficies sync/async; watch wire sigue fuera del subset soportado. Las bases
UUID de captura se eliminan en finally; los servicios preexistentes se conservan
para el trabajo restante. La instancia habitual no se modifica.

## Revisión de D01b

La revisión fija base, workset y hashes; no añade cambios productivos preventivos.
No quedan findings demostrados abiertos en el alcance D01b tras los controles
de este corte y la evidencia previa. Esto no demuestra ausencia de errores en
otros contratos ni sustituye las matrices y gates finales D05.

| Garantía del bloque | Evidencia y disposición |
| --- | --- |
| Identidad física, política clásica de ID y excepciones pipeline/replacement | Helper D01a y cut write-effects; root_identity/root_id, comandos y rechazos del nuevo corpus; mismos tipos/orden/NaN/scopes en los consumidores alcanzados. |
| Estado, contadores, índices y storage keys | write-effects, 12.155 checks de consumidores y archivo creado por runtime anterior en update-events; sin cambios de fuente actuales que invaliden esa aceptación. |
| Contenido del evento por operación | Nueve acciones update_event aceptadas: rutas aplicadas, whole/dotted, arrays, umbrales, imágenes calculada/guardada e ID anterior; las cuatro acciones nuevas amplían las entradas públicas. |
| Múltiples y errores | Bulk ordered/unordered y update_many en los cortes previos; nuevo prefijo confirmado prueba entrega tras error sin requerir otra petición. |
| Atomicidad y entrega | Fault después de registro, rollback de estado/índices/outbox, retry, commit/abort nativos y replay incremental/compactado del corte previo; 74 checks de contratos reejecutados, con 8 subtests. |
| Persistencia e identidad de eventos | Journal V2 conserva orden y widths; V1 y archivo SQLite anteriores se verificaron en update-events. No se promete reconstruir información ya perdida en un journal viejo. |
| SPI, ownership y costes | No se cambian firmas ni producción; contratos SPI reejecutados. Se reutilizan typing, SDK-free, propiedades y medidas del runtime idéntico, sin repetir gates no afectados. |

La sonda diagnóstica sync previa queda reemplazada, para estas garantías, por
regresiones permanentes sync/async. D04 conserva preparación/contexto watch,
fullDocument soportado y fixtures de replica set de jobs nativos; D06 conserva
sus consumidores y fronteras restantes, H53 su disposición y D05 las matrices,
wheel/global/cobertura/costes finales. No se cierra B07 general ni se amplía la
semántica distribuida del stream. El receipt registra los resultados terminales
de esta aceptación y distingue las capturas/feedback intermedios.

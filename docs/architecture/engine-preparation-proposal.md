# Propuesta: simplificar la preparación conservando SPI v2

## Estado y decisión propuesta

Revisión 2, 2026-10-17. **Alcance T1–T6 autorizado por el encargo del usuario;
implementado y verificado en el candidato delimitado en el
[registro de implementación](../engine-preparation-validation.md).** El estado formal del registro ADR-018 se
conserva; no se exige una transición RFC que el proyecto no adopta.
Base productiva: Mongoeco 4.10.1, `738fd0eb5ece0adabf071e312ee0ff7b82b8589d`.
[ADR-018](decisions/ADR-018-propuesta-operaciones-preparadas-spi-v3.md) conserva
la identidad de la decisión propuesta; ADR-017 sigue siendo la autoridad vigente.

El objetivo es reducir preparación repetida y autoridades internas ambiguas sin
cambiar las obligaciones de autores de motores ni las aplicaciones PyMongo.
La alternativa autorizada para desarrollar es **A mínima**:
reutilizar compiladores y tipos actuales, consolidar sus productores internos y
corregir divergencias concretas. No incorpora las requests públicas de la primera
propuesta ni las crea internamente para volver a traducirlas a v2.

La hipótesis de SPI v3 **se cierra para este alcance**. Tras completar también
S1/S2, el usuario acordó [mantener v2 y aparcar v3](engine-spi-v3-proposal.md).
Ese documento posee la decisión posterior y sus condiciones de reconsideración.
El control de T1–T6 es el código 4.10.1, sin una implementación B.

La [síntesis de los tres informes](spi-proposal-review.md) posee la agrupación,
contraste y disposición de cada idea. La
[revisión 1 archivada](../evidence/spi-v3-proposal/revision-1.json) conserva texto
y hashes del diseño descartado; no es un contrato aceptado ni operativo.

## Hechos que determinan el alcance

La [evidencia de revisión](../evidence/spi-v3-proposal/review-probes.json), producida
por [sondas reproducibles](../evidence/spi-v3-proposal/probe_review.py), confirma:

- Los cinco shapes originales tienen campos paralelos, pero no la misma frontera
  contractual. `UpdateOperation` y `EngineFindSemantics` están reexportados;
  `FindOperation` entra en `AsyncReadPlanningEngine.plan_find_execution` y por ello
  tampoco es libremente reemplazable. Agregación y semántica interna de update
  admiten consolidación interna sin trasladar esa libertad a los demás tipos.
- Las guardas son asimétricas: una operación permite collation/let ausentes y
  hereda del contexto al convertir; la semántica ligada exige coherencia exacta.
  Esto no demuestra por sí solo un error de resultados en la fachada.
- `with_overrides(plan=...)` acepta un plan distinto de su filtro. Hay productores
  internos que sustituyen filtro y plan juntos por motivos legítimos. No se
  puede prohibir ese override público indiscriminadamente para cumplir el diseño.
- Una operación find **sin ligar**, dialecto 8.0, convertida por el helper sin
  argumento de dialecto obtiene 7.0. Con argumento explícito conserva 8.0. Las
  fachadas ligadas recuperan el dialecto de su contexto; no se acredita el mismo
  fallo en ellas. Es un defecto de propagación concreto, abordable dentro de v2.
- En el escenario simple medido, `update_many` con N=1 y N=3 provoca N entradas
  de preparación de selector en Memory y 2N en SQLite, en sync y async. SQLite
  prepara la semántica en el coordinador y de nuevo en su helper privado. No es
  una medición de latencia ni prueba de pérdida de atomicidad.
- El cursor captura semántica/plazo de forma diferida, reutiliza esa semántica y
  `rewind()` la invalida: mantiene contexto y `$$NOW`, pero vuelve a capturar el
  deadline. `clone()` crea otro contexto. Identidad y plazo son vidas distintas.
- Explain llama al motor directamente; profiling consulta su planner mediante
  `getattr`. CRUD cruza el adapter. Sus diferentes parámetros `context` (sesión)
  y `operation_context` (contexto completo) son un riesgo de mantenimiento.
- El canario independiente reimplementa un subconjunto de BSON y omite contexto
  en algunas primitivas. Acredita ese subconjunto y el SPI aplicable; no que un
  motor externo pueda reutilizar toda la ejecución compilada de Mongoeco.

## Diseño mínimo y compatibilidad

### T1 — Una autoridad resuelta en los productores internos

Resolver opciones, contexto y fase de compilación en los coordinadores existentes;
compilar las entradas de aplicación con el dialecto, collation y expresiones ya
resueltos, conservando la fase de error observada. No mover validación dinámica
antes de saber si hay documentos ni repetir la captura de `$$NOW` al convertir.

El helper de conversión sin contexto debe usar el dialecto de su operación cuando
no haya un override explícito; un contexto ligado sigue siendo autoridad. Probar
también collation y variables para no arreglar solo el caso 8.0.

Los DTO v2 conservan campos, firmas, valores por defecto, métodos y semántica de
overrides. Los productores nuevos entregan valores canónicos derivados; esos
campos son proyecciones de compatibilidad y no nuevas entradas independientes.
No se promete eliminar físicamente todo campo duplicado ni toda guarda mientras
v2 los permita. `AggregateOperation` queda expresamente dentro de esta revisión
de productores; sus usos internos se consolidan sin crear un SPI de agregación.

**Aceptación T1:** negativos de contexto siguen pasando; conversión 7/8/9 sin
contexto no degrada dialecto; operaciones ligadas no recapturan reloj ni bindings;
no se altera el manifiesto de aplicación o SPI v2.

### T2 — Preparación coherente y ownership con mecanismos existentes

Consolidar la construcción interna de filtro, plan y opciones en los compiladores
existentes. Sus coordinadores reciben especificación/contexto, no combinaciones
arbitrarias de plan y filtro. Para selección por identidad, el mismo productor
genera ambos y conserva por separado el selector original que debe revalidarse.

Se mantienen los puntos de extensión v2 y sus planes explícitos. La garantía
nueva se limita a los productores internos controlados: no añade validación
criptográfica de procedencia, tokens, sellos, un IR nuevo ni protección contra
reflexión de Python. Las comprobaciones de tipo/coherencia de frontera continúan
donde haya estados externos representables.

Reutilizar las copias defensivas de BSON, contextos, outcomes y snapshots. Un
`frozen=True` no garantiza ownership profundo; `MappingProxyType` tampoco congela
sus descendientes. No devolver referencias a filtros/operandos privados mutables
para ahorrar copias sin una prueba de aislamiento. No se impone congelación
global de BSON ni una nueva capacidad que confíe en el motor.

**Aceptación T2:** cambiar entradas, un resultado o buffers del motor no altera
otra operación; la selección interna conserva filtro/plan/contexto correspondientes;
overrides v2 legítimos siguen funcionando y los negativos externos conservan
sus fallos. Compilación estática y evaluación por documento conservan sus fases.

### T3 — Plazos por ejecución, separados del contexto semántico

No se mueve el inicio de todos los deadlines al despacho. No se añaden deadline,
timeout ni reloj monotónico nuevo a `OperationContext`. Se caracterizan primero
las rutas de la tabla siguiente y se conserva cada comportamiento alcanzable.

| Ruta | Base inspeccionada | Decisión del refactor |
| --- | --- | --- |
| Find/cursor | `_base_semantics()` prepara tarde y cachea; crear `find` no captura plazo | Conservar diferimiento y caché por ejecución |
| `rewind` / `clone` | Rewind elimina semántica, conserva contexto; clone crea otro contexto | Rewind vuelve a capturar plazo y conserva tiempo de expresiones; clone inicia vida independiente |
| Lectura/count preparados | `compile_find_semantics` captura plazo; readers consumen `semantics.deadline` | Reutilizar la semántica del mismo intento, sin adelantar la captura al construir el cursor |
| Memory update | Captura plazo al entrar en primitiva, antes del lock | Conservar fase y espera incluida |
| SQLite update | Captura antes de `_run_blocking`; pasa el plazo al helper; entrada privada sin plazo lo calcula | Conservar el plazo del llamador, también durante espera de executor/lock |
| Agregación | Sus iteradores/ejecutores capturan y transmiten plazo propio; subconsultas preparan sus lecturas | Caracterizar sus ramas y conservarlas; no imponer un plazo global de cursor nuevo |
| `getMore` wire | Consume el cursor registrado; no convierte automáticamente `maxTimeMS` en un plazo nuevo del core | Conservar flujo actual; paridad temporal de comandos nueva requiere caso y decisión propios |
| SQLite `_iter_scan_documents_sync` | Admite semántica o filtro; la rama cruda recompila con max_time_ms. Solo se encontraron callers en tests | Conservar compatibilidad privada probada o eliminarla con sus usos justificados; no presentarla como renovación pública demostrada |
| Helper SQLite de borrado | Compila lecturas auxiliares sin max_time_ms | No asumir que pierde un timeout público; revisar la entrada y límites del llamador antes de cambiarla |

La tabla es una línea base inicial, no un grafo completo de plazos. Antes de
editar una rama se añade su caracterización de inicio, propagación, renovación
y cierre. Una renovación que resultase defectuosa requiere reproducción y fix
identificado; no se normaliza silenciosamente dentro del refactor.

Los tests sustituyen la función local de captura/verificación y usan barreras
para executor/lock. No parchean el reloj global de asyncio. Solo si una ruta no
puede probarse así se justificaría un hook privado adicional; no se crea ahora
un proveedor de tiempo por anticipación.

**Aceptación T3:** inicio de plazo y efectos de rewind/batches preservados;
cancelaciones y doble cierre no amplían plazos ni pierden el primer error.

### T4 — Planning y explain por una frontera interna coherente

Concentrar su invocación en `EngineSpiAdapter` con métodos internos tipados que
conserven los nombres, argumentos y resultados SPI v2 del motor. El coordinador
resuelve sesión/contexto y llama a esa frontera; no introduce requests v3 ni
renombra primitivas públicas. Se conservan los dos entrypoints v2 de planning.

El profiler mantiene su carácter best-effort y sus valores vacíos al no disponer
de planner o fallar este; no convierte una inspección en fallo de la escritura.
Explain conserva sus errores y plan observado. No se exige planning a un motor
que hoy pasa el contrato mínimo sin implementarlo, ni se añade introspección de
firmas para adivinar semántica. El adapter sigue siendo interno y único por cliente.

**Aceptación T4:** mismo plan, lineage, degradación y errores en find/count,
explain y profiling; explain no muta datos. Motor mínimo y canario v2 conservados.

### T5 — Reutilizar la semántica SQLite ya preparada

El coordinador SQLite ya construye `EngineUpdateSemantics`; pasar ese objeto a
su helper de mutación mediante un argumento **privado**, conservando la ruta
actual de compilación cuando el helper se prueba directamente sin él. No cambia
`UpdateOperation`, `selector_filter` ni las firmas del SPI.

El objeto reutilizado corresponde al mismo intento, contexto y selector. No
se almacena en una caché global, atributo del engine, ContextVar, diccionario
marcado ni side channel del documento. El fallo de compilación conserva su fase
y el helper conserva sus validaciones dependientes del documento y transacción.

El objetivo medido para los casos N=1/3 es SQLite **2N → N** preparaciones de
selector; Memory conserva N. Esa evaluación del selector por candidato protege
la revalidación bajo lock y no se elimina. Compilar una vez para toda la
operación sería otro diseño: v2 transmite selector crudo por llamada y aquí no
se añade una extensión para transportar su programa compartido.

**Aceptación T5:** contadores de preparación cumplen ese presupuesto; CAS,
selección concurrente, no-match y errores parciales siguen iguales. Probar también
upsert, replacement, pipeline y borrado antes de extender la reutilización a ellos;
no asumir que todos usan la misma primitiva o el mismo plan.

### T6 — Piloto representativo y cierre con evidencia

El piloto incluye una lectura/count, update, **delete y planning/explain**, junto
con update_many N=1/3, upsert con seed y replacement. No es una implementación
amplia de gramática; estos casos detectan diferencias de preparación y overrides.
Memory/SQLite y sync/async son obligatorios. El canario independiente queda como
control sin adaptar sus expectativas; se añaden checks focales para la autoridad
de contexto y métodos de planning donde los implemente un motor de prueba.

| Corpus | Observación requerida |
| --- | --- |
| Contexto | Dialectos 7/8/9, collation, let, NOW, sesión, ordinales de lote/lookup/merge; mutación de entradas y clones |
| Lectura/inspección | Query, sort, proyección, vacío, count, explain y profiling; errores y planes equivalentes |
| Mutación | Update/replacement/pipeline, upsert e ID null, delete; selector original e identidad, CAS con barrera |
| Ausencias y errores | Validación constante/dinámica, no-match y colecciones vacías; documento de error completo y primera causa |
| Lotes y wire | Ordered/unordered, writeErrors[].op, w=0, códigos, contadores, efectos parciales y reutilización física |
| Recursos | Snapshots nativos y fallback, cancelación, agotamiento, cierre repetido, executor y cleanup fallido |
| Persistencia | Fixture SQLite 4.5 y fichero creado por wheel 4.10.1 con hashes; BSON, índices, outbox, checkpoints y reapertura |

El cierre ejecuta el corpus nativo 7/8/9 de nuevo y su replay direct/wire, sin
modificar expectativas para ocultar diferencias. Se preservan las normalizaciones
explícitas existentes, sin prometer identidad literal de todo errmsg. Después se
ejecuta una suite global final, conformance, manifiesto, typing, lint/coverage y
wheel instalado en la matriz soportada; el kit pytest incluye xdist -n 2/-n 4.
Las evidencias de revisión iniciales no acreditan el candidato implementado;
su [registro de implementación](../engine-preparation-validation.md) conserva
los gates finales y sus resultados.

## Plan y decisiones de avance

| Bloque | Entrega y checkpoint |
| --- | --- |
| P0 — revisión | Síntesis de informes, fuentes, sondas y propuesta; el encargo posterior autoriza T1–T6 |
| P1 — caracterización | Tests de contexto, plazos, planning y contadores T1–T5 antes de cambiar código; ampliar corpus focal representativo |
| P2a — preparación | T1/T2/T3 en coordinadores actuales; detener y revisar si exige alterar tipos SPI v2 |
| P2b — rutas compartidas | T4 y T5, con delete/planning y fallos; checkpoint de complejidad y presupuesto de compilación sobre código implementado |
| P3 — cierre | T6 completo y prueba global final; elegir versión de entrega por cambios reales, sin presuponer 5.0 |

Los criterios de P2 se verifican únicamente sobre el código implementado y las
firmas preservadas. El trabajo pendiente es estimación, nunca ahorro acreditado.
Se inventarían fuentes de contexto, llamadas de preparación por escenario,
copias, guardas, conversiones y wrappers nuevos. No se exige una disminución
artificial de todas las guardas; quitar una exige demostrar que su estado ya no
es representable en esa ruta. El neto incluye helpers y tests de mantenimiento.

El gate determinista es el conteo de compilaciones y la paridad. Para estimar
rendimiento se comparan baseline/candidato, mismo equipo y wheel, orden alterno,
al menos siete procesos y calentamiento igual: find/count/update y lotes de
1/100/1.000, separando preparación y ejecución. Registrar mediana, IQR y pico de
memoria con método identificado. Una regresión reproducible superior al 5 % en
tiempo o pico exige investigar antes de cerrar; ruido o muestras solapadas dejan
el dato inconcluso. CI compartida puede aportar señales, no acreditar rendimiento
ni convertir ruido en éxito elevando automáticamente el límite al 10 %.

## Piezas: conservar, consolidar o eliminar del diseño

| Pieza | Requisito y mecanismo vigente; consecuencia de retirarla | Disposición |
| --- | --- | --- |
| OperationContext | Identidad/tiempo/bindings/sesión ya capturados; retirarlo recaptura o diverge | **reuse** |
| DTO UpdateOperation/EngineFindSemantics y FindOperation de planning | SPI v2 y usos directos; retirarlos o reinterpretarlos rompe consumidores | **reuse** con productores **consolidate**, atributos conservados |
| AggregateOperation y EngineUpdateSemantics internos | Coordinación y ejecución; duplicar autoridad no aporta garantía | **consolidate** en productores actuales, sin nueva jerarquía |
| Planes, preparación de proyección/expresiones y copias | Semántica y ownership existentes; retirarlos reinterpreta o comparte datos | **reuse**, no IR ni congelación global |
| Semántica SQLite preparada dos veces | Preparación antes de ejecución; la segunda construcción no añade garantía para los mismos argumentos | **consolidate** con argumento privado, fallback de tests conservado |
| Adapter único | Validar outcomes/snapshots y aislar firmas de engine; retirarlo dispersa garantías | **reuse / consolidate** llamadas de planning sin ampliar contrato mínimo |
| Outcomes, diagnóstico y cleanup | Códigos, imágenes, efectos, errores y recursos; contexto no sustituye esas garantías | **reuse**, con todas sus guardas necesarias |
| Request pública/interna paralela, constructor spi_version, protocolo v3 y ReadExecutionPlan nuevo | Se proponían para retirar estados de una frontera nueva; A no los necesita | **remove** de este alcance |
| Evaluador público para engines | No hay demanda/contrato acreditado; el canario actual no demuestra la necesidad | **remove** de este alcance; no inferir garantía inexistente |
| Tokens de procedencia, reloj público, caché global de selectores | Ninguna garantía adicional acreditada frente a construcción controlada y tests locales | **remove** del diseño |
| Informes/schema SQLite/Search nuevos | No resuelven preparación y romperían garantías ajenas | **remove** del alcance |

## Reabrir la hipótesis de v3

La [decisión posterior a T1–T6 y S1/S2](engine-spi-v3-proposal.md) aparca v3 y
posee los requisitos para una propuesta nueva. No es una continuación automática
de P2 ni una opción a elección del implementador de A. ADR-016 conserva valor
histórico, sin autorizar una migración o retirada actual.

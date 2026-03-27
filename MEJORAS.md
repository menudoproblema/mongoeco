# MEJORAS

Este documento ya no es solo una lista teórica. Ahora recoge:

1. las mejoras arquitectónicas identificadas para llevar `mongoeco` hacia un diseño "como si hubiera nacido desde cero";
2. qué partes ya están aplicadas en el repositorio;
3. cuál es el siguiente orden razonable para seguir refactorizando sin perder el estado verde de la suite.

Escala usada:

- `Estado`: `Aplicado`, `Aplicado con matices`, `En progreso`, `Reabierto`, `Pendiente`
- `Impacto`: `Alto`, `Medio-Alto`, `Medio`
- `Esfuerzo`: `Bajo`, `Medio`, `Medio-Alto`, `Alto`, `Muy Alto`

Ordenadas por prioridad práctica actual: más impacto, menos esfuerzo relativo y mejor capacidad para desbloquear el resto.

## 1. Fuente Única Declarativa de Compatibilidad

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio`
- `Descripción`: definir un catálogo maestro único para dialectos MongoDB, perfiles PyMongo, operadores, stages, acumuladores, opciones por operación y deltas de comportamiento.
- `Motivación`: la compatibilidad estaba repartida entre hooks, flags y matrices parciales.
- `Aporte real`: ya existe una fuente central desde la que se deriva la mayor parte de la compatibilidad observable.
- `Aplicado en`:
  - `1bf5bce` `refactor: centralize compatibility catalog`

## 2. Subsistema Separado de Administración y Comandos

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio`
- `Descripción`: extraer administración de colecciones, índices, estadísticas y `Database.command()` a un subsistema propio, separado del flujo CRUD principal.
- `Motivación`: la administración crece con reglas, resultados y semántica distintas al flujo de datos convencional.
- `Aporte real`: reduce acoplamiento en `Database`, aclara responsabilidades y prepara un futuro dispatcher tipado y resultados administrativos más limpios.
- `Aplicado ya`:
  - `436b7dc` `refactor: extract async database admin service`
  - `c39fdf1` `refactor: extract async database command service`
  - `75444e7` `refactor: add sync database admin service`
  - `1681e98` `refactor: parse typed admin commands before execution`
  - `4484998` `refactor: add sync database command service`
  - `fd11f01` `refactor: unify admin command document execution`
  - `bfbecee` `refactor: type admin listing metadata snapshots`
  - `dcf0cab` `refactor: unify admin listing snapshot loaders`
  - `56af815` `refactor: extract shared admin command parsing`
  - `3395070` `refactor: centralize admin option normalization`
  - `d42c00a` `refactor: type findandmodify and index command results`
  - `5a9ddbb` `refactor: keep admin result serialization at the boundary`
  - `3c3b211` `refactor: keep stats and validation as typed snapshots`
  - `4b767b7` `refactor: compile admin aggregate and lookup reads`
  - `ccf1a08` `refactor: parse typed read admin commands`
  - `6712e86` `refactor: split findandmodify admin execution`
- `Cierre`: el subsistema ya separa parseo, ejecución y serialización pública; los comandos principales de lectura, escritura e introspección viajan por servicios propios y los handlers complejos restantes ya están descompuestos en rutas internas más pequeñas.

## 3. Tipado Estricto en el Core Semántico y la Metadata Interna

- `Estado`: `Aplicado`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio-Bajo`
- `Descripción`: sustituir `dict[str, Any]` internos por dataclasses o records privados en piezas críticas como índices, stats, explain, resultados administrativos y estados internos complejos.
- `Motivación`: los diccionarios internos vuelven opaca la estructura real y facilitan drift entre layers y engines.
- `Aporte real`: más claridad interna, menos errores estructurales y mejor base para contributors o nuevos backends.
- `Aplicado ya`:
  - `6076c4e` `refactor: type admin metadata contracts`
  - `33caa69` `refactor: type admin command metadata internals`
  - `cd0d5a5` `refactor: type internal admin command results`
  - `0f399ff` `refactor: type explain payloads internally`
  - `bfbecee` `refactor: type admin listing metadata snapshots`
  - `d42c00a` `refactor: type findandmodify and index command results`
  - `3c3b211` `refactor: keep stats and validation as typed snapshots`
  - `eb85f03` `refactor: type complex write metadata`
  - `8137d48` `refactor: type engine index metadata`
  - `30eec95` `refactor: type aggregation intermediate buckets`
- `Cierre`: el tipado interno ya cubre admin, listados, stats, validación, `explain`, writes complejos, metadata de índices en engines y buckets intermedios de agregación. Los `dict` que siguen existiendo a partir de aquí responden sobre todo a documentos públicos o a estructuras deliberadamente flexibles del lenguaje MongoDB.

## 4. Arquitectura Basada en Planes de Operación

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: introducir una IR explícita entre API y engines, con objetos como `FindPlan`, `UpdatePlan`, `AggregatePlan` o `AdminCommandPlan`.
- `Motivación`: parte de la validación, normalización y resolución de opciones seguía repartida entre `Collection`, cursores, servicios y engines.
- `Aporte real`: los engines y servicios reciben instrucciones ya validadas, tipadas y normalizadas; eso reduce ambigüedad y duplicación.
- `Aplicado ya`:
  - `0069fd8` `refactor: add compiled find operations`
  - `f1640a4` `refactor: compile write operations in api layer`
  - `1681e98` `refactor: parse typed admin commands before execution`
  - `8ce97aa` `refactor: compile aggregate operations before execution`
  - `5770e05` `refactor: route compiled operations into engines`
  - `1429e9f` `refactor: route aggregate source loads through operations`
  - `57fb615` `refactor: route aggregate explain through compiled operations`
  - `5bcc88a` `refactor: route aggregate execution through compiled find operations`
  - `4b767b7` `refactor: compile admin aggregate and lookup reads`
  - `ccf1a08` `refactor: parse typed read admin commands`
  - `d9c5956` `refactor: require compiled find operations in collection`
  - `0a660f5` `refactor: close operation plan gaps in admin helpers`
- `Cierre`: la frontera basada en operaciones compiladas cubre lectura, agregación, `explain`, selección previa de writes y rutas admin. Los engines propios ejecutan ya writes desde `UpdateOperation` y `CompiledUpdatePlan`, sin depender de `update_spec` raw como contrato de frontera.

## 5. Motor de Updates Formal Basado en Paths Compilados

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: separar formalmente parsing de rutas, binding posicional, resolución de `arrayFilters` y aplicación de operadores de update sobre paths compilados.
- `Motivación`: la semántica de updates con arrays y rutas profundas es una de las zonas más propensas a errores sutiles.
- `Aporte real`: base mucho más estable para operadores complejos y menos fragilidad al ampliar soporte sobre arrays.
- `Aplicado ya`:
  - `a41ca5b` `refactor: compile update paths explicitly`
  - `1e4b40e` `refactor: separate update target resolution from mutation`
  - `58e7bbc` `refactor: compile update operator instructions`
  - `5a7fc5a` `refactor: add explicit update execution context`
  - `c06860b` `refactor: compile reusable update plans in engines`
  - `b28b28d` `refactor: resolve update applications before mutation`
  - `a5ba59b` `refactor: finalize array update execution plans`
- `Cierre`: los updates siguen ya un flujo formal de paths compilados, contexto explícito, planes reutilizables y aplicaciones resueltas antes de mutar también en operadores de arrays y rutas posicionales complejas. La ejecución de engine consume estos planes compilados como IR real de escritura.

## 6. Compatibilidad y Tooling Derivados Automáticamente

- `Estado`: `Aplicado`
- `Impacto`: `Medio`
- `Esfuerzo`: `Medio`
- `Descripción`: derivar automáticamente desde el catálogo de compatibilidad los helpers `supports_*`, snapshots, exports JSON/Markdown y documentación técnica.
- `Motivación`: aunque ya hay catálogo central, todavía no toda la observabilidad del soporte sale de él de forma automática.
- `Aporte real`: evita drift entre código, tests, documentación y tooling.
- `Aplicado ya`:
  - exports públicos de compatibilidad desde `src/mongoeco/compat/catalog.py`
  - tests de snapshot/consistencia en `tests/unit/test_compat.py` y `tests/unit/test_architecture.py`
  - `53dbc90` `refactor: export versioned compatibility artifacts`
  - `fc4c6e0` `refactor: derive runtime compatibility hooks from catalog`
  - `1ed0cd9` `refactor: derive compatibility tooling from catalog`
- `Cierre`: el catálogo ya alimenta hooks de runtime, exports JSON/Markdown y snapshots versionados para tooling externo; la compatibilidad observable, el tooling y la documentación derivada parten de la misma fuente de verdad.

## 7. Separación Más Fuerte entre Core Semántico y Ejecución por Engine

- `Estado`: `Aplicado con matices`
- `Impacto`: `Alto`
- `Esfuerzo`: `Alto`
- `Descripción`: endurecer la frontera entre semántica MongoDB y ejecución concreta en `MemoryEngine`/`SQLiteEngine`.
- `Motivación`: a medida que crecen query, update y aggregation, pequeñas decisiones del engine pueden contaminar la semántica observable.
- `Aporte real`: mejora la paridad entre engines y reduce divergencias sutiles.
- `Aplicado ya`:
  - `f856531` `refactor: share read semantics across engines`
- `Cierre`: la lectura y `explain` ya comparten núcleo semántico, y eso reduce mucho la deriva entre engines. El cierre estricto sigue pendiente en la zona de writes y de ciertas rutas físicas específicas del backend.

## 8. Estado Transaccional Explícito por Sesión y Engine

- `Estado`: `Aplicado`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: modelar explícitamente `SessionState`, `TransactionState` y `EngineTransactionContext`.
- `Motivación`: las transacciones son una zona donde lifecycle, ownership y limpieza de estado importan mucho más que en CRUD simple.
- `Aporte real`: simplifica el razonamiento sobre transacciones y reduce inconsistencias difíciles de detectar.
- `Aplicado ya`:
  - `12ce218` `refactor: model explicit session transaction state`
- `Cierre`: la sesión ya mantiene estado transaccional y de engine a través de records explícitos, no mediante diccionarios genéricos y hooks implícitos; el ownership y la limpieza de transacciones quedan modelados de forma más clara.

## 9. Agregación Enchufable por Stages o Handlers Registrados

- `Estado`: `Reabierto`
- `Impacto`: `Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: rediseñar la agregación como un sistema de handlers registrados o stages compilados, donde cada etapa tenga contrato propio.
- `Motivación`: la agregación es una de las áreas con más complejidad acumulada y crecimiento monolítico.
- `Aporte real`: simplifica extensión, reduce regresiones cruzadas y hace más mantenible seguir ampliando analítica avanzada.
- `Aplicado ya`:
  - `0757799` `refactor: dispatch aggregation through stage handlers`
- `Cierre`: existe ya un dispatcher por handlers registrados y `mongoeco.core.aggregation` es un paquete modular, pero el runtime principal sigue demasiado concentrado en `runtime.py`. Para considerar este punto estrictamente cerrado haría falta descomponer la lógica por familias o por stages reales.

## Estado Global

La base del refactor arquitectónico está muy avanzada, pero una revisión estricta obliga a matizar algunos cierres anteriores.

El proyecto queda bastante más cerca de una base "como si hubiera nacido así" desde el principio:

- compatibilidad derivada desde una fuente única;
- subsistema administrativo separado del CRUD;
- metadata interna y resultados críticos tipados;
- planes de operación explícitos entre API y engines;
- motor de updates formalizado;
- semántica compartida separada de la estrategia concreta de cada engine;
- estado transaccional explícito;
- agregación separada ya en paquete y handlers, aunque aún no completamente descompuesta por stages.

Además, después del cierre inicial de esta hoja se han endurecido tres cortes estructurales adicionales para subir el listón de arquitectura:

- los dialectos oficiales y perfiles PyMongo oficiales consumen directamente el catálogo como fuente declarativa, sin depender de lógica específica por clase para sus flags/capacidades base;
- `mongoeco.core.aggregation` ya es un paquete modular y el dispatcher de stages vive en un módulo propio, separado del resto del runtime de agregación;
- los updates compilados ya son planes ejecutables (`CompiledUpdatePlan.apply(...)`) y la frontera de escritura en los engines propios se apoya ya en `UpdateOperation`/`CompiledUpdatePlan`, no en `update_spec` raw como contrato principal.

## Qué Queda a Partir de Aquí

Lo siguiente ya no pertenece a esta hoja de refactor base, sino a evolución futura:

- seguir ampliando funcionalidad sin reabrir la base arquitectónica;
- endurecer o tipar zonas adicionales solo si el crecimiento real del proyecto lo exige;
- usar este documento como referencia histórica de las decisiones de base ya consolidadas, no como backlog activo.

## Evolución Arquitectónica Posterior

### 17. Paridad como Código

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio`
- `Descripción`: convertir la paridad con MongoDB real en una infraestructura declarativa, donde los casos diferenciales vivan como datos reutilizables y el runner pueda filtrarlos, listarlos y ejecutarlos por versión objetivo.
- `Motivación`: los tests diferenciales manuales eran útiles, pero demasiado acoplados a un `TestCase` concreto y poco preparados para crecer por versión y por families semánticas.
- `Aporte real`: la paridad ya se puede mantener como un inventario explícito de casos, reutilizable por CI y por tooling local, en vez de una colección de tests artesanales.
- `Aplicado ya`:
  - `9f39fe5` `test: codify real mongodb parity cases`

### 18. Catálogo Formal de Errores MongoDB

- `Estado`: `Aplicado`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio`
- `Descripción`: centralizar códigos, `codeName` y `errorLabels` por descriptor de error, y hacer que las excepciones compatibles con PyMongo se construyan a partir de ese catálogo.
- `Motivación`: la compatibilidad no es solo éxito funcional; también depende de fallar con shape y metadatos estables.
- `Aporte real`: la paridad de errores deja de depender de clases con lógica dispersa y pasa a apoyarse en una fuente formal y extensible.
- `Aplicado ya`:
  - `e2314a8` `refactor: formalize mongo error descriptors`

### 19. Spill-to-Disk Formal para Agregación Bloqueante

- `Estado`: `Aplicado con matices`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio`
- `Descripción`: introducir una política explícita de spill para stages bloqueantes de agregación, desacoplada del engine y propagada también a pipelines anidados.
- `Motivación`: la agregación en memoria necesitaba un subsistema formal para poder descargar resultados intermedios sin mezclar esa lógica con cada stage o cursor.
- `Aporte real`: existe ya una frontera clara de spill (`AggregationSpillPolicy`), la agregación puede usarla de forma uniforme en stages bloqueantes y facets/joins anidados, y `allowDiskUse` ya forma parte del contrato de `AggregateOperation`, de `aggregate()` y de `Database.command({"aggregate": ...})`.
- `Cierre`: queda con matices porque el backend actual hace round-trip temporal a disco pero no persigue aún una reducción agresiva de memoria por streaming fino; la arquitectura ya está preparada para evolucionar ahí sin reabrir el core ni volver a tocar la API pública.

### 20. Fidelidad BSON Escalar en Decodificación Interna y Updates

- `Estado`: `Aplicado con matices`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: conservar wrappers BSON en la rehidratación interna de documentos y usar helpers aritméticos BSON-aware en updates numéricos, degradando a tipos públicos solo en el borde de salida.
- `Motivación`: la fidelidad BSON no debía depender de que todo el core trabajase siempre con `int`/`float` nativos, pero tampoco convenía exponer wrappers al usuario.
- `Aporte real`: engines, codec y updates numéricos ya distinguen mejor entre representación interna BSON y representación pública Python, y esa misma frontera ya se ha extendido también a conversiones, expresiones escalares y rutas numéricas menos frecuentes de agregación.
- `Cierre`: queda con matices porque la fidelidad total todavía requiere seguir llevando esta lógica a más casos finos de tipos BSON y a más semántica de servidor real, pero la frontera arquitectónica correcta ya existe y la salida pública ya no expone wrappers por accidente.

## Correcciones de Cierre por Revisión Estricta

Estas líneas no sustituyen al refactor base ya hecho, pero sí corrigen el exceso de optimismo de cierres previos cuando se usa un listón arquitectónico más exigente.

### A. IR de Escritura Real Hasta el Engine

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Alto`
- `Descripción`: hacer que el engine ejecute una IR de escritura cerrada, sin recibir `update_spec` raw como semántica residual.
- `Motivación`: hoy la lectura está mejor desacoplada que la escritura.
- `Aporte real`: deja la frontera API/core/engine verdaderamente homogénea.
- `Cierre`: los engines propios ejecutan ya actualizaciones desde `UpdateOperation` y `CompiledUpdatePlan`; `update_matching_document(...)` queda como shim de compatibilidad que compila y reenvía, no como camino semántico principal.

### B. Descomposición Real de Agregación por Familias o Stages

- `Estado`: `Reabierto`
- `Impacto`: `Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: dividir la lógica aún concentrada en `src/mongoeco/core/aggregation/runtime.py` en submódulos por families o stages.
- `Motivación`: el sistema de handlers existe, pero el runtime sigue siendo demasiado grande.
- `Aporte real`: baja el riesgo de mantenimiento y hace creíble la extensibilidad a largo plazo.

### C. Pushdown/Spill como Subsistema Formal

- `Estado`: `Reabierto`
- `Impacto`: `Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: separar y cerrar de forma explícita dos líneas distintas:
  1. pushdown seguro y negociado,
  2. spill-to-disk para etapas bloqueantes.
- `Motivación`: hoy el pushdown SQLite sí existe, pero el spill-to-disk no está.
- `Aporte real`: mejora fidelidad, observabilidad y comportamiento bajo carga.

### D. Fidelidad BSON Escalar Total

- `Estado`: `En progreso`
- `Impacto`: `Alto`
- `Esfuerzo`: `Alto`
- `Descripción`: completar la capa de wrappers BSON para que las rutas críticas del core operen con semántica estricta de tipos y no solo con una base inicial.
- `Motivación`: ya existe una buena fundación para `Int32`, `Int64`, `Double` y `Decimal128`, pero todavía no es una fidelidad numérica total en todas las rutas.
- `Aporte real`: reduce divergencias con MongoDB real en comparaciones, coerciones y overflows.

## Siguiente Ola Arquitectónica

Estas mejoras ya no forman parte del refactor base original, pero sí son la continuación más coherente si el objetivo pasa de "arquitectura nacida bien" a "arquitectura preparada para acercarse a MongoDB real y a usos más amplios". Se listan en orden recomendado de aplicación.

### 10. Validación de Esquemas con Paridad MongoDB (`$jsonSchema`)

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: integrar validación de `$jsonSchema` en los planes de escritura para que la semántica de aceptación/rechazo de documentos no dependa del backend.
- `Motivación`: hoy una app puede insertar datos que una instancia real rechazaría si la colección usa validación de esquema.
- `Aporte real`: endurece la promesa de paridad de escritura y convierte la validación en parte explícita del core semántico.
- `Aplicado ya`:
  - `2b84f91` `feat: add collection json schema validation`
- `Cierre`: las opciones de colección compilan un validador compartido, la validación se ejecuta desde el core semántico común y ambos engines la aplican de forma consistente en inserts, updates, replacements y upserts.

### 11. Telemetría Interna y `system.profile`

- `Estado`: `Aplicado`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: registrar latencia, memoria, decisiones de pushdown/fallback y fases de ejecución internas, y exponerlo con una semántica tipo `system.profile`.
- `Motivación`: `explain()` ayuda a entender el plan, pero no basta para entender costes reales ni regresiones de rendimiento.
- `Aporte real`: da observabilidad de runtime, facilita diagnósticos y prepara mejor explain/planning futuros.
- `Aplicado ya`:
  - `ec82006` `feat: add profiling telemetry and system profile`
- `Cierre`: ambos engines exponen `profile` y `system.profile`, y la telemetría se registra desde una frontera común de comandos, lecturas materializadas y escrituras, incluyendo lineage y fallbacks cuando están disponibles.

### 12. MVCC Virtual y Aislamiento Más Fiel

- `Estado`: `Aplicado con matices`
- `Impacto`: `Muy Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: introducir una capa de versionado lógico de documentos o snapshots para emular con más fidelidad `readConcern`, visibilidad y aislamiento transaccional.
- `Motivación`: el comportamiento concurrente real de MongoDB no coincide por completo con memoria/SQLite apoyados solo en el backend subyacente.
- `Aporte real`: reduce la distancia con MongoDB real en una de las zonas más difíciles: consistencia y concurrencia.
- `Aplicado ya`:
  - `02d96a4` `refactor: add explicit mvcc engine state`
- `Cierre`: existe ya estado MVCC explícito por engine y sesión, con snapshots aislados reales en `MemoryEngine` y contexto MVCC formal en `SQLiteEngine`. El cierre sigue con matices porque la emulación todavía no pretende igualar toda la semántica distribuida de MongoDB real.

### 13. SDK de Extensión para Operadores y Stages

- `Estado`: `Aplicado`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Alto`
- `Descripción`: permitir registrar operadores, expresiones o stages externos sin tocar el código fuente principal.
- `Motivación`: con el catálogo y la agregación ya modularizados, el siguiente paso natural es abrir extensión controlada.
- `Aporte real`: convierte `mongoeco` en plataforma extensible y no solo en implementación cerrada.
- `Aplicado ya`:
  - `59c8dcc` `feat: add aggregation extension sdk`
- `Cierre`: la agregación expone ya un registro formal de operadores de expresión y stages, con alta/baja explícita, decoradores de registro y reconocimiento desde runtime y planificación relajada, sin depender de tocar el catálogo base ni de subclasificar dialectos.

### Orden Consolidado Después de las Tres Primeras

Una vez completadas `10`, `11` y `12`, el siguiente paso arquitectónico recomendado es:

1. **SDK de extensión para operadores y stages**
2. **motor de indexación virtual de alta fidelidad**
3. **proxy server con MongoDB wire protocol**

Razón:

- para entonces ya habría:
  - validación semántica más fuerte,
  - observabilidad seria,
  - y una base transaccional/concurrente bastante más madura;
- eso deja las fronteras internas mucho más estables;
- y solo entonces compensa abrir la arquitectura a terceros sin congelar APIs demasiado pronto.

No conviene adelantar:

- el **motor de indexación virtual**, porque es potentísimo pero muy caro e invasivo;
- el **wire protocol proxy**, porque eso ya se parece más a una plataforma/producto que a una mejora de arquitectura base del core.

### 14. Motor de Indexación Virtual de Alta Fidelidad

- `Estado`: `Aplicado con matices`
- `Impacto`: `Muy Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: desacoplar la semántica de indexación MongoDB del índice físico del backend, especialmente para multi-key, sparse y parciales.
- `Motivación`: SQLite no representa de forma nativa la semántica exacta de índices MongoDB.
- `Aporte real`: mejora explain, selección de planes, fidelidad de hint y comportamiento de índices complejos.
- `Aplicado ya`:
  - `88883ee` `feat: add virtual index engine semantics`
- `Cierre`: memoria y SQLite comparten ya una capa virtual para semántica `sparse` y `partialFilterExpression`, tanto en metadata como en unicidad, hints, persistencia y `explain`, con inferencia más conservadora y reusable de cuándo una query puede apoyarse en esa semántica virtual. Lo dejo con matices porque el salto final de fidelidad todavía pediría un indexador físico/lógico más ambicioso para planificación automática y escenarios multikey avanzados.

### 15. Proxy Server con MongoDB Wire Protocol

- `Estado`: `Aplicado`
- `Impacto`: `Muy Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: exponer `mongoeco` como servidor compatible con el protocolo MongoDB para usar drivers oficiales de otros lenguajes.
- `Motivación`: hoy el proyecto está limitado al ecosistema Python.
- `Aporte real`: abre el proyecto a Java, Go, Node.js, C# y cualquier cliente compatible con MongoDB sin reimplementar el core.
- `Aplicado ya`:
  - proxy async mínimo en `src/mongoeco/wire/`
  - soporte real para `OP_QUERY`/`OP_REPLY` legacy de handshake y `OP_MSG`
  - bridge BSON hacia los tipos internos del core
  - registro explícito de capacidades wire por comando
  - servicio de handshake/capability advertisement separado del executor
  - `WireSurface` explícita para opcodes, comandos soportados, límites y sesiones/transacciones
  - contexto de conexión wire explícito, con `connectionId`, metadata de cliente y compresión
  - subsistema de ejecución y store de cursores wire desacoplados del adaptador TCP
  - soporte real para `getMore` y `killCursors` sobre cursores materializados básicos
  - store de sesiones wire con traducción de `lsid` a `ClientSession`
  - `WireRequestContext` y dispatcher explícito por capacidades, sin branching disperso en el adaptador TCP
  - validación protocolaria explícita de flags y tamaños en `OP_MSG` y `OP_QUERY`
  - normalización explícita de `OP_QUERY` legacy con `$query`/`$orderby` y propagación de `numberToReturn` a tamaños de batch
  - mayor superficie real probada a través del proxy para índices, writes, `findAndModify`, cursores y comandos cursorizados
  - integración validada con `pymongo.MongoClient`
- `Cierre`: el bloque arquitectónico queda cerrado. La superficie sigue siendo seleccionada, pero ya no es solo una base mínima: el proxy tiene normalización legacy explícita, store de cursores/sesiones real, surface declarativa, handshake separado, contexto de conexión, contexto de petición y executor desacoplado del adaptador TCP, de forma que crecer en comandos soportados ya no exige reabrir el diseño.

### 16. Políticas de Comportamiento 100% Derivadas del Catálogo

- `Estado`: `Aplicado con matices`
- `Impacto`: `Muy Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: hacer que los dialectos oficiales construyan su `BehaviorPolicy` desde una `policy_spec` declarativa en el catálogo, en vez de depender de lógica embebida en clases concretas.
- `Motivación`: añadir nuevas versiones no debería implicar ir sembrando `if version >= ...` por el core ni mantener semántica duplicada entre catálogo y clases.
- `Aporte real`: convierte al catálogo en fuente no solo de superficie, sino también de deltas semánticos finos, y prepara el sistema para futuras versiones sin dolor.
- `Aplicado ya`:
  - `520178d` `refactor: add behavior policies and read execution lineage`
- `Cierre`: los dialectos oficiales ya consumen una `policy_spec` declarativa exportable y los engines dejan de depender de condicionales por identidad de versión en rutas básicas de proyección/ordenación. Lo dejo con matices porque la fundación ya está, pero la ganancia máxima llegará cuando haya más deltas semánticos versionados viviendo en esa spec en vez de en flags sueltos.

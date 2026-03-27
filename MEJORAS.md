# MEJORAS

Este documento ya no es solo una lista teórica. Ahora recoge:

1. las mejoras arquitectónicas identificadas para llevar `mongoeco` hacia un diseño "como si hubiera nacido desde cero";
2. qué partes ya están aplicadas en el repositorio;
3. cuál es el siguiente orden razonable para seguir refactorizando sin perder el estado verde de la suite.

Escala usada:

- `Estado`: `Aplicado`, `En progreso`, `Pendiente`
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
- `Cierre`: la frontera basada en operaciones compiladas ya cubre el flujo principal y los helpers auxiliares relevantes: `find`, `count`, `distinct`, `aggregate`, `explain`, selección previa de writes y rutas admin dejan de recomponer operaciones a mano y pasan por planes explícitos compartidos.

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
- `Cierre`: los updates ya siguen un flujo formal de paths compilados, contexto explícito, planes reutilizables y aplicaciones resueltas antes de mutar también en operadores de arrays y rutas posicionales complejas; la semántica de ejecución ya no depende de caminos especiales fuera de ese pipeline.

## 6. Compatibilidad y Tooling Derivados Automáticamente

- `Estado`: `En progreso`
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
- `Pendiente para cerrar de verdad`:
  - derivar más helpers de runtime desde el catálogo maestro
  - publicar snapshots o artefactos versionados para tooling externo

## 7. Separación Más Fuerte entre Core Semántico y Ejecución por Engine

- `Estado`: `Pendiente`
- `Impacto`: `Alto`
- `Esfuerzo`: `Alto`
- `Descripción`: endurecer la frontera entre semántica MongoDB y ejecución concreta en `MemoryEngine`/`SQLiteEngine`.
- `Motivación`: a medida que crecen query, update y aggregation, pequeñas decisiones del engine pueden contaminar la semántica observable.
- `Aporte real`: mejora la paridad entre engines y reduce divergencias sutiles.

## 8. Estado Transaccional Explícito por Sesión y Engine

- `Estado`: `Pendiente`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: modelar explícitamente `SessionState`, `TransactionState` y `EngineTransactionContext`.
- `Motivación`: las transacciones son una zona donde lifecycle, ownership y limpieza de estado importan mucho más que en CRUD simple.
- `Aporte real`: simplifica el razonamiento sobre transacciones y reduce inconsistencias difíciles de detectar.

## 9. Agregación Enchufable por Stages o Handlers Registrados

- `Estado`: `Pendiente`
- `Impacto`: `Alto`
- `Esfuerzo`: `Muy Alto`
- `Descripción`: rediseñar la agregación como un sistema de handlers registrados o stages compilados, donde cada etapa tenga contrato propio.
- `Motivación`: la agregación es una de las áreas con más complejidad acumulada y crecimiento monolítico.
- `Aporte real`: simplifica extensión, reduce regresiones cruzadas y hace más mantenible seguir ampliando analítica avanzada.

## Orden Recomendado A Partir de Ahora

Si el objetivo es que el proyecto termine pareciendo diseñado desde cero, el siguiente orden recomendado es:

1. cerrar `Compatibilidad y Tooling Derivados Automáticamente`;
2. reevaluar y, si compensa, abrir `Separación Más Fuerte entre Core Semántico y Ejecución por Engine`;
3. modelar `Estado Transaccional Explícito por Sesión y Engine`;
4. dejar `Agregación Enchufable por Stages o Handlers Registrados` como el gran refactor final.

## Hoja de Ruta Operativa Inmediata

Para ejecutar lo pendiente sin perder el estado verde ni reabrir semántica a lo loco, el siguiente bloque operativo recomendado es:

### Iteración 1. Compatibilidad Derivada del Catálogo

- `Estado`: `Pendiente`

- `Objetivo`: terminar de derivar helpers y artefactos de compatibilidad directamente del catálogo maestro.
- `Criterio de hecho`:
  - más helpers de runtime salen del catálogo central;
  - los snapshots/artefactos versionados sirven también para tooling externo;
  - documentación y runtime reducen todavía más cualquier drift manual.
- `Commit esperado`: `refactor: derive compatibility tooling from catalog`

### Iteración 2. Frontera Semántica vs Engines

- `Estado`: `Pendiente`

- `Objetivo`: decidir si ya compensa endurecer más la separación entre semántica MongoDB y ejecución concreta en engines.
- `Criterio de hecho`:
  - revisión explícita del drift residual entre `MemoryEngine` y `SQLiteEngine`;
  - decisión documentada sobre qué semántica debe vivir solo en el core;
  - si se aborda, primer corte sin romper la suite verde.
- `Commit esperado`: `refactor: separate semantic core from engine execution`

### Iteración 3. Estado Transaccional Explícito

- `Estado`: `Pendiente`

- `Objetivo`: modelar de forma explícita estado de sesión, transacción y contexto engine.
- `Criterio de hecho`:
  - existen `SessionState`, `TransactionState` o equivalentes con roles bien separados;
  - la propiedad de la transacción y su limpieza dejan de depender de hooks genéricos dispersos;
  - la semántica observable actual se conserva.
- `Commit esperado`: `refactor: model explicit transaction state`

### Iteración 4. Agregación Enchufable

- `Estado`: `Pendiente`

- `Objetivo`: transformar la agregación monolítica en handlers o stages con contrato propio.
- `Criterio de hecho`:
  - cada familia de stage tiene punto de entrada propio;
  - el pipeline principal reduce branching monolítico;
  - la extensión de nuevas etapas deja de requerir tocar un único bloque gigante.
- `Commit esperado`: `refactor: split aggregation into stage handlers`

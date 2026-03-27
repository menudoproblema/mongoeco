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

- `Estado`: `En progreso`
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
- `Pendiente para cerrar de verdad`:
  - reducir todavía más lógica administrativa residual en `Database`
  - separar mejor ejecución interna y serialización pública de comandos
  - consolidar un dispatcher tipado todavía más homogéneo entre async y sync

## 3. Tipado Estricto en el Core Semántico y la Metadata Interna

- `Estado`: `En progreso`
- `Impacto`: `Medio-Alto`
- `Esfuerzo`: `Medio-Bajo`
- `Descripción`: sustituir `dict[str, Any]` internos por dataclasses o records privados en piezas críticas como índices, stats, explain, resultados administrativos y estados internos complejos.
- `Motivación`: los diccionarios internos vuelven opaca la estructura real y facilitan drift entre layers y engines.
- `Aporte real`: más claridad interna, menos errores estructurales y mejor base para contributors o nuevos backends.
- `Aplicado ya`:
  - `6076c4e` `refactor: type admin metadata contracts`
  - `33caa69` `refactor: type admin command metadata internals`
  - `cee0689` `refactor: type internal admin command results`
  - `0f399ff` `refactor: type explain payloads internally`
- `Pendiente para cerrar de verdad`:
  - reducir más `dict[str, object]` internos en la capa admin
  - extender el mismo patrón tipado a más payloads internos fuera de admin/explain

## 4. Arquitectura Basada en Planes de Operación

- `Estado`: `En progreso`
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
- `Pendiente para cerrar de verdad`:
  - extender la frontera planificada hasta más rutas de engine
  - consolidar mejor explain/admin sobre operaciones ya compiladas

## 5. Motor de Updates Formal Basado en Paths Compilados

- `Estado`: `En progreso`
- `Impacto`: `Alto`
- `Esfuerzo`: `Medio-Alto`
- `Descripción`: separar formalmente parsing de rutas, binding posicional, resolución de `arrayFilters` y aplicación de operadores de update sobre paths compilados.
- `Motivación`: la semántica de updates con arrays y rutas profundas es una de las zonas más propensas a errores sutiles.
- `Aporte real`: base mucho más estable para operadores complejos y menos fragilidad al ampliar soporte sobre arrays.
- `Aplicado ya`:
  - `a41ca5b` `refactor: compile update paths explicitly`
  - `1e4b40e` `refactor: separate update target resolution from mutation`
  - `58e7bbc` `refactor: compile update operator instructions`
- `Pendiente para cerrar de verdad`:
  - separar más claramente validación, resolución y aplicación final
  - formalizar mejor el estado de ejecución de un update complejo

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

1. seguir empujando `FindOperation` / `UpdateOperation` / `AggregateOperation` hasta la frontera con engines;
2. derivar más helpers y decisiones de runtime directamente del catálogo de compatibilidad;
3. reducir la lógica administrativa residual que aún vive en `Database` o en servicios mixtos;
4. seguir formalizando el estado de ejecución de updates complejos;
5. después volver a evaluar si ya compensa endurecer la frontera semántica/engine.

## Hoja de Ruta Operativa Inmediata

Para ejecutar lo pendiente sin perder el estado verde ni reabrir semántica a lo loco, el siguiente bloque operativo recomendado es:

### Iteración 1. Frontera API → Engine

- `Objetivo`: que `FindOperation`, `UpdateOperation` y `AggregateOperation` no se queden solo en la API y empiecen a cruzar la frontera con los engines de forma explícita.
- `Criterio de hecho`:
  - los engines aceptan operaciones compiladas en más rutas de lectura/escritura;
  - la API deja de desempaquetar parte de esas operaciones en parámetros sueltos;
  - `explain` y `count` reutilizan esa misma frontera.
- `Commit esperado`: `refactor: route compiled operations into engines`

### Iteración 2. Estado Explícito de Updates

- `Objetivo`: introducir un estado/contexto de ejecución de updates más formal para que validación, resolución de targets y aplicación final no dependan de una larga lista de argumentos.
- `Criterio de hecho`:
  - existe un contexto de ejecución tipado;
  - los operadores principales lo consumen;
  - `arrayFilters`, selector original y flags de upsert viven en un solo sitio.
- `Commit esperado`: `refactor: add explicit update execution context`

### Iteración 3. Runtime Derivado del Catálogo

- `Objetivo`: seguir moviendo decisiones de runtime que aún dependen de overrides manuales hacia el catálogo de compatibilidad.
- `Criterio de hecho`:
  - hooks/version flags relevantes salen del catálogo;
  - `capabilities` y `behavior_flags()` reducen duplicación manual;
  - tests de compatibilidad fijan que runtime y catálogo siguen alineados.
- `Commit esperado`: `refactor: derive runtime compatibility hooks from catalog`

### Iteración 4. Dispatcher Admin Más Homogéneo

- `Objetivo`: cerrar mejor la separación entre parseo, ejecución y serialización pública de comandos admin, evitando caminos ligeramente distintos entre async y sync.
- `Criterio de hecho`:
  - async y sync reutilizan el mismo punto de serialización;
  - el servicio de comandos expone un camino explícito para ejecutar y serializar documentos;
  - `Database` sigue adelgazando.
- `Commit esperado`: `refactor: unify admin command document execution`

### Iteración 5. Revisión de Frontera Semántica

- `Objetivo`: reevaluar si, después de las cuatro iteraciones anteriores, ya compensa endurecer más la frontera entre core semántico y ejecución por engine.
- `Criterio de hecho`:
  - revisión explícita de drift residual entre `MemoryEngine` y `SQLiteEngine`;
  - decisión informada sobre si abrir ya el bloque grande de separación semántica/engine.
- `Commit esperado`: no obligatorio; depende del resultado de la revisión.

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
  - `1681e98` `refactor: parse typed admin commands before execution`
- `Pendiente para cerrar de verdad`:
  - simetría completa en la capa sync
  - resultados administrativos internos tipados de forma homogénea
  - separar mejor parseo, ejecución y serialización pública de comandos

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
- `Pendiente para cerrar de verdad`:
  - tipar resultados internos de comandos admin más allá de stats
  - tipar explain/admin payloads internos con records privados
  - reducir más `dict[str, object]` internos en la capa admin

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
- `Pendiente para cerrar de verdad`:
  - `AggregateOperation`
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
- `Pendiente para cerrar de verdad`:
  - compilar también instrucciones de update por operador
  - separar más claramente validación, resolución y aplicación final
  - formalizar mejor el estado de ejecución de un update complejo

## 6. Compatibilidad y Tooling Derivados Automáticamente

- `Estado`: `Pendiente`
- `Impacto`: `Medio`
- `Esfuerzo`: `Medio`
- `Descripción`: derivar automáticamente desde el catálogo de compatibilidad los helpers `supports_*`, snapshots, exports JSON/Markdown y documentación técnica.
- `Motivación`: aunque ya hay catálogo central, todavía no toda la observabilidad del soporte sale de él de forma automática.
- `Aporte real`: evita drift entre código, tests, documentación y tooling.

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

1. tipar también los resultados internos del subsistema admin;
2. llevar la simetría del subsistema admin a la capa sync;
3. introducir `AggregateOperation` y seguir ensanchando la frontera de planes explícitos;
4. compilar también las instrucciones de update por operador;
5. después volver a evaluar si ya compensa endurecer la frontera semántica/engine.

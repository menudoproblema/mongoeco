# ADR-012 - Planning Search mediante efectos semanticos

## Contexto

Las ventanas directas, los prefijos iterativos y los filtros downstream son
correctos solo bajo precondiciones sobre cardinalidad, orden, metadata y
writeback. Mantener esas precondiciones como listas de operadores dentro del
cursor dificulta demostrar una optimizacion y explicar por que se rechazo.

## Decision

Un planner Search dedicado compila el pipeline posterior a Search. Cada stage
aporta efectos tipados sobre cardinalidad, orden, monotonia, dominio
(`document`, `stream`, `full-set`, `writeback` o `unknown`), dependencias de
metadata, materializacion, collectors y writeback. Las dependencias se
reconocen por referencias exactas, no por heuristicas sobre nombres.

El resultado es un `SearchPipelinePlan` inmutable que posee estrategia,
ventana, limite, filtro downstream, residual, ownership de fases, reglas
aplicadas y rechazos normalizados. La misma instancia se entrega a ejecucion,
expansion iterativa y `explain()`.

Existe una estrategia interna de referencia que desactiva shortcuts y sirve de
oracle. Toda regla optimizada debe producir el mismo resultado, orden,
metadata, collectors, errores, writeback y eventos que esa estrategia. El
oracle usa una semilla reproducible y compara contadores solo cuando comparten
dominio semantico; los contadores fisicos pueden diferir por diseno.

## Consecuencias

- Los engines ejecutan un request ya planificado y no reinterpretan el
  pipeline posterior.
- `explain()` describe la decision real, no una reconstruccion paralela.
- Las reglas tienen identificadores estables y pruebas positivas y negativas.
- El planner permanece acotado a Search; no se crea un planner universal de
  aggregation en 4.6.

## Alternativas descartadas

- Seguir ampliando condiciones privadas en el cursor.
- Hacer que Memory y SQLite decidan independientemente el top-k seguro.
- Mantener un registro de reglas cuya prioridad sustituya a precondiciones
  semanticas demostrables.

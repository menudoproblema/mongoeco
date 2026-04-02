# ADR-004 - Engines con protocolos delgados

## Contexto

`MemoryEngine` y `SQLiteEngine` comparten bastante semantica, pero su modelo
fisico y sus rutas de ejecucion no son iguales. Una superclase monolitica
habria obligado a compartir demasiado.

## Decision

Definir el contrato de engine como composicion de protocolos delgados por
capacidad: CRUD, indices, admin, explain, planning, profiling, sesiones,
search-index admin.

## Consecuencias

- Los engines comparten solo lo necesario.
- La arquitectura es mas flexible para extraer helpers compartidos puntuales.
- El contrato se expresa por capacidad, no por herencia forzada.

## Alternativas descartadas

- Una superclase abstracta grande con hooks para todo.
- Duplicacion total de contratos sin protocolos comunes.
- Compartir reads/writes complejos a la fuerza entre engines muy distintos.

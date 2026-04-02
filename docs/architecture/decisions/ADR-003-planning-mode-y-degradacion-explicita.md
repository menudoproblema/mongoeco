# ADR-003 - `planning_mode` y degradacion explicita

## Contexto

Un runtime local con soporte parcial de semantica MongoDB necesita decidir que
hacer cuando recibe shapes no ejecutables o solo parcialmente soportados.

Los no-ops silenciosos o los fallbacks implicitos son dificiles de detectar y
rompen la honestidad contractual.

## Decision

Mantener dos politicas explicitas:

- `PlanningMode.STRICT` para fallo temprano;
- `PlanningMode.RELAXED` para conservar metadata y exponer `planning_issues`.

## Consecuencias

- Los limites del runtime se vuelven inspeccionables.
- `explain`, tooling y superficies de compatibilidad pueden degradar sin ocultar
  el gap.
- El sistema necesita fijar en tests los shapes de degradacion y mensajes
  publicos.

## Alternativas descartadas

- Fallbacks silenciosos a paths mas debiles.
- No-op cuando el shape no es soportado.
- Un unico modo siempre estricto o siempre degradado.

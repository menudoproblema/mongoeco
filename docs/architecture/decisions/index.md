# ADRs de arquitectura

## Formato

Cada ADR recoge:

- **Contexto**
- **Decision**
- **Consecuencias**
- **Alternativas descartadas**

El objetivo no es contar toda la historia del repositorio, sino congelar las
decisiones que siguen vivas y explican la arquitectura actual.

## Indice

- [ADR-001 - Async-first y superficie sync adaptadora](ADR-001-async-first-y-sync-adaptador.md)
- [ADR-002 - Dialecto MongoDB y perfil PyMongo como ejes distintos](ADR-002-dialecto-y-perfil-como-ejes-distintos.md)
- [ADR-003 - `planning_mode` y degradacion explicita](ADR-003-planning-mode-y-degradacion-explicita.md)
- [ADR-004 - Engines con protocolos delgados](ADR-004-engines-con-protocolos-delgados.md)
- [ADR-005 - `SQLiteEngine` modular y `MemoryEngine` como baseline](ADR-005-sqlite-modular-y-memory-como-baseline.md)
- [ADR-006 - Change streams locales con persistencia opcional](ADR-006-change-streams-locales-y-persistencia-opcional.md)
- [ADR-007 - SDAM parcial y explicito](ADR-007-sdam-parcial-y-explicito.md)
- [ADR-008 - `PyICU` opcional y `pyuca` como fallback](ADR-008-pyicu-opcional-y-pyuca-como-fallback.md)
- [ADR-009 - Parity tests como politica de aceptacion](ADR-009-parity-tests-como-politica-de-aceptacion.md)

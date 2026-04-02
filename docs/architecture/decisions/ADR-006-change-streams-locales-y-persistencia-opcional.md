# ADR-006 - Change streams locales y persistencia opcional

## Contexto

El proyecto necesitaba `watch()` util para desarrollo local y testing, pero no
pretendia convertirse en una infraestructura distribuida de streams.

## Decision

Implementar change streams como runtime local con:

- historial retenido en memoria y acotado;
- persistencia opcional a journal local;
- reanudacion dentro del mismo entorno local;
- introspeccion explicita del backend y del estado;
- sin soporte distribuido entre nodos.

## Consecuencias

- El contrato es util y honesto para local/testing.
- `resume_after` y `start_after` pueden sobrevivir a recreacion de cliente con
  journal.
- El sistema evita prometer semantica de cluster que no implementa.

## Alternativas descartadas

- No soportar change streams.
- Simular una semantica distribuida no respaldada por la implementacion.
- Persistencia obligatoria siempre.

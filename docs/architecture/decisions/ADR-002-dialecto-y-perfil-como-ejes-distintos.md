# ADR-002 - Dialecto MongoDB y perfil PyMongo como ejes distintos

## Contexto

La compatibilidad observable del proyecto mezcla dos problemas distintos:

- semantica de servidor MongoDB;
- superficie publica del driver Python.

Atarlos a una sola opcion haria imposible explicar muchos deltas reales.

## Decision

Modelar `mongodb_dialect` y `pymongo_profile` como ejes separados, ambos con
catalogos, resoluciones e introspeccion explicita.

## Consecuencias

- Se puede razonar por separado sobre semantica de servidor y API Python.
- La documentacion y los tests pueden fijar mejor el origen de cada delta.
- La arquitectura gana claridad a costa de mas metadata explicita en la
  superficie publica.

## Alternativas descartadas

- Inferir compatibilidad solo desde la version instalada de `pymongo`.
- Unificar dialecto y perfil en una sola configuracion.
- No modelar compatibilidad de forma explicita.

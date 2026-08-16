# ADR-014 - Informes de conformidad versionados

## Contexto

Un resultado booleano no distingue un check no aplicable, un incumplimiento,
un error del runner o un fallo de cleanup. Tampoco permite evolucionar el
formato de CI independientemente de la version del SPI comprobado.

## Decision

El kit publico mantiene un runner sin dependencia de pytest y emite un informe
con `schemaVersion` separado de `contractVersion`. Cada check tiene ID estable,
capability, fase, estado, duracion, evidencia y errores de cleanup separados.

Las capabilities seleccionan escenarios automaticamente. Una capability
ausente produce `not-applicable`; una capability declarada que incumple su
contrato produce `failed`. Errores de infraestructura producen `error`.

Factories publicas construyen contextos, relojes, snapshots, outcomes,
barreras, cancelacion, batches parciales, change delivery y Search. Pytest es
solo un adaptador de asercion y presentacion.

Los fallos esperables del contrato de una capability declarada se normalizan a
`failed`, aunque el engine los exprese como `TypeError`, `ValueError` o error
de batch. `error` queda reservado a infraestructura o defectos del runner. La
matriz negativa cubre capabilities falsas, snapshots mutables o live, batches
incompletos y eventos duplicados, ademas de los defectos de atomicidad y Search.

## Consecuencias

- El informe tiene serializacion JSON determinista y resumen humano.
- Checks y cleanup nunca se ocultan entre si.
- El propio kit se valida con engines defectuosos de fault injection.
- El schema puede evolucionar sin fingir una nueva version de SPI.

## Alternativas descartadas

- Omitir silenciosamente checks no aplicables.
- Acoplar parametrizacion y semantica contractual a pytest.
- Inferir soporte a partir de la presencia de metodos.

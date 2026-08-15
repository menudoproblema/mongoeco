# ADR-010 - `search-v1` como contrato local estable

## Contexto

MongoEco acepta una sintaxis inspirada en Atlas Search, pero ejecuta un
runtime local sobre Memory, Python, FTS5 y usearch. Presentar esa superficie
como paridad completa con Atlas ocultaria diferencias de backend; inventar un
dialecto sin relacion con MongoDB reduciria en cambio su utilidad.

La implementacion provisional mezcla hoy tres responsabilidades:

- compilacion y matching de queries;
- materializacion de hits y metadata;
- previews de ejecucion usados por `explain()`.

`$searchMeta` materializa todos los hits antes de calcular collectors y
`highlight` anade un campo de resultado con nombre fijo. Esas decisiones no
deben convertirse en el contrato estable por accidente.

## Decision

MongoEco publica `search-v1` como contrato local estable. El contrato:

- acepta sintaxis Atlas-like solo dentro del subset documentado;
- garantiza la misma semantica entre Memory y SQLite;
- documenta de forma explicita toda diferencia respecto a Atlas;
- separa hits, metadata, plan y estadisticas de ejecucion;
- usa `OperationContext` como unica frontera temporal y semantica;
- permite optimizaciones fisicas solo cuando prueban equivalencia con el
  runtime semantico comun;
- mantiene `countPreview`, `facetPreview` y `highlightPreview` como aliases
  deprecados durante 4.x, sin ampliarlos;
- reserva la retirada de esos aliases y de la inyeccion automatica legacy de
  `searchHighlights` para 5.0.0.

El contrato normativo de valores, collectors, highlight y explain vive en
[`search-contract-v1.md`](../search-contract-v1.md).

## Consecuencias

- `$searchMeta` debe poder ejecutarse sin materializar hits publicos.
- Los engines reciben un request tipado y retornan un outcome estable, no
  listas o unions dependientes del modo.
- `queryPlanner` no ejecuta la consulta; `executionStats` ejecuta una sola vez.
- La metadata de highlight viaja internamente separada del documento y puede
  proyectarse mediante `$meta`.
- SQLite puede hacer pushdown de collectors solo cuando el plan no conserva
  residual semantico.
- La compatibilidad de Search es un contrato propio y no una afirmacion de
  compatibilidad con Atlas Search administrado.

## Alternativas descartadas

- Declarar paridad completa con Atlas Search sin ejecutar contra Atlas.
- Mantener un dialecto local sin relacion explicita con la sintaxis MongoDB.
- Estabilizar las previews actuales como API definitiva.
- Resolver count, facetas, highlight y explain mediante caminos de ejecucion
  independientes que puedan divergir entre engines.

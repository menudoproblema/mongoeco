# Release 4.5.0

Status: candidata; no etiquetada ni publicada.

## Resumen

MongoEco 4.5.0 estabiliza el contrato local `search-v1`: ejecucion tipada,
`$searchMeta`, highlight como metadata, explain con verbosidad y pushdown
SQLite exacto. Tambien publica el kit de conformidad SPI v2 y convierte los
diferenciales MongoDB 7/8 en una señal recurrente y reproducible.

SPI v1 sigue deprecado y operativo durante 4.x. Su retirada permanece
reservada para 5.0.0.

## Compatibilidad

- Python 3.13 y 3.14.
- Dialectos MongoDB 7.0 y 8.0.
- PyMongo 4.9 o superior.
- Engines externos SPI v2 sin Search no necesitan implementar la capability
  opcional.
- Engines Search 4.x basados en `search_documents()` siguen pasando por el
  adapter legacy, pero deben migrar a `SearchRequest` y
  `SearchExecutionOutcome` antes de 5.0.0.

Las previews `countPreview`, `facetPreview` y `highlightPreview` se conservan
como aliases deprecated durante 4.x. No deben usarse para integraciones nuevas.

## Evidencia

- Diferencial real MongoDB 8.0.24: 14/14 escenarios verdes.
- Python 3.13 y 3.14: suite completa obligatoria en CI sobre el wheel inmutable.
- Python 3.14: `3256` tests pytest, `15` skipped y `2356` subtests; cobertura
  `99.01%`, cumpliendo el gate estricto del 99%.
- Runner `unittest`: `3380 tests`, `2` skipped; ratchet Ruff verde sin ampliar
  la baseline historica.
- Python 3.13 local: el mismo wheel instalado en un entorno limpio importa
  desde `site-packages` y supera los `3380 tests` con `2` skipped.
- Matriz PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0: verde y sin deltas en el
  snapshot publicado.
- `search_meta_diagnostics` SQLite, 100/1.000/10.000 documentos: lower-bound
  estable en torno a 0,052 s a 10.000 documentos; fallback semantico en torno
  a 7,56 s; total y facets mantienen el camino exacto validado por contratos.
- Comparacion estabilizada frente a 4.4.0: 90 casos equivalentes, ninguna
  regresion simultaneamente superior al 20% y 5 ms, y ratios geometricos por
  engine entre -0,7% y +0,3%. Los workloads no soportados se registran como
  `SKIPPED`, no como fallos del engine.
- Wheel y sdist pasan `twine check`, smoke desde `site-packages` y conformidad
  SPI v2 sobre Memory/SQLite. Dos builds con el mismo epoch producen bytes
  identicos.
- Los SHA-256 finales de wheel y sdist se generan en `SHA256SUMS` por el job de
  build despues del commit/tag final. No se consideran validos hashes de
  artefactos locales construidos sobre un worktree mutable.

MongoDB 7 no esta disponible en el entorno local y sigue siendo un gate externo
obligatorio del workflow diferencial antes de publicar. La configuracion del
Trusted Publisher en PyPI tambien debe verificarse externamente; no se atribuye
ninguna de esas dos evidencias mientras permanezcan pendientes.

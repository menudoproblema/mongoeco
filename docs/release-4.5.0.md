# Release 4.5.0

Status: publicada el 15 de agosto de 2026 con la etiqueta `v4.5.0` sobre el
commit `b20d4d150807a04c4f641fe0b1bd31149a3f1808`.

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

- Diferenciales reales MongoDB 7.0 y 8.0: verdes en el workflow de la etiqueta;
  MongoDB 8.0.24 mantiene ademas los 14/14 escenarios de la ejecucion local.
- Python 3.13 y 3.14: suite completa verde en CI sobre el wheel inmutable.
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
- SHA-256 publicados en PyPI: wheel
  `f168ab9f4172abbf1a7e35f8996c3e01463a26557b213028c83ef64d102a2fd3` y sdist
  `2c3f62a19d9c83370f5997b2175b1e39711569e1591a0e47594594e2b466375e`.

El workflow de la etiqueta supero build, imports minimos, Python 3.13/3.14,
cobertura, snapshots, benchmarks, smokes y diferenciales MongoDB 7/8. El
intercambio OIDC de publicacion fallo con `invalid-publisher` porque PyPI no
tenia registrado el Trusted Publisher para `menudoproblema/mongoeco`,
`.github/workflows/ci.yml` y el environment `pypi`. Wheel y sdist se
reconstruyeron dos veces desde el tag limpio con las constraints de CI y el
mismo `SOURCE_DATE_EPOCH`, se compararon byte a byte, pasaron `twine check` y
smokes independientes y se publicaron con la credencial local existente. El
smoke posterior instalo `mongoeco==4.5.0` desde `https://pypi.org/simple`.

# Release 4.6.0

Status: publicada el 17 de agosto de 2026 con la etiqueta `v4.6.0` sobre el
commit `306496d977b813bcbca006e125f68075818013b8`.

## Alcance

La candidata 4.6.0 consolida Search alrededor de cuatro contratos mantenibles:
provenance fuera del documento BSON, planning semantico compartido por runtime
y explain, observabilidad tipada y un kit de conformidad versionado. No cambia
el contrato `search-v1` ni elimina SPI v1; sus adapters 4.x siguen deprecados.
Ademas endurece el consumo externo mediante PEP 561, un engine canario SPI v2,
JSON Schema publico, propiedades generativas y diferenciales de agregacion.
Completa el bridge con catalogo de deprecaciones, manifest semantico de API,
fixture SQLite 4.5 y CLI publica de conformidad. La arquitectura futura queda
especificada, pero SPI v3 y `search-v2` no se publican en esta release.

## Compatibilidad

- Los documentos publicos conservan su shape. `searchHighlights` sigue siendo
  el alias virtual 4.x y `$meta: "searchHighlights"` la referencia explicita.
- Los campos planos de `executionStats` permanecen y se derivan del modelo
  canonico de estado, fases y metricas. Los callers legacy pueden omitir aun la
  evidencia de contexto/snapshot; las ejecuciones internas ya la exigen.
- `ConformanceCheckResult(..., passed=...)` sigue funcionando; los informes
  nuevos anaden schema, estados, evidencia y errores de cleanup.
- El namespace privado Search solo se acepta en la frontera legacy SPI v1 y no
  es una representacion publica ni persistible.

## Evidencia requerida antes del cierre

1. Suite completa con Memory y SQLite.
2. Paridad sync/async y oraculo Search optimizado/reference.
3. Lint, formato y type checking soportado.
4. Fixtures consumidoras PEP 561 y canario SPI v2 contra el wheel instalado.
5. Build reproducible de wheel y sdist, incluida inspeccion de `py.typed` y del
   JSON Schema publico.
6. Informes de conformidad JSON validos para ambos engines y el canario.
7. Property tests rapidos y profundos reproducibles.
8. Diferenciales MongoDB 7/8; mientras no haya servicio real, los casos nuevos
   permanecen marcados como captura pendiente y no se atribuyen como verdes.
9. Benchmark SQLite conservado; no se acepta el candidato que no mejora el
   baseline materialmente.
10. Catalogo y manifest reproducibles desde source y wheel instalado.
11. Fixture SQLite 4.5 verificable y reproducible, sin replay inventado.
12. CLI de conformidad ejecutada desde un directorio ajeno al checkout.

La evidencia versionable incluye el candidato `sql-prefilter` rechazado y un
perfil de 1.000 documentos con predicados, sort/window, Search y collectors.
No se incorpora un pushdown nuevo en 4.6.0: las reglas existentes se conservan
porque mantienen equivalencia y muestran mejora material; el candidato nuevo
fue ligeramente peor que Python.

El bump de version, el commit, la etiqueta y la publicacion se ejecutaron tras
la autorizacion expresa de cierre.

## Evidencia de cierre

Ejecucion final del 17 de agosto de 2026 sobre Python 3.13.13 y 3.14.5:

- `unittest`: 3.459 tests, 2 skipped en cada version de Python;
- `pytest`: 3.464 passed, 26 skipped y 2.509 subtests passed;
- cobertura: 99,00 %, cumpliendo el gate sin reducir el umbral;
- mypy estricto: fixture positiva verde y 14 errores negativos esperados
  contra el wheel instalado;
- conformidad: canario externo con 5 checks passed y 5 `not-applicable`, y
  Memory 4/4 en los smokes de wheel y sdist;
- lint ratchet: verde sobre 102 ficheros Python modificados; snapshots,
  `git diff --check` y benchmarks Memory/SQLite: verdes;
- wheel y sdist 4.6.0 reproducibles byte a byte y aceptados por `twine check`;
  smokes desde `site-packages` y entorno minimo sin PyMongo: verdes;
- hashes del candidato pre-tag: wheel
  `deaf18b861b102339d594a9022974b9d3f3d47f278e01b888f50bd91489e7351`
  y sdist
  `46f283f77bcf802d7dc1206ec55484e722d4e277edf95c96dae4836f5e333cd3`;
- Hypothesis `deep`: 6 tests y 4 subtests verdes para cada semilla 4600,
  4601 y 4602 del workflow periodico;
- diferencial real: 25/25 casos verdes contra MongoDB 7.0.40 y 25/25 contra
  MongoDB 8.0.24, con seed 42;
- golden de replay capturado desde MongoDB 8.0.24 en Extended JSON canonico:
  los 25 casos son reproducibles y `REAL_CAPTURE_PENDING_CASES` queda vacio;
- fixture SQLite 4.5 conservada con SHA-256
  `e671484cd736c42eb3c4e11dbc5ca2f5564c35072f0e1a5fcc4dded172400efe`.

## Publicacion

El workflow de la etiqueta supero el build reproducible, imports minimos,
Python 3.13/3.14, cobertura y diferenciales MongoDB 7/8. El intercambio OIDC de
publicacion fallo con `invalid-publisher`: PyPI no tiene registrado el Trusted
Publisher para `menudoproblema/mongoeco`, `.github/workflows/ci.yml` y el
environment `pypi`.

Como fallback ya usado en 4.4.0 y 4.5.0, wheel y sdist se reconstruyeron dos
veces desde el commit etiquetado con `SOURCE_DATE_EPOCH` fijado, se compararon
byte a byte, pasaron `twine check` y se publicaron con la credencial local
existente. Los SHA-256 publicados en PyPI son:

- wheel `mongoeco-4.6.0-py3-none-any.whl`:
  `8c148ae6e491ef0c37243b70070904138367b0f148c7accd4695091b3834b58d`;
- sdist `mongoeco-4.6.0.tar.gz`:
  `1ea30ff876bef8b21d5fab9045f6f626cff327d4356cc8d196677eac3f750cff`.

El smoke posterior instalo `mongoeco==4.6.0` sin cache desde
`https://pypi.org/simple`, lo importo desde `site-packages` en Python 3.14 y
valido una operacion completa sobre `MemoryEngine`.

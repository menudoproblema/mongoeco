# Release Checklist

Esta lista prepara una release sin obligar a publicar nada.

## 1. Alcance y narrativa publica

- revisar [README.md](../README.md) para que el
  alcance embebido/local y sus limites sean explicitos;
- preparar o revisar la nota de release y la guía de migración de la versión
  objetivo (para 4.6, [release-4.6.0.md](release-4.6.0.md)) para
  confirmar que la narrativa y los cambios incompatibles son explícitos;
- revisar [COMPATIBILITY.md](../COMPATIBILITY.md)
  y confirmar que runtime, compat catalog, docs y tests cuentan la misma
  historia;
- revisar [MISSING_FEATURES.md](../MISSING_FEATURES.md)
  y [TODO.md](../TODO.md) para que no mezclen ya
  backlog de producto con deuda arquitectonica cerrada.
- actualizar la versión en `src/mongoeco/_version.py`, fechar la sección
  correspondiente de `CHANGELOG.md` y comprobar que la guía de migración
  describe todos los cambios incompatibles de la major.

## 2. Packaging y artefactos

- fijar `SOURCE_DATE_EPOCH` al timestamp del commit y ejecutar
  `python -m build --no-isolation --sdist --wheel` dentro del entorno de build
  constrained;
- ejecutar los dos builds de forma secuencial en un checkout o en checkouts
  separados. Dos procesos `setuptools` concurrentes no pueden compartir los
  directorios temporales `build/` y `<distribution>-<version>/`;
- normalizar el sdist con `python scripts/normalize_sdist.py
  dist/mongoeco-*.tar.gz`, confirmar raiz unica, `PKG-INFO`, nombres NFC sin
  colisiones portables y ausencia de links; verificar que dos builds aislados
  producen wheel y sdist identicos byte a byte;
- ejecutar `python -m twine check dist/*`;
- calcular y conservar el SHA-256 de wheel y sdist;
- ejecutar:
  - `python scripts/smoke_installed_wheel.py`
  - `python scripts/smoke_installed_wheel.py --sdist`
- comprobar `pyproject.toml`:
  - clasificaciones de Python soportado;
  - dependencia core de `usearch`;
  - extras opcionales vigentes;
  - inclusion de `LICENSE`, `py.typed` y schemas JSON publicos.
- ejecutar `python scripts/check_public_typing.py` y confirmar que usa el wheel
  instalado mediante `MONGOECO_TEST_INSTALLED_ARTIFACT=1`;
- ejecutar `python scripts/smoke_external_engine.py` contra el wheel instalado.
- ejecutar `python scripts/update_public_api_manifest.py --check` contra source
  y contra el wheel instalado; revisar cualquier diff semantico, no solo
  regenerar la fixture.
- validar `deprecation_catalog()` contra su schema y comprobar que wheel/sdist
  incluyen ambos recursos.
- ejecutar `python -m mongoeco.conformance` desde un directorio ajeno al
  checkout contra Memory, SQLite y el canario externo.
- abrir una copia de la fixture SQLite 4.5 y comprobar indices, Search y replay
  exacto del sufijo de outbox; verificar su SHA-256 antes y despues.
- regenerar y revisar el catálogo de compatibilidad con
  `python scripts/update_compat_snapshots.py`.
- en CI, comparar dos builds y promover solo una pareja verificada como
  artifact inmutable para la suite completa, los smokes y Trusted Publishing;
  verificar SHA-256 e import desde `site-packages` y no reconstruir en
  `publish`.
- antes de crear una etiqueta, comprobar en PyPI que el Trusted Publisher del
  proyecto coincide exactamente con owner, repositorio, workflow y environment
  declarados por el job de publicacion. El environment de GitHub no registra
  por si mismo el publisher en PyPI.
- aplicar `requirements/ci-constraints.txt` solo a la instalacion del repo y
  del tooling. Los smokes de wheel/sdist deben resolver dependencias en un
  entorno limpio sin heredar constraints internas de CI.

## 3. Validacion funcional

- ejecutar `pytest -q`;
- ejecutar `python -m unittest discover -s tests -p 'test*.py'`;
- ejecutar `pytest --cov=mongoeco --cov-report=term -q`;
- ejecutar los property tests con los perfiles `ci` y `deep`, conservando el
  ejemplo reducido si Hypothesis encuentra una divergencia;
- confirmar que `[tool.coverage.report]` aplica `precision = 2` y
  `fail_under = 99.00`, y que la cobertura real sigue en `>=99.00%`.
- derivar la ultima etiqueta con `git describe --tags --abbrev=0
  --match 'v[0-9]*' HEAD^`, pasarla a `check_lint_ratchet.py` y resolver
  cualquier diagnostico Ruff nuevo en los ficheros modificados;
- no ampliar `scripts/ruff_ratchet_baseline.json`; reducirla cuando se corrija
  deuda conocida y reiniciar su referencia despues de etiquetar la release;
- ejecutar `python scripts/run_pymongo_profile_matrix.py --versions 4.9.2
  4.11.3 4.13.2 4.17.0 --summary-output
  tests/fixtures/pymongo_profile_matrix.json` y revisar los deltas publicados.
- ejecutar las suites de paridad real MongoDB 7.0 y 8.0 cuando haya servicios
  disponibles; para valores temporales, comprobar invariantes y no timestamps
  exactos. Ningun caso puede salir de `REAL_CAPTURE_PENDING_CASES` sin golden
  capturado de un servidor real.
- para 4.6, no recomendar publicacion si los 25 casos por version no se han
  ejecutado contra servidores reales; el listado y workflow verdes no
  sustituyen ese gate.

## 4. Benchmarks y rendimiento

- correr una matriz corta del harness sobre lectura, agregacion, `search` y
  `vectorSearch`;
- guardar resultados locales bajo `benchmarks/reports/`;
- revisar si hay regresiones claras antes de etiquetar una release.
- para cualquier regla nueva de pushdown SQLite, conservar baseline y
  candidato, demostrar paridad Memory/SQLite y sync/async, y rechazar la regla
  si no aporta una mejora material repetible;
- ejecutar `mongoeco.conformance` para Memory, SQLite y cualquier engine
  externo soportado, conservar el informe
  `mongoeco-conformance-report/v1` y exigir cero estados `failed` o `error`.

Comandos base:

```bash
python -m benchmarks.report \
  --engine all \
  --size 1000 \
  --warmup 1 \
  --repetitions 5 \
  --output-json benchmarks/reports/pre-release-1000.json \
  --output-markdown benchmarks/reports/pre-release-1000.md

python -m benchmarks.run \
  --engine memory-sync \
  --size 1000 \
  --warmup 0 \
  --repetitions 1 \
  --workload search_diagnostics \
  --workload vector_search_diagnostics \
  --format json

python -m benchmarks.run \
  --engine sqlite-sync \
  --size 1000 \
  --warmup 0 \
  --repetitions 1 \
  --workload search_diagnostics \
  --workload vector_search_diagnostics \
  --format json
```

## 5. Decision final

- solo entonces decidir si compensa:
  - cortar version;
  - etiquetar;
  - publicar;
  - o seguir acumulando producto sin release.

# Release Checklist

Esta lista prepara una release sin obligar a publicar nada.

## 1. Alcance y narrativa publica

- revisar [README.md](../README.md) para que el
  alcance embebido/local y sus limites sean explicitos;
- preparar o revisar la nota de release y la guía de migración de la versión
  objetivo (para 4.5, [release-4.5.0.md](release-4.5.0.md)) para
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
- normalizar el sdist con `python scripts/normalize_sdist.py
  dist/mongoeco-*.tar.gz` y verificar que dos builds producen los mismos bytes;
- ejecutar `python -m twine check dist/*`;
- calcular y conservar el SHA-256 de wheel y sdist;
- ejecutar:
  - `python scripts/smoke_installed_wheel.py`
  - `python scripts/smoke_installed_wheel.py --sdist`
- comprobar `pyproject.toml`:
  - clasificaciones de Python soportado;
  - dependencia core de `usearch`;
  - extras opcionales vigentes;
  - inclusion de `LICENSE`.
- regenerar y revisar el catálogo de compatibilidad con
  `python scripts/update_compat_snapshots.py`.
- en CI, construir una sola vez y descargar ese mismo artifact inmutable para
  la suite completa, los smokes y Trusted Publishing; verificar SHA-256 e
  import desde `site-packages` y no reconstruir en `publish`.
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
- ejecutar `pytest --cov=src/mongoeco --cov-fail-under=99 --cov-report=term -q`;
- confirmar que la cobertura global sigue en `>=99%`.
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
  exactos.

## 4. Benchmarks y rendimiento

- correr una matriz corta del harness sobre lectura, agregacion, `search` y
  `vectorSearch`;
- guardar resultados locales bajo `benchmarks/reports/`;
- revisar si hay regresiones claras antes de etiquetar una release.

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

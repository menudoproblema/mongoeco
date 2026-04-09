# Release Checklist

Esta lista prepara una release sin obligar a publicar nada.

## 1. Alcance y narrativa publica

- revisar [README.md](../README.md) para que el
  alcance embebido/local y sus limites sean explicitos;
- revisar [release-3.4.0-draft.md](/Users/uve/Proyectos/mongoeco2/docs/release-3.4.0-draft.md)
  para confirmar que la narrativa de release sigue el capability model CXP
  canónico;
- revisar [COMPATIBILITY.md](../COMPATIBILITY.md)
  y confirmar que runtime, compat catalog, docs y tests cuentan la misma
  historia;
- revisar [MISSING_FEATURES.md](../MISSING_FEATURES.md)
  y [TODO.md](../TODO.md) para que no mezclen ya
  backlog de producto con deuda arquitectonica cerrada.

## 2. Packaging y artefactos

- ejecutar `python -m build --sdist --wheel`;
- ejecutar `python -m twine check dist/*`;
- ejecutar:
  - `python scripts/smoke_installed_wheel.py`
  - `python scripts/smoke_installed_wheel.py --sdist`
- comprobar `pyproject.toml`:
  - clasificaciones de Python soportado;
  - dependencia core de `usearch`;
  - extras opcionales vigentes;
  - inclusion de `LICENSE`.

## 3. Validacion funcional

- ejecutar `pytest -q`;
- ejecutar `python -m unittest discover -s tests -p 'test*.py'`;
- ejecutar `pytest --cov=src/mongoeco --cov-report=term -q`;
- confirmar que la cobertura global sigue en `>=99%`.

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

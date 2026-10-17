# Línea base para evaluar SPI v3

Fecha: 2026-10-16. Base: Mongoeco 4.10.1,
`738fd0eb5ece0adabf071e312ee0ff7b82b8589d`. El código productivo no cambia.
Esta evidencia conserva P0 de la propuesta inicial, archivada en
[revision-1.json](revision-1.json). No acredita un contrato v3 implementado ni
su utilidad comparada. La [propuesta vigente](../../architecture/engine-preparation-proposal.md)
elige preparación interna conservando v2 tras revisar tres informes.

## Inventario y probes

[observe_baseline.py](observe_baseline.py) produce [baseline.json](baseline.json).
Exige importar el checkout inventariado, registra hashes de diez fuentes y
recorre las llamadas AST por nombre a cuatro constructores/compiladores.
No abre servidor, socket, SQLite ni motor. El único subproceso obtiene el commit
Git mediante argumentos fijos. No captura variables de entorno ni rutas privadas.

```bash
.venv/bin/python docs/evidence/spi-v3-proposal/observe_baseline.py
```

El inventario tiene 29 referencias sintácticas. Hay cinco shapes con contexto
opcional y tres campos semánticos paralelos cada uno. `SearchRequest` es el
control que exige contexto sin esos campos paralelos. No se cuentan aliases,
dispatch dinámico ni usuarios fuera del repositorio como ausencias probadas.

Los probes de find/update/aggregate verifican estado sin ligar, identidad,
binding idempotente, rechazo de segundo contexto y de dialecto incoherente.
Con contexto semánticamente equivalente hay cero compilaciones adicionales;
con dialecto diferente hay una por probe. Instrumentan el compilador de alto
nivel, no sus instrucciones internas. No son un benchmark de latencia ni una
reproducción de un fallo actual.

## Verificación ejecutada

[validation.json](validation.json) conserva comandos, entorno, resultados y
hashes de los artefactos de esta línea base. La selección focal cubre:

```bash
.venv/bin/python -m pytest \
  tests/contracts/engines \
  tests/unit/core/test_operation_context.py \
  tests/unit/core/test_engine_spi_v2.py \
  tests/unit/core/test_semantic_core.py \
  tests/unit/core/test_read_snapshots.py \
  tests/integration/api/test_cursor_snapshot_contract.py \
  tests/integration/api/test_aggregation_semantic_context.py \
  tests/integration/api/test_selected_mutation_snapshot_contract.py \
  tests/integration/api/test_sqlite_45_fixture_compatibility.py -q
```

Resultado: **140 tests y 1.080 subtests aprobados**, incluidos contratos de
ambos motores, contexto/ownership/cleanup y la fixture SQLite histórica.

Se ejecuta también la suite `tests/integration/api/test_error_phases.py` completa
contra las referencias nativas almacenadas. Comprueba direct/wire, sync/async,
Memory/SQLite y dialectos 7/8/9, con diagnósticos, contadores, efectos parciales y
reutilización de conexión. Los hashes del corpus y las capturas se validan en
los propios tests. El resultado final se registra en `validation.json`.

El primer intento, dentro del sandbox, aprobó 12.868 tests y falló las 12 celdas
wire antes de ejecutar escenarios: `PermissionError` al abrir localhost. Se
conserva ese resultado como fallo de infraestructura y se repiten únicamente
las 12 celdas con permiso de bind local; no se eliminan ni cambian expectativas.
La repetición aprobó las 12 celdas. El resultado combinado es **12.880 tests
aprobados**, sin fallos pendientes; el intento original permanece en el registro.

**No se han arrancado MongoDB 7/8/9 ni recapturado los oráculos en este trabajo.**
Su procedencia y ejecución de publicación pertenecen al
[cierre de 4.10.1](../../release-4.10.1.md). Un candidato A/B debe ejecutarlos
de nuevo; estas referencias no acreditan código futuro.

El manifiesto público y el typing pasan sin cambios. El canario independiente
se ejecuta con el Python del wheel publicado 4.10.1, con origen `site-packages`
comprobado: cinco checks aplicables aprobados y cinco no aplicables visibles.
No se presenta como una suite de adopción de una aplicación externa.

```bash
.venv/bin/python scripts/update_public_api_manifest.py --check
.venv/bin/python scripts/check_public_typing.py
MONGOECO_TEST_INSTALLED_ARTIFACT=1 \
  .tmp/release-4.10.1/published-venv/bin/python scripts/smoke_external_engine.py
.venv/bin/python -m pytest tests/integration/api/test_error_phases.py -q
.venv/bin/python -m ruff check docs/evidence/spi-v3-proposal/observe_baseline.py
```

El último entorno es local al cierre anterior; para reproducirlo en otro equipo
se instala `mongoeco==4.10.1` y las dependencias del smoke en un entorno limpio,
sin editable ni `PYTHONPATH=src`, y se verifica el origen antes de ejecutarlo.
No se instala ni modifica ningún proyecto consumidor.

## Revisión posterior (2026-10-17)

La [síntesis](../../architecture/spi-proposal-review.md) separa ideas incorporadas,
matizadas y descartadas. [probe_review.py](probe_review.py) y
[review-probes.json](review-probes.json) reproducen guardas asimétricas, override
de plan, conversión de dialecto sin contexto, captura de plazo/rewind y ocho
celdas multi-update (Memory/SQLite, sync/async, N=1/3). SQLite prepara el selector
2N veces frente a N en Memory. Son contadores de entrada a compilación, no
latencia. No se abre red; SQLite usa memoria.

```bash
.venv/bin/python docs/evidence/spi-v3-proposal/probe_review.py
```

[review-validation.json](review-validation.json) registra únicamente los checks
de esta revisión. [validation.json](validation.json) conserva el registro P0
anterior sin atribuirle nuevas ejecuciones ni reescribir su resultado.

## Pendiente de la propuesta revisada

T1–T6, rendimiento, matrices del candidato y fixture persistente de última 4.x
siguen pendientes. B y la conformidad v3 han salido del alcance; no son trabajo
aceptado pendiente. No hay código productivo nuevo, bump, deprecación nueva,
aceptación arquitectónica ni publicación en este paquete. El censo externo solo
sería necesario para reconsiderar una migración, no para este refactor interno.

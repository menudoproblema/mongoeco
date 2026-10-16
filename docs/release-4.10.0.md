# Release 4.10.0

Fecha de publicación: **16 de octubre de 2026**. Estado: **publicada y verificada**
en PyPI como **4.10.0**, con la etiqueta **v4.10.0**.
Este documento es el owner del cierre de versión. El
[changelog](../CHANGELOG.md#4100---2026-10-16) resume los cambios para consumidores.

## Versión y alcance

4.10.0 es una minor de Semantic Versioning: añade el kit oficial de pytest,
Python 3.15, el perfil PyMongo 4.19 y opciones compatibles de la API. Mantiene
Python >=3.13, los defaults MongoDB 7.0/PyMongo 4.9, los import roots, Engine SPI v2
y el formato de archivos SQLite. Las correcciones ajustan comportamientos
inválidos o divergentes al subset documentado de MongoDB.

La prioridad de esta entrega son las pruebas de aplicaciones Python que usan
MongoDB. La persistencia embebida y la búsqueda local conservan sus contratos.

- [Kit pytest](pytest.md): URI local y clientes PyMongo reales sync/async,
  selección Memory/SQLite, puertos dinámicos, aislamiento por test, adaptadores
  pytest-asyncio/AnyIO y cierre después de fallos y cancelaciones. El diagnóstico
  del resumen no abre recursos y aparece una sola vez con xdist.
- [PyMongo 4.19](pymongo419.md): perfil explícito y detección exacta del driver,
  validación SRV mediante API pública y `srv_host_validator`.
- [Python](python-support.md): CPython 3.13/3.14/3.15 estándar con GIL; soporte
  de Python 3.13 hasta octubre de 2028, incluido ese mes. Dependencias nativas
  comunes en los tres intérpretes.
- [Opciones de API](pymongo-api-conveniences.md): proyecciones por nombres,
  comentarios en inserts e inputs Iterable de `insert_many`.
- [Escrituras](write-parity-validation.md): ordered/unordered, contadores y
  efectos parciales, `writeErrors[].op`, parsing de comandos, identidad BSON,
  upserts null, replacement/pipeline y publicación de cambios.
- [Wire y write concern](unacknowledged-writes.md): `moreToCome`, respuestas
  `w:0`, validación previa y confirmación efectiva por `j`/`fsync`. El
  [preflight](write-command-preflight.md) conserva los deltas nativos 7/8/9.

## Migración y límites

Para adoptar las fixtures, instala `mongoeco[pytest]>=4.10` y carga explícitamente
el plugin o el adaptador async. No hay autoload ni fixtures autouse. Una aplicación
puede conservar su constructor PyMongo y cambiar únicamente la URI de preparación.
La [guía desde mongomock](migrating-from-mongomock.md) describe esa transición.

Las pruebas que dependían de aceptar operaciones inválidas deben actualizar sus
expectativas. Los errores de forma rechazan el comando antes de escribir; los
fallos por documento conservan los éxitos parciales que permite MongoDB.
No se requiere migrar archivos SQLite existentes.

El corpus finito no acredita compatibilidad universal. No incorpora replicación,
sharding, transacciones distribuidas, failover, Atlas Search ni garantías de
durabilidad de un clúster. Los límites vigentes pertenecen a
[COMPATIBILITY.md](../COMPATIBILITY.md).

## Evidencia y gates de entrega

El [último cierre de implementación](evidence/write-command-preflight/validation.json)
conserva el workset anterior al cambio de versión. Sus artefactos usan el número
provisional 4.9.1 y hashes distintos de la distribución 4.9.1 publicada. Son
antecedentes identificados; la CI de 4.10.0 construye y prueba sus propios
artefactos inmutables.

| Comprobación previa al renombrado | Resultado |
| --- | --- |
| Suite completa por Python 3.13/3.14/3.15 | 25.801 passed, 31 skips esperados, 14.451 subtests |
| Cobertura Python 3.14 | 99,05 %, floor 99,00 % |
| Unittest por Python | 3.627 tests, 3 skips esperados |
| Corpus de comandos y write concern | 2.088 wire + 1.272 directos, Memory/SQLite y sync/async |
| Referencias MongoDB | 7.0.43 / 8.0.32 / 9.0.2, FCV exacta; PyMongo 4.9.2 / 4.19.0 |
| Matriz pytest/PyMongo | 12 celdas, 3.442 pruebas por celda |
| Canario anónimo instalado | 9 ejecuciones, 684 casos, workers 0/2/4 |
| Instalaciones limpias | 9 smokes con extras y una instalación mínima |
| Contratos | Typing, manifiesto, snapshots, SPI independiente y lint ratchet verdes |

## Publicación verificada

- [PyPI 4.10.0](https://pypi.org/project/mongoeco/4.10.0/) y
  [GitHub Release](https://github.com/menudoproblema/mongoeco/releases/tag/v4.10.0).
- Commit etiquetado: `e763c2a75be6aac9de46dbc8e9a6627af0fb02f5`.
- Objeto de etiqueta anotada: `3b722d592a2b1004bc7e64cd4eaed8ea4009c437`.
- [CI de main](https://github.com/menudoproblema/mongoeco/actions/runs/38834949307) y [CI de etiqueta](https://github.com/menudoproblema/mongoeco/actions/runs/38836040920):
  terminadas en verde sobre ese mismo commit.
- [Recibo de publicación](evidence/mongoeco-4.10.0/published-verification.json):
  wheel y sdist descargados de PyPI, idénticos byte a byte a los artefactos
  aprobados por CI. Las atestaciones identifican el repositorio, `ci.yml` y el
  entorno `pypi`. Trusted Publishing publica sin reconstruir.

| Artefacto publicado | SHA-256 |
| --- | --- |
| `mongoeco-4.10.0-py3-none-any.whl` | `dc3246314d2de35ae7df09c870784b1bd02497d765d9ff16d429c929db1a0ea4` |
| `mongoeco-4.10.0.tar.gz` | `15f456ae8160e0ce326c950ed2a0c155a17a7b4d03d75feb1ee3bff157142f71` |

| Gate final del artefacto instalado | Resultado |
| --- | --- |
| Suite completa en Python 3.13/3.14/3.15, main y etiqueta | 25.801 passed, 31 skips previstos y 14.451 subtests por intérprete y ejecución |
| Cobertura de la etiqueta en Python 3.14 | 99,05 %, floor 99,00 % |
| Matriz del kit, Python × pytest × PyMongo | 12 celdas, 3.442 pruebas aprobadas por celda |
| Referencias MongoDB del wheel etiquetado | 7.0.43 / 8.0.32 / 9.0.2, FCV exacta; jobs completos aprobados |
| Instalación limpia desde PyPI | Contrato CXP/imports, `pip check`, 3.442 regresiones sin skips y conformance Memory/SQLite aprobados |
| Identidad del runtime | 281 archivos coinciden entre checkout, wheel de CI, wheel publicado e instalación pública |

Los [logs y resultados de CI](evidence/mongoeco-4.10.0/tag-job-logs.json) y el
[resumen diferencial final](evidence/mongoeco-4.10.0/native-tag-summary.json)
conservan los códigos, contadores, efectos parciales y reutilización de conexión
observados. Cada job nativo ejecuta 3.360 regresiones de comandos y el corpus de
13.395 pruebas con 9.528 subtests; son ejecuciones repetidas del corpus por
servidor, no ese número de escenarios nativos distintos. Las capturas nativas
incluyen 172 casos por fachada y cuatro casos wire directos para write concern,
además de 104 casos de opciones de API y los corpus adicionales del observador.

Los registros de [colección archivada](evidence/mongoeco-4.10.0/archived-script-discovery.json)
y [setup diferencial](evidence/mongoeco-4.10.0/native-collection-recovery.json)
preservan los fallos originales: la colección accidental de un driver histórico
archivado y la ausencia de Hypothesis en el setup diferencial. Se corrigieron
las entradas de verificación, sin cambiar runtime, límites, presupuestos ni
aserciones. El primer intento no llegó a publicar en PyPI; su objeto de etiqueta
se conserva. El [reintento](evidence/mongoeco-4.10.0/tag-retry.json) mantuvo
**4.10.0**, y todos los gates del candidato corregido terminaron en verde.

El [primer intento de instalación pública](evidence/mongoeco-4.10.0/published-initial-index-attempt.json)
falló durante la propagación del índice de PyPI. Los dos errores se conservan;
el mismo gate terminó en verde cuando pip recibió el índice actualizado.

La evidencia local previa y sus hashes permanecen identificados como historia;
el recibo final distingue expresamente los artefactos publicados. El índice
[SHA256SUMS.json](evidence/mongoeco-4.10.0/SHA256SUMS.json) permite comprobar
los archivos del cierre. Los consumidores del canario permanecen anónimos.

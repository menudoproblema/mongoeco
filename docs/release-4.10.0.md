# Release 4.10.0

Fecha de preparación: **16 de octubre de 2026**. Estado: **candidato de entrega**;
la etiqueta y la publicación se verifican al finalizar los gates de CI.
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

El cierre final registrará el commit, la CI de main y de etiqueta, los hashes
alojados, la procedencia de PyPI y la verificación desde una instalación pública.
La publicación usa Trusted Publishing y los artefactos aprobados por los gates,
sin reconstruirlos en el job de publicación.

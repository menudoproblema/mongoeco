# Revisión 8: autocrítica y garantías restantes

Owner de obligaciones: [plan](../../../bson-semantics-consolidation-plan.md).
Owner de estado/gates: [registro](../../../bson-consolidation-implementation.md).
Solo documentación y caracterización: no modifica ni acepta el WIP de watch.
Base `bc8fb1c`; manifest de runtime y fingerprints en receipt.

El adjunto tiene el mismo SHA-256 que revisiones 3–7. No abre otros siete
hallazgos. La retractación sobre causa Binary, in/distinct y propiedades es
correcta; SDK/local/owned, parser DBRef y fallback binario SQL tienen regresión.
Las 18 filas nuevas del matcher observan documento y operando por separado;
69 checks de propiedades/fronteras y 345 de replay pasan. Replay usa capturas
existentes: no se recapturan esos corpora. Se descargan de nuevo las 16 fuentes
r8.0.0 y todos sus hashes coinciden. TTL y H53 no se ejecutan en este corte.

## Caracterización nativa nueva

Replica sets temporales propios: 7.0.43/8.0.32/9.0.2, FCV 7.0/8.0/9.0,
puertos 49973/49974/49975, directConnection. No se modifica la instancia
habitual ni la configuración de servicios. Bases UUID limpiadas en finally.
Solo sync en estas sondas; no aceptación async/transaccional/persistida.

- Imágenes: 16 escenarios × 3 nativos y 6 pares locales. Se escribe v0→v1→v2
  y se borra opcionalmente antes de consumir. MongoDB updateLookup entrega v2
  o null, whenAvailable sin imágenes entrega null, required sin imágenes falla
  con OperationFailure 47 y cursor cerrado. Con imágenes habilitadas ambos
  entregan v1, incluso después de borrar. Default omite fullDocument. Cada par
  local diverge en ocho escenarios; coinciden los tres nativos.
- DDL: dos acciones (drop de colección/base) × tres ámbitos × 3 nativos y
  6 pares locales. Tras el drop se recrea/inserta una barrera con `_id:2`.
  Colección nativa recibe drop→invalidate y cierra; DB/cliente sobreviven al
  drop de colección. DropDatabase invalida colección y DB, no cliente.
  Local drop de colección publica solo invalidate y cierra los tres; el
  camino rápido de drop_database omite DDL y deja los tres abiertos.
  Invalidate local añade ns/documentKey ausentes en los eventos nativos.

`invalidate.json` conserva namespaces UUID. Las comparaciones de estructura
normalizan únicamente ese nombre efímero y su prefijo en documentKey local;
no borran campos ni normalizan las divergencias. No se comparan valores opacos
de token/clusterTime; rename, filtro de invalidate, resumeAfter/startAfter,
reapertura y fallos/cancelación tienen testigos pendientes explícitos en plan.

## Simplificación adoptada

D04 mantiene un padre y tres garantías de aceptación, sin nuevos IDs ni runner.
Preparación/contexto reutilizan owners existentes; imágenes necesitan separar
captura de escritura y lookup al consumir; ciclo de vida separa metadatos de
ámbito y forma pública. El fixture nativo debe ser replica set: el workflow
actual sigue standalone. Un catálogo de reconocimiento de stages no equivale
a capacidades de ejecución del dialecto; la consolidación no debe romper
Search ni registro de extensiones. Los cambios compartidos se contrastan también
con aggregate/update vacíos/poblados, sin redefinir sus políticas particulares.

Fuentes explicativas oficiales consultadas: [update](https://www.mongodb.com/docs/manual/reference/change-events/update/),
[invalidate](https://www.mongodb.com/docs/manual/reference/change-events/invalidate/)
y [parser r8.0.0](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/matcher/expression_parser.cpp).
Las referencias capturadas por versión gobiernan la aceptación, no la lectura
de una sola etiqueta ni una página de documentación mutable.

El error inicial al importar la sonda de imágenes (`PYTHONPATH=src` sin raíz
para `tests`) se resolvió con `PYTHONPATH=src:.`; la repetición termina con
exit 0. No fue fallo de producto ni rechazo de aprobación automática.

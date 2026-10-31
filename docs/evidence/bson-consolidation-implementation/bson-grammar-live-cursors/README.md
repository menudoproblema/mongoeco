# D06: checkpoint de cursores vivos, sin cierre del bloque

El [receipt del checkpoint](receipt.json) continúa la
[evidencia anterior](../bson-grammar-context/receipt.json).
La [implementación](../../../bson-consolidation-implementation.md) posee el
estado y los pendientes; esta carpeta posee fuentes, comandos y observaciones.
El contrato conserva el [plan vigente](../../../bson-semantics-consolidation-plan.md).

- `workset.json` y `runtime-sources.json`: hashes del candidato. El wheel final
  se instala con pip sin dependencias/red; los 283 módulos coinciden y su import
  se verifica antes del replay. `wheel.json` identifica ese artefacto.
- `native-eleven-actions.tar.gz`: fuente y referencias nuevas de once acciones,
  capturas, checks y fixtures físicos 7.0.43/8.0.32/9.0.2 con FCV correspondiente.
  Cinco controles DBRef históricos por versión se comprueban sin modificarlos.
  Las capturas nativas son sync; el replay alcanza las otras superficies.
- `getmore-*.json`, `fixture-*.json`, `getmore-native-probe.py.txt`: códigos
  nativos ante namespace/sesión incorrectos, con fixture por versión.
  [Fuente oficial r8.0.0](https://github.com/mongodb/mongo/blob/r8.0.0/src/mongo/db/commands/getmore_cmd.cpp)
  respalda también la autorización por principal. No se afirma haber probado
  autenticación física multiusuario ni el hash de esa descarga en esta revisión.
- `merge-zero-*.json` y su sonda: diagnóstico separado con shorthand `$merge`
  que distingue API SDK y comando raw. El testigo permanente usa la forma
  documental admitida por el subset local y verifica estado antes de consumir.
- `baseline-merge-zero.txt.gz`: import y 283 hashes de la baseline `57b5faf`
  verificados; 12 fallos esperados y 12 controles correctos sobre el nuevo caso.
- `core-wire.txt.gz`, `wheel-replay.txt.gz`, `wheel-sdk-free.txt.gz`:
  verificaciones del candidato final. `public-manifest-drift.txt.gz` conserva
  el gate rojo previo, y `public-manifest-vs-base.json` demuestra ausencia de
  cambio adicional del manifiesto efectivo.
- `extension-mutations.py.txt` y logs: cada mutación corre en otro proceso sin
  modificar fuentes; retirar cualquiera de las dos guardas hace fallar los
  testigos. Los siete subcasos no se cuentan como siete capturas nativas.
- `getmore-cleanup.json` y `native-cleanup.json`: inspección independiente de
  ausencia de contenedores y volúmenes propios, además del cleanup del fixture.
- `suffix-native-*.json`, `suffix-local.json` y sondas: caracterización pendiente
  de set/sort/project, con y sin allowDiskUse. Los tres servidores entregan 101
  filas antes del error 2; los doce pares locales fallan al abrir. `suffix-cleanup.json`
  acredita ausencia de los recursos propios de esas sondas.
- `stream-benchmark-idle.json`: dos secuencias ABBA por motor con 3.000 filas,
  hashes BSON idénticos y ningún otro gate del repositorio activo. La primera
  medición concurrente se conserva como feedback y no acredita este gate.

Los replays interrumpidos y el primer gate core rojo son feedback. Un proceso
interrumpido no acredita éxito ni se presenta como ejecución completa. El
receipt final distingue sus logs de la verificación sobre el candidato congelado.

La frontera por bytes, los callbacks por respuesta, los sufijos materializantes,
raw batches y el resto de D06 siguen pendientes. Una suite verde sin esos
testigos no cierra el bloque. No se alteran observadores protegidos, fixtures
históricos, manifiestos anteriores ni sus receipts.

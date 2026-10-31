# D06: sufijos de respuesta y conservación de tipos internos

Base de producción `7328feb`. El [receipt](receipt.json) identifica el cierre
funcional acotado en `d9e9579`. El [registro de implementación](../../../bson-consolidation-implementation.md)
posee el estado y los pendientes del programa; esta carpeta conserva inputs y
observaciones del corte. El [checkpoint anterior](../bson-grammar-live-cursors/receipt.json)
mantiene sus resultados y el diagnóstico que inicia este trabajo.

La familia nueva tiene siete acciones: sort, group/sort, dos grupos, unwind,
sort/unwind, un documento con 226 salidas y tipos antes/después del prefijo.
Observa batchSize omitido/0/1/2, allowDiskUse false/true, errores en posiciones
1/3/103, controles válidos y límites 1/102. Fija el orden explícitamente y recoge
clase, código, codeName, fase y filas recibidas con sus tipos BSON.

Las referencias se capturan y comprueban en MongoDB 7.0.43/8.0.32/9.0.2,
FCV correspondiente, mediante el fixture propio con imágenes fijadas. Son
capturas sync nativas; sync/async y wire corresponden al replay. No se editan
referencias históricas ni observadores protegidos.

- `native-final.tar.gz`: fuente exacta, fixtures y capturas definitivas.
- `baseline.json`, `baseline-runtime.json`, `baseline-run.py.txt`: baseline
  aislada con los 283 módulos e import real comprobados. Su resultado final
  distingue 120 fallos de 51 controles correctos; no cuenta errores de setup.
- `runtime-sources.json`, `workset.json`, `wheel.json`: candidato congelado y
  artefacto instalado sin dependencias/red, con módulos/import verificados.
- `mutations.py.txt` y logs: retirar demanda, evaluar la expansión completa,
  materializar todo el sufijo o convertir públicamente el prefijo vuelve a
  hacer fallar los testigos. Cada mutación se ejecuta en un proceso separado.
- `sdk-free.py.txt`: perfil real Python 3.13 sin SDK desde el wheel. Comprueba
  72 escenarios de respuesta/spill y 24 de conservación de Long, estos últimos
  con ambos modos de disco y superficies sync/async.
- `native-cleanup.json`: inspección independiente de ausencia de los recursos
  propios, separada del reporte de teardown del fixture.

El primer intento usó una semilla vacía que el replay no admite; sus fallos de
insert_many son feedback. El siguiente asumía orden natural para un caso de
SQLite. Se fija un sort de entrada para probar respuestas sin exigir ese orden
no contractual. Ninguno de esos fallos se presenta como una divergencia BSON.

El replay de 7.030 checks sobre la versión anterior pasa, pero no acredita el
arreglo posterior de Long sin SDK. Esa versión convertía el prefijo antes de
evaluar su sufijo: con SDK conservaba Int64 y sin SDK lo reducía a int. Se
conserva como feedback; la evidencia final corresponde al código que ejecuta
prefijos sobre valores internos y materializa solo después de evaluar.

La demanda de este corte es por número de documentos. Fronteras por bytes,
callbacks por respuesta, raw y el resto de D06 conservan sus pendientes.
Este corte no acepta todo D06 ni sustituye D05.

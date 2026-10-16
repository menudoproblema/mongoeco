# Escrituras no confirmadas y reutilización de la conexión

Corrección incluida en **4.10.0**, autorizada por el encargo de corregir
`w=0` y comprobar errores, efectos y reutilización con clientes PyMongo reales.
No cambia el SPI, los motores, el formato SQLite ni las dependencias.

## Defecto y corrección

El decoder ya aceptaba `OP_MSG.moreToCome`, pero el proxy respondía siempre.
PyMongo no consume una respuesta a ese mensaje: al reutilizar la conexión para
un `ping`, encontraba la respuesta anterior y lanzaba `ProtocolError` por un
`responseTo` incorrecto. La escritura podía haberse aplicado.

El proxy ahora ejecuta la petición y omite su respuesta si lleva `moreToCome`,
tanto en éxito como cuando el executor produce un error. El handler continúa
leyendo peticiones en la misma conexión. Las peticiones que esperan respuesta
reciben su respuesta, incluidas las que contienen `writeConcern.w=0`
sin ese flag. La decisión de enviar una respuesta corresponde al protocolo.

Una segunda divergencia afectaba al contenido de esa respuesta: los comandos
explícitos `insert`, `update` y `delete` ignoraban `writeConcern.w=0` al
construir el resultado. Ahora ejecutan la escritura con sus reglas de orden y
parcialidad y después devuelven `n: 0`, además de `nModified: 0` en `update`,
sin `upserted` ni `writeErrors` cuando no solicita sincronización, como MongoDB.
`j:true` y `fsync:true` conservan la confirmación efectiva descrita en la
[validación de comandos](write-command-preflight.md). Los errores de validación del
comando siguen devolviéndose: por ejemplo, un `q` ausente produce 40414 incluso
con `w=0`. No se omite esa respuesta si falta `moreToCome`.

Esto cumple la [especificación OP_MSG](https://specifications.readthedocs.io/en/latest/message/OP_MSG/#moretocome-on-requests).
No añade garantías de replicación o durabilidad de un clúster.

## Corpus y aceptación

El catálogo [unacknowledged_cases.py](../tests/differential/unacknowledged_cases.py)
conserva 33 escenarios, cada uno con `w=0` y control `w=1`: **66 casos**.
El [corpus de preflight](write-command-preflight.md) añade **106 comandos**,
con distintos concerns y formas de entradas: **172 casos por fachada** en total.
Cubre inserciones, actualizaciones, reemplazos, borrados y bulk, ordered y
unordered cuando corresponde. Incluye duplicados, schema, incremento inválido,
`_id` inválido y efectos parciales de lotes y `update_many`. Catorce escenarios
usan `database.command` explícito: inserciones, actualizaciones single/multi,
reemplazo, upsert, borrados single/multi, duplicados ordered/unordered,
incrementos inválidos, efectos parciales y formas de update/delete inválidas.

El [observador independiente](../tests/support_unacknowledged.py) solo importa
PyMongo, BSON y herramientas de pruebas. Recoge:

- `acknowledged`, IDs y contadores públicos disponibles; con `w=0`, los
  contadores que PyMongo no expone conservan `InvalidOperation`;
- familias y códigos de error, índices, operaciones y contadores parciales de
  `BulkWriteError` en los controles confirmados;
- presencia o ausencia de los contadores `n`/`nModified`, `upserted`, códigos,
  `codeName`, etiquetas y errores de escritura en respuestas de comandos,
  incluidos los controles raw. Se excluye el texto diagnóstico `errmsg`, que
  difiere entre implementaciones;
- documentos después de la operación y después de una escritura confirmada
  posterior, incluida la ausencia de cambios por operaciones inválidas;
- éxito del `ping`, lectura y escritura siguientes usando el mismo cliente;
- un único ID de conexión física del pool durante toda la secuencia, observado
  mediante eventos públicos CMAP, con pool de tamaño uno y retries desactivados.
  Las conexiones de monitorización del driver quedan fuera de ese contador.

Con la confirmación efectiva desactivada, los errores de cada escritura del
servidor no se entregan al cliente:
se comprueban sus efectos y la continuidad de la conexión. El control `w=1`
acredita los códigos 11000, 14, 53 y 121, la envolvente bulk 65 y los contadores
parciales. Los errores de transporte no se convierten en resultados semánticos
ni en skips.

Otros **cuatro casos raw** combinan flag presente/ausente y éxito/error de
comando. El error por `q` ausente conserva el código 40414 cuando corresponde
responder. Cada caso concatena la operación y un `ping` en el mismo socket y
comprueba los IDs de respuesta, los documentos y otra inserción confirmada.
Esto detecta respuestas sobrantes sin esperar un timeout para inferir su ausencia.

Las [regresiones](../tests/integration/wire/test_unacknowledged.py) ejecutan
**2.088 casos wire**: 2.064 con PyMongo sync/async y 24 raw, en Memory/SQLite y los
dialectos 7.0/8.0/9.0. SQLite utiliza archivos temporales reales. La
parametrización funciona con pytest 8.4 y 9 sin necesitar `pytest-subtests`.
Cliente y proxy se ejecutan en loops independientes, como en el kit y frente
a un servidor real. Así el `select` bloqueante del cliente async de PyMongo 4.9
no impide que el servidor avance cuando SQLite cede el control al executor.

## Contraste nativo y reproducción

Los siguientes registros conservan los candidatos anteriores. El estado de la
entrega está en [release-4.10.0.md](release-4.10.0.md).

Se han capturado MongoDB **7.0.43, 8.0.32 y 9.0.2**, con sus FCV exactas.
PyMongo **4.9.2 y 4.19.0** coinciden en los 172 casos sync/async y cuatro raw por
servidor: **2.088 observaciones nativas** entre los dos SDK y los tres servidores.
Cada captura exige igualdad entre las fachadas, reutilización física y limpieza
de las bases temporales. Comprueba también que no se ha importado Mongoeco.

Los goldens conservan los hashes del catálogo y de sus observadores. El workflow
diferencial vuelve a capturarlos y compararlos; la matriz del kit incluye ahora
este corpus contra el wheel instalado.

```sh
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
  python scripts/capture_unacknowledged_parity.py --target 8.0 \
  --output /tmp/unacknowledged.json \
  --check tests/fixtures/unacknowledged_mongodb_8_0.json

MONGOECO_TEST_INSTALLED_ARTIFACT=1 \
  python -m pytest tests/integration/wire/test_unacknowledged.py -q
```

El [registro del arreglo de transporte](evidence/unacknowledged-writes/validation.json)
conserva su evidencia histórica. El [registro del candidato ampliado](evidence/command-unacknowledged-writes/validation.json)
identifica el wheel, el fallo esperado anterior, los resultados y los límites
de la validación del contenido de las respuestas. El número de la próxima
versión y su publicación siguen fuera de este arreglo.

La evidencia ampliada de preflight y confirmación efectiva se conserva en
[su registro de candidato](evidence/write-command-preflight/validation.json).

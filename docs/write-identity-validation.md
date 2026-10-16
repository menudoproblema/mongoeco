# Identidad y validación en escrituras

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

Esta ampliación cierra los dos casos acordados después del corpus de escrituras:
un `_id` array dentro de `insert_many`, y errores de identidad/validator en
`find_one_and_update` y `find_one_and_replace`. Incluye las rutas hermanas en las
que la reproducción encontró el mismo defecto. No publica una versión.

La ampliación posterior de [opciones de API PyMongo](pymongo-api-conveniences.md)
añade proyecciones por nombres, comentarios en inserciones y tipado Iterable.
Este informe conserva el candidato de identidad; la validación del candidato
posterior se registra en esa ampliación.

## Reproducción y causa

Se ejecutaron los mismos estímulos con PyMongo 4.19.0 contra MongoDB **7.0.43,
8.0.32 y 9.0.2**, con FCV exacta, y contra Memory/SQLite antes del arreglo.

| Caso | MongoDB real | Defecto del candidato anterior |
| --- | --- | --- |
| `_id` array en mitad de insert_many ordered | BulkWriteError, item 53, conserva prefijo | WriteError 53, ninguna inserción |
| Mismo lote unordered | BulkWriteError, item 53, continúa tras el error | WriteError 53, ninguna inserción |
| findAndModify cambia `_id` | OperationFailure 66 / ImmutableField | WriteError 66 sin codeName |
| findAndModify incumple validator | OperationFailure 121 / DocumentValidationFailure | Subclase WriteError 121 |
| findAndModify colisiona con índice único | DuplicateKeyError 11000 / DuplicateKey | Falta codeName |
| Reemplazo de documento existente con `_id` array | 53 / InvalidIdField | 66 |
| Upsert por reemplazo con `_id` array | 54 / NotSingleValueField | 53 |
| Upsert por reemplazo con filtro no basado en `_id` | Inserta únicamente el reemplazo y `_id` | Copia también campos del filtro |
| findAndModify con bypassDocumentValidation | Aplica el bypass del validator | TypeError por keyword desconocida |

La preparación de `insert_many` confundía un error del servidor por elemento
con un error cliente de forma del lote. `bulk_write(InsertOne(...))` ya lo
contabilizaba correctamente: se conserva como control. El adaptador público de
findAndModify solo contemplaba cuatro códigos de rutas/tipos y dejaba escapar
los errores de identidad y schema con la familia del engine. Los reemplazos
comprobaban inmutabilidad antes del tipo de `_id`, y su upsert reutilizaba el
seed completo de los updates con operadores. La comprobación anticipada de
conflicto con el `_id` del filtro también ocultaba el error 54 del array en un
upsert; el engine resuelve ahora ese caso después de seleccionar o descartar
el documento existente, preservando el 53 del reemplazo que sí encuentra uno.

## Corrección y garantías

- La frontera pública de insert_many ejecuta el prefijo válido antes de
  contabilizar el `_id` inválido. Conserva índices originales, nInserted,
  ordered/unordered y asignación de ObjectId al caller. Un error de forma cliente
  sigue rechazándose antes de escribir. No cambia la atomicidad del SPI ni exige
  a engines externos devolver un prefijo de fallos de identidad.
- findAndModify expone OperationFailure con codeName para 53, 54, 66 y 121;
  conserva DuplicateKeyError y añade DuplicateKey para 11000. Los helpers
  update/replace y bulk conservan sus familias de error propias.
- Un reemplazo existente valida el tipo de `_id` antes de inmutabilidad. Los
  upserts distinguen el código 54 del reemplazo y el 53 del update con operadores.
  Un `_id` documento que contiene un array anidado continúa siendo válido.
- Los upserts por reemplazo heredan únicamente la igualdad sobre `_id` del
  filtro; el documento final procede del reemplazo. Se conserva el rechazo de
  identidades contradictorias. No se cambia el seed de updates con operadores.
- Las dos APIs find_one_and_* admiten la opción nativa bypassDocumentValidation
  y conservan la opción Python existente bypass_document_validation. La grafía
  nativa tiene precedencia cuando se proporcionan ambas.

## Regresiones y ejecución

`tests/differential/write_identity_cases.py` define **82 escenarios adicionales**,
separados del inventario original de 170 casos. Incluye errores al principio,
en medio y al final del lote, ordered/unordered, errores mixtos de identidad,
schema y unicidad, upserts, ausencia de coincidencias, bypass y controles válidos.
Cada escenario añade una escritura válida posterior para comprobar recuperación.

El observador independiente existente compara familias, códigos, codeName,
contadores parciales, documentos BSON y mutaciones del caller. Las referencias
nativas viven en `tests/fixtures/write_identity_mongodb_{7_0,8_0,9_0}.json` y ligan
hashes del catálogo y del observador. El capturador no importa Mongoeco y verifica
la limpieza de la base temporal. Las capturas anteriores permanecen intactas.

La matriz directa ejecuta 1.968 escenarios (tres dialectos, dos perfiles SDK,
dos motores y dos facades). La ruta wire añade 984 escenarios en 12 celdas,
con PyMongo sync y async. Las regresiones de publicación comprueban que cada
éxito emite exactamente un evento, con índices globales y recuperación, incluyendo
un engine que declara `batch_inserts=False`.

```bash
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
  python scripts/capture_write_parity.py --target 8.0 \
  --output-dir /tmp/write-capture --histories-dir /tmp/write-histories \
  --check-dir tests/fixtures

python -m pytest -q tests/integration/api/test_write_identity_parity.py \
  tests/integration/api/test_insert_many_write_contract.py
```

El workflow diferencial ejecuta el capturador y esta nueva matriz sobre el
candidato. Los resultados de esta ampliación corresponden a ejecución local;
no acreditan un run alojado de GitHub. La identidad final del wheel, resultados
de gates y evidencia de antes/después se registran en
[validación](evidence/write-identity/validation.json). El informe anterior de
[escrituras fiables](write-parity-validation.md) describe el candidato previo;
sus resultados históricos no sustituyen la validación de este arreglo.

## Resultado final

- El wheel anterior presenta 330 observaciones discrepantes de 492 escenarios
  de la matriz de contraste (82 casos, tres dialectos, dos motores).
- La matriz adicional final coincide con las tres referencias nativas,
  incluyendo códigos 53, 54, 66, 121 y 11000, codeName cuando lo devuelve la ruta,
  y código 65 de la envolvente BulkWriteError. El corpus original conserva sus
  fixtures y pasa la recaptura con `--check-dir`.
- Las 2.124 pruebas focalizadas pasan, con 94 subtests. Las suites completas
  del wheel instalado en Python 3.13 y 3.14 dan **10.974 passed, 31 skips
  opcionales esperados y 10.699 subtests passed** en cada versión.
  Cobertura: **99,04 %**, 417 líneas pendientes de 43.384.
- Unittest: 3.613 pruebas, tres skips esperados. Propiedades deep: nueve pruebas.
  Typing, manifiesto público, snapshots de compatibilidad, conformance de los dos
  motores y canario SPI externo: verdes. El canario consumidor pasa sus 16 flujos
  y 48 replays nativos sin correctores semánticos ni modificación de sus fuentes.
- Ratchet de lint contra v4.9.1 y formato de los módulos nuevos: verdes. Se ha
  revisado el diff y alineado la documentación, conservando el trabajo previo.
  Los 275 ficheros de runtime del wheel coinciden byte a byte con el checkout.
- Se comprueba la ausencia de bases temporales y se detienen los tres servidores
  nativos. No se crea tag, se publica distribución ni se decide número de versión.

SHA-256 del wheel ensayado:
`6b91102576c280a6ab2821458b0caed8bfe298f189593d7e5987d21debdd568f`.

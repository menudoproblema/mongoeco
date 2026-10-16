# Operación fallida en errores de escritura

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

## Contrato y alcance aprobado

El encargo del usuario autoriza corregir `writeErrors[].op`, ampliar el
observador diferencial y contrastar MongoDB real en sync/async y Memory/SQLite,
con códigos, contadores y efectos parciales. Se conservan los cambios previos
de la siguiente entrega. No se publica una versión.

`insert_many` debe incluir el documento fallido, con el `_id` asignado durante
la preparación. `bulk_write` debe incluir el documento de comando de la
operación (`q`, `u`, `multi`, `upsert`, `limit` y opciones aplicables), no el
nombre de la clase. Cada error conserva su índice original incluso cuando un
lote unordered agrupa familias diferentes. Los detalles locales se separan
de las entradas mutables del caller. La respuesta raw de MongoDB no recibe
el enriquecimiento `op` que corresponde al driver.

## Censo y aceptación

| Superficie | Mecanismo suficiente | Disposición |
| --- | --- | --- |
| Insert batches | Preparación única y errores por índice en `_collection_insert` | Derivar snapshots públicos tras asignar IDs |
| Bulk clásico | `PreparedBulkWriteRequest` conserva índice y preparación | Conservar la representación pública antes de normalizarla para el core |
| Serialización de errores | `WriteErrorEntry` / `BulkWriteErrorDetails` | Ampliar payload sin retirar el constructor histórico con string; copiar al materializar |
| Oráculo | Observador BSON independiente y capturadores reales existentes | Comparar presencia, tipo y contenido de `op`; recapturar referencias afectadas |
| Matriz | Tests directos y wire, perfiles 4.9/4.19 | Reutilizar consumidores y añadir escenarios de opciones, BSON e índices |
| Gates | Suites instaladas 3.13/3.14/3.15 y contratos públicos | Ejecutar sobre el candidato final; mantener cobertura >=99 % |

## Plan acotado

1. Capturar operaciones fallidas reales con PyMongo sync y async en MongoDB
   7.0.43, 8.0.32 y 9.0.2, sin importar Mongoeco en el capturador.
2. Ampliar observación y reproducir la divergencia del candidato anterior.
3. Corregir preparación y materialización preservando la ejecución válida,
   los códigos y la atomicidad nativos. Cubrir opciones de los seis modelos, ordered/unordered, BSON,
   IDs generados, errores de preparación y conservación de entradas.
4. Recapturar los corpus afectados con el nuevo observador, ejecutar matriz
   directa/wire y gates globales, revisar diff y documentar evidencia final.

## Implementación y contraste

La preparación asigna IDs y conserva una copia del payload público antes de
normalizarlo para el core. Los errores se enriquecen en la API; `WriteErrorEntry`
materializa otra copia y mantiene el constructor histórico con string.
`UpdateOne` conserva `upsert: false` cuando PyMongo lo incorpora por defecto;
`UpdateMany` y `ReplaceOne` conservan la ausencia de esa opción cuando el
modelo oficial no la proporciona. Hint y sort se expresan como documentos,
collation se conserva y los errores mantienen los índices originales.

El observador compara ahora `write_error_operations`: índice, presencia de
`op` y payload BSON estructural, además de familias, códigos, codeName donde
lo devuelve el servidor, contadores y estado persistido. Los tests negativos
detectan ausencia, null, string, documentos distintos y cambios Int32/Int64.
En raw se verifica también que `op` está ausente: es un enriquecimiento del
driver. La conservación local comprueba referencias separadas, valores BSON,
fechas con microsegundos e IDs generados también en engines sin batch insert.

El nuevo catálogo contiene **42 casos**. Sus capturas son independientes de
Mongoeco y coinciden entre PyMongo 4.19 sync/async en MongoDB 7.0.43, 8.0.32 y
9.0.2: **252 observaciones nativas**. Las fachadas y motores locales consumen
**504 observaciones directas** y **504 wire**. El corpus de opciones con sort
usa el perfil 4.19; los corpus previos siguen contrastando 4.9 y 4.19.

La ampliación descubrió y corrigió estas diferencias adicionales:

- Código `2` perdido al indicar un índice inexistente, y omisión de esa
  validación en scans SQLite con fallback Python. La selección especializada
  de índices de texto conserva sus controles y sus regresiones.
- SQLite comprobaba el duplicado de `_id` antes del validator en la inserción
  individual: se aplica validación primero, como MongoDB, conservando el bypass.
- MongoDB 7 rechaza `sort` en updates clásicos con `OperationFailure` 40415 y
  `IDLUnknownField`. Se valida el lote completo antes de aplicar sus updates,
  conservando lotes anteriores, tanto en bulk directo como en raw/wire.
- La ruta raw propagaba incompletamente `sort` a updates/replacements de MongoDB
  8/9. Se propaga mediante la implementación pública ya existente.
- El operando no numérico de `$inc` omitía el código 14. Se conserva en el error.

Los casos de fallo parcial limitan explícitamente el conjunto de documentos
objetivo para que el resultado no dependa del orden natural de scan de cada
motor. No se amplía ni se relaja la envolvente de resultados admisibles.

Se recapturaron también 170 casos de escritura, 82 de identidad y 8 comandos raw
por servidor, comprobando sync/async, y los 104 casos de opciones de API que
consumen el observador común. Los golden se obtienen del servidor; no se
regeneran a partir de la implementación local. Los informes anteriores
conservan sus candidatos y evidencias históricas.

## Cierre de implementación

Completado el 15 de octubre de 2026. El candidato anterior, identificado por
el wheel `25157c8b94aa7c2dceb5f7b1575faf7fb3b82e82bb653b64a0e9a104f7bd10f3`,
divergía en las 168 observaciones de los 42 escenarios sobre MongoDB 8
(dos fachadas y dos motores). El nuevo candidato satisface la matriz completa
de los tres servidores. Se comprueban códigos `2`, `14`, `53`, `66`, `121` y
`11000`, la envolvente bulk `65` y el error de comando `40415`, junto con
contadores, upserts, cambios del caller y documentos persistidos tras cada paso.

El wheel final tiene SHA-256
`9030e0e05a0c0e34a8087ca4a6f120afc90a94abecb94f22c29b7e85172e4947`.
Sus 275 archivos de runtime coinciden con el checkout y las tres instalaciones.
Conserva provisionalmente la etiqueta 4.9.1 para las pruebas: es un build de
trabajo no publicado, distinto de la versión 4.9.1 distribuida.

| Gate del candidato instalado | Python 3.13.15 | Python 3.14.5 | Python 3.15.0 |
| --- | --- | --- | --- |
| Pytest completo | 11.562 passed | 11.562 passed | 11.562 passed |
| Subtests / skips esperados | 14.445 / 31 | 14.445 / 31 | 14.445 / 31 |
| Unittest | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK |
| Typing / SPI externo / manifiesto / compat | Conformes | Conformes | Conformes |
| Smokes limpios wheel y sdist | Conformes | Conformes | Conformes |
| Conformance Memory y SQLite | Conforme | Conforme | Conforme |

La cobertura en 3.14 es **99,04262174353309 %** (mínimo 99 %). También pasan
las nueve propiedades deep, los smokes Search/vector de ambos motores, Ruff
sobre 83 archivos Python modificados y el formato de los siete archivos
seleccionados de este bloque. Se validaron los YAML, 33 bloques shell y tres
bloques Python de los workflows. Wheel y sdist se reprodujeron byte a byte
en dos builds secuenciales y superan `twine check`.

El canario consumidor usa ese mismo wheel y conserva **16 flujos y 48 replays
nativos conformes**, sin correctores semánticos ni modificaciones permanentes
del consumidor. Las bases temporales se eliminaron y los tres servidores de
prueba se detuvieron.

El diff final está revisado y la documentación está alineada. Se preserva el
trabajo autorizado anterior y no se observan cambios ajenos nuevos. No quedan
pendientes técnicos de este bloque. La publicación y la elección del número de
la próxima entrega quedan fuera de este encargo. La evidencia es local en
macOS arm64 y cubre el corpus finito indicado; no acredita ejecuciones de los
jobs alojados en GitHub ni compatibilidad universal fuera del corpus.

Identidad del workset, comandos, entornos, capturas y logs:
[registro de validación](evidence/write-operations/validation.json).

La ampliación posterior de [identidades null y regex](null-and-regex-identities.md)
renueva los golden afectados al observar `did_upsert` y acredita un candidato
nuevo. Este registro conserva la identidad histórica de su propio candidato.

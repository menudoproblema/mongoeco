# Validación previa de comandos de escritura

Mongoeco 4.10.0 corrige dos divergencias de comandos clásicos `insert`,
`update` y `delete`. El alcance incluye clientes PyMongo reales sync/async por
el proxy y las fachadas directas de Mongoeco, Memory/SQLite, códigos, contadores,
efectos parciales y reutilización de la conexión. No cambia motores, SPI ni
formato de archivos SQLite.

## Forma del lote y errores de ejecución

Los tipos inválidos de `hint`, `collation` y `sort` se validan en todas las
entradas antes de escribir el primer documento. Un `hint` escalar, array o
`null` produce **9 / FailedToParse**. Una `collation` escalar/array y, en MongoDB
8/9, un `sort` escalar/array producen **14 / TypeMismatch**. MongoDB 7 rechaza
la presencia de `sort` con **40415 / IDLUnknownField**, también si vale `null`.

`collation:null` y `sort:null` en 8/9 conservan su comportamiento válido. Un hint
bien formado que referencia un índice inexistente sigue siendo un error por
entrada **2 / BadValue**: ordered conserva el prefijo ejecutado; unordered
continúa con las entradas siguientes. La validación de forma y el fallo de
selección del índice tienen fronteras distintas.

## Write concern y confirmación efectiva

El comando valida `writeConcern` antes de aplicar escrituras. Los documentos de
concern inválidos producen **14**; `w` booleano, array, NaN o infinito produce **9**, igual que un valor numérico
truncado fuera de 0–50. Los modos que un host standalone no
admite producen **2**; campos desconocidos producen **40415**. Se conserva la
aceptación nativa de valores numéricos y Decimal128, truncamiento de `w`, tags
numéricos, campos heredados y concerns vacíos.

`writeConcern:null` es un delta comprobado: MongoDB **7/8** lo rechaza con **14**;
MongoDB **9** lo interpreta como el valor por defecto. El normalizador conserva
ambos comportamientos según el dialecto.

`w:0` sin solicitud de sincronización devuelve contadores a cero y omite
`upserted` y `writeErrors`. `j:true` o `fsync:true` hacen que MongoDB entregue
la respuesta confirmada, incluidos contadores y errores como **11000**. Ambos
flags activos a la vez producen **9**. Las decisiones de enviar respuesta por
`moreToCome` siguen perteneciendo al protocolo.

La normalización representa respuestas de un servidor local standalone. No
simula replicación ni añade garantías de durabilidad de un clúster.

## Corpus y aceptación

[106 casos nuevos](../tests/differential/write_preflight_cases.py) se añaden a
los 66 casos anteriores: **172 casos por fachada**, más cuatro controles raw.
El [observador independiente](../tests/support_unacknowledged.py) comprueba
presencia y ausencia de campos, códigos/codeName, contadores, IDs de upsert,
parcialidad, documentos finales y una escritura confirmada posterior.

El capturador exige MongoDB **7.0.43, 8.0.32 y 9.0.2** con su FCV exacta;
PyMongo **4.9.2 y 4.19.0** coinciden entre sí y entre sync/async. Las referencias
no importan Mongoeco, conservan los hashes de catálogo/observador y verifican
limpieza de las bases temporales.

Las regresiones ejecutan **2.088 casos wire** y **1.272 directos** con ambos
motores y las dos fachadas en los tres dialectos. SQLite utiliza archivos
reales. Los casos wire comprueban un único ID de conexión física mediante
CMAP, con retries desactivados, seguido de ping, lectura y escritura. Los cuatro
controles raw encadenan peticiones en el mismo socket y verifican sus IDs.

El workflow diferencial recaptura y compara las referencias; la matriz del kit
incluye también las nuevas regresiones directas contra el wheel instalado.
Los fallos del candidato anterior y los intentos intermedios se conservan en
el [registro de validación](evidence/write-command-preflight/validation.json).
Este trabajo no publica una versión.

# Documentos de error de findAndModify

Corrección posterior a 4.10.0, todavía sin publicar. No cambia la versión del
paquete, el formato SQLite ni las firmas de los métodos públicos de cliente.

## Defecto y corrección

La adaptación de errores introducida en 4.10.0 convertía `details=None` en
`{"codeName": "DuplicateKey"}`. Un cliente que consumiera `details["errmsg"]`
fallaba antes de poder recuperar una colisión de índice único. El código 11000
y la protección del documento eran correctos; el documento de error era parcial.

La frontera de `find_one_and_update`, `find_one_and_replace` y
`find_one_and_delete` completa el error de comando con `ok`, `code`, `codeName`
y el diagnóstico original en `errmsg`. Los motores conservan por separado el
índice y la clave rechazados en una colisión, sin reconstruirlos a partir del
texto ni volver a consultar documentos después del fallo. La frontera incorpora
`keyPattern`, `keyValue` y el diagnóstico `E11000` de MongoDB. La ampliación posterior de
[errores compartidos](shared-error-documents.md) corrige también los documentos
por elemento y la representación BSON de sus claves.

El contraste wire detectó también que un `bson.Binary` público ya materializado
se recodificaba como bytes de subtipo 0. El puente conserva ahora su subtipo,
tanto en los documentos devueltos como en `keyValue`.

## Corpus y observador

El corpus ejecutable pertenece a
[`find_modify_cases.py`](../tests/differential/find_modify_cases.py): 38 casos
de update/replacement con índices simples, compuestos y multikey, paths
anidados, upserts y colisiones de `_id`, null y claves BSON. Cada caso consulta
el documento existente y ejecuta una escritura válida después del rechazo.

Las capturas de MongoDB 7.0.43, 8.0.32 y 9.0.2 se obtienen con PyMongo nativo,
sin importar Mongoeco, y exigen igualdad sync/async. La matriz local compara
las referencias en Memory/SQLite, perfiles PyMongo 4.9/4.19 y las dos fachadas;
también usa clientes PyMongo reales contra el proxy local.

El observador conserva la distinción entre detalles ausentes, vacíos y
parciales para `findAndModify`. Compara presencia y tipos de los campos del
comando, `errmsg` no vacío, patrón/valor BSON, índice y representación de la
clave en el diagnóstico. Del mensaje duplicado solo normaliza el namespace
dinámico, comprobando que pertenece a la colección ejercitada. Mantiene los
contadores y efectos de la operación en las observaciones existentes.

El contenido literal de todos los demás mensajes y los metadatos volátiles de
clúster, así como el detalle de cada validador en `errInfo`, quedan fuera de
esta ampliación. No es una acreditación universal de todos los documentos de
error de MongoDB.

## Regresión y evidencia histórica

Los resultados de esta sección pertenecen al candidato inicial de la corrección.
El [cierre de errores compartidos](shared-error-documents.md) acredita el árbol
posterior; los recuentos históricos no se reutilizan como gates actuales.

Ocho pruebas anónimas de traducción/recuperación reproducen `KeyError('errmsg')`
con el wheel público de 4.10.0. Las mismas pruebas pasan con la corrección.
El [registro de validación](evidence/find-modify-errors/validation.json) conserva
la identidad del candidato, los comandos y resultados finales; las referencias
nativas actualizadas permanecen en `tests/fixtures`.

El workflow diferencial recaptura este corpus y lo comprueba junto con el
corpus anterior. Los resultados históricos de 4.10.0 no se reescriben.

La suite completa del wheel pasa en Python 3.13/3.14/3.15: **26.739 tests y
14.456 subtests por intérprete**, con 31 skips previstos. Unittest ejecuta
3.628 tests por intérprete, con 3 skips previstos. La cobertura de 3.14 es
**99,06 %**, por encima del floor de 99,00 %.

El arnés wire usa bucles independientes para cliente y servidor: el driver
async experimental de PyMongo 4.9 contiene polling de sockets bloqueante.
Tras este último ajuste exclusivo del arnés se repiten las **950 regresiones
focales** en los tres Python y se agrega su cobertura en 3.14. El mismo conjunto
pasa con PyMongo 4.9.2 y 4.19.0, incluidos 4 subtests de conservación de Binary.
El wheel, los estímulos del corpus y el resto de pruebas permanecen idénticos;
el registro distingue la suite completa de esta comprobación focal final.

Los snapshots, typing, SPI independiente, manifiesto público y dependencias
permanecen en verde. Las instancias privadas 7/8/9 se han cerrado y los datos,
logs y entornos de validación se han creado dentro del repositorio.

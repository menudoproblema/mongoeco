# Errores en rutas espejo

Corrección Unreleased posterior a 4.10.0. Continúa el corpus de
[errores compartidos](shared-error-documents.md), sin cambiar la versión publicada
ni el formato de almacenamiento SQLite.

## Contrato corregido

- Un modificador como `{"$set": 3}` es un error de servidor por elemento, código
  `9`. El lote ordenado conserva el prefijo válido; el no ordenado ejecuta también
  los elementos válidos posteriores, con los contadores correspondientes.
- Los filtros ausentes o con identificador inválido conservan `2`; filtros vacíos,
  no utilizados, con varias raíces y modificadores desconocidos conservan `9`.
  Las respuestas de escritura incluyen `writeErrors[].code`; PyMongo puede
  interpretarlas sin lanzar `KeyError` después de haber aplicado escrituras.
- Los dos errores numéricos ejercitados conservan sus códigos: raíz de un número
  negativo `28714` y división con un operando no numérico `14`. Esto no declara
  cobertura universal de diagnósticos para todos los operadores numéricos.
- Los errores de agregación conservan `ok`, `code`, `codeName` conocido y `errmsg`,
  tanto durante la preparación como al consumir el cursor. Se preservan también
  familia, etiquetas y el atributo público `code_name` de descriptores existentes.
  Un `output` de ventana mal tipado conserva `14` en los tres dialectos.
- Las escrituras clásicas conservan `DuplicateKeyError`, código `11000`, índice,
  clave rechazada y diagnóstico BSON. Se reutiliza el contexto obtenido bajo la
  autoridad de escritura. La inserción en lote comparte el formateador. El prefijo
  de ejecución de updates de MongoDB 9 se conserva como delta de dialecto.
- En un servidor standalone, `$$CLUSTER_TIME` es una variable reservada no
  disponible. La [ampliación posterior](error-contract-refactor.md) distingue
  operación y dialecto: `51144` en 7; en 8, count y `$match` de agregación usan
  `5611301`; en 9, distinct, borrado y findAndModify remove conservan `51144`
  mientras las otras rutas ejercitadas usan `10071200`. Las restantes rutas de 8
  conservan `51144`. Una variable desconocida conserva `17276`.

## Regresiones y observación

El corpus conserva los 102 casos anteriores y añade
[71 testigos ejecutables](../tests/differential/shared_error_mirror_cases.py):
update_one/update_many, findAndModify, replacement, pipelines, agregación,
comandos raw y lotes ordered/unordered. Cada testigo observa los documentos
posteriores y ejecuta una escritura válida de recuperación. Los casos bulk
incluyen elementos válidos antes y después del rechazo.

El observador conserva la estructura completa de errores y contadores. Para los
duplicados distingue el prefijo del diagnóstico, su índice, la representación
BSON de la clave y si el namespace corresponde a la colección solicitada. Solo
normaliza ese namespace; un mensaje genérico, un índice o clave distintos ya no
pasan por tener un `errmsg` no vacío. Las regresiones mutan cada componente.
También observa un `code` raw ausente como una diferencia, sin que falle el arnés.

La matriz contrasta MongoDB 7.0.43, 8.0.32 y 9.0.2 con clientes PyMongo reales,
fachadas sync/async, Memory/SQLite y clientes contra el proxy. Un listener del
pool exige una conexión física y su reutilización. Las regresiones adicionales
consumen cursores en streaming mediante iteración y `first()` y comprueban que
el cliente puede seguir escribiendo tras el error.

## Validación

La evidencia de esta ampliación se registra en
[`evidence/error-mirrors/`](evidence/error-mirrors/validation.json).
Las capturas nativas originales están en `tests/fixtures`; el registro anterior
sigue siendo un antecedente y no acredita el candidato actual.

El alcance es finito: no declara paridad de cada operador ni de cada topología,
ni compara literalmente todos los mensajes de error. La garantía del diagnóstico
de duplicado se limita a los tipos y caminos ejercitados por el corpus.

Validación local final del wheel: **30.973 tests y 14.456 subtests** por
intérprete en Python 3.13/3.14/3.15, con 31 skips previstos. Cobertura de
3.14: **99,03 %**, con floor 99,00 %. Unittest: **3.628 tests** y 3 skips
por intérprete. Las **4.226 regresiones focales** pasan con PyMongo 4.9.2
realmente instalado, incluida la matriz del proxy y ejecución con cuatro workers.
Pasaron tipado, SPI, manifiesto, lint, propiedades deep, snapshots, smokes de
búsqueda, builds reproducibles e instalaciones limpias de wheel/sdist/pytest
sin constraints. Fuentes, wheel y los cuatro runtimes instalados coinciden byte
a byte. Las referencias MongoDB privadas quedaron cerradas y sus datos eliminados.

Los fallos intermedios del arnés y las expectativas antiguas se conservan en el
registro de intentos originales. La comprobación independiente de firmas de
los seis SDK reutiliza evidencia con entradas causales sin cambios; no se
presenta como una ejecución nueva. La CI remota no se ha disparado en este
cambio. Los cambios permanecen en Unreleased.

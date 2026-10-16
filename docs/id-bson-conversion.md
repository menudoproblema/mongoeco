# Conversión explícita del tipo BSON de _id

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

## Contrato y alcance aprobado

El encargo del usuario autoriza permitir conversiones de representación BSON
numérica equivalente de `_id` en replacement y pipeline, añadir regresiones y
contrastar MongoDB real en sync/async y Memory/SQLite. Se exige abrir y mantener
archivos SQLite existentes. No incluye publicación ni cambios al consumidor.
Se reutilizan el corpus, observador y gates del bloque previo, cuyo workset es
`fe22ffa88173da33a4379415aac0de3aef58f017833bd4a01407fac6c2f4e2e6`.
No hay AGENTS.md ni lifecycle RFC adicional exigido en este repositorio.

## Censo y mecanismo suficiente

- Componer la validación lógica de identidad existente con una detección de
  representación BSON: cambiar solo el tipo cuenta como modificación.
- Mantener el rechazo clásico 66 y los errores de identidad 53/54/66. No admitir
  un valor lógico distinto ni confundir booleanos con números.
- Reutilizar las fronteras de rollback/transacción y publicación. Mover la clave
  física únicamente de la fila convertida y reconstruir sus referencias de
  índices en la misma frontera; conservar el orden de candidatos en Memory.
- Mantener esquema, formato de clave y serialización SQLite; abrir copias de las
  fixtures anteriores, verificar reapertura, índices, Search, TTL y outbox.
- Ampliar el catálogo de identidad existente y capturarlo exclusivamente con
  PyMongo nativo sync/async en MongoDB 7/8/9. Validar código, contadores, efectos
  parciales, tipo almacenado, no-op posterior y operaciones tras la conversión.

## Plan acotado

1. Capturar pares equivalentes en ambos sentidos, conversión aislada de `_id`,
   pipeline `$set`/`$replaceWith`, replacement, bulk, upserts y controles inválidos.
2. Corregir validación, detección de modificación y movimiento de claves con
   conservación de atomicidad, snapshots, índices y publicación.
3. Verificar archivos anteriores y fallos inyectados; ejecutar el corpus por
   API directa/raw/wire, construir artefactos reproducibles y cerrar gates.

Plan ejecutado; el resultado y la evidencia de cierre se registran abajo.

## Corrección y aceptación observable

La validación compara el valor lógico de `_id` y observa por separado sus tipos
numéricos BSON. Un cambio `int32 → int64/double/decimal128`, o la conversión
inversa equivalente, cuenta como modificación aunque no cambie el valor. El
segundo update idéntico es un no-op. Los updates clásicos siguen rechazando el
cambio de tipo con **66**; los cambios a otro valor, booleano o string también
se rechazan. Los guards anteriores de filas heredadas sin `_id` se conservan.

Los pipelines llevan su política de conversión por el `EvaluationEnvironment`
existente, fuera de los bindings visibles al usuario. Los ámbitos anidados la
conservan: se comprueban `$toLong`, `$toDouble`, `$toDecimal`, `$toInt`, `$convert`,
`$let`, constantes, campos y booleanos. `onError` conserva el tipo de su expresión
de fallback. La agregación ordinaria mantiene su política anterior. También se
conserva el contador nativo de un pipeline que elimina `_id` y lo restaura.

Memory cambia la clave dentro de sus raíces persistentes, conservando ordinal,
orden natural, snapshots y journal de rollback. SQLite hace `UPDATE` de la clave
y del payload, elimina las referencias antiguas y reconstruye multikey, escalar,
TTL y Search en la misma transacción. Conserva el `rowid`, esquema, formato de
clave y serialización. La publicación usa el ID convertido y ocurre una sola
vez; abortos y fallos inyectados no publican cambios.

## Corpus y contraste nativo

Se añaden **75 escenarios** al catálogo de identidad, que pasa de **246 a 321**,
y **70 vistas raw**, que pasan de **110 a 180**. Incluyen conversiones aisladas de
`_id`, ambos sentidos entre representaciones, IDs documento, no-op posterior,
selección por las representaciones vieja/nueva, conversión inversa, eliminación,
upserts, documentos retornados y recuperación tras errores.

PyMongo **4.19.0 sync y async** coincide en MongoDB **7.0.43, 8.0.32 y 9.0.2** con
FCV exacta: **870 observaciones nativas nuevas**. Las referencias solo proceden
del servidor real y ligan los hashes del catálogo y observador.

La matriz directa usa ambos motores, fachadas sync/async y perfiles 4.9/4.19:
**3.480 observaciones nuevas**, **12.024 de identidad en total**. Wire usa los
clientes oficiales sync/async con perfil 4.19 en doce celdas: **1.740 nuevas**,
**6.012 en total**. Se comparan BSON almacenado y retornado, familias, códigos,
codeName, contadores, `writeErrors[].op`, índices originales y efectos parciales.

Un update_many que convierte el primer documento y falla en el segundo conserva
el primero convertido y los demás intactos. Un modelo UpdateMany fallido dentro
de bulk conserva ese efecto, pero MongoDB no acredita matches/modificaciones
parciales de ese modelo en los contadores del bulk. Ordered/unordered conserva
los éxitos posteriores permitidos. El error es **66**, dentro de la envolvente
**65** de BulkWriteError. La unicidad numérica sigue usando **11000**.

El wheel anterior
`ecb65044a3d47ef9d2792cd10131907192179170acd30d5c738618251bf1a294`
presenta **404 divergencias en 580 observaciones** nuevas sobre MongoDB 8,
incluidas ambas fachadas y motores. El candidato intermedio
`7643dd9d84d982a949b1da03befe2cd5e51ce04b27dc12e69946774f6e19c6fe`
se descartó por relajar el guard de filas heredadas sin `_id`; se corrigió y se
actualizaron los test doubles del helper privado de limpieza de índices.

## Archivos existentes y conservación

Las regresiones abren copias de `numeric-identities-before-fix.sqlite`, creada
con el wheel anterior a la corrección de unicidad numérica, y verifican ambas
fachadas con replacement/pipeline. Cambia únicamente la clave de cada fila
convertida: el esquema y `rowid` permanecen idénticos. Se verifica reapertura,
lectura mediante ambas representaciones, índices, duplicados 11000, upsert que
encuentra el documento, eliminación y reutilización del ID. La fixture original
conserva su hash. La fixture histórica SQLite 4.5 también sigue en la suite.

Un lector aislado del wheel anterior `ecb650...` abre el archivo convertido por
el candidato nuevo y valida cinco filas por cada fachada, con tipos e índices
secundarios correctos. Se comprueba el runtime instalado del lector contra el
wheel anterior para evitar confundirlos por compartir el número provisional.

Las pruebas específicas cubren además índices escalares/multikey, Search textual
y vectorial, referencias TTL, orden y snapshots de Memory, commit/abort y fallos
inyectados después de mover la clave. El módulo de conservación y las regresiones
core aportan **20 pruebas** nuevas. El guard heredado y helpers se verifican
junto a ellas: **79 pruebas y 34 subtests** focalizados en el wheel final.

## Validación del candidato final

El wheel tiene SHA-256
`34b3dcd52d7728b284292dc5e66297b600ab7b4193b68e90a280bf63abefed32`;
el sdist,
`12c93bcbda199a4d5e2fd0641f010278c985a729e9a5e8750e83558137fd9fd5`.
Ambos se reproducen byte a byte en dos builds secuenciales y pasan twine.
Los 275 archivos de runtime coinciden con el checkout y las tres instalaciones.
El número 4.9.1 es provisional para el workset, distinto de la distribución
4.9.1 publicada; no se decide número ni se publica una versión en este bloque.

Las suites completas del wheel en Python **3.13.15, 3.14.5 y 3.15.0** pasan
**22.356 pruebas y 14.445 subtests** por versión, con **31 skips opcionales
esperados**. La cobertura es **99,05 %**: 415 líneas pendientes de 43.557.
Los gates typing, SPI externo, manifiesto y snapshots de compatibilidad pasan
sobre las tres instalaciones. Se conserva el trabajo previo y la política de
Python 3.13 hasta octubre de 2027.

Unittest pasa **3.613 pruebas**, con tres skips esperados, en cada intérprete.
Pasan los seis smokes limpios wheel/sdist, las seis ejecuciones de conformance
Memory/SQLite, las nueve propiedades deep y los smokes Search/vector de ambos
motores. El ratchet Ruff pasa sobre 104 archivos Python modificados; el formato
pasa en los siete seleccionados de este bloque. Se validan los workflows, sus
33 bloques shell y tres bloques Python, incluida la continuación del comando
pytest con las nuevas regresiones.

Los 324 probes de admisión de seis SDKs se reutilizan: introspeccionan los SDKs
oficiales y sus entradas, scripts y contratos de perfil no cambian. Las referencias
nativas de opciones de API también conservan sus entradas y observador; esa
independencia queda registrada por separado. No se reutilizan gates del runtime
corregido de candidatos anteriores.

El canario final pasa **16 flujos y 48 replays nativos** con el wheel exacto,
sin correctores semánticos ni cambios permanentes del consumidor. Las bases de
pruebas se eliminaron, los tres servidores se detuvieron y se verifican sus
puertos cerrados y directorios temporales eliminados.

Se revisó el diff final y se alineó la documentación, preservando el trabajo
previo. [validation.json](evidence/id-bson-conversion/validation.json) liga los
hashes de workset, runtime, artefactos, fixtures y logs. La evidencia es local
macOS arm64, sobre el corpus finito descrito; no acredita un run alojado de
GitHub ni paridad universal. No se migran o fusionan identidades duplicadas ya
almacenadas por versiones anteriores. No se crea commit, tag o publicación.

Estado: **implementación cerrada**.

# Identidad null y regex en escrituras

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

## Contrato y alcance

El usuario autoriza corregir los dos defectos observados en la revisión del
15 de octubre de 2026: upserts con `_id: null` y aceptación de un `_id` regex.
La aceptación exige paridad con PyMongo y MongoDB 7/8/9 en sync/async,
Memory/SQLite, incluidos códigos, contadores y efectos parciales. Se conserva
el trabajo previo y no se publica una versión en este bloque.

## Censo y mecanismo suficiente

- Reutilizar `UpdateResult` con un indicador `did_upsert` opcional en su
  constructor, compatible con los constructores anteriores. Este dato ya
  existe en PyMongo desde 4.9. Un ID null no puede representar ausencia.
- Propagar ese indicador por outcomes, adaptación, publicación, bulk y raw.
  Los conteos del SPI siguen describiendo matches reales; la API individual
  conserva la representación observada de PyMongo para un ID null.
- Ampliar la validación común de identidad para regex, conservando la
  distinción entre ID raíz y un documento ID que contiene valores regex.
- Componer regresiones sobre el catálogo y capturador de identidad existentes,
  y ampliar sus vistas raw. Los golden solo se obtienen de servidores reales.
- Ejecutar los gates del candidato instalado en 3.13/3.14/3.15 y conservar
  evidencia por hash. La adición compatible del resultado no cambia la versión
  del SPI ni exige migrar los constructores de engines externos.

## Plan acotado

1. Incorporar al corpus los testigos null/regex, controles de IDs falsy válidos,
   no-match/match, retornos, lotes ordered/unordered y recuperación posterior.
2. Capturar referencias nativas sync/async en los tres servidores y acreditar
   las divergencias del candidato anterior.
3. Corregir representación, consumidores del resultado y validación compartida;
   añadir regresiones de contrato y publicación de cambios.
4. Contrastar API directa, raw y wire; ejecutar gates globales, revisar diff y
   alinear documentación y evidencia del candidato final.

## Implementación y contraste nativo

`UpdateResult.did_upsert` conserva el hecho de haber insertado con independencia
del ID. Los constructores anteriores siguen funcionando, incluido el cuarto
argumento posicional `acknowledged`: el nuevo argumento es keyword-only. Los
IDs no null infieren el indicador por compatibilidad. Los engines integrados
lo fijan explícitamente al insertar, y los consumidores de outcomes, secuencias
de commit, eventos, bulk y comandos raw lo usan en lugar de comprobar el ID.
El export estático permite comprobar también el tipo bool del nuevo atributo.
El manifiesto público registra la adición compatible de constructor, campo y
miembro; no se cambia la versión del SPI.

Se conserva una particularidad observada de PyMongo 4.19: un upsert individual
con ID null expone `matched_count=1`, `modified_count=0`, `upserted_id=None` y
`did_upsert=True`. El outcome interno mantiene cero matches reales. Bulk
expone `nMatched=0`, `nUpserted=1` y el índice asociado a null; raw update
expone `n=1` y un elemento `upserted` con `_id: null`. findAndModify conserva
`lastErrorObject.upserted: null`, `n=1` y `updatedExisting=false`, incluso
cuando el retorno BEFORE es null.

La validación común rechaza regex públicos BSON, regex internos y patrones
Python como ID raíz. El código nativo es **53**, también en replacement
upserts; **54** sigue reservado al array inválido en esa ruta. Los IDs documento
con un valor regex anidado siguen siendo válidos. Los errores de lote conservan
índice, `op`, éxitos anteriores y posteriores permitidos por ordered/unordered.

El catálogo de identidad pasa de **82 a 143 escenarios**: **61 nuevos**.
Incluye null en inserción por upsert, match y no-match, BEFORE/AFTER,
controles `0`, `""` y `false`, los tres modelos bulk de update/replacement,
lotes con duplicados posteriores, regex en cada posición de insert batches,
upserts regex y controles de regex anidados válidos. Cada escenario conserva
una escritura posterior de recuperación. Se añaden **26 vistas raw** de esos
escenarios, sin un segundo catálogo de entradas.

Las capturas nativas coinciden entre PyMongo sync y async en MongoDB **7.0.43,
8.0.32 y 9.0.2**, con su FCV correspondiente. La ampliación añade **366
observaciones nativas de helpers** y **156 raw**. Los 143 escenarios completos
y sus 26 vistas raw se contrastan localmente en ambas fachadas, ambos motores
y perfiles 4.9/4.19: **4.056 observaciones directas**. Los clientes oficiales
sync/async comprueban además **2.028 observaciones por wire** con perfil 4.19.

También se recapturan los 170 escenarios de escritura, 42 de operación
fallida, ocho comandos y 104 casos de opciones que consumen el observador
compartido. Este observa ahora `did_upsert`; una regresión negativa distingue
un upsert null de un match con los mismos contadores públicos. Las evidencias
históricas conservan sus candidatos anteriores.

El candidato previo, wheel
`9030e0e05a0c0e34a8087ca4a6f120afc90a94abecb94f22c29b7e85172e4947`,
divergía en **208 de 244 observaciones** de los 61 casos nuevos sobre MongoDB 8
(dos fachadas y dos motores). Los controles que ya coincidían permanecen
conformes. La selección focal final pasa **4.093 pruebas**, incluidas 19 de
conservación y seis del observador. Se comprueban un único evento insert para
el upsert null, ausencia de eventos para un update posterior sin cambios,
publicación del siguiente update y mantenimiento de invariantes del SPI.

## Cierre de implementación

Completado el 15 de octubre de 2026. El wheel final tiene SHA-256
`fe4ea943e6b1d7f72fbe8f5fc68630840b68b3d8ed9ce075aae8822e0034b932`.
Sus 275 archivos de runtime coinciden con el checkout y las tres instalaciones.
El sdist también coincide con los 283 archivos correspondientes del checkout.
Ambas distribuciones se reproducen byte a byte en dos builds secuenciales y
superan `twine check`. Conservan provisionalmente el número 4.9.1 para pruebas;
son builds de trabajo no publicados, distintos de la 4.9.1 distribuida.

| Gate del wheel instalado | Python 3.13.15 | Python 3.14.5 | Python 3.15.0 |
| --- | --- | --- | --- |
| Pytest completo | 13.670 passed | 13.670 passed | 13.670 passed |
| Subtests / skips esperados | 14.445 / 31 | 14.445 / 31 | 14.445 / 31 |
| Unittest | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK |
| Typing / SPI externo / manifiesto / compat | Conformes | Conformes | Conformes |
| Smokes limpios wheel y sdist | Conformes | Conformes | Conformes |
| Conformance Memory y SQLite | Conforme | Conforme | Conforme |

La cobertura en 3.14 es **99,04 %** y supera el mínimo 99 %. Pasan además las
nueve propiedades deep, los smokes Search/vector de ambos motores, el ratchet
Ruff sobre 88 archivos Python modificados y el formato de los diez archivos
seleccionados de este bloque. Se validaron los workflows, sus 33 bloques shell
y tres bloques Python. Los seis SDKs PyMongo aislados (4.9.2, 4.11.3, 4.13.2,
4.17.0, 4.18.2 y 4.19.0) pasan sus 324 probes de admisión sin cambiar la fixture.

El canario consumidor usa el wheel exacto y pasa **16 flujos y 48 replays
nativos**, sin correctores semánticos ni cambios permanentes del consumidor.
Los tres servidores temporales se detuvieron después de comprobar que solo
quedaban las bases admin/config/local; sus directorios temporales se eliminaron.

Se revisó el diff, incluido el delta de runtime frente al wheel anterior, y la
documentación quedó alineada. Se mantiene el trabajo autorizado anterior y no
se observan cambios ajenos nuevos. Un doble unitario que devolvía un
`SimpleNamespace` incompleto se sustituyó por el `UpdateResult` real; las suites
finales incluyen esa corrección. No quedan pendientes técnicos de estos dos
defectos. La evidencia es local en macOS arm64 y cubre este corpus finito; no
acredita ejecuciones alojadas en GitHub. La publicación y la selección del
número de la próxima entrega quedan fuera de este encargo.

Identidad, hashes, comandos, entornos y logs:
[registro de validación](evidence/null-regex-identities/validation.json).

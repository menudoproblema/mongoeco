# Errores de comandos y escrituras por elemento

Corrección Unreleased posterior a 4.10.0. Las [rutas espejo](shared-error-mirrors.md)
amplían este corpus con 71 testigos adicionales. Complementa la corrección inicial de
[findAndModify](find-modify-errors.md), sin acreditar compatibilidad universal.

## Alcance de las correcciones

| Hallazgo | Corrección observable |
| --- | --- |
| Errores por elemento sin código | División por cero conserva `2`, módulo por cero `16610` y filtros de array con identificador repetido `9`; el driver wire recibe el error de escritura, sin `KeyError('code')`. |
| Documentos de comando parciales | La frontera del comando conserva `ok`, `code`, `codeName` cuando se conoce y `errmsg`, también en errores de parseo y variables de pipeline. |
| Familia de excepción incorrecta | Los errores de compilación o ejecución de un elemento se adaptan a `WriteError`; los errores del comando conservan `OperationFailure`. La decisión depende de la fase, sin una lista cerrada de códigos. |
| Resumen bulk incompleto | `BulkWriteError.details` incluye `writeConcernErrors: []` cuando no hay errores de write concern. |
| Mensajes contaminados | Los serializadores y el profiler usan el diagnóstico original; `errmsg` no incorpora la representación Python `full error:`. |
| Metadatos de duplicado incorrectos | Se conserva el índice y la clave rechazados bajo la autoridad de escritura. Un array vacío distingue el `null` de `keyValue` del `undefined` del diagnóstico; la colación conserva claves opacas, `hexEncoded` cuando corresponde y su documento completo. |
| Representación BSON incorrecta | El diagnóstico de duplicado conserva bytes y subtipos, UUID, `Code` y scope, MinKey/MaxKey y las reglas de truncamiento ejercitadas por el corpus. |
| Claves JavaScript no hashables o confundidas | Memory y SQLite distinguen texto, `Code` sin scope y scopes distintos. Las consultas, agrupaciones e índices no pierden esa identidad. Los archivos SQLite antiguos siguen siendo legibles. |
| Observador permisivo | Las regresiones distinguen detalles ausentes/vacíos, campos ausentes y mal tipados, mensajes vacíos o contaminados, códigos por elemento y `writeConcernErrors`. |

Ocho regresiones adicionales comprueban el documento completo del rechazo directo
de `sort` de MongoDB 7; se contrasta también con los dos métodos y fachadas nativos.

La misma revisión corrige dos espejos: el comando raw `update` aplica la
capacidad del servidor para `sort`, sin heredar la restricción de firma del
perfil PyMongo 4.9; las consultas SQLite con valores BSON compuestos recurren a
la evaluación semántica en vez de enviarlos como parámetros SQL escalares.

MongoDB 7 conserva `Code` sin scope en el identificador de `$group`; 8 y 9 lo
materializan como texto. El corpus conserva esa diferencia de versión y sigue
exigiendo grupos separados para texto y JavaScript.

## Corpus y aceptación

Los [173 casos ejecutables](../tests/differential/shared_error_cases.py) incluyen
update/replacement, pipelines, agregación, comandos raw y lotes ordered/unordered
con elementos válidos antes y después del error. Las observaciones conservan
códigos, familia y estructura pública de la excepción, contadores, operaciones
rechazadas y documentos después de cada paso. Cada caso ejecuta una operación
válida posterior para comprobar que el cliente puede seguir usándose.

Las referencias se capturan con PyMongo nativo contra MongoDB **7.0.43, 8.0.32 y
9.0.2**, con FCV correspondiente e igualdad sync/async. Se comparan con las
fachadas directas y clientes PyMongo reales contra el proxy, Memory/SQLite y
perfiles 4.9/4.19. El listener del pool exige una sola conexión física y su
reutilización. El [arnés](../tests/integration/api/test_shared_error_parity.py)
no comparte el bucle del servidor con el cliente async experimental de 4.9.

Otros [851 vectores](../tests/differential/collation_diagnostic_cases.py) por
versión comparan literalmente los bytes de las claves opacas con colación `en`:
strength 1/2/3, caseLevel, numericOrdering, backwards, shifted, maxVariable y
normalization. Cubren Unicode, números largos, contracciones y separadores.
Las opciones avanzadas siguen requiriendo el backend opcional previamente
establecido; validar su diagnóstico no amplía la admisión del motor.

Se conserva el corpus anterior completo. Las pruebas de mutación del observador
introducen las omisiones y contaminaciones originales y exigen detectarlas.
La [regresión de SQLite existente](../tests/integration/api/test_code_index_legacy.py)
reabre una representación física antigua de `Code`, conserva esquema, payload y
storage key, comprueba igualdad tipada y unicidad y ejecuta `integrity_check`.
La evidencia incluye además un archivo producido por el candidato anterior.

## Datos de colación y límites

El diagnóstico necesita la representación de ICU 57.1 usada por los servidores
comprobados. Se distribuye un codec Python y datos raíz derivados de
[FractionalUCA de ICU 57.1](https://github.com/unicode-org/icu/blob/release-57-1/icu4c/source/data/unidata/FractionalUCA.txt).
El [extractor](../scripts/update_collation_diagnostics.py) exige el SHA256 del
origen oficial y reproduce byte a byte el recurso incluido. Se conserva la
[licencia y los avisos de ICU](../src/mongoeco/core/resources/ICU-LICENSE.txt)
en las distribuciones; la expresión de licencia del paquete incluye ICU.
No cambia el comparador de almacenamiento ni requiere una dependencia nativa
adicional. El recurso y su extractor usan UTF-8 explícito; una regresión carga
las claves en un proceso con locale ASCII y modo UTF-8 desactivado. El arnés de
archivos SQLite cierra también sus conexiones auxiliares explícitamente y pasa
con los avisos de recursos tratados como errores.

La garantía es finita. Fuera de los diagnósticos de duplicado ejercitados, el
observador comprueba la forma de `errmsg`, sin comparar todos los textos
literales. No compara relojes de clúster ni todos los detalles de validadores en
`errInfo`. No afirma paridad para cada carácter Unicode u opción de colación.
Una exploración con una cadena acentuada dentro de un documento indexado con
colación produjo BSON UTF-8 inválido en MongoDB 8, rechazado por PyMongo antes de
poder observar el error. Ese caso no se cuenta como paridad de documentos de
error; el corpus sí incluye un documento anidado cuyo diagnóstico es decodificable.

## Evidencia del candidato anterior

El registro de cierre y los comandos se conservan en
[`evidence/shared-errors/`](evidence/shared-errors/validation.json).
Las capturas completas están en `tests/fixtures`; los fallos iniciales de esta
implementación se conservan como antecedentes, separados de los gates finales.
El registro de findAndModify anterior corresponde a otro candidato y permanece
como evidencia histórica, no como comprobación del árbol actual.

Estos resultados corresponden al candidato anterior a la ampliación de rutas
espejo; la [validación actual](shared-error-mirrors.md) se registra por separado.

Validación local de aquel wheel: **29.239 tests y 14.458 subtests** por
intérprete en Python 3.13/3.14/3.15, con 31 skips previstos y sin avisos de
recursos en la repetición final. Cobertura de 3.14: **99,03 %** (floor 99,00 %).
Unittest: 3.628 tests y 3 skips por intérprete. Las 3.440 regresiones focales
pasan también con el SDK 4.9.2 instalado. Los seis perfiles aislados de SDK,
tipado, SPI, manifiesto, dependencias, snapshots, propiedades, lint e
instalaciones limpias pasan. La CI remota no se ha disparado en este encargo;
no se cambia la versión ni se publica el candidato.

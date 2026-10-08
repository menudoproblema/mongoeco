# Soporte de MongoDB 9.0 y PyMongo 4.18 en Mongoeco 4.9.0

Mongoeco permite seleccionar `mongodb_dialect="9.0"` y
`pymongo_profile="4.18"` en clientes sync y async con Memory o SQLite. Los
defaults continúan siendo `7.0` y `4.9`. `9` es alias de `9.0`; `9.1` y una
major desconocida se rechazan. La autodetección de PyMongo reconoce patches
de 4.18 exactamente; una minor 4.x posterior usa el último perfil conocido en
modo flexible y falla en modo estricto.

```python
from mongoeco import MongoClient

with MongoClient(mongodb_dialect="9.0", pymongo_profile="4.18") as client:
    collection = client.example.records
```

`pip install 'mongoeco[mongodb9]'` añade PyMongo >=4.18.2,<5. No cambia la
selección del dialecto. Dialecto, perfil y capacidades wire siguen siendo
independientes: el proxy conserva `maxWireVersion=20` y su subset existente.
No hay cambio de Engine SPI v2, `search-v1`, CXP exchange ni formato SQLite.

## Semántica verificada de 9.0

| Familia | Comportamiento local y evidencia |
| --- | --- |
| Arrays | `$map` y `$filter` admiten `arrayIndexAs`, con `$$IDX` por defecto; `$reduce` admite también `as` y `valueAs`. Los bindings son léxicos, incluidos arrays anidados y `let`. Los aliases inválidos y colisiones se rechazan antes de leer filas. `$$IDX` no es una variable global. |
| Conversiones | `$convert.base` admite 2, 8, 10 y 16 para el subset escalar numérico. Las cadenas deben contener dígitos de esa base, sin prefijos ni espacios. La validación de una base inválida no se oculta con `onError`; una entrada null conserva `onNull`. `$toString` admite objetos, arrays y los tipos BSON cubiertos por el codec, con representación compacta verificada. |
| Fechas | `$dateAdd`/`$dateSubtract` reutilizan los evaluadores existentes, verificados para nueve unidades, cambios de hora y fechas anteriores al epoch. Las ventanas de rango con `unit` incluyen ambos extremos y admiten `current`/`unbounded`. Sus límites internos pueden superar el rango de `datetime` sin cambiar fechas almacenadas. Offsets fuera de Int32 fallan con 9/FailedToParse; los errores de dateAdd se conservan. |
| Densify | Los límites explícitos semiabiertos y la conservación de originales son comunes a 7/8/9. `full` usa extremos globales, incluidos por partición. Los bounds iguales y la entrada vacía con particiones mantienen los deltas reales indicados abajo. En 9.0 se rechazan los solapamientos entre el campo y `partitionByFields`. |
| Validación | `$group` rechaza nombres de campo vacíos. Trim limita `chars` a 4096 bytes UTF-8. `$$CLUSTER_TIME` falla explícitamente porque el runtime local es standalone. Estos controles cubren entrada vacía, pipelines anidados y rutas compiladas. |
| Índices | `list_indexes`, `index_information` y `listIndexes` presentan `collation: {locale: simple}` cuando corresponde, sin reescribir la metadata persistente. Los índices ordinarios se recrean con collation simple explícita o implícita conservando identidad. Esto no acredita paridad completa de metadata ni de creación de `_id_`. |
| Orden de campos | La inserción de `$merge` conserva el orden de campos de la entrada en 9.0. Las actualizaciones de documentos existentes mantienen la ordenación de campos añadidos observada en el servidor. El orden de campos de documentos anidados se conserva. |

Con `bounds: full`, un step que no puede avanzar por precisión de un float
enorme o infinito conserva el único original en 7.0; 8.0/9.0 fallan con
5897900/Location5897900. Mongoeco detecta ese bloqueo numérico antes del límite
de generación del servidor. Las capturas por versión protegen esta diferencia.

Los operadores, queries y updates existentes heredan el subset de 8.0. Las
optimizaciones físicas de `$lookup` que solo acreditan 7.0/8.0 conservan el
fallback semántico para 9.0. Las capabilities nuevas se limitan a los deltas
implementados; la lista ejecutable se obtiene del catálogo público.

Las correcciones de `$densify` cambian intencionalmente resultados de 7/8:
con entrada `[-1,1,4]`, bounds `[0,3]` y step 1, se conserva
`[-1,0,1,2,4]`. También se preservan duplicados y valores desalineados, y la
entrada vacía sin particiones genera `[0,1,2]`. Los bounds iguales generan un
valor en 7 y ninguno en 8/9; con entrada vacía y particiones, 7 genera valores
sin claves de partición y 8/9 devuelve vacío. `bounds="partition"` y `$out`
continúan fuera del subset; se verifica `$merge` y el rechazo de `$out` sin
escrituras. Las comparaciones usan `$sort` explícito; cuando quedan empates se comparan
multiconjuntos que conservan todas las ocurrencias y campos. Los originales
con campo ausente o nulo pasan íntegros; las particiones ausentes y nulas son
distintas. Las claves de partición pueden ser documentos o arrays. Bounds y
step numéricos tienen el mismo tipo BSON; las unidades deben concordar con
los valores de fecha. Los errores de especificación se validan aun sin filas.

## Extensiones aplazadas y límites

Con `base`, un entero Long conserva su rango; los double y Decimal128
integrales deben caber en Int32. Los valores fuera de ese subset siguen
`onError` o producen `ConversionFailure` (241), según las capturas reales.

La conversión JSON/EJSON a objetos o arrays, conversiones `binData`, opciones
`format`, `byteOrder` y opciones de target como `subtype` quedan fuera del
subset. En 9.0 se rechazan explícitamente, incluso con `onError`, entrada
vacía o pipelines anidados. Un target dinámico también se valida al resolverlo.
`wildcardProjection` se rechaza antes de crear cualquier índice de un lote;
es un límite de Mongoeco bajo 9.0: el servidor real sí lo soporta. Los
dialectos 7.0/8.0 conservan su aceptación histórica sin efecto.

La extracción geodésica de claves de índices 2dsphere está fuera del subset
planar local. El cambio de error del servidor 9.0 a `GeoKeyExtractionFailed`
se conserva en las capturas; no se anuncia esa capacidad ni se aplica su
código a errores de queries planares. Tampoco se añade semántica distribuida,
Queryable Encryption, `Database.aggregate` ni nuevas garantías Atlas Search.

La metadata de collation conserva el subset local existente; presentar
`locale: simple` no promete reproducir todos los defaults ICU del servidor.
El engine conserva su contrato histórico de `_id_`: lo lista incluso antes
de crear el namespace, presenta `unique=True` y permite recrearlo con esa
opción. MongoDB real lista vacío antes del namespace, omite `unique` en la
metadata y rechaza una opción `unique` explícita con código 197. La API acepta ahora la recreación por defecto y devuelve el nombre pedido
(`_id_1` implícito), conservando `_id_` en storage. Mantiene la aceptación
histórica de `unique=True`; su firma pública `unique=False` tampoco distingue
omisión de `False` explícito. Esa opción en `_id_` es una extensión local,
no paridad de la especificación nativa. Las capturas de esos límites no se
cuentan como paridad.
La metadata local tampoco reproduce `v: 2` ni la omisión nativa de
`unique=False`. El roundtrip acreditado compara nombre, claves y collation
de índices ordinarios; el roundtrip local incorpora también `_id_` y verifica
el almacenamiento completo por separado.
Los contratos de almacenamiento y las fachadas 7/8 no se canonicalizan.

`$const` sigue fuera del catálogo de expresiones. En 9, `"$$"`, `"$$."` y
`"$$.campo"` fallan con 9/FailedToParse durante la preparación y evaluación;
nombres inválidos también fallan con 9. Una variable desconocida conserva
17276/Location17276 y tiene precedencia sobre errores de su path. Los paths
con punto final, componentes vacíos, `$` inicial o byte nulo conservan los
códigos nativos 40353, 15998, 16410 y 16411 respectivamente. `$literal` mantiene
el texto y los scopes léxicos no se amplían. Los dialectos 7/8 conservan su
validación histórica por fila; sus capturas vacías no acreditan rechazo temprano.

## Perfil y driver 4.18

La frontera común de agregación rechaza opciones reservadas `aggregate` y
`pipeline` con `ConfigurationError` antes de crear cursores o ejecutar I/O.
Se aplica a `aggregate`, `aggregate_raw_batches` y helpers de índices Search
en ambas fachadas. Los argumentos Python duplicados siguen siendo `TypeError`.

El driver asigna un `operation_id` por invocación del plan y un `request_id`
por intento. Retry y cancelación conservan la correlación y el cleanup del
pool. Los IDs se publican en spans y eventos, sin etiquetas de métricas de
alta cardinalidad. Esta identidad pertenece al driver y no modifica las
identidades persistentes del outbox ni el SPI de engines.

La resolución SRV normal del perfil 4.18 usa la API pública
`pymongo.uri_parser.parse_uri`, con timeout, service name, límite de hosts,
TXT y `srvAllowedHostsSuffix`. Requiere PyMongo >=4.18 y convierte rechazos
de configuración a la excepción pública local. Los resolvers inyectados
mantienen el contrato de simulación anterior; se rechaza el suffix con ellos.

Las pruebas con PyMongo 4.18.2 real cubren BSON y RawBSON, y propiedad de
buffers `bytes`, `bytearray` y `memoryview` a ambos lados de 4096 bytes. No fue
necesario modificar el codec ni elevar la versión wire declarada.

## Captura y reproducción

`tests/differential/version_delta_cases.py` contiene 154 casos sin importar
Mongoeco. Las fixtures `mongodb_version_deltas_7_0.json`, `8_0.json` y
`9_0.json` conservan versión real, FCV, versión de PyMongo, manifest de casos
y SHA-256 del corpus. Se capturaron en 7.0.43, 8.0.32 y 9.0.2 con PyMongo
4.18.2. Las expectativas proceden exclusivamente de servidores reales.
Las fixtures históricas anteriores se conservan.

El corpus de revisión `tests/differential/review_improvement_cases.py`
añade 72 capturas por versión con el mismo registro de runtime/FCV.
La matriz histórica [de cobertura efectiva](evidence/mongodb9-pymongo418-improvements/capture-coverage.json)
distingue comparaciones completas, comparaciones de un subset y capturas
de caracterización. Una captura almacenada no constituye una prueba de paridad.
Se reproduce con `--case-set review-improvements` y un `--output` propio.
El corpus `semantic_guarantee_cases.py` añade las regresiones de 4.9.0 y se
reproduce con `--case-set semantic-guarantees`. La matriz vigente y los gates
del candidato se enlazan desde el registro de mejoras; los recuentos anteriores
conservan su fecha y no sustituyen su verificación final.

```bash
mkdir -p artifacts
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
python scripts/capture_differential_replay_golden.py \
  --target 9.0 --case-set version-deltas \
  --output artifacts/mongodb_version_deltas_9_0.json \
  --check tests/fixtures/mongodb_version_deltas_9_0.json

MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
python scripts/run_mongodb_real_differential.py 9.0 \
  --json-report artifacts/mongodb9.json
```

La captura usa bases con UUID y las elimina en `finally`. El runner falla
ante suite vacía, conexión fallida, versión o FCV incorrectas, transición FCV
y cualquier omisión inesperada. El filtro por glob nunca convierte cero
casos en éxito. La suite común comprende 29 casos por versión.

CI fija imágenes oficiales por digest para las tres versiones y establece y
verifica su FCV. La release ejecuta el diferencial contra el mismo wheel
verificado que sus otros gates. La matriz PyMongo comprueba en entornos
aislados 4.9.2, 4.11.3, 4.13.2, 4.17.0 y 4.18.2. Un timeout de selección
solo acredita aceptación de argumentos; `ConfigurationError` es rechazo y
los resultados indeterminados no se convierten en positivos.

La evidencia del candidato corregido y sus gates se registra en
[el roadmap de mejoras](mongodb9-pymongo418-improvements-progress.md).
El registro anterior conserva exclusivamente la evidencia de su candidato.

La [matriz vigente de 4.9.0](evidence/mongoeco-4.9.0/capture-coverage.json)
distingue paridad ejercitada, caracterización y límites de cada comparación.
El [registro de mejoras](mongodb9-pymongo418-improvements-progress.md) conserva
el estado de los gates; los artefactos anteriores no acreditan este candidato.

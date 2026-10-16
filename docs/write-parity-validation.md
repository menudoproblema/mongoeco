# Escrituras fiables para testkits: implementación y validación

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

La implementación ejecuta el [plan acordado](plan-write-parity.md), junto al
soporte PyMongo 4.19 ya preparado. Esta es una entrega pendiente de decisión de
versión y publicación. El número del paquete en el checkout sigue siendo 4.9.1;
los wheels de ensayo se identifican por SHA, y no equivalen a la 4.9.1 publicada.

La ampliación posterior de [identidad y validación](write-identity-validation.md)
añade 82 escenarios y nuevas correcciones. Este informe conserva la evidencia
del primer candidato; la identidad y los gates del candidato posterior se
registran por separado en esa ampliación.

## Correcciones

- `insert_many` admite `ordered=True/False` en sync y async. Un error del servidor
  conserva éxitos previos y, en unordered, permite los posteriores. Expone
  `BulkWriteError`, índices originales y contadores parciales. Los errores de
  forma detectados antes del envío siguen sin escribir.
- La frontera pública divide los lotes rechazados por la validación atómica de
  Memory/SQLite y conserva el camino batch de los lotes válidos. Los engines sin
  batch se consumen por elemento, contabilizando el prefijo sin reintentarlo.
  El SPI v2 y la preparación por bloques de SQLite no cambian de contrato.
- Los modelos bulk inválidos se rechazan antes de ejecutar el prefijo. Se
  conservan las familias públicas de error y el código 14 de `$inc` sobre un
  valor no numérico; findAndModify conserva los nombres de código nativos.
- Los comandos clásicos devuelven `ok: 1` con `writeErrors` por elemento; `n`
  incluye los upserts. El parsing inválido de updates/deletes rechaza el comando
  completo antes de escribir, con códigos 9 o 14 en las regresiones capturadas.
- El reemplazo idéntico que omite `_id` informa `modified_count=1`, como los tres
  servidores comprobados. Con `_id` explícito se conserva el no-op. Esto corrige
  una suposición del plan inicial, sin cambiar sus entradas.
- SQLite conserva la unicidad entre `Int32(1)` e `Int64(1)`, usando igualdad BSON
  en el fallback semántico. No se reescriben índices persistidos ni su formato.
  Los sub-lotes de insert usan ordinales globales de outbox y publican cada éxito
  una vez.

Las capturas iniciales de `insert_many` están en
[evidencia B0](evidence/write-parity/insert-many-before-native.json). Su MongoDB
7.0.39 es preliminar; los goldens de admisión usan **7.0.43 / 8.0.32 / 9.0.2**, FCV
exacta y PyMongo 4.19.0. Los códigos, contadores y documentos proceden del servidor.

## Corpus y consumidores efectivos

El inventario sigue siendo [el manifiesto aprobado](write-parity-corpus.json).
`tests/differential/write_cases.py` materializa los 56 especímenes nuevos en
Python y reutiliza los 114 inputs posicionales. La prueba de inventario compara
esas entradas con el plan; el JSON no se ejecuta como un lenguaje de runtime.
El hash del fixture posicional histórico permanece intacto.

| Frontera | Referencia / ejecución exigida | Consumidor |
| --- | ---: | --- |
| API directa | 510 capturas; 4.080 observaciones locales | `test_write_parity_corpus.py` |
| Historias Q01–Q06 | 180 nativas; 1.440 locales | `test_write_histories.py` |
| Raw / wire | 24 referencias raw; 96 + 96 vistas locales | `test_write_command_parity.py` |
| Consumidor C01–C08 | 16 flujos; payloads contrastados en los tres servidores | `run_write_consumer_canary.py` |
| Parsing auxiliar | 36 capturas; guards de ausencia de efectos | `test_write_command_parity.py` |
| Observador / publicación / recuperación | Tipos BSON, errores, eventos y motor utilizable | `test_write_observer.py`, `test_insert_many_write_contract.py` |

`tests/support_write_parity.py` es el observador independiente común. Conserva
resultados, familias/códigos, errores bulk, contadores parciales y caller. La
proyección histórica posicional lo reutiliza. El BSON conserva tipos y orden
anidado; solo los ObjectId generados se tokenizan mediante una biyección. M08
mantiene exclusivamente la envolvente de dos estados admisibles declarada en el
plan. Timeouts y fallos de transporte no se guardan como resultados semánticos.

Las historias registran invocación y respuesta por actor y repetición. Las
barreras coordinan el inicio y no fijan ganador; Q04 usa conexiones SQLite
independientes al mismo fichero. No se reintenta una historia fallida.

El canario copia el testkit real a un directorio temporal y superpone el wheel y
PyMongo 4.19. Conserva adaptación async, codecs, clock y lifecycle. Retira los
correctores semánticos y hace fallar cualquier llamada a ellos; captura las
peticiones que llegan a Mongoeco y las reproduce con PyMongo nativo. C08 usa
`MongoStorage`, attachment y su probe reales. C04 y C05 son las dos fases del
mismo antecedente consumidor, separadas con su estado de entrada explícito.
No se modifica el repositorio consumidor ni su pin. La adopción permanente es
trabajo de su owner. La evidencia identifica fuentes, imports, overlay y wheel.

## Comandos efectivos

```bash
python scripts/capture_write_parity.py --target 8.0 \
  --output-dir /tmp/write-capture --histories-dir /tmp/write-histories \
  --check-dir tests/fixtures

python -m pytest -q \
  tests/integration/api/test_write_parity_corpus.py \
  tests/integration/api/test_write_histories.py \
  tests/integration/api/test_write_command_parity.py \
  tests/integration/api/test_insert_many_write_contract.py \
  tests/unit/test_write_observer.py tests/unit/test_write_corpus_inventory.py

python scripts/run_write_consumer_canary.py \
  --consumer-root "$CONSUMER_ROOT" --consumer-python "$CONSUMER_PYTHON" \
  --source-root "$CONSUMER_SOURCE_ROOT" --consumer-module "$CONSUMER_MODULE" \
  --consumer-tests "$CONSUMER_TESTS" \
  --wheel "$WHEEL" --output /tmp/consumer-canary.json \
  --uris "$MONGODB_7_URI" "$MONGODB_8_URI" "$MONGODB_9_URI"
```

Las rutas de fuentes y tests son relativas a `CONSUMER_ROOT` o absolutas;
`CONSUMER_MODULE` identifica el módulo adaptador del checkout privado. El
runner no fija identidades de consumidores en código. La
[anonimización de evidencia](evidence/consumer-privacy/redaction.json) conserva
las observaciones y los hashes originales de los informes anteriores.

El capturador requiere `MONGOECO_REAL_MONGODB_URI`, verifica patch/FCV, inventario,
limpieza y ausencia de imports de Mongoeco. `--allow-local-patch` solo identifica
ensayos preliminares; el workflow de admisión no lo utiliza. CI ejecuta captura
con `--check-dir` y los consumidores de las vistas sobre el candidato instalado.

El registro final de gates e identidad del candidato se conserva en
[evidencia de validación](evidence/write-parity/validation.json). La comparación
Memory/SQLite, los goldens generales de agregación y la evidencia histórica de
PyMongo 4.19 no sustituyen estas referencias nativas de escritura.

## Resultado de los gates

- Wheel final contrastado byte a byte con los 275 ficheros de código/recursos
  del runtime. SHA-256:
  `7a703985c5f2a5e83e2101ba6948065617b0d70b40a009392e4e12d3533c6c2c`.
- Python 3.13 y 3.14: **8.976 passed, 31 skipped esperados y 10.699 subtests**
  en cada suite completa instalada. Los skips corresponden a gates opcionales
  sin su configuración; la matriz nativa seleccionada se ejecutó separadamente
  sin omitir celdas. Cobertura: **99,04 %** (417 líneas pendientes de 43.359).
- Unittest: **3.613 pruebas**, tres skips esperados. Propiedades `deep`: nueve
  pruebas; el perfil `ci` también está incluido en las suites completas.
- Corpus instalado: 4.080 observaciones directas, 1.440 historias y 96 + 96 vistas
  raw/wire; ningún fallo ni celda omitida. Las referencias nativas son 510
  escrituras, 180 historias y 24 comandos, con los tres patches/FCV exactos.
- Canario: **16 flujos reales**; se reprodujeron sus payloads en los tres
  servidores para cada motor (48 comprobaciones, cubriendo las 24 referencias
  exigidas). Cero invocaciones de correctores; imports de wheel/testkit,
  aislamiento con nombres únicos y limpieza comprobados.
- Seis SDK: **324 sondas**, cero indeterminadas. Typing: contrato positivo y
  16 errores negativos exactos. Manifiesto, snapshots actuales, compatibilidad
  SQLite, conformance Memory/SQLite y canario SPI externo: verdes. También
  pasan los imports del core sin PyMongo/BSON.
- Se mantienen el diferencial general, los 114 posicionales y las 284 capturas
  generales por servidor. Estas últimas siguen siendo evidencia de sus corpus
  de agregación/índices y no se cuentan como escrituras adicionales.
- Ratchet de lint conforme a la base de CI `v4.9.1`, formato de los módulos
  nuevos y revisión de diff/whitespace: verdes. El workflow de CI se amplía;
  esta evidencia corresponde a ejecución local, no a un run alojado de GitHub.

Los benchmarks conservan baseline y candidato, size=1000, warmup=1, cinco
repeticiones y hashes de harness/dataset/salidas idénticos. La matriz inicial
mostró hasta un 24,4 % en algunas medianas mientras coexistían otros gates. La
confirmación aislada de SQLite queda dentro del 4,1 %; Memory dio hasta un
14,7 % en la primera repetición y un 2,8 % al invertir el orden candidato/base.
Las variaciones mayores no se reproducen de forma estable en estos ensayos.
Se conserva toda la evidencia, sin atribuir una mejora ni prometer latencia.

## Compatibilidad y siguiente decisión

Son correcciones de fidelidad nativa dentro del alcance aprobado. Los tests o
consumidores que capturaban `DuplicateKeyError` o `DocumentValidationFailure` de
`insert_many` deben capturar `BulkWriteError` y consultar `details.writeErrors`.
Los consumidores raw deben examinar `writeErrors`; los helpers de colección
siguen exponiendo las excepciones que corresponden a su ruta PyMongo.

No se aumenta ningún presupuesto, no se añade perfil/modo de runtime, no cambia
el wire anunciado y no se amplía el SPI ni el formato SQLite. La cobertura es
finita y no acredita transacciones distribuidas, retries de red ni throughput.
Los benchmarks comparan el baseline `c0746ab` y el candidato con el harness
existente; el fallback de igualdad numérica prioriza corrección y compatibilidad
persistida. No constituye una promesa universal de latencia.

El siguiente paso acordado con el usuario es analizar si se cierra la entrega
con este objetivo o se añade otra funcionalidad. No se ha creado tag ni publicado
una distribución.

# Ampliaciones de la API PyMongo para la siguiente entrega

El contrato y la autorización proceden
del encargo del usuario: proyecciones como lista de campos, `comment` en
`insert_one`/`insert_many`, y tipado Iterable de `insert_many`. El repositorio
se gobierna mediante ADRs; no se exige una RFC adicional para estas ampliaciones
compatibles. Aplican ADR-001, ADR-002 y ADR-009.

## Alcance y aceptación

1. Las fachadas sync y async aceptan proyecciones con nombres de campos en
   `find`, `find_one`, `find_raw_batches` y los tres helpers find_one_and_*.
   Se normalizan en la frontera pública y llegan al core como documentos de
   proyección. El resultado, `_id`, listas vacías, campos anidados y errores de
   elementos inválidos coinciden con PyMongo y MongoDB real.
2. `insert_one` e `insert_many` admiten `comment`, lo conservan en profiling y
   operaciones activas, sin incorporarlo a los documentos insertados. Mantienen
   sus resultados, errores y efectos parciales. No basta aceptar y descartar
   la opción.
3. La firma pública de insert_many declara Iterable[Document]. Tuplas y
   generadores ya soportados siguen consumiéndose una vez, conservan los IDs
   del caller y tienen pruebas positivas/negativas del contrato de tipos.

No se amplía bulk_write a tuplas, no se enriquecen sus errores `op`, no cambia
el SPI, el formato SQLite ni Search. Publicación y número de versión quedan
fuera de este encargo.

## Censo y reutilización

| Requisito | Mecanismo inspeccionado | Disposición |
| --- | --- | --- |
| Formas públicas de proyección | AsyncCollection._normalize_projection y los helpers read/modify; FindOperation conserva un dict | Derivar la normalización común, reutilizar core y engines |
| Fachada sync | Adapta sobre AsyncCollection | Reutilizar delegación y ampliar firmas |
| Comment en inserciones | Profiling, track_active_operation y metadata de sesión existentes; command_insert conserva ruta raw/wire | Reutilizar mecanismos y propagar la opción |
| Iterable en insert_many | _require_documents materializa y valida una vez | Ya satisfecho en runtime; corregir typing y acreditar regresiones |
| Contraste nativo | PyMongo 4.19, capturadores existentes y observador BSON independiente | Reutilizar observador; añadir catálogo finito y capturas por servidor |

## Secuencia y gates

- Fijar casos nativos de proyección, inserciones con comment, errores e iterables
  en MongoDB 7.0.43 / 8.0.32 / 9.0.2, con ambos clientes PyMongo.
- Implementar normalización y propagación sin duplicar semántica en sync.
- Añadir regresiones directas Memory/SQLite y sync/async, observabilidad local,
  mutabilidad del caller, typing y manifest/catálogos afectados; mantener wire.
- Construir el wheel final y ejecutar pytest en Python 3.13/3.14, cobertura
  >=99 %, unittest, propiedades, contratos y conformance pertinentes, junto al
  diferencial nativo. Revisar diff y documentación y guardar identidad/evidencia.

El trabajo anterior de escrituras e identidad permanece en el checkout y se
preserva. Sus informes describen candidatos anteriores; los gates afectados
de este bloque se ejecutan sobre el wheel nuevo.

## Comportamiento implementado

Las proyecciones de campos se normalizan mediante `normalize_projection_spec`.
El tipo de entrada queda en la API; `Projection`, `FindOperation` y los engines
siguen consumiendo un documento de proyección. Se aceptan los mappings,
secuencias y conjuntos que normaliza PyMongo 4.19. Una proyección vacía devuelve
el documento completo; las listas incluyen `_id` por defecto. Los elementos
inválidos y generadores de proyección se rechazan con TypeError antes de una
mutación findAndModify. El cursor conserva una copia independiente de la lista.

`comment` se copia al iniciar la inserción y se conserva en profiling, tanto
en éxito como en los fallos de duplicados y schema comprobados. También llega
a las operaciones activas y a la metadata de sesión. No altera el documento
insertado ni la asignación de IDs, los contadores, los índices de error o el
comportamiento ordered/unordered. El error de duplicado por `_id` de insert_one
se perfila ahora dentro del mismo camino de error que los demás fallos.

Las dos firmas insert_many declaran `Iterable[Document]`. Se mantiene la
materialización única existente, con tuplas, generadores y un iterable de un solo
uso. Las pruebas de typing aceptan esas entradas y rechazan elementos de tipo
incorrecto y proyecciones cuyos nombres no son strings. Esta anotación no
promete inserción en streaming ni un consumo de memoria independiente del lote.

El manifiesto público, el catálogo de opciones y la declaración CXP anuncian
los comentarios de inserción. Los valores CXP existentes pasan de
`acceptsComment=false` a true y `supportedOptions=[]` a `["comment"]`; no se añade
vocabulario ni cambia la versión 1.2.0 de su esquema de catálogo. Se actualizan
el hash de su fuente, su revisión por contenido y los pins exactos del catálogo
en snapshots/perfiles/tiers actuales. Las capturas históricas no se reescriben.

## Corpus y reproducción

`tests/differential/api_option_cases.py` define **104 casos**. El observador
independiente de `tests/support_api_options.py` recoge resultados, documentos
BSON, mutación del caller, familias/códigos de error y los comentarios que
efectivamente aparecen en system.profile. Las referencias se capturan sin
importar Mongoeco y ligan los hashes del catálogo y los observadores.

Cada servidor real ejecuta los 104 casos con PyMongo sync y async; ambas vistas
deben coincidir. La matriz local cubre tres dialectos, perfiles 4.9/4.19, ambos
motores y ambas fachadas: **2.496 observaciones directas**. Las doce celdas wire
añaden **1.248 observaciones** con clientes PyMongo sync/async.

Se conserva la política del corpus previo para codeName: se contrasta cuando la
ruta nativa lo devuelve; los errores por elemento pueden retener metadata
adicional local. Se comparan siempre familia y código. Los errores nativos
observados incluyen TypeError cliente, validación 121 y duplicados 11000;
insert_many conserva la envolvente BulkWriteError 65 y sus contadores parciales.

```bash
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
  python scripts/capture_api_option_parity.py --target 8.0 \
  --output /tmp/api-options.json \
  --check tests/fixtures/api_options_mongodb_8_0.json

python -m pytest -q tests/integration/api/test_api_option_parity.py \
  tests/integration/api/test_api_option_conservation.py
```

El workflow diferencial ejecuta esa captura y sus consumidores sobre el wheel
de entrega cuando se invoca con un artefacto instalado. Esta validación se
ejecuta localmente; no acredita un run alojado de GitHub.

La identidad del candidato, resultados y logs finales se conservan en
[la evidencia](evidence/api-options/validation.json). Este bloque no publica
una versión ni modifica el repositorio consumidor.

## Cierre de implementación

La ampliación posterior de [soporte Python](python-support.md) añade 3.15,
unifica las dependencias nativas y corrige la representación de alias del
manifiesto. Conserva un candidato y una validación nuevos; los resultados de
este apartado corresponden al candidato anterior indicado a continuación.

Completado el 15 de octubre de 2026 sobre el wheel de trabajo con SHA-256
`7aedfcf4a771ca8f9c350564c73fb319438f888799cb41c755d29e09c9d568f1`.
Sus 275 archivos de runtime coinciden byte a byte con el checkout. El wheel
conserva temporalmente la etiqueta 4.9.1; no es una nueva publicación de esa
versión ni fija el número de la siguiente entrega.

Los **624 resultados nativos** de MongoDB 7.0.43, 8.0.32 y 9.0.2 coinciden entre
PyMongo 4.19 sync/async. Las **3.744 observaciones directas y wire** coinciden
con esas referencias en Memory y SQLite. Se contrastan también los códigos
121, 11000 y la envolvente 65, además de los TypeError de validación cliente.
El candidato anterior difería en 480 de las 624 observaciones comparadas;
los controles válidos existentes permanecen cubiertos.

| Gate final | Resultado |
| --- | --- |
| Pytest con wheel instalado, Python 3.13 y 3.14 | 11.018 passed, 31 skips esperados y 14.445 subtests en cada versión |
| Cobertura Python 3.14 | 99,04 %, mínimo 99 % |
| Unittest | 3.613 pruebas, 3 skips esperados, OK |
| Conservación local | 8 pruebas: comment activo/sesión/profiling, copia de entradas y consumo único |
| Contrato de tipos | Positivo aceptado; 20 diagnósticos negativos esperados |
| Contratos instalados | Manifest público, exportación de compatibilidad, SPI externo y 9 pruebas deep, conformes |
| Conformance de engines | Memory y SQLite conformes |
| Probes SDK existentes | 324 probes en seis versiones, sin resultados indeterminados |
| Canario consumidor | 16 flujos y 48 replays nativos; fuente permanente intacta |
| Corpus previo | Recapturas nativas de 170 casos de escritura, 82 de identidad y 8 raw por servidor, sin divergencias |
| Lint, formato y whitespace | Conformes |

La revisión del diff y la documentación está completada. Se preserva el trabajo
autorizado anterior. Se verificó la eliminación de las bases temporales y se
detuvieron los tres servidores de contraste. No quedan pendientes técnicos
dentro de este alcance; la decisión de cierre y publicación de la entrega es
posterior.

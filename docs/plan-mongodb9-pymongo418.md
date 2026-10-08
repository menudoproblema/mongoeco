# Plan de soporte de MongoDB 9 y PyMongo 4.18

Estado: plan autorizado por el encargo de implementación de principio a fin.
El [roadmap original](mongodb9-pymongo418-progress.md) conserva su historia.
El [plan vigente de mejoras para 4.9.0](mongodb9-pymongo418-improvements-progress.md)
registra el contraste crítico, los criterios de aceptación y los gates pendientes. Fecha: 8 de octubre de 2026. Base:
Mongoeco 4.8.1, commit `aa626c51fea8a055510f89a2acf2f8ed1ce33761`.

El objetivo es que las aplicaciones que usan el subset público de Mongoeco puedan seleccionar MongoDB 9.0 y PyMongo 4.18 con resultados, errores y límites verificables. La solución conserva los dos ejes de compatibilidad existentes, concentra cada diferencia en su frontera y reutiliza el runtime común. La sencillez se obtiene reduciendo mecanismos y duplicación; las pruebas siguen cubriendo todas las garantías afectadas.

La propuesta desarrolla [ADR-002](architecture/decisions/ADR-002-dialecto-y-perfil-como-ejes-distintos.md), [ADR-009](architecture/decisions/ADR-009-parity-tests-como-politica-de-aceptacion.md) y [ADR-015](architecture/decisions/ADR-015-pushdown-sqlite-sujeto-a-prueba-de-equivalencia.md). El Engine SPI v2 y `search-v1` conservan sus contratos. La revisión debe comprobar que las elecciones de API, SRV y observabilidad descritas aquí caben en esas fronteras antes de ejecutar el plan.

**Punto de partida y alcance**

El catálogo admite dialectos 7.0/8.0 y perfiles 4.9/4.11/4.13/4.17. PyMongo 4.18.2 se resuelve mediante fallback a 4.17 en modo flexible y se rechaza en modo estricto. La matriz del driver puede confundir un rechazo de configuración con aceptación; el runner diferencial puede terminar correctamente aunque una conexión fallida haya omitido la suite. Ambos problemas deben corregirse antes de usar sus informes como evidencia del soporte nuevo.

La comprobación previa con Python 3.14.5 y PyMongo 4.18.2 pasó 85 pruebas seleccionadas y 17 de wire. No acredita la semántica de MongoDB 9: esa aceptación requiere servidor real. Los 61 archivos del paquete de impacto mantienen sus hashes respecto a la base indicada.

La primera entrega incluye el perfil 4.18, el dialecto 9.0 para operaciones ya soportadas, sus incompatibilidades relevantes, la validación de opciones nuevas y la infraestructura que demuestra la compatibilidad. Las ampliaciones completas de validación `constraint`, `$merge` con claves generales, nuevos operadores independientes, Queryable Encryption y comportamiento distribuido quedan como capacidades separadas. `Database.aggregate`, ausente actualmente, tampoco se añade solo por aparecer en la actualización del driver.

**Garantías que condicionan el diseño**

- Conservar los defaults MongoDB 7.0 y PyMongo 4.9, los aliases y los contratos de los perfiles antiguos. La selección nueva es explícita o consecuencia de una autodetección exacta del driver instalado; el driver no determina el dialecto del servidor.
- Mantener una sola implementación semántica compartida por Memory/SQLite y async/sync. La superficie sync sigue adaptando la async. Los errores conservan tipo, código y etiquetas cuando formen parte del contrato.
- Validar las opciones antes de ejecutar o escribir. Un parámetro que cambia resultados, validación o selección de hosts no puede aceptarse sin efecto. `STRICT` y `RELAXED` conservan su contrato de degradación explícita.
- Aplicar diferencias de dialecto también en pipelines anidados, colecciones vacías y comandos equivalentes. Los atajos de SQLite solo permanecen activos si conservan equivalencia; en caso contrario se usa la ruta semántica existente.
- Separar versión semántica y capacidades wire. Un dialecto 9.0 no justifica anunciar un `maxWireVersion` mayor ni habilitar compresión, transacciones distribuidas u otras capacidades ausentes.
- No cambiar el formato persistente para representar diferencias de listado o de API. La adopción de un perfil nuevo no requiere migrar bases SQLite.

**Mecanismos elegidos y complejidad descartada**

| Pieza | Requisito y mecanismo existente | Disposición | Garantía diferencial |
| --- | --- | --- | --- |
| Dialecto 9.0 y perfil 4.18 | Catálogos, clases públicas y resoluciones en `compat` | `reuse` | Selección explícita, identidad estable y metadata de fallback. Son nuevas entradas, no otro sistema de versiones. |
| Aliases, listas de versiones y detección | Datos del catálogo y comprobaciones de alineación de `base.py` | `derive` | Evitar listas divergentes. Conservar nombres públicos y las comprobaciones actuales; no exigir un refactor general del registro. |
| Diferencias semánticas | `MongoBehaviorPolicySpec`, flags y contexto de expresión | `reuse` | Aislamiento entre dialectos. Añadir un campo o política concreta solo cuando una diferencia observada lo exija. |
| Parámetros de operadores por dialecto | Catálogo común de operadores y validación existente | `justify` | Evitar aceptar sintaxis de 9.0 en 7/8. Añadir únicamente los conjuntos o descriptores mínimos que falten; no crear un motor general de reglas. |
| Validación de agregación | Normalizador público común | `consolidate` | Igualdad async/sync y rechazo temprano. Una función común valida las palabras reservadas según el perfil. |
| `ConfigurationError` | Puentes opcionales de `errors.py` | `reuse` | Compatibilidad de captura de excepciones con PyMongo y funcionamiento del paquete base sin PyMongo. |
| Identidad lógica de ejecución | Pipeline de requests, identidad por intento y eventos existentes | `justify` | Correlacionar retries sin confundir operaciones concurrentes. Añadir un ID por invocación, sin otro contexto global. |
| Resolución SRV real | `SrvResolution` y parser público de PyMongo | `reuse` | Validar hosts, sufijo y TXT con reglas mantenidas por el driver. Eliminar la resolución manual duplicada donde se adopte el adaptador. |
| Resolver simulado | Inyección explícita de semillas/resolver existente | `reuse` | Pruebas deterministas y subset local. No equiparar semillas inyectadas con DNS validado ni ignorar un sufijo solicitado. |
| Conversión BSON | Codec y bridge wire existentes | `reuse` | Lecturas correctas y propiedad de los buffers. Adaptar solo las fronteras que fallen con entradas reales de 4.18. |
| Runner y capturas reales | Casos, manifests, replay, JSON y JUnit existentes | `consolidate` | Evidencia no vacía y reproducible. Extender el runner común a 9.0, sin otro framework ni un script por versión. |
| Pushdown nuevo, nuevo SPI y migración de storage | Runtime común, fallback y SPI v2 vigentes | `remove` | No aportan una garantía necesaria para este soporte. No crear trabajo para estas piezas. |

**1. Hacer fiable la evidencia y cerrar el inventario**

Owners: testing/compatibilidad. Archivos principales: `scripts/run_pymongo_profile_matrix.py`, `scripts/run_mongodb_real_differential.py` y `tests/differential/`.

Corregir primero la clasificación de la matriz PyMongo. Distinguir aceptación de argumentos, rechazo de firma, rechazo de configuración y resultado indeterminado. Un timeout de selección puede demostrar que un argumento superó validación, pero no que su operación funciona. Las excepciones desconocidas deben fallar la comprobación. Verificar la versión efectiva de cada entorno aislado, incluso si se reutiliza un entorno ya creado.

El runner de integración será estricto: servidor inaccesible, versión equivocada, cero casos ejecutados u omisiones inesperadas producen salida no exitosa. La selección previa de casos excluye los no aplicables; la suite local ordinaria puede conservar sus skips cuando no se ha solicitado integración real. Los informes registran versión efectiva del servidor, FCV, driver, Python, semilla y casos ejecutados, junto a JSON/JUnit/logs. El FCV requerido debe establecerse y comprobarse; la etiqueta de una imagen no sustituye esa evidencia.

Completar el inventario acumulado 8.0→8.1→8.2→8.3→9.0 y asociar cada delta a una operación soportada, una ampliación o un límite existente. Capturar primero los casos que cambian comportamiento. El oráculo es el servidor real; Memory sirve de baseline local para SQLite. Reutilizar el formato de replay y separar fixtures por dialecto cuando difieran los resultados.

Aceptación: pruebas del propio runner demuestran fallo ante servicio caído, versión incorrecta y suite omitida; una ejecución real válida produce casos no vacíos. Los probes reservados de 4.18 se clasifican como rechazo de configuración. No se genera un golden a partir de Mongoeco.

**2. Extender el catálogo sin distribuir condiciones de versión**

Owner: `compat`. Archivos principales: `_catalog_dialects.py`, `_catalog_profiles.py`, `_catalog_models.py`, `base.py`, `registry.py` y exports públicos.

Añadir las entradas y nombres públicos siguiendo las convenciones existentes. El perfil 4.18 hereda capacidades ya probadas y declara su validación nueva de agregaciones y SRV. El dialecto 9.0 compone la base soportada y sus deltas semánticos explícitos. Las clases consumen los datos del catálogo; las fachadas y engines preguntan por comportamiento o capacidad, sin comparaciones numéricas repartidas por el código.

Probar selección explícita, aliases, versiones patch de 4.18, detección exacta, fallback flexible a una minor conocida y rechazo estricto de versiones desconocidas. Mantener visible la diferencia entre perfil solicitado, resuelto y motivo de fallback. Seleccionar 4.18 con dialecto 7.0 u 8.0 debe seguir siendo posible.

Aceptación: coherencia catálogo/exports/resoluciones, defaults sin cambios y pruebas anteriores verdes. La entrada pública 4.18 se integra junto a su comportamiento del paso 3; la entrada 9.0 junto a los deltas del paso 4. Ninguna entrega intermedia presenta una versión como soportada solo por registrarla.

**3. Cerrar las fronteras de PyMongo 4.18**

Owners: API/errores y driver. La [guía oficial de actualización](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/reference/upgrade/) documenta los cambios de agregación, identidad lógica y buffers BSON.

En `api/public_api.py` y las fachadas de colección, rechazar `aggregate` y `pipeline` cuando llegan como opciones reservadas de los helpers afectados. Con perfil 4.18 el error es `ConfigurationError`, mediante el puente opcional existente. Con perfiles anteriores se conserva el rechazo local vigente. `aggregate([], pipeline=[])` sigue siendo un argumento Python duplicado y produce `TypeError`. `list_search_indexes` recibe solo la adaptación necesaria para validar esas opciones: no se abre una aceptación indiscriminada de kwargs. La validación ocurre antes de crear el cursor o iniciar una request.

En requests/eventos, generar `operation_id` una vez al iniciar cada ejecución lógica y propagarlo a sus intentos. Conservar `request_id` por intento. El campo público es aditivo, con default compatible; el ID debe ser distinto para dos invocaciones concurrentes que reutilizan el mismo objeto request. Aplicar la identidad coherentemente al runtime, sin hacerla depender del perfil. Conservar el agrupamiento y `trace_id` actuales de CXP; incorporar el nuevo ID como correlación sin migrar la semántica de trazas en esta entrega. No añadir un ID de cardinalidad alta a etiquetas de métricas.

Para DNS real, usar un adaptador pequeño de la API pública [`pymongo.uri_parser.parse_uri`](https://pymongo.readthedocs.io/en/stable/api/pymongo/uri_parser.html), que admite `srv_allowed_hosts_suffix`. Pasar su resultado validado a `SrvResolution`; mantener las políticas del runtime en Mongoeco. No importar `_SrvResolver`, `_psl` ni otros helpers privados, ni mantener una copia propia de la lista de sufijos públicos. La dependencia sigue siendo opcional: el cliente local ordinario funciona sin PyMongo; la ruta que requiere el resolver oficial falla con un error de configuración claro si falta la versión necesaria.

La adopción del adaptador se acota a la ruta nueva de 4.18 para evitar modificar implícitamente el fallback de perfiles antiguos. En esa ruta, un fallo DNS no se transforma en `hostname:27017`. Las semillas explícitas siguen siendo una simulación declarada. Si una ruta inyectada no puede establecer la validación del sufijo con el adaptador público, rechaza esa combinación; no publica esa capacidad ni la acepta sin efecto. Las pruebas del resolver real controlan la frontera pública de DNS, sin depender de Internet ni de clases privadas de PyMongo.

Ejercitar buffers BSON inmutables y mutables, documentos/subdocumentos a ambos lados de 4 KB y roundtrip de valores admitidos. Modificar producción solo si esos casos revelan un defecto del codec o bridge actual. Comprobar igualmente PyMongo 4.18 contra el proxy, incluyendo conexión, CRUD, cursores, errores y cierre, sin aumentar artificialmente la versión wire anunciada.

Aceptación: matriz de excepciones por perfil en async/sync; identidad estable en éxito tras retry, fallo final, cancelación y concurrencia; leases liberados y sin eventos terminales duplicados; SRV válido aceptado y hosts fuera de dominio, sufijos públicos y errores DNS rechazados; ausencia de cambios de datos por buffers compartidos. La compatibilidad BSON y wire se prueba desde sus superficies públicas.

**4. Implementar los deltas semánticos de MongoDB 9**

Owners: core/API; engines verifican equivalencia. La [compatibilidad de MongoDB 9](https://www.mongodb.com/docs/manual/release-notes/9.0-compatibility/) introduce, entre otros cambios, el rechazo de acumuladores con nombre vacío en `$group` y códigos geoespaciales nuevos. El inventario incorpora también las [novedades de 8.3](https://www.mongodb.com/docs/manual/release-notes/8.3/).

| Frontera | Solución mínima | Aceptación observable |
| --- | --- | --- |
| `$group` | Validación de nombres mediante política de dialecto en el compilador común | 9.0 rechaza el nombre vacío incluso sin documentos; 7/8 conservan su resultado. Incluir pipelines anidados. |
| Fechas anteriores a 1970 | Ajustar el evaluador compartido únicamente al comportamiento capturado | Comparar época exacta, instantes negativos, unidades y zonas horarias. Revisar operadores que reutilizan el helper si la evidencia muestra afectación. No introducir un desplazamiento general sin prueba real. |
| `$convert`, `$toString`, `$map`, `$filter`, `$reduce` | Validar parámetros/variables por dialecto; implementar o rechazar explícitamente cada extensión | Parámetros como `base` y `arrayIndexAs`, y variables como `$$IDX`, producen el resultado real si se declaran soportados. Si se aplazan, error y capacidad explícitos; nunca aceptación ignorada. Conservar el subset previamente soportado. |
| `listIndexes` y collation simple | Proyección de metadata en la frontera de listado, condicionada por la evidencia de versión/FCV | Listado y roundtrip coinciden, sin persistir diferencias de presentación ni cambiar identidad de índices. |
| Errores dentro del subset geoespacial | Adaptación específica de los errores que realmente correspondan al contrato local | Capturas conservan clase/código/etiquetas relevantes. No extrapolar errores de geometría geodésica a un contrato planar. |
| Validación y opciones nuevas fuera del subset | Rechazo explícito con la clasificación de capacidades existente | `constraint`, `errorAndLog` y otras opciones aplazadas no debilitan la validación ni producen efectos parciales. |

Las extensiones de parámetros se añaden solo cuando comparten el evaluador existente y pasan su aceptación real. Una extensión que exige otro subsistema queda fuera de la primera entrega, con sintaxis detectada y rechazo claro. El dialecto 9.0 conserva todo el subset anterior que siga existiendo en el servidor; una exclusión nueva sobre ese subset bloquea la entrega hasta resolverla o revisar expresamente su contrato.

Aceptación: resultados y errores contra servidor real 7/8/9, paridad Memory/SQLite y async/sync, y evidencia de que los deltas nuevos no se filtran a los dialectos antiguos. Las rutas compiladas y wire que exponen esas operaciones consumen el mismo comportamiento. No se añade pushdown para compensar esta actualización; se desactiva el afectado si pierde equivalencia.

**5. Mantener una matriz pequeña y suficiente**

Owner: CI/distribución. No ejecutar por defecto el producto cartesiano de servidor, driver, engine, superficie y Python. Separar las garantías por eje y añadir interacciones identificadas.

| Lane | Cobertura obligatoria |
| --- | --- |
| API de driver | Perfiles 4.9.2, 4.11.3, 4.13.2, 4.17.0 y 4.18.2; probes de firma/configuración y pruebas locales de comportamiento afectado. |
| Semántica de servidor | Diferenciales reales 7.0/8.0/9.0 con driver 4.18.2 fijado; casos aplicables y ambos engines. |
| Runtime local | Python 3.13/3.14, paridad async/sync y Memory/SQLite; entorno base sin PyMongo y entorno con 4.18.2. |
| Interacciones | Ejemplos que cruzan ejes: `sort` de escritura, errores de agregación 4.18 con dialectos antiguos, dialecto 9 con perfil antiguo y nuevas opciones SRV. |
| Artefacto | Wheel instalado, manifest, typing, conformance y smokes de wire/driver; conserva las comprobaciones actuales del sdist y de builds reproducibles. |

Actualizar GitHub y GitLab desde el runner común. Fijar patch/digest del servidor después de comprobar su disponibilidad y registrar la versión/FCV efectivos; no usar `latest` como evidencia. Mantener los dialectos 7/8 en el gate requerido y añadir 9.0. La suite normal protege cada cambio; la matriz completa real corre en los puntos recurrentes existentes y es obligatoria para el candidato de entrega.

Actualizar `requirements/ci-constraints.txt` y `uv.lock` a 4.18.2 para la lane principal. Los entornos de perfiles históricos usan su pin aislado, sin heredar un constraint incompatible. Mantener el rango público `>=4.9,<5` de los extras existentes y añadir `mongodb9` con `>=4.18.2,<5`. Un extra instala dependencias; no cambia automáticamente el dialecto ni los defaults. Validar instalaciones limpias, sin constraints internos, como exige el checklist vigente.

Aceptación: una ejecución de cada lane demuestra qué garantía cubre, sin errores desconocidos convertidos en aceptación ni omisiones tratadas como éxito. Todas las lanes se ejecutan contra el mismo candidato de artefacto donde corresponde.

**6. Cerrar documentación, contratos derivados y adopción**

Owners: API/compatibilidad y responsable de entrega. Actualizar `COMPATIBILITY.md`, `DIALECTS.md`, límites de producto y guía de diferenciales, con ejemplos de selección y errores nuevos. Regenerar los snapshots vigentes mediante `update_compat_snapshots.py` y revisar el manifest público actual mediante `update_public_api_manifest.py`; conservar las fixtures históricas. La metadata de compatibilidad y CXP solo declara capacidades con evidencia. Cambiar el catálogo CXP y sus pins coordinadamente únicamente si se añaden términos de su contrato; añadir una versión de driver no implica versionar CXP ni cambiar su schema.

Completar los gates de [release-checklist.md](release-checklist.md): suite, cobertura mínima de 99 %, ratchet de lint, typing público, manifest, conformance de Memory/SQLite/canario externo, fixtures persistentes y builds reproducibles. El wheel instalado se verifica además con la selección nueva y con la antigua. No basta con pruebas desde el checkout.

Los consumidores observados, `cosecha-provider-mongodb` y `mochuelo-testkit`, mantienen sus pins y contratos durante este cambio. Su adopción posterior usa el candidato de wheel y sus pruebas propias; este plan no modifica otros repositorios. El aislamiento por instancia permite continuar usando perfiles antiguos en el despliegue parcial. Las políticas derivadas del perfil no se persisten como propiedades globales del engine.

Aceptación final: todas las capacidades anunciadas tienen implementación y evidencia; los casos reales aplicables se han ejecutado; no hay cambios accidentales de defaults, datos, SPI ni contratos antiguos. Las extensiones aplazadas aparecen como límites, no como soporte implícito. La entrega no requiere coordinarse con Mongoeco 5.0.

**Orden de trabajo y criterio para aceptar complejidad**

Primero se entrega la corrección de los instrumentos de prueba. Después se ejecutan los bloques del perfil 4.18 y del dialecto 9.0, cada uno con su registro y comportamiento completos. La matriz y los contratos derivados cierran el candidato conjunto. SRV, observabilidad y cada familia semántica pueden revisarse en cambios pequeños; no deben publicarse con soporte anunciado incompleto.

Antes de añadir una abstracción, comprobar que un dato del catálogo, una política existente o una función común resuelve el problema. Se acepta una pieza nueva solo si protege una garantía observable que esos mecanismos no cubren. Las decisiones pendientes son de evidencia: comportamiento exacto de fechas, metadata de índices bajo FCV y alcance de conversiones nuevas. El paso 1 las resuelve antes de codificar sus deltas; no se dejan a suposiciones durante la implementación.

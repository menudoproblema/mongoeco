# Plan de integración de PyMongo 4.20

Estado: **implementación autorizada y en curso** por el encargo del usuario.
Fecha: 19 de noviembre de 2026. Publicación, tags y cambios en consumidores
siguen fuera de esta implementación.
Owner: compatibilidad SDK y fronteras BSON de Mongoeco.

Base inspeccionada: Mongoeco 5.0.0,
`429d90648943533b057c7c09cad4cb974295aa9b`. La integración se realizará en una
rama propia desde la base vigente comprobada al comenzar; no se moverán los tags
publicados ni las referencias congeladas de entregas anteriores. Se conservará
el archivo ajeno no versionado `benchmarks/execution.json`.

## Resultado y alcance

Permitir seleccionar PyMongo 4.20 explícitamente o por autodetección exacta,
comprobar el driver real contra el proxy y corregir las diferencias reproducidas
de UUID y ObjectId en sus autoridades compartidas. Mantener las dos fachadas,
Memory/SQLite, Engine SPI v2, los archivos SQLite existentes y la instalación
base sin PyMongo. Los defaults siguen siendo MongoDB 7.0 y perfil PyMongo 4.9;
el mínimo de los extras no aumenta por incorporar un SDK reciente.

Quedan fuera CSFLE/Queryable Encryption, implementación de pools o topologías
reales en el engine, TLS nuevo, pruebas de asignaciones de 1 GiB, ampliaciones
de operadores y adopción automática en gdynamics-pre. Tampoco se reabren
O1–O8, D05, P2, F11, Q00–Q06 ni otras auditorías generales. Las mejoras upstream
de TLS y recuperación de réplicas se documentan sin atribuirlas al runtime local.

La entrega de código podrá proponer una minor, previsiblemente 5.1.0, por los
nuevos exports públicos. El número definitivo y su publicación pertenecen al
encargo posterior de preparación de versión, no se cambian en este plan.

## Evidencia disponible y causa raíz

Fuentes upstream: [changelog del tag 4.20.0](https://github.com/mongodb/mongo-python-driver/blob/4.20.0/doc/changelog.rst)
y [guía de actualización](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/reference/upgrade/).
Se conservan las observaciones de la revisión en
[analysis.json](evidence/pymongo420/analysis.json), obtenidas con el wheel
publicado 5.0.0, Python 3.14.5 y entornos separados de SDK 4.19.0 y 4.20.0.

- Las 54 sondas de argumentos por SDK tienen la misma clasificación. Aceptar
  argumentos no acredita ejecución contra servidor: el runner distingue ambas cosas.
- Pasan 194 tests y 16 subtests por SDK. Incluyen BSON, SRV controlado y driver
  real sync/async contra el proxy, pero no acreditan toda la suite ni otros Python.
- Las 50 combinaciones de subtipo/longitud/representación ensayadas coinciden
  entre los decoders C y Python de 4.20. No se debe conservar como contrato el
  error del decoder C de 4.19 que rechazaba UUID cortos incluso en UNSPECIFIED.
- `4.20` no está registrado; strict auto falla y auto flexible recurre a 4.19.
  Es la política vigente para versiones desconocidas, no un fallo del resolver.
- El helper `DocumentCodec._apply_uuid_codec_options` devuelve un binario cuya
  longitud no es 16 antes de decidir si la representación exige conversión.
  Por eso find/aggregate directos devuelven Binary donde el driver y las rutas
  con document factory o findAndModify lanzan InvalidBSON. Ocurre en ambos
  motores con los dos SDK: es un defecto previo, no una regresión de 4.20.
- El wrapper ObjectId delega construcción y validez al SDK. Con 4.19,
  `"00" * 11 + "  "` se acepta y genera 11 bytes; con 4.20 se rechaza. La rama
  sin SDK ya rechaza esa cadena. Falta un invariante común independiente del SDK.
- El handshake con `client.env.agent` y el pool `maxPoolSize=0, minPoolSize=1`
  funcionan con clientes reales 4.20 sync/async contra el proxy.

La evidencia anterior es diagnóstico aceptado como punto de partida; no es la
aceptación de código todavía no escrito. Repetir solamente el foco invalidado
por cada cambio y los gates que exijan expresamente el candidato final.

## Diseño y simplificaciones

1. Añadir `PyMongoProfile420` heredando de 419, con su instancia, catálogo y
   exports públicos. Reutilizar el resolver actual, sin condiciones especiales
   para 4.20 ni flags sin diferencias observadas en la superficie implementada.
2. Corregir UUID en el codec común y reutilizarlo en los consumidores afectados.
   Conservar el materializador rápido: no pasar todas las lecturas por un ciclo
   BSON encode/decode para obtener el error correcto.
3. Compartir la validación de entrada ObjectId entre wrapper y fallback sin
   copiar el generador del driver ni crear otra representación de ObjectId.
4. Extender corpus, parametrizaciones, capturador y runner existentes. Las
   sondas de análisis no se convierten en otro runner de aceptación.
5. Mantener la matriz del kit en doce celdas, sustituyendo el extremo 4.19.0
   por 4.20.0. Conservar 4.19 en la matriz de perfiles y en un foco diferencial
   de regresión; no multiplicar todos los ejes por otro SDK.
6. El export de perfiles PyMongo es independiente del catálogo CXP MongoDB.
   Se actualizan sus snapshots mediante los scripts propietarios; se exige
   conservar los documentos y hash CXP 2.0.0, el protocolo CXP 5 y SPI v2.

## Bloques y dependencias

| Bloque | Prioridad | Depende de | Resultado revisable |
| --- | --- | --- | --- |
| P00 — Base e inventario | P0 | Ninguno | SHA, workset y evidencia reutilizable fijados |
| P01 — Oráculos y regresiones | P0 | P00 | Testigos independientes y diferenciales nativos |
| P02 — Conversión UUID compartida | P0 | Testigos UUID de P01 | Misma decisión de conversión/error por todas las rutas |
| P03 — Invariante ObjectId | P1 | Testigos ObjectId de P01 | Entradas válidas siempre de 12 bytes, con/sin SDK |
| P04 — Perfil y contrato público 4.20 | P1 | P00, caracterización SDK de P01 | Registro, exports y autodetección exacta |
| P05 — Integración SDK/CI | P1 | P02, P03, P04 | Dependencias y matrices existentes adaptadas |
| P06 — Documentación y revisión | P1 | P02–P05 | Contrato final, límites, snapshots y diff revisados |
| P07 — Aceptación integrada | P0 | P06 | Un candidato final con todos sus gates exigidos verdes |

Secuencia recomendada: P00 → P01 → P02 → P03 → P04 → P05 → P06 → P07.
P03 y P04 son técnicamente independientes de P02; eso no justifica ejecutar
globales en paralelo con cada bloque. Cada bloque cierra con diff revisado,
evidencia focal y commit semántico. «Comprobado focalmente» no equivale a
«aceptado en la entrega», reservado a P07.

### P00 — Base, inventario y reutilización

- Comprobar rama, SHA, WIP y procedencia efectiva del paquete/SDK en los entornos.
- Identificar todos los registros/exports de 419, pruebas de versión futura,
  snapshots, constraints, matriz de perfiles, kit y canarios con SDK fijado.
- Conservar las observaciones y relacionarlas con su wheel y versiones. No
  modificar ni reinstalar los entornos de consumidores durante la implementación.

Cierre: inventario acotado sin owners ambiguos; referencia de trabajo propia y
WIP conservado; cada comprobación reutilizada tiene sus entradas identificadas.

### P01 — Testigos antes de corregir

Usar `tests/differential/semantic_bson_uuid_policy_cases.py`, sus helpers y el
capturador registrado para ampliar el corpus sin crear un formato paralelo.
Capturar con driver 4.20 contra MongoDB 7/8/9 los casos nuevos que dependan del
servidor. Ningún caso se considera nativo aceptado mientras figure pendiente
de captura; no convertir la salida de Mongoeco en golden.

Separar dos oráculos: PyMongo decide decodificación/errores SDK y ObjectId;
MongoDB real acredita admisión, bytes guardados y efectos de escrituras. Una
excepción de decodificación no prueba que una escritura haya sido revertida.

Selección mínima, con parametrización existente:

- BSON: subtipos 3/4; longitudes 0, 2, 15, 16 y 17; UNSPECIFIED y las cuatro
  representaciones de conversión; decoder C y Python. Son 50 combinaciones por
  decoder; el caso de 17 bytes protege también el recorte legacy observado en 4.19.
- Superficies: find/find_one, aggregate, command, findAndModify; sync/async,
  Memory/SQLite; dict normal, document factory y registro de decoders. Distribuir
  los casos para cubrir cada frontera sin hacer un producto cartesiano completo.
- Posiciones: raíz, documento anidado y array; Code/DBRef en el foco del codec.
- Raw BSON/batches: conservar bytes sin exigir una conversión UUID que la API raw
  no ha solicitado. Mantener controles de framing roto separados de UUID corto.
- Persistencia: SQLite temporal y reapertura con otra representación; conservación
  de bytes y subtipo, incluido el fixture histórico existente cuando sea aplicable.
- ObjectId: hex válido mayúscula/minúscula, cadena de 24 caracteres con espacios,
  otros blancos y no hex; bytes de longitudes inválidas y 12 válidos; ObjectId
  y objetos estructurales ya admitidos; generación normal e is_valid.

Cierre: los testigos nuevos muestran los fallos del candidato base donde
corresponde; controles válidos pasan; capturas nativas nuevas completas y con
versiones registradas; excepciones/efectos observados sin normalización permisiva.

### P02 — Corregir UUID en la autoridad común

Owner principal: `src/mongoeco/core/codec.py`; consumidores:
`core/_public_materialization.py` y el bridge wire, únicamente donde lo exija
el recorrido demostrado. No añadir comprobaciones duplicadas por engine o método.

Contrato:

- UNSPECIFIED conserva Binary, bytes y subtipo aunque la longitud sea irregular.
- STANDARD convierte subtipo 4; los modos legacy convierten subtipo 3. Sólo
  cuando la conversión solicitada corresponda al subtipo se exige longitud 16.
- Longitud inválida en esa conversión produce InvalidBSON al decodificar, también
  sin SDK. Un subtipo no convertible por la representación elegida sigue siendo Binary.
- El cambio no prohíbe almacenar binarios cortos admitidos por MongoDB ni modifica
  filtros, orden BSON o persistencia. No se anticipa el error de lectura a la escritura.
- La forma del documento o la elección de una ruta rápida no cambia la decisión.
  Mantener orden/efectos de document factories y type decoders existentes.
- findAndModify conserva la modificación confirmada aunque después falle la
  decodificación de la respuesta, según el testigo nativo.

Cierre: regresiones de P01 verdes para todas las fronteras afectadas; mismos
tipos/bytes/errores y fase que el oráculo; escritura y reapertura conservadas;
sin reserialización general nueva ni cambios de formato SQLite.
Commit orientativo: `fix(codec): validate requested UUID conversions consistently`.

### P03 — Validación ObjectId independiente del SDK

Owner: `src/mongoeco/_types/_bson_objectid.py`. La validación común debe excluir
blancos/no hex y garantizar 12 bytes antes de aceptar una entrada, incluso al
recibir un ObjectId irregular creado previamente por un driver antiguo.

Reutilizar esa decisión en wrapper y fallback y en las comprobaciones de validez
que correspondan. Conservar los tipos de entrada públicos históricos, el protocolo
estructural admitido por el constructor, generación, igualdad y orden. No ampliar
silenciosamente el dominio de `is_valid` ni sustituir el ValueError público local
por InvalidId sólo porque PyMongo use esta última clase.

Cierre: mismas decisiones para entradas contractuales con SDK 4.19, 4.20 y sin
SDK; ningún éxito construye un ObjectId de tamaño distinto de 12; constructor e
is_valid quedan coherentes en su dominio compartido; no se modifica el proceso
global ni se reemplaza el generador de PyMongo.
Commit: `fix(types): enforce ObjectId input invariants across SDK versions`.

### P04 — Perfil público 4.20 y proyecciones

Actualizar `_catalog_profiles.py`, `compat/base.py`, exports de `compat` y
proyecciones afectadas. Exponer `PyMongoProfile420` y `PYMONGO_PROFILE_420`.
Mantener igualdad de capacidades/flags con 419 donde no haya delta observable;
no inventar capacidades de cifrado, pools o telemetría local.

Probar `4.20`, instancia de perfil y versiones instaladas 4.20.x en ambos modos
auto: resolución exacta. Usar 4.21 como versión futura controlada: fallback a
4.20 en modo flexible, rechazo en strict. Mantener defaults, aliases anteriores,
rechazo de majors desconocidos y separación del dialecto MongoDB.

Regenerar manifiesto público y snapshots mediante `update_public_api_manifest.py`
y `update_compat_snapshots.py`; revisar el delta, no aceptar un reemplazo ciego.
El catálogo exportado completo cambia por el perfil, los documentos CXP no.

Cierre: exports tipados e inventariados, resolución y catálogos alineados;
snapshots con delta esperado; identidad/hash CXP sin cambios y sin nuevas APIs SDK
obligatorias en una instalación base sin PyMongo.
Commit: `feat(compat): add the PyMongo 4.20 API profile`.

### P05 — Constraints, matrices y consumidores de prueba

- Subir sólo el pin de CI a `pymongo==4.20.0` después de P02–P04. Conservar
  mínimos y límites `<5` de los extras; revisar resolución sin upgrades laterales.
- Añadir 4.20.0 a `DEFAULT_VERSIONS` y regenerar el fixture del runner: siete
  SDK × 54 sondas = 378 observaciones mientras el inventario siga en 54.
  Conservar 4.19 y los cinco perfiles anteriores, y la clasificación fail-closed.
- Sustituir 4.19.0 por 4.20.0 en el extremo alto de las doce celdas del kit.
  El foco 4.19 de P01–P03 conserva comprobación de regresión, sin añadir seis
  celdas globales redundantes.
- Revisar canarios con pin 4.19. Cambiar sólo los que deban probar el SDK actual;
  mantener los que posean explícitamente un contrato histórico. No reescribir
  evidencias de releases previas ni atribuir al consumidor privado una ejecución.
- Usar el circuito existente para los nativos con SDK 4.20 y conservar el SDK
  mínimo aislado que ya exige el workflow. El SDK y MongoDB siguen siendo ejes distintos.
- Promover a tests existentes la sonda compatible de handshake con agente y pool
  sin límite: driver real sync/async, ping/CRUD y cierre de recursos propios.

Cierre focal: selección no vacía, sin resultados indeterminados; pin nuevo
efectivo; matrices/configuración válidas; negativos de perfil y BSON conservados.
La aceptación de la CI completa espera a P07.
Commit: `ci(test): exercise PyMongo 4.20 through the existing SDK matrices`.

### P06 — Documentación, revisión y congelación

Actualizar este owner con resultados y los documentos vivos afectados:
`docs/README.md`, guía de compatibilidad, `DIALECTS.md`, política de testing,
referencia de runtime y changelog según su contenido real. Añadir la guía de
soporte 4.20 sólo si aporta el contrato de uso que no posea otro documento.
Las notas históricas 4.19 y los informes de releases permanecen históricos.

Documentar los cambios visibles: rechazo de ObjectId inválido, InvalidBSON por
conversión UUID solicitada, conservación de Binary en UNSPECIFIED y efectos de
escritura anteriores a un fallo de decodificación. Las retiradas de cifrado son
upstream y no anuncian soporte de cifrado en Mongoeco.

Revisar diff y dependencias; fijar SHA del candidato y material empaquetado.
Registrar qué evidencia focal sigue válida. No ampliar el workset por incidencias
ajenas: sólo entran regresiones demostradas o incumplimientos obligatorios del plan.

Cierre: documentación y contrato implementado coincidentes, sólo los deltas
previstos en snapshots/exports y candidato inequívoco listo para el gate final.
Commit: `docs(compat): document PyMongo 4.20 support and BSON corrections`.

### P07 — Una aceptación integrada del candidato final

Ejecutar los gates propietarios existentes una vez sobre el candidato congelado:

- Wheel/sdist reproducibles, manifiesto, instalación y procedencia del paquete.
- Tres Python 3.13/3.14/3.15: suites completas y unittest, typing público, SPI,
  manifiestos y smokes exigidos por CI. Cobertura instalada exacta >=99 % en 3.14,
  sin redondear el cociente ni modificar exclusiones para obtener el umbral.
- Matriz del kit de doce celdas y matriz de siete perfiles; sin selección vacía.
- Deep y demás checks ya exigidos por CI, agrupados en esa misma evaluación.
- Diferenciales reales MongoDB 7/8/9, con sus particiones standalone/replica set
  existentes y las capturas nuevas UUID. Skips obligatorios, versiones erróneas
  o golden pendiente impiden aceptación. No se añade un escenario de failover
  sólo por la corrección interna del monitor de PyMongo.
- Foco sin SDK para ObjectId/UUID e imports mínimos; compatibilidad y reapertura
  del fixture SQLite existente. Extender una selección existente si falta esta
  frontera, sin otro runner ni entornos compartidos con consumidores.

No se inicia una campaña temporal ni se redefine F11. No cambia una regla de
plan SQLite; bastan los smokes funcionales de coste ya exigidos por CI. Si una
corrección posterior invalida evidencia, identificar sus entradas y repetir sólo
el foco afectado, más el gate global final cuando el circuito lo exija para ese SHA.

Cierre: todos los gates requeridos terminales verdes sobre la identidad declarada;
errores nuevos explicados/corregidos, cero obligaciones exigidas sin comprobar;
recibo con SHA, artefactos, versiones, comandos y límites; cambios propios
commiteados semánticamente y WIP ajeno conservado. No cambiar el SHA aceptado
sólo para guardar un recibo: usar los artefactos existentes de CI/evidencia.
Publicación, tags y migración de otros repositorios requieren el encargo posterior.

## Seguimiento de ejecución

Los ocho bloques están **planificados**, ninguno aceptado como implementación.
Al ejecutarlos, informar bloques comprobados/8, bloqueos y siguiente gate.
Registrar evidencia en el directorio existente de este incremento; no abrir otro
plan, un runner por versión o una capa adicional de aprobación.

La entrega se considera completa sólo al cerrar P07. Los límites explícitos
—TLS externo, failover físico específico del monitor, cifrado y buffers gigantes—
no se presentan como comprobados ni se convierten en obligaciones implícitas.

### Registro inicial de implementación

P00 comprobado en `feat/pymongo-4.20`, base productiva 429d9064 y commit
documental 2e7508ff. El archivo ajeno benchmarks/execution.json sigue intacto.
P01 en curso: el foco inicial reproduce 16 combinaciones UUID inválidas
aceptadas indebidamente y cuatro variantes de blancos en ObjectId con SDK
4.19.0. Son testigos rojos deliberados de la base; falta completar el corpus
nativo antes de aceptar P01. Registro del foco: p01-base-regressions.log en
el directorio de evidencia de este incremento. No se han ejecutado globales.

P02/P03 implementados parcialmente en 9780de7a y 1dc8c050: el helper UUID
rechaza longitudes inválidas sólo al solicitar la conversión correspondiente;
ObjectId valida forma/tamaño antes de delegar. Pasan 105 tests con SDK 4.19
y 129 con SDK 4.20 (cuatro subtests en cada foco), incluyendo el corpus público
de política UUID en el segundo. Ratchet de lint conforme. Esta evidencia no
cierra todavía P02/P03: faltan testigos ampliados, instalación sin SDK y
comparación nativa. MongoDB 9.0.2 está disponible y Docker se ha comprobado
operativo para las capturas 7/8/9. Ningún global ejecutado.

P01: capturadas 300 observaciones nuevas por MongoDB 7.0.43/8.0.32/9.0.2
con SDK 4.20 y las imágenes fijadas en CI. Las cuatro acciones anteriores
conservan sus observaciones exactas. Se actualizan los fixtures propietarios
semantic_bson_uuid_policy_7_0/8_0/9_0.json; no se duplican en otro archivo.
Los 12 replays directos pasan. Los 12 wire fallaron inicialmente al abrir
puertos en el sandbox y pasan al repetir sólo esa selección con permiso local.
Los contenedores propios terminaron y se eliminaron con sus volúmenes.
P04 implementado en 7fa4168e y comprobado focalmente: 69 tests verdes,
resolución exacta 4.20.x y fallback futuro 4.21, catálogo/profiles anteriores
y documentos/hash CXP preservados. Su aceptación integrada queda para P07.
P01–P03 conservan pendiente el foco ampliado de factories/decoders y sin SDK.

### Pausa solicitada — 19 de noviembre de 2026

Se conserva el candidato productivo y se detiene la implementación antes de P07.
El foco ampliado UUID y sin SDK pasó: 123 tests en 4,40 s con SDK 4.20
(registro p02-extra-frontiers.log). Cubre factories, registro de tipos,
Code/DBRef, error de lectura tras escritura find-and-modify y ejecución sin SDK.
Después sólo se reorganizó la selección de la operación fuera de pytest.raises;
esa modificación de estilo queda pendiente de comprobar al reanudar.
No se declara aceptación integrada de P01–P04.

P05 queda como trabajo sin commitear: pin de CI, eje del kit, runner y fixture
de perfiles, y smoke de metadata/pool. El proceso existente de perfiles terminó
con código 0; sus resultados crudos están en
/tmp/mongoeco-pymongo420-review/p05-profile-raw.json y todavía no se han
revisado para aceptar el bloque. El smoke wire nuevo no se ha ejecutado.
Siguiente paso: revisar ese resultado y comprobar focalmente los cambios de
P05 y el ratchet de lint antes de su commit. P06/P07 siguen pendientes.
No hay campañas de coste ni suites globales iniciadas por esta pausa.
El archivo ajeno benchmarks/execution.json se conserva fuera del incremento.

### Comprobaciones focales tras reanudar

P01–P04 conservan sus capturas y pruebas. La repetición focal de la frontera
BSON, imports sin SDK, lotes raw, perfil y wire pasa con 4.20 (147 tests y
11 subtests). El foco UUID/ObjectId con 4.19 pasa (186 tests y cuatro subtests).
Se corrige el estilo de la selección de operaciones detectado por el ratchet.

P05 comprobado focalmente. El runner aislado clasificó las 378 observaciones
de los siete SDK sin indeterminados. Las 54 decisiones de aceptación de
4.20.0 coinciden con 4.19.0. El smoke con sockets locales verifica el saludo
con metadata de agente, ping y escritura/lectura con pool sin límite en sync
y async. El pin 4.19.0 del canario de escritura conserva el entorno histórico
documentado en write-parity-validation.md. La matriz completa del kit y la
aceptación del artefacto se comprueban en P07.

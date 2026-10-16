# Refactor de errores y preparación

## Alcance aprobado

El encargo autoriza corregir las 13 familias de la auditoría y consolidar sus
mecanismos compartidos. Se conservan API pública, SPI v2, contexto de operación
y compatibilidad con archivos SQLite existentes. No incluye publicación.
La evidencia anterior permanece como antecedente; no acredita este candidato.

## Bloques y aceptación

| Bloque | Mecanismo reutilizado o consolidado | Resultado que permite cerrarlo | Estado |
| --- | --- | --- | --- |
| Regresiones | Corpus diferencial y oráculos nativos 7/8/9 | Reproducciones originales conservadas, incluidas ausencias, efectos y recuperación | Verificado |
| Diagnóstico | Catálogo, serializadores y resultados existentes | Código/contexto íntegros; fronteras comando/elemento explícitas; F06–F09/F12 | Verificado |
| Preparación | Planes, proyecciones y preparación de expresiones | Validación en fase nativa, previa a mutaciones cuando corresponde; F01–F03 | Verificado |
| Semántica | Operadores compartidos y reglas BSON | Extremos numéricos, regex, filtros lógicos y orden sobre escalares; F04/F05/F10/F11 | Verificado |
| Ejecución y cierre | Primitivas internas, snapshots y runner sync | Adaptadores convergen sin revalidación cliente de comandos; limpieza inline/async; F13 | Verificado |
| Verificación final | Gates reales y corpus 7/8/9 | Sync/async, direct/wire, Memory/SQLite; API, SPI y archivos antiguos; documentación y diff revisados | Verificado |

El diagnóstico interno completo es el único constructo nuevo propuesto: permite
exigir código y ámbito a fallos internos sin cambiar las firmas opcionales de
las excepciones públicas. Los planes, contextos y outcomes existentes se
reutilizan. Se consolidan productores y adaptación, conservando agrupación de
lotes, ordered/unordered y errores de comando posteriores a efectos parciales.

Las comprobaciones de fases distinguen ausencia de documentos de operandos
inválidos: no se evalúan indiscriminadamente expresiones de updates sin match.
La selección diferencial observa errores, contadores, BSON, índices, efectos
parciales y reutilización. El caso de limpieza incluye Memory síncrono sin
loop, además de SQLite y clientes async.

## Implementación y regresiones

Se conserva un solo catálogo de códigos. `ServerDiagnostic` exige código,
mensaje y ámbito, posee su contexto y produce copias independientes para las
respuestas de comando y de elemento. Los adaptadores públicos conservan sus
firmas y traducen cada ámbito a la familia de excepción nativa; los comandos BSON ya analizados entran en
las mismas primitivas internas que la API, sin repetir la validación cliente.
Se elimina el marcador lateral `write_item_phase`.

| Familia | Corrección y garantía comprobada |
| --- | --- |
| F01 | La proyección se prepara antes de modificar, reemplazar o borrar; se reutiliza al devolver la imagen. |
| F02 | Consultas, proyecciones y expresiones se preparan sin depender de filas. La evaluación dinámica conserva la fase observada en MongoDB. |
| F03 | Forma BSON de todo el comando, validación cliente y compilación por petición tienen fronteras distintas; los lotes retienen prefijos y sufijos válidos según el oráculo. |
| F04 | `$exp` admite Infinity y `$pow` admite NaN cuando corresponde; no escapan excepciones numéricas de Python. |
| F05 | La compilación de regex produce 51111 por elemento y permite continuar un lote no ordenado. |
| F06 | Las guardas ejercitadas de números, arrays, strings y consultas conservan códigos y documentos de error completos. |
| F07 | Selección y borrado conservan la familia `WriteError` para errores por elemento, incluida la selección con `$expr`. |
| F08 | Validación 121 conserva `errInfo`, identificador y el árbol de `required`, `properties` y tipos BSON ejercitado. |
| F09 | Construcción de índices conserva contexto de duplicado 11000; conflictos y ausencias conservan sus códigos. Borrar un índice ausente es un no-op en MongoDB 9. |
| F10 | Los arrayFilters reutilizan la gramática de consultas, incluidos `$and`, `$or` y `$nor`. |
| F11 | `$push` con orden por documento admite arrays de escalares y conserva su orden estable. |
| F12 | `$$CLUSTER_TIME` se resuelve con operación y dialecto: incluye 5611301 en count/agregación 8 y las rutas 51144 de MongoDB 9. |
| F13 | La limpieza del cursor se ejecuta también en Memory síncrono sin loop; conserva la excepción primaria si el cierre falla. |

El corpus estable contiene 407 escenarios y 82 controles de ausencia: **489 por
servidor y superficie**. Se excluyen tres controles exploratorios de alias TTL y
dirección de índice ajenos al contrato de estas 13 familias; no se añaden esas
capacidades. Los oráculos independientes usan MongoDB 7.0.43, 8.0.32 y 9.0.2,
FCV correspondiente y PyMongo 4.19.0. La captura verifica que no importa Mongoeco,
compara clientes nativos sync/async y exige una conexión física reutilizada.

Las 12 celdas directas y 12 de proxy suman 11.736 ejecuciones de escenarios;
junto con las seis celdas nativas, 14.670. Se añaden cuatro controles de limpieza
y 24 de pipelines vacíos: PyMongo rechaza `update_one(..., [])` en el cliente,
mientras que los comandos BSON `update` y `findAndModify` aceptan `u: []` como
no-op. Esta distinción tiene también un oráculo nativo separado.

El observador conserva resultados, BSON de aplicación, códigos, contadores,
metadatos de índices, efectos parciales y la escritura posterior de recuperación.
Para estos escenarios normaliza el módulo/texto renderizado de la excepción, la
unicidad implícita de índices y la redacción de `errmsg`, comprobando que este
último sea un string no vacío y no una representación de excepción anidada.
No acredita coincidencia literal de todos los mensajes. Las pruebas de mutación
comprueban que no desaparezcan campos BSON de aplicación llamados `message`,
`module`, `errmsg` o `unique` ni códigos ausentes.

El gate de productores impide añadir llamadas sin código a `OperationFailure`
por encima de una línea base de 904 construcciones legacy. Esa cifra es un
inventario, no 904 defectos ni una acreditación de compatibilidad de esas rutas.
Los árboles de validación ajenos a los keywords ejercitados y la compatibilidad
universal quedan fuera de esta evidencia.

## Verificación del candidato

- Matriz ampliada: 5.908 pruebas aprobadas, incluyendo direct/wire, sync/async,
  Memory/SQLite, códigos, contadores, efectos y recuperación.
- Manifiesto público sin cambios; canario independiente SPI v2 aprobado.
- El esquema y formato SQLite permanecen sin cambios. La suite incluye archivos
  existentes e identidades BSON de índices.
- Gate de tipado público aprobado: un programa positivo y 20 errores negativos
  esperados; módulos de pytest y lifecycle también pasan mypy estricto.
- Suites completas del wheel en Python 3.13, 3.14 y 3.15: 36.912 pruebas y
  14.458 subtests aprobados por versión, con 31 skips previstos.
- Cobertura instalada en 3.14: **99,01 %**, umbral intacto de 99,00 %.
- Unittest: 3.628 pruebas por versión, tres skips previstos. Perfil profundo:
  nueve pruebas con `max_examples=300`.
- PyMongo 4.9.2: 5.989 pruebas aprobadas; pytest 8.4.2 con ese SDK: 3.454.
- Imports sin PyMongo, seis perfiles de driver, tipado, lint ratchet, snapshots,
  smokes limpios de wheel/sdist/extra pytest y búsqueda Memory/SQLite aprobados.
- Wheel y sdist normalizado reproducibles byte a byte; los 288 archivos de
  runtime coinciden con las fuentes. Los seis procesos MongoDB privados usados
  para capturas finalizaron con código 0 y se eliminaron sus bases temporales.
- Diff final revisado y documentación alineada; los cambios anteriores del
  workset permanecen conservados.

La captura reproducible está en `scripts/capture_error_phases.py`; la matriz
MongoDB de CI la contrasta con `tests/fixtures/error_phases_mongodb_*.json` antes
de ejecutar las regresiones. La versión del paquete no se cambia ni se publica
como parte de este encargo.

La comprobación adicional con PyMongo 4.9.2 captura también los 489 escenarios
nativos en sync/async y sus conexiones reutilizadas. Solo difieren de 4.19.0 los
dos escenarios multi-update parcial: el SDK anterior conserva `upsert: false`
en `writeErrors[].op`. Se guardan los deltas nativos con el hash de la referencia
base, sin normalizar ni eliminar ese campo; las 12 celdas de proxy pasan con la
referencia correspondiente. CI contrasta los deltas con MongoDB y ejecuta esas
celdas en la matriz del kit pytest.

El [registro de validación](evidence/error-refactor/validation.json) conserva
identidad, resultados y hashes de logs, incluidos intentos rojos corregidos. Las
[referencias nativas](evidence/error-refactor/native-references.json) identifican
servidores, FCV, SDK, corpus y oráculos. Se ejecutaron los gates locales sobre el
wheel final; los workflows de CI quedan preparados para el siguiente push.

Durante el cierre, las regresiones existentes de identidad detectaron que el
nuevo camino raw de findAndModify devolvía algunos escalares internos. Se
restauró la materialización con las opciones de codec de la colección antes de
formar la respuesta. El inventario AST normaliza los contextos de expresiones
que Python 3.15 omite de su representación, conservando el mismo criterio en las
tres versiones. Ninguno de esos intentos rojos acredita el candidato final.

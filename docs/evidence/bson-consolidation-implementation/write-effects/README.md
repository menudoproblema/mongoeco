# D01b: identidad y efectos públicos de escrituras

Owner: [registro de implementación](../../../bson-consolidation-implementation.md).
Contrato: [plan, revisión 6](../../../bson-semantics-consolidation-plan.md).
Base archivada: `c15dfb5` (runtime equivalente a `ef604c7`).
D01b sigue abierto para construcción/contenido y entrega de eventos.

El corpus `write_effect` reutiliza registro/capturador/replay existentes. Sus
nueve acciones contrastan `_id` classic/pipeline/replacement, NaN en mantenimiento,
candidatos con valores no finitos, update_many con índice, bulk ordered/unordered,
comandos/findAndModify y unicidad parcial tras cambio de tipo. El observador usa
BSON del SDK para estado físico; no importa comparador/clave nuevos. Las tres
capturas 7.0.43/8.0.32/9.0.2 con FCV correspondiente se verifican con --check.
Los fixtures nuevos no sustituyen referencias históricas.

`before-final-corpus.txt.gz` carga explícitamente mongoeco desde git archive y
falla 84 checks/pasa 135. El candidato pasa 219 checks focales y 5.695 integrados.
`final-expanded-core.txt.gz` registra 2.564 checks, 936 subtests y un skip ajeno a
aceptación física; `final-write-consumers.txt.gz`, 12.155 checks y 1.478 subtests.
Orjson pasa 233 (corpus y fronteras); typing público/boundary, smoke SDK-free 3.13 y lint sin deuda
nueva pasan. Las dos propiedades nuevas cubren vistas DBRef/documento y NaNs
clonados por codec/BSON frente a la guarda de clave, incluyendo wrappers
double/Decimal128 quiet/signaling, con 100 ejemplos cada una. Ambas propiedades
fallan sobre la base archivada. La adaptación al SDK de los wrappers se hace
en el test; no usa un comparador productivo para decidir el esperado.

`legacy-file-probe.py.txt` escribe un SQLite con el runtime archivado y lo
reabre con el candidato. Los tipos/payloads, claves y esquema se conservan en
actualizaciones no relacionadas y relectura; funcionan no-op, índice y borrado.
`legacy-final-before.json` identifica origen y hash del archivo final desechable.
`legacy-final-after.json` registra el resultado; los anteriores son feedback. No se redefine ninguna clave.

La firma auxiliar SQLite continúa limitada a valores finitos: un valor válido
sin firma devuelve None, permite la escritura y utiliza las guardas existentes
de fallback. El corpus y el test de ruta prueban que no elimina candidatos,
incluyendo arrays, consultas finitas/no finitas, hints, sort y count.
No se introduce otro formato numérico ni se activa pushdown aproximado.
Cuatro controles permanentes de unicidad comprueban error/estado atómico, con
referencia nativa independiente en las tres versiones (12 sondas), conservada
en `unique-index-probe.py.txt`/`unique-index-native.json`.

`benchmark.py.txt` compara secuencialmente cinco muestras de 300 pares de
update/find sobre 100 documentos, sin otros gates activos. Checksums idénticos;
razones de medianas 1,05 Memory/1,01 SQLite. Es diagnóstico pequeño,
no aceptación de costes D05.

Los logs de candidatos intermedios se conservan: el primer adaptador async no
exponía delete_one y el test inicial de reapertura consultaba client._engine,
que era un atributo de base de datos. Se corrigieron los tests; no se cuentan
esos errores como defectos productivos. Al ampliar wrappers, el observador de
la propiedad necesitó una adaptación explícita a tipos SDK; se corrigió y se
repitieron los gates afectados. Se descartó un intento transitorio de renombrar
el detector NaN y se reutiliza su nombre/owner vigente. Ningún log de import o
colección fallida se cuenta como gate verde. Una invocación orjson usó un flag no
admitido y no ejecutó tests; la ejecución final usa MONGOECO_JSON_BACKEND.

## Diagnósticos que no se aceptan como correcciones

`event-inputs-probe.py.txt` observa seis formas de mutación, con barrera insert,
en los tres replica sets y seis pares locales. Classic whole/dotted tienen la
misma postimagen y distinta updateDescription nativa; el componente de efectos
necesita conservar procedencia/rutas. Al cambiar el tipo de `_id`, documentKey
conserva su tipo anterior en MongoDB, pero el local publica el posterior.
No-op no publica antes de la barrera. Estos testigos no aceptan contenido de
updateDescription, operación replace/update ni entrega transaccional D01b.

`generated-id-probe.py.txt` verifica mantenimiento de IDs generados como NaN
mediante expresiones en MongoDB 9 y los dos motores. Quiet NaN coincide. La
conversión string sNaN revela una divergencia adicional: servidor 241 frente
a aceptación local. Queda registrada para el owner de conversión escalar,
con referencias 7/8 y testigo permanente pendientes; no se declara corregida
por el trabajo de identidad. No equivale a prohibir Decimal128 signaling NaN
almacenado ni cierra la frontera general B07.

Los servidores temporales propios permanecen disponibles para D01b; las bases
de cada sonda/captura se eliminan. La instancia habitual no se modifica.
D06c conserva consumidores restantes; D04, H53, decisión D07 y gates D05 siguen
pendientes. No se afirma cierre de bloque completo ni de programa.

# Proyecciones: preparación, contexto y metadatos

Corte B07 unidad 5, sobre `a7d27fc9`; el commit documental `480b89cc` conserva
la relectura del informe. El [receipt](receipt.json) vincula la fuente exacta,
wheel, referencias, logs y límites. No cierra toda la unidad 5 ni D05.

## Causa y corrección

Find solo trataba flags y operadores especiales, mientras agregación tenía
preparación de expresiones. La gramática ahora comparte expansión de rutas y
aplicación de campos calculados sobre el evaluador existente; cada superficie
conserva sus operadores y reglas. Los flags BSON numéricos incluyen doubles y
Decimal128, sin convertir sNaN a un número Python. La asignación de missing/`$$REMOVE` conserva los objetos padres de las rutas
calculadas; las referencias históricas de composición/REMOVE permanecen
intactas y verifican esa garantía. Se preservan objetos vacíos
en arrays, forma anidada y orden del `_id` calculado.

La ruta de estados de `$project` evaluaba dos veces cada expresión: primero
contra el documento público y luego contra el estado. Ahora evalúa una vez
contra el estado original y conserva el tratamiento de campos virtuales. La
prueba con `$rand` usa efectos distintos por llamada para detectar reevaluación.

La metadata de texto se perdía al convertir estados a dict y al copiar esos
contenedores en Memory. Un dict interno transporta el `RuntimeMetadata`
existente fuera de los campos BSON; conversiones/copia conservan ese estado
hasta el consumidor. La materialización pública continúa devolviendo BSON sin
atributos internos ni campos auxiliares. No cambia el SPI público ni formatos
persistidos. Las pruebas comprueban además que mutar el resultado no modifica
el documento almacenado.

Se separan disponibilidad declarada y valor de metadata por fila. La
preparación del pipeline lógico comprueba fuentes, también en facets y
pipelines extranjeros; no vuelve a inferirlas de un sufijo físico. Después de
`$count` o de un facet exterior, una fuente declarada puede haber perdido el
valor: no equivale a una consulta sin proveedor. La detección recursiva de
`textScore` se comparte con find y respeta `$literal`/`$const`. Se reutiliza el
split de `$match` con `$text` existente y se conserva su restricción de posición.

Las proyecciones reciben `let`, collation y reloj de la operación en find,
findAndModify y rutas optimizadas de ambos motores, incluida la selección
unitaria SQLite que usa wire. FindAndModify prepara constantes antes de
escribir, pero proyecta la imagen seleccionada después del commit: el error
dependiente del documento conserva el efecto nativo de escritura. Las pruebas
de eventos locales usan una barrera explícita para distinguirlo del error
constante que impide la escritura.

## Referencia y verificación

El corpus nuevo tiene siete acciones y 252 observaciones por versión sobre
MongoDB 7.0.43, 8.0.32 y 9.0.2. Captura y `--check` coinciden byte a byte. Los
307 fixtures y observadores históricos protegidos no cambian. Las cinco
rondas temporales dejan 45 recursos Docker ausentes, comprobados con inspect.

La aceptación instalada se registra en el receipt y los logs: el corpus nuevo
cubre find/find_one/aggregate/comando, sync/async, Memory/SQLite y wire; la
cohorte conserva explícitamente los dos fallos anteriores de batching. Estos
fallos siguen pendientes y no se ocultan ajustando sus expectativas sin
testigo físico.

- Gate diferencial amplio: 10.473 pruebas correctas, sin fallos.
- Cohorte instalada: 2.432 correctas, 4.709 subtests y los dos pendientes previos.
- Baseline `a7d27fc9`: 84 fallos y tres controles de procedencia correctos.
- Focales: 181 correctas y seis subtests; reapertura SQLite instalada: una.
- API pública: 18 correctas y 13 subtests; typing instalado y ratchet correctos.
- Coste seleccionado: dos pruebas y dos subtests correctos, sin cierre D05.

SDK-free se ejecuta con Python 3.13.15 y ausencia comprobada de `bson`: 1.488
observaciones. Omite expresamente las cuatro rutas del constructor Code por
motor/dialecto, que ese perfil no proporciona; los observadores no se modifican.
Las referencias completas con SDK sí incluyen Code. Los contratos públicos,
typing y controles de coste seleccionados conservan sus gates propios.

## Pendientes conservados

B07 unidad 5 conserva batching/getMore, sobres/DTO, resultados bulk y las
combinaciones restantes de fábricas/callbacks y respuestas que no acrediten
sus receipts previos. D06 conserva los contextos restantes; H53, decisión D07
y D05 siguen abiertos. No se acredita paridad Atlas Search, watch distribuido
por estas barreras locales ni ejecución remota CI.

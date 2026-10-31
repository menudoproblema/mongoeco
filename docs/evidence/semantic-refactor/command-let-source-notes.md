# Let de comando: programa y bindings evaluados

Fecha: 31 de octubre de 2026. Segundo corte B04, H30 y parte de H18/H35.
El [receipt](command-let-validation.json) posee fuentes, referencias y gates;
el [registro](../../semantic-parity-implementation.md) conserva los pendientes.

## Owner y fases

`core/aggregation/preparation.py` posee nombres, alcance y evaluación de `let`.
La compilación de operaciones reutiliza su validación estática; las fronteras
de ejecución evalúan el programa con el contexto de la operación. El visitante
de variables existente devuelve además si una expresión requiere documento.
Reconoce literales, CURRENT cerrado y alcances let/map/filter/reduce; no busca
cadenas `$...` de forma indiscriminada ni crea otra gramática.

Las definiciones se procesan en orden. Una definición puede usar una anterior,
pero no a sí misma ni una posterior. Se valida también un binding no utilizado,
una rama inactiva y una colección vacía. La sintaxis se comprueba sin ejecutar
constantes indiscriminadamente: una división en una rama no elegida no falla,
pero una referencia a campo en esa misma rama invalida el programa. Los testigos
de cuerpo y let simultáneamente inválidos fijan la precedencia.

La comprobación estática previa y la evaluación tienen fases distintas. No se
añade una caché, otro IR ni un flag público de validación para ahorrar ese
recorrido. No se evalúa azar/reloj durante el chequeo estático. El programa
se evalúa una vez por frontera de ejecución aplicable, antes de examinar filas.

`ExpressionExecutionContext.with_bindings` sigue superponiendo **valores**.
`with_command_let` prepara un **programa**, salvo cuando recibe un contexto ya
evaluado. `$literal` conserva cadenas, documentos de operador y documentos que
parecen Extended JSON; no se interpretan nuevamente al derivar un frame.
El contexto congela programa/base para reconstruir la petición del cursor sin
usar sus resultados anteriores como sintaxis. CURRENT puede ser objeto, null o
escalar y altera `$field`; ROOT conserva el documento original. El entorno
consulta CURRENT desde su binding, sin una copia de atributo que quede obsoleta
al abrir un alcance léxico.

## Rutas y errores

Se integran lecturas, selección/escrituras, aggregate, comandos y elementos bulk
en sus owners actuales. Un bulk de solo inserts ignora let; para update/delete
se prepara dentro de la frontera de error de cada sentencia. Un let inválido
no cancela retroactivamente el insert anterior. Se conservan índices de error,
ordered/unordered, estado y recuperación, sin convertir todo en error global
de comando ni reenviar la petición después de un fallo.

Nombres inválidos usan código 9 y prefijo `Invalid 'let' parameter`; asignar NOW
usa 4738901. Dependencia de documento usa 4890500; referencia indefinida, 17276.
Los diagnósticos se adaptan a cada frontera existente: WriteError, BulkWriteError,
OperationFailure o entrada raw, según la petición. No se cambian los nombres de
variables de lookup para presentar un error de let de comando.

Raw count y distinct rechazan let con 40415. `count_documents` usa aggregate y
sí lo admite. El nombre IDL del campo distinct es `distinctCommandRequest.let`
en 8 y `distinct.let` en 7/9. El dato reside en el catálogo de dialectos y su
hook registrado, no en tres implementaciones de comando.

## Reloj, cursor y SDK

Las capturas 7/8/9 distinguen lectura y escritura: `let: {x: '$$NOW'}` funciona
en lecturas de 7; en 8/9 falla con 51144. Las escrituras lo admiten en las tres
versiones. El frame de preparación limita NOW únicamente para ese contexto de
lectura. El cuerpo de la operación sigue disponiendo de NOW; no se desactiva la
variable para todo el dialecto. El hook
`supports_now_in_read_command_let` y el flag de IDL anterior están registrados
en los hooks y datos de dialecto. La hora inyectada sigue siendo de la operación.

La [sonda adicional de fases](characterize_command_let_now.py) conserva
[seis observaciones de 9](command-let-now-branches-9.json): NOW en una rama no
elegida o dentro de literal no falla; un binding léxico evaluado sí falla aunque
su nombre no se use. La disponibilidad aquí es de evaluación, no un rechazo
global de toda referencia durante el parseo. Esta comprobación adicional no
sustituye al corpus de aceptación 7/8/9 ni amplía sus números.

El corpus común conserva find original, rewind y petición nueva con literales.
El clone de la API directa se contrasta con esa petición nueva nativa, antes y
después de consumir el original. No se sustituye el contrato local de preservar
opciones por una omisión del SDK. Se conservan los tests existentes de identidad
de OperationContext en rewind sin programa; con programa se renuevan sus bindings
y caches manteniendo la identidad de operación y el reloj local.

El [reproductor SDK](characterize_command_let_sdk.py) y su
[observación nativa](command-let-sdk-8.json) usan CommandListener de PyMongo
4.19.0: el find original envía let y funciona, clone no lo envía y falla con
17276, y un find nuevo vuelve a enviarlo y funciona. Esa diferencia pertenece
al SDK; no se crea un delta del servidor ni se normaliza fuera del observador.
Esto caracteriza ese SDK/servidor, sin extrapolarlo a todos los perfiles.

## Evidencia y pendientes

Las 26 acciones contienen matrices de definiciones válidas/inválidas sobre
peticiones públicas, entradas vacías/pobladas, efectos y recuperación. El
observador de writes y el comparador BSON permanecen independientes del núcleo.
Las tres capturas se obtienen directamente con PyMongo; el módulo de acciones
no importa el evaluador productivo. El corpus final se ejecuta también sobre
la copia anterior al cambio para demostrar el fallo, sin editar sus resultados.

Este corte corrige H30 focalmente; no cierra B04, B08 ni H35. Permanecen las
estructuras y acumuladores restantes H14, incluidos los argumentos obligatorios
de getField; la preparación actual aún admite su forma sin input. También siguen
pendientes fases completas de cursor/getMore, contratos temporales nativos de
clone/rewind y otros perfiles SDK, y el rechazo de `distinct(let=...)` en API
directa: la superficie local aún da TypeError, mientras PyMongo llega al error
de servidor 40415. El rechazo raw aquí cubierto no acredita esa otra superficie.
La validación BSON estricta de B07, resto de bloques, consumidor y gates globales
de distribución conservan su alcance pendiente.

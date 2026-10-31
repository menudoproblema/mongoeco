# Gramática de expresiones y validación contextual

Fecha: 31 de octubre de 2026. Primer corte B04, H09–H13 y parte de H14/H35.
El [receipt](expression-grammar-validation.json) posee fuentes, capturas y gates;
el [registro vivo](../../semantic-parity-implementation.md) posee los pendientes.

## Autoridades y retiradas

`compat/_catalog_operators.py` posee los argumentos de las familias migradas:
control booleano/comparación/in/ifNull, acceso a arrays y cmp. El catálogo y los
sets de dispatch derivan esos nombres. Preparación y runtime delegan en la
misma comprobación de argumentos. Los operadores de objetos y las familias no
migradas mantienen sus handlers; no se crea una DSL ni un registro universal.

Se retiran los helpers field-bound y su validación especial. Un `$and`/`$or`
de agregación evalúa expresiones: ni objetos literales ni arrays se convierten
en filtros. `$exists`/`$elemMatch`/`$all`/`$nin` permanecen como operadores de
consulta, pero no son expresiones. Los tests positivos de lookup pasan a usar
comparaciones binarias válidas y predicados ordinarios fuera de `$expr`,
conservando sus datos y resultados. No hay un flag de transición ni otro camino
que repare pipelines de consumidores.

`$not` y los operandos escalares de `$and`/`$or` usan la veracidad común. First,
last y arrayElemAt comparten acceso: null/ausencia de input produce null, un
array vacío o índice fuera de rango produce ausencia y un escalar inválido
falla. Índices double/decimal/int64 integrales se aceptan si caben en int32;
booleanos, fracciones y valores fuera de rango conservan sus rechazos nativos.
`$in` exige un array incluso cuando el segundo argumento es null o ausente.

## Gramática, fase y presentación

La preparación comprueba sintaxis/aridad también sin filas y en ramas
inactivas. El mismo validador no fuerza evaluación de datos. El corpus conserva
la diferencia entre agregación que optimiza constantes y update por pipeline
que no las evalúa indiscriminadamente antes de encontrar documentos.

Los diagnósticos internos distinguen parseo, optimización y ejecución; el
adaptador existente añade presentación contextual. El parser raíz de project
conserva su rechazo 31325 y el caso especial de elemMatch. Las expresiones
anidadas y `$expr` conservan el código 168. Aridades ordinarias usan 16020 y
ifNull insuficiente 1257300; input inválido de in usa 40081. Los errores de
escritura mantienen su familia, estado y presentación por versión.

En ejecución de agregación, 7/8 usan el prefijo PlanExecutor y 9 incluye el
namespace. El observador normaliza únicamente ese prefijo cuando coincide
exactamente con la colección que ejecutó la petición; un namespace erróneo y
los textos presentes en datos exitosos permanecen observables. No normaliza
mensajes completos ni usa la implementación productiva para compararlos.

## Contextos y alcance

Las capturas incluyen project, set, match/expr, find, count y update, con
colección vacía y poblada, estado posterior y recuperación. Facet, lookup con
let y union conservan tanto rechazos como formas válidas. El corpus es Python
con llamadas públicas PyMongo y no importa el evaluador de mongoeco.

Se conserva el selector especializado de finalización de promedio como su
único owner; soporte sintáctico no se convierte en una afirmación general de
elegibilidad SBE. Los testigos nuevos contrastan not/first/last sobre la misma
cancelación int64: not permite la forma SBE 8/9, first/last conservan el
resultado clásico, y 7 conserva el clásico en las tres formas.

Este corte no cierra H14 completo: faltan otras estructuras de objetos,
acumuladores unarios y sus consumidores. H30 (`let` de comando) sigue pendiente.
Tampoco acredita fases de cursor/getMore, todo H35, B00 completo, el consumidor
externo ni los gates de distribución B10. Los receipts anteriores conservan su
historia; solo se actualizan exportaciones y deltas del catálogo vigente.

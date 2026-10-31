# Implementación de consolidación BSON

Contrato autorizado por el usuario: implementar el
[plan refinado](bson-semantics-consolidation-plan.md), revisando cada bloque y
commiteando semánticamente. Base: `eef9c09`. Estado: implementación en curso.
No cambia SPI v2 ni declara implementados los pilotos opcionales.

## D00 — Activo

Se reutiliza el runner/capturador/replay existente. El corpus
`tests/differential/semantic_binary_cases.py` añade selección por operadores,
eq/in, deduplicación pública por subtipo/contenido y no-op clásico/pipeline.
Sus observaciones son independientes del comparador y claves de producción.

Primera [caracterización anterior al arreglo](evidence/bson-consolidation-implementation/d00-initial/receipt.json):
MongoDB 9.0.2, FCV 9.0, standalone desechable; los tres actions difieren en
Memory y SQLite con dialecto 9.0. Se comprobaron selección, valores distinct
por contenido/subtipo y contadores no-op. Es evidencia inicial, todavía fuera
del registro/capturador/replay, no un cierre de bloque. El corpus usa 16 bytes
para subtipo 5: la ejecución inicial nativa rechazó su literal de expresión de
dos bytes, un rechazo que los motores locales no aplicaron. No se admite esa
ejecución fallida como referencia de igualdad; su caracterización de gramática
queda pendiente dentro del dominio Binary.

Pendientes antes de aceptar D03a: integrar el corpus en el registro, capturas
nativas 7/8/9, fallo previo del replay, propiedades generadas de coherencia en
fronteras reales y regresión pública. Un corpus preparado no cierra D00.

### Corte Binary: integración D00 y primer arreglo D03a

Se integra `binary` en SemanticCorpus, el capturador y la matriz existentes,
sin runner paralelo. Capturas nativas 7.0.43, 8.0.32 y 9.0.2 con FCV de su
major; las tres referencias se verificaron otra vez con `--check`, sin
reescribirlas. Las tres propiedades generadas atraviesan bytes, SDK, local,
owned, decode y salida pública, y comprueban rango/igualdad/claves/subtipos.

El runtime previo aislado reproduce 60 fallos semánticos del replay público
(15 checks pasan). La primera ejecución sin permiso de sockets mezclaba
fallos reales y PermissionError: queda como diagnóstico, no como gate previo.
Las propiedades previas fallan en sus tres leyes. Después del arreglo, el
replay focal tiene 75 checks verdes y las tres propiedades pasan.

El cambio introduce la vista neutral `bson_values.binary_parts`, empleada por
orden, igualdad/hash lookup, claves semánticas e identidad de asignación. Se
conservan las claves físicas y el formato almacenado. El orden binario compara
longitud, subtipo y contenido. SQLite conserva fallback semántico para eq/in
binario, sin perder Binary 0 por diferencia de tags históricos.

Evidencia del corte en [receipt](evidence/bson-consolidation-implementation/binary-first-cut/receipt.json).
Regresión semántica ampliada: 4.964 verdes sobre el primer candidato; después
del cleanup de imports/dispatch, 145 checks focales verdes y ratchet de lint
sin nueva deuda. El receipt distingue ambas revisiones y la ejecución previa
con permisos de sockets corregidos. No es la suite global ni el gate D05.
Los contenedores/servidor propios se retiraron y sus tres puertos están cerrados.

Este corte no cierra D03a: faltan la extracción de clasificación general fuera
del matcher, los testigos ampliados de longitudes/arrays/gramática y observación
de caminos SQL y storage/cache. Tampoco cierra D01a: Code y la reubicación del
helper físico siguen pendientes. D05 aún exige matrices, coste y paquete final.

### D01a y clasificación común — implementación y validación focal

La clasificación atómica y la vista documental se extraen a `bson_values`;
matcher conserva una delegación y schema/virtual_indexes consumen directamente
el owner neutral. La gramática de aliases sigue en query_plan: no se fusionan
traversal MQL, schema y reglas físicas. No cambian hooks de dialecto.

`bson_identity.same_bson_update_value` es ahora la única implementación de
identidad física, consumida por paths, arrays, pipelines y reemplazos. Code
compara cuerpo y scope recursivo con orden/tipos; DBRef usa su vista documental
ordenada. Se preserva distinción scope ausente/vacío y la identidad numérica.
La guardia del observador independiente se amplía al módulo nuevo, sin cambiar
las reglas de los observers protegidos.

El corpus update_identity registra 17 escenarios por classic/pipeline/replacement:
scope reordenado, cambio numérico/nesting, scope ausente/vacío/no-op, Binary,
ancho/tipo numérico, signed zero, payload NaN, cohortes/signo Decimal128 y DBRef.
Observa contadores y bytes BSON producidos por el SDK, no igualdad de clases
Python ni comparadores de producción. Las tres referencias nativas 7/8/9 se
verificaron nuevamente con `--check`. El runtime archivado b661e7a falla los
72 casos públicos del replay; el candidato pasa sus cuatro superficies.

La ampliación descubrió pérdida de payload NaN en JSON SQLite. El commit
`19ed3fa` la corrige independientemente del helper: conserva el tag double con
payload de bits para no finitos y distingue ownership raw/wrapped, sin cambiar
la representación usada por las claves físicas. Soporta lectura del payload
double anterior y float legacy; no reconstruye bits perdidos anteriormente.
La extensión se verifica con 22 casos stdlib/orjson y 67 controles del codec
sobre b661e7a aislado con solo codec.py sustituido. No cierra B07 general.

La [evidencia del corte](evidence/bson-consolidation-implementation/identity-and-classification/receipt.json)
registra 4.969 checks de regresión semántica verdes, 193 controles de unidad/propiedades,
150 checks Binary/identidad con orjson, typing público/boundary, ejecución sin
SDK y lint sin deuda nueva. Las ejecuciones con seed vacío y la comparación
inicial por clase SDK no son gates: el primero no llegaba al fallo semántico y
la segunda distinguía bytes/Binary 0 con idéntico BSON.

Revisión D01a: no quedan cuerpos duplicados ni llamadas de producción al helper
antiguo; paths importa el owner nuevo porque lo usa. D01b sigue abierto para
eventos/outbox/índices y operaciones múltiples. D03a aún necesita los testigos
ampliados de longitudes/arrays/gramática y de caminos físicos; la extracción ya
está hecha. La aceptación global de paquete/matrices permanece en D05.

## D02 — Coordenadas exactas y consumidores tipados; publicación aún abierta

El corte reemplaza timestamp/total_seconds flotantes por coordenadas enteras
UTC/BSON e inversa común en bson_scalars. Conversión, resta, dateDiff, ventanas
y TTL consumen esa autoridad; runtime reexporta los helpers aritméticos en vez
de mantener otra implementación. La inversa conserva truncado hacia cero y la
aritmética de fechas distingue los empates double y Decimal128 observados en
MongoDB. Truncado por milisegundos usa el ancla nativa de 2000, también antes
de epoch y con binSize que no divide esa distancia.

Conservar long al salir de toLong/dateDiff es necesario para toDate anidado.
La revisión recorre sus consumidores: dateAdd/parts/trunc, arrays/strings,
range/round/percentile, ventanas, fill y densify usan la vista numérica común
en sus validaciones. Fill deja de ordenar por comparación Python. El testigo
de percentiles detecta otro error previo: extremos escalares devueltos como
int frente a double nativo; se corrige en el owner que comparte con group y
se contrasta el BSON físico de ambos consumidores. No se declara completa la
paridad de todos los operadores de ventanas por arreglar estos testigos.

El corpus datetime incorpora seis acciones al registro/runner existentes.
Capturas 7.0.43, 8.0.32 y 9.0.2, con sus FCV, verificadas mediante --check.
El runtime archivado 93b9b34 reproduce 144 fallos públicos y tres checks de
metadata verdes. El candidato pasa los 147 checks focales, también con orjson;
2.382 checks de core/propiedades/TTL y 936 subtests pasan, con un skip local
que no se cuenta como ejecución física. Las dos propiedades de coordenadas
usan el SDK BSON como oráculo, incluyendo fechas aware. Pasan typing público
y boundary, controles sin SDK y lint sin deuda nueva. La regresión semántica integrada pasa 5.116 checks sobre source/corpus
congelados. La evidencia queda en el
[receipt del corte](evidence/bson-consolidation-implementation/exact-dates/receipt.json).

SQLite reconstruye los auxiliares TTL de versión 1 al abrir con coordenadas
exactas de versión 2. Se conservan storage keys y documentos; se comprueban
expiración−1/exacta/+1 ms en ambos motores, índice antes/después de datos,
reapertura, rollback/reintento ante fallo y rechazo de versión futura sin
reconstrucción. Los controles previos de almacenamiento/publicación transaccional
no se reinterpretan como entrega a watch.

**D02 sigue abierto para publicación.** La sonda pública local encuentra que
TTL elimina el documento pero no entrega un evento delete en ninguno de los
motores. No se ha repetido aún ese testigo contra un replica set nativo ni se
ha corregido el efecto. El próximo paso es obtener esa referencia y recorrer
el owner de cambios/outbox, coordinado con D01b/D04. El cálculo correcto y sus
checks no cierran esa obligación ni D05.

## Trabajo restante

- D03a: completar testigos de longitudes/arrays/gramática y caminos físicos.
- D01b: consecuencias públicas de modificación, eventos, índices y operaciones múltiples.
- D02: completar referencia física y publicación de borrados TTL; el corte de
  coordenadas, consumidores y auxiliares compatibles está implementado.
- D03b: tipos/SQL bajo prueba de equivalencia.
- D06a/D06b/D06c: gramática, preservación BSON wire y consumidores DBRef.
- D04: preparación/contexto de watch, referencia con replica set.
- H53: caracterización y disposición de observabilidad conforme al plan.
- D07: piloto opcional con criterio de continuidad, sin migración obligatoria.
- D05: revisión y gates integrados, matrices y commits de cierre.

Los cortes focales no acreditan el cierre integrado del programa ni D05.

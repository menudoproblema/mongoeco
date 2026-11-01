# D06a/b: construcción y asignación de documentos en respuestas

Corte sobre `220cedc`, contrato autorizado en la revisión 16 del
[plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución).
El [receipt](receipt.json) identifica fuente final, corpus, referencias y límites.
No cierra D06a/b completos, D06c ni D05.

## Causa y cambio

La igualdad de las filas ocultaba efectos distintos. El materializador aplicaba
el codec al subdocumento cursor, omitiendo la construcción de la raíz del
comando. Con TypeRegistry faltaba además el contenedor intermedio de selección.
El codec portable construía cada documento a partir de un dict ya convertido:
cambiaba argumentos, orden de construcción y llamadas a __setitem__.

La materialización comparte ahora la envoltura y selección de una respuesta
completa. Sin registro se decodifica cursor/ok; con registro se construyen primero
los contenedores de selección y se decodifica el subárbol del cursor mediante
las APIs públicas BSON. Las asignaciones de selección conservan sus errores
sin wrapping BSON; el SDK mantiene el wrapping propio de los documentos
embebidos. El batch transformado se devuelve sin consumirlo. No se usan APIs
privadas, otro driver, evaluador, journal ni caché. El camino dict sin opciones
continúa utilizando la conversión fusionada existente.

La ruta portable crea documentos sin argumentos y asigna campos en orden. La
prueba realmente sin SDK descubrió además un callback int extra sobre cursor.id:
el control int64 había perdido su ancho al convertirse a int público. La
selección portable evita ese callback sobre metadatos, conservando los callbacks
de un campo id perteneciente a los datos. Una regresión unitaria distingue ambos
contextos en firstBatch y nextBatch; no se excluyen todos los campos llamados id.

## Topología y oráculo

Un replica set agrega metadatos de reloj que también pueden activar fábricas.
No se normalizan esos efectos ni se simulan relojes distribuidos en standalone.
El fixture existente admite standalone explícito y conserva replica set por
defecto; comparte ownership, cancelación, límites y limpieza. El preflight exige
la topología declarada, build y FCV estables. Una selección que exige ambas
requiere las dos particiones, pues una única URI no puede satisfacerla.

El corpus declara requires_standalone en los registros existentes. Capturador,
runner, metadata y CI consumen esa declaración; los manifiestos históricos no
cambian. CI comprueba las referencias de corpora sensibles a standalone. Watch
sigue exigiendo RS. El control físico negativo sobre un RS 9.0.2 demuestra rechazo
antes de escribir una referencia; no es solo una prueba con mocks.

Cuatro acciones congeladas del capturador/replay existente cubren find,
find_one y aggregate, con/sin TypeRegistry: argumentos/construcción/setters,
fallos por ordinal o campo, anidados/Code.scope, vacío/poblado y batch 0/1/2 con
getMore. Referencias MongoDB 7.0.43/8.0.32/9.0.2, FCV correspondiente, imágenes
fijadas por digest, PyMongo 4.19.0. La captura final solo comprueba las referencias
nuevas; no recaptura goldens para acomodar el arreglo. Los observadores y
catálogos protegidos permanecen intactos.

## Verificación y límites

El replay completo del wheel final instalado pasa **7.909 checks** (234.91 s). Los cuatro workers verifican todos los hashes de los 284 archivos y la procedencia de sus módulos cargados; no importan el checkout.

La base archivada 220cedc, con imports/hashes verificados, falla 48 casos y pasa
51 controles del corpus final. Las superficies wire permanecen como
controles positivos donde ya era el SDK quien construía la respuesta. Las
pruebas locales iniciales del candidato pasan 143 checks de corpus/fixture;
posteriormente se añaden dos guardas portables y una variante del preflight.
El gate final de infraestructura pasa 45, y el de materialización 47.

El wheel final pasa 362 pruebas de codec/cursores/SPI con 43 subtests, y typing:
dos positivas y 20/25 errores negativos esperados. Python 3.13.15 realmente sin
SDK pasa 72 escenarios nuevos de fábricas sync/async en ambos motores/dialectos,
además de 186 controles anteriores. La política de excepciones de la dependencia
opcional no se sustituye por una clase SDK inexistente; las clases/fases nativas
de fábrica se contrastan en las superficies con SDK. El manifiesto público
efectivo coincide con la base; lint ratchet no introduce deuda.

ABBA dos veces compara 16 pares motor/opción/operación, con resultados BSON
idénticos: ratios CPU 0,91–1,07 Los gates propios estaban inactivos; se observó
una suite Python 3.15 ajena. Es diagnóstico de CPU y no aceptación del presupuesto
D05. La auditoría independiente comprueba ausencia de 13 contenedores y 26
volúmenes propios. Las fallas de bootstrap y de entornos restringidos se guardan
como feedback, sin contarlas como regresiones ni gates aceptados.

Quedan otros contextos/representaciones y combinaciones de consumo/estado
público D06a/b, consumidores D06c, H53, sNaN, decisión D07, D05 y B07. Este corte
no amplía las clases/opciones públicas admitidas ni declara equivalencia de
cualquier efecto arbitrario de un Mapping con un driver completo. El default
UUID continúa STANDARD hasta coordinar admisión B07.

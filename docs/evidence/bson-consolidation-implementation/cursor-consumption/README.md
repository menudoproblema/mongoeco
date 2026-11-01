# D06a/b: una posición pública y un propietario del cursor

Corte sobre `d50ab62`, conforme al
[plan autorizado](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución).
El [receipt](receipt.json) identifica fuente, corpus, wheel y límites. No cierra
D06a/b completos, D06c ni D05.

## Causas y simplificación

La caché del adaptador query sync conservaba resultados después de consumirlos:
iterar otra vez podía devolverlos de nuevo. El atajo de limit=1 consumía al
construir el iterador. Alive solo miraba el flag local y omitía tanto el estado
async como las filas aún pendientes en el buffer sync.

Ahora las lecturas avanzan sobre el delegado async y los buffers de transporte.
Se retira la caché de query, su copia adicional al devolver una lista y el atajo
separado de iteración. Alive combina estado async y filas pendientes sync. Se
retiran también la caché/camino de carga muertos de aggregate. La función privada
_consume_remaining nombra su efecto; no aparece otro objeto de estado o caché.

La revisión encontró además un error compartido de propiedad: los finalizadores
de iteradores prestados podían cerrar la fuente o descartar filas aunque el
cursor siguiera vivo. Se retiran esos finalizadores y el estado suspended que
solo protegía tandas vaciadas por un decoder. El cursor sigue siendo el dueño:
cierre explícito y finalización diferida del propietario abandonado permanecen.
Los controles deterministas drenan la cola de cleanup antes de continuar, sin
sleeps, y comprueban también que abandonar el dueño sí cierra una vez.

Aggregate sync buscaba aclose en su iterador, pero el delegado async real es su
propio iterador y ofrece close. El fake anterior tenía la interfaz contraria.
Ahora se cierra el propietario real incluso antes de iterar; se conserva fallback
para iteradores legacy y reintento tras fallo. Se alinea el fake con el protocolo
real y se comprueba el cierre sobre ambos motores concretos, no solo el doble.

El primer replay completo pasó pero emitió 114 RuntimeWarnings de corutinas de
cierre sin esperar: se conserva como feedback. La causa estaba en la admisión
del SyncRunner: el rechazo por reentrada disponía el awaitable entregado; el
rechazo por closing/closed no. Ambos usan ahora una única disposición en ese
runner existente. La prueba observa la corutina antes del cleanup del propio
test, que antes ocultaba el defecto. No se suprime el aviso ni el error original;
el gate final eleva RuntimeWarning y errores de GC/hilos a fallo.

## Oráculo y comprobaciones

Cinco acciones congeladas del registro/capturador/replay existente cubren lectura
completa y acotada, iteración intercalada, iteradores temporales, rewind/clone,
alive query, respuestas vacías y decoders identity/expand/empty-first/None.
Casos con 0/1/3/5 documentos, límites positivos o ausentes, batch 0/1/2 y getMore.
Find y aggregate conservan sus diferencias; no se añade cursor_id/try_next a una
superficie que no los admite ni se cambia la conveniencia local length=0.

Referencias capturadas y comprobadas con MongoDB 7.0.43/8.0.32/9.0.2 standalone,
FCV correspondiente, imágenes fijadas por digest y PyMongo 4.19.0. La versión 3
del corpus evita importar el SDK para el adaptador local, permitiendo ejecutar
las mismas acciones sin shims ni transpilar tests. Solo las referencias nuevas,
aún en preparación, se recapturaron al ampliar/congelar sus funciones; no se
modifican goldens históricos, observadores ni catálogos protegidos.

La base archivada d50ab62 conserva 284 módulos verificados. Con la fuente final
falla 24 checks nativos, cuatro controles de ownership y dos subtests de rechazo
del runner. Las otras superficies son controles positivos. La ejecución focal de pytest
de ese counterfactual informa 30 fallos/100 positivos; el padre del test con
subtests fallidos se cuenta separadamente. No es un gate aceptado.

El replay completo final del wheel instalado pasa **8.032 checks** (210.83 s), sin avisos y con las tres categorías de warnings elevadas a error. Cada worker verifica los 284 archivos disponibles y la procedencia de sus módulos cargados; no importa el checkout.

Pasan 421 pruebas instaladas de codec/cursor/runner/SPI con 49 subtests y 20 de
integración pública con 19 subtests. El focal previo de consumo/cierre/snapshot
pasó 261 con 58 subtests y RuntimeWarning como error. Tipado: dos positivas y
20/25 errores negativos esperados; manifiesto público efectivo sin cambios.
Python 3.13.15 realmente sin SDK compara 30 acciones/1.278 trazas sync contra las
referencias tipadas de los tres servidores, además de los 258 controles previos
que incluyen async. Los cambios de adaptación están en sync; el corpus instalado
con SDK conserva sus controles async y wire. No se inventa equivalencia por
comparar solo Memory y SQLite.

ABBA dos veces mide 40 pares motor/opción/operación, incluyendo limit=1 y primera página: resultados BSON idénticos, ratios CPU 0.94–1.17. Gates propios inactivos; no se controla el aislamiento de tareas externas. Es diagnóstico, no aceptación del presupuesto D05. En Memory, find(limit=1) con opciones default/STANDARD muestra ratios 1,16/1,17; se conserva ese coste para la revisión D05, sin afirmar rendimiento equivalente.

La auditoría Docker confirma ausencia de 12 contenedores y 24 volúmenes propios.
Los logs y controles de fuentes anteriores permanecen como feedback. No se
acredita CI remoto, matriz completa Python/SDK/pytest, cobertura global, toda la
persistencia ni presupuesto de rendimiento D05.

Quedan otros contextos/representaciones D06a/b, consumidores y claves D06c, H53,
sNaN, decisión opcional D07, D05 y admisión B07. El default UUID sigue STANDARD.

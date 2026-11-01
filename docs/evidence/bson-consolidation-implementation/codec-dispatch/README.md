# D06a/b: dispatch y consumo de respuestas BSON

Base `181a922`, runtime previo `381aa10`; entrega funcional acotada. El
[registro de implementación](../../../bson-consolidation-implementation.md)
posee el progreso completo. Este corte no cierra D06 ni D05.

## Causa y cambio

Había tres decisiones confundidas: producir una respuesta BSON, convertirla
a valores públicos y consumir el batch transformado por el cliente. Comparar
evaluadores o motores no detectaba los defectos compartidos de esos límites.

- Se reutiliza `ResponseBatchReader` y un constructor de cursor con orden
  batch/id/ns. `find_one` conserva selección/contexto y acceso por `_id`, y usa
  el lector/materializador común incluso cuando no devuelve documentos.
- El dispatch usa tipo exacto y una transformación por valor. La respuesta
  completa precede a los decoders. Para los codecs SDK se reutiliza BSON público;
  la ruta portable amplía el recorrido existente, con contextos distintos para
  raíz, fila, batch, DBRef y Code.scope. Se prueban tipos dict/SON, no toda clase
  arbitraria de fábrica documental ni todos sus efectos de construcción.
- El materializador conserva el batch transformado. Los adaptadores de query
  cuentan su longitud después de consumirlo; los de comando aceptan iterables
  sin longitud. Posición del origen y cuenta del cliente tienen dueños separados.
- Iterar aggregate omite `None`; `to_list` lo conserva, también al intercalar
  ambos métodos. Find entrega `None` como dato y usa una señal privada de EOF.
  Se mantienen las tandas vaciadas por un decoder y su posible reanudación.
- La finalización GC de un iterador temporal suspendido no cierra el origen
  que aún posee su cursor. Una prueba drena la limpieza diferida sin sleeps:
  antes repetía `[1], [1]`; ahora continúa `[1], [2]` en ambos motores.
- EOF conocido por una ventana o un origen limitado de sufijo vacío se propaga
  sin evaluar otra fila. Un documento final pendiente por bytes conserva su dueño
  hasta la respuesta siguiente. No se infiere cardinalidad por nombre de stage.
- Lectura UUID interpreta subtipos 3/4 y los órdenes legacy correctos; seleccionar
  legacy conserva un Binary 4 almacenado. El default sigue siendo STANDARD:
  cambiarlo a UNSPECIFIED queda coordinado con admisión B07. Se reconocen hints
  `_id_` por nombre/patrón y collation simple en el owner actual de orden/residual.

## Evidencia final

| Garantía | Resultado |
| --- | --- |
| Native 7/8/9 | 15 acciones capturadas y comprobadas en 7.0.43/8.0.32/9.0.2, FCV correspondiente, replica sets propios y efímeros. Cohorte final v13 en `native.tar.gz`. |
| Focal público | 363 casos correctos, 9,35 s; ambos motores, sync/async, directa/wire y tres dialectos. |
| Baseline independiente | 300 fallos y 63 correctos sobre runtime archivado de `181a922`, con el mismo corpus final; no son errores de importación ni referencias ausentes. |
| Replay instalado completo | 7.810 correctos, 177,24 s, cuatro workers. Cada worker verifica los 284 archivos instalados y la procedencia de sus 262 módulos cargados. |
| Unitarias/SPI instaladas | 360 correctas y 43 subtests; incluye barreras de GC, límite antes de codecs y documento pendiente al conocer EOF. |
| Sin SDK | Python 3.13.15 real, wheel instalado, 186 escenarios: ambos motores/dialectos, sync/async, contenedores, None, consumo mixto, expansión, UUID y DBRef. No es pytest completo ni la matriz D05. |
| Typing instalado | Dos fixtures positivas; 20 errores negativos públicos y 25 de frontera, exactamente los marcados. |
| ABI | Manifiesto efectivo idéntico a `181a922`, SHA-256 `18f336380aa25bc6fecff86f5e353671059f3576b3913964f20dabfeae19ad7a`. No se regeneran snapshots históricos. |
| Recursos | Ausencia comprobada por nombre de 33 contenedores y 66 volúmenes propios; instancia habitual intacta. |
| Coste | ABBA dos veces con gates propios inactivos; 16 pares motor/opción/operación y resultados BSON idénticos. Ratios CPU candidato/base 0,78–1,03 en la muestra. Tendencia diagnóstica; no aceptación de coste D05 completo. |

El [receipt](receipt.json) identifica código, corpus, wheel, comandos, resultados,
hashes y límites. `gc-counterfactual.txt.gz` conserva dos fallos y 356 correctas
contra el candidato anterior al arreglo GC, identificado por `pre-gc-runtime.json`.
Los logs y scripts finales están junto al receipt; el archivo native conserva
también los intentos anteriores y sus distintas fuentes.

## Límites y disposición

La suite amplia de feedback dio 42.290 correctas, 111 reportes de fallo y un
skip, con 13.971 subtests. No estaba congelada y no acepta el candidato final.
Al contrastar sus 99 nodos únicos con `181a922`, 97 fallan también y dos pasan.
Los dos deltas de este cambio eran expectativas de Binary local frente a Binary
SDK en lecturas: se corrigen con la referencia SDK y escritura STANDARD explícita.
Los 97 nodos anteriores conservan disposición D05; dos rojos equivalentes no
constituyen un gate global verde. Los replays de candidatos anteriores y el
interrumpido se conservan como feedback, nunca como aceptación final.

La sonda v10 alcanzó proyecciones calculadas de find, fuera del subconjunto
de lectura declarado en COMPATIBILITY.md. Se conserva su captura como
caracterización; no se añade esa capacidad. El caso final de errores find_one
usa `$expr` soportado, con String límite −1/0/+1, cola null y división dinámica.

Siguen pendientes otros contextos D06a/b y consumidores D06c. Antes de cerrar
D06a/b completo deben caracterizarse también fábricas `document_class` con
efectos propios y mezclas adicionales de consumo/estado público, y completar
la matriz de representaciones portable según su contrato. H53/C03/B08, sNaN,
decisión opcional D07, matrices/cobertura/gates D05 y admisión B07 conservan sus
owners. No se han ejecutado CI remota, publicación ni esas matrices completas.

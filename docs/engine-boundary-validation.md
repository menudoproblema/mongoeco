# Frontera SPI v2: identidad, selector y tipado

S1 y S2 implementan el alcance aprobado de la
[propuesta](architecture/spi-v2-boundary-proposal.md). No introducen SPI v3,
requests públicas, capacidades, perfiles ni migraciones SQLite.

## Implementación

- El canario combina el filtro de operación y el selector independiente. Ambos
  se evalúan dentro del lock ya existente; conserva su subconjunto de igualdad,
  `$set` e incremento, sin incorporar otro intérprete general.
- El check existente `spi-v2-atomicity:compare-and-set` comprueba update/delete,
  selectores ausentes/vacíos/coincidentes/rechazados, repeticiones, competidores,
  imágenes, contadores y documentos independientes. Motores deliberadamente
  incorrectos acreditan que el runner detecta cada condición ignorada y efectos
  falseados. Su ID, perfil y esquema de informe se conservan.
- Las ocho entradas CRUD/read del mismo adapter tienen firmas explícitas y el
  coordinador conserva `EngineSpiAdapter`. El despacho usa los protocolos CRUD y
  batch vigentes. El fallback scan tiene una descripción privada de tipos; no
  cambia la superficie pública. Se retira el extractor genérico de argumentos.
- El contexto sigue validándose en runtime, incluidos None y contextos
  divergentes. Se conservan validación de outcomes, imágenes, secuencias,
  cardinalidad, descarte de snapshots, callbacks locales, outbox y ordinales del
  fallback batch. Solo se convierte el contenedor exterior de batch cuando la
  primitiva requiere una lista; los documentos no se renormalizan.
- Las anotaciones auxiliares describen el payload de eventos y la caché del
  cursor, y aceptan los bindings mapping ya transportados. No cambian capturas
  de reloj, fases de deadline, evaluadores, planes ni selección bajo lock.

## Aceptación y evidencia

La comprobación adicional de adapter/coordinador exige cero errores, partiendo
de 23 en el análisis. El checker conserva el contrato público existente y añade
un fixture positivo para las ocho entradas y 18 diagnósticos negativos esperados,
ejercitados directamente y a través del campo del coordinador. El gate CI de los
dos módulos no relaja el tipado público existente ni añade ignores de mypy.

El corpus nuevo contiene 26 escenarios de comandos equivalentes a identidad más
condición, con imágenes findAndModify, rechazos, códigos de error, colisiones,
efectos parciales y recuperación. Se captura con PyMongo real sync/async en
MongoDB 7/8/9 sin importar Mongoeco y comprobando una sola conexión física.
El replay cruza Memory/SQLite y ambas fachadas, directas y wire. La primitiva SPI
de dos filtros se contrasta con la condición combinada del comando MongoDB.

Estado: **S1 y S2 implementados y verificados**. La
[evidencia de cierre](evidence/engine-boundary/validation.json) fija comandos,
versiones, hashes, resultados y límites; el
[manifiesto de inputs](evidence/engine-boundary/inputs.json) identifica las fuentes,
pruebas y configuración del árbol integrado.

## Candidato y gates

El wheel de desarrollo conserva la etiqueta 4.10.1; no es una publicación.
SHA-256: `c5c3bbd9dc8a09359b78e9cf46405c78a8ab967c2b6fd5873fda80be5652d72c`. Sus 291 archivos de runtime coinciden con
sdist, fuentes fijadas y las instalaciones Python 3.13/3.14/3.15. La copia
integrada incluye el trabajo existente de preparación y diagnósticos. No se
reutilizan las globales aisladas de T1–T6 como prueba de este candidato.

| Gate | Resultado |
| --- | --- |
| Focales del candidato instalado | 740 correctas y 318 subtests |
| SDK mínimo PyMongo 4.9.2 | 636 correctas en Python 3.14, ambos motores/fachadas y paralelismo |
| Unittest completo | 3.628 pruebas; 3 omisiones previstas; sin fallos |
| Propiedades, perfil deep | 9 correctas, hasta 300 ejemplos |
| Tipado público y frontera | 2 fixtures positivos; 20 + 18 diagnósticos negativos esperados |
| Mypy de adapter/coordinador | Cero errores en ambos módulos; línea base 23 |
| Mypy strict existente | 7 módulos correctos |
| Conformance completa | 10 checks correctos por motor; canario 5 correctos/5 no aplicables |
| API y almacenamiento | Manifiesto público sin delta; protocolos, capacidades, esquema y 37 fuentes/fixtures SQLite intactos |
| Packaging | Wheel, sdist y extra pytest instalados en entornos limpios; recursos, CXP, collation y kit correctos |
| Global Python 3.13/3.14/3.15 | 44.773 correctas, 31 omitidas y 14.461 subtests por versión; sin fallos |
| Cobertura instalada Python 3.14 | 99.02 %, superior al mínimo del 99 % |

La matriz global usa PyMongo 4.19 y pytest 9.1.1. La comprobación adicional de
PyMongo 4.9.2 usa Python 3.14, ambos motores/fachadas, corpus wire existente y kit
pytest con paralelismo. No se presenta como una ejecución de toda la matriz CI
remota de versiones de pytest y SDK.

## Incidencias conservadas

- Antes del arreglo, las nuevas regresiones dieron ocho fallos: cuatro efectos
  incorrectos del canario y cuatro motores defectuosos que el runner aceptaba.
- La primera focal que abrió proxies encontró la restricción de sockets del
  entorno; su repetición autorizada permite conexiones exclusivamente locales.
- Una pasada diferencial se lanzó antes de existir las referencias nativas de
  MongoDB 8/9 y falló por archivos ausentes. Se conserva como intento de
  preparación y no como resultado de paridad; la ejecución final requiere las
  tres capturas terminadas.
- La unittest completa detectó la anotación de caché del cursor sin un nombre
  ligado en runtime según el control del proyecto. Las alternativas con
  comillas/comentario chocaron con lint. El resultado final tipa la caché y sus
  dos productores privados con el tipo existente; imports, mypy y ratchet pasan
  juntos. La primera global se interrumpió al detectar esa incompatibilidad y
  se conserva como superada. Los gates finales se repiten sobre el artefacto
  reconstruido.
- La preparación inicial de entornos no resolvió los `.pth` anidados de las
  dependencias. Se corrigió el bootstrap, sin modificar runtime ni dependencias
  de los entornos base; las instalaciones finales verifican todos los archivos.
- La primera matriz global del candidato final compartía el directorio temporal
  del host. Python 3.13/3.15 terminaron correctamente; 3.14 superó cobertura, pero
  el censo global de archivos de sort detectó archivos ajenos durante una prueba.
  Una sonda con un archivo ajeno reproduce esa aserción; sin él pasa. Se repitió
  únicamente la global completa de 3.14 con TMPDIR aislado, conservando runtime,
  datos, presupuestos y aserciones. Se retienen el fallo y su cobertura.
- Las focales iniciales del adapter encontraron fixtures de test que omitían
  parámetros del SPI y pruebas del extractor privado retirado. Se actualizaron
  a llamadas válidas y se conservaron las comprobaciones de resultados
  inválidos, callbacks y fallback. No se retiran primitivas públicas.

## Adopción

Estos bloques mejoran acreditación y mantenimiento de v2. No demuestran una
necesidad de v3, compatibilidad universal con MongoDB ni una mejora de latencia.
La demanda de un contrato público de ejecución preparada continúa sin acreditar.
La [decisión posterior a este cierre](architecture/engine-spi-v3-proposal.md)
mantiene SPI v2 y aparca v3. Ese documento posee las candidatas de mantenimiento
pendientes, sin modificar los resultados históricos de este registro.

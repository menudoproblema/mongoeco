# Lecturas v2: apertura tipada, reader SQLite y errores regex

## Alcance y estado

R1/R2 autorizados por el encargo de implementación posterior a la
[decisión de mantener v2](architecture/engine-spi-v3-proposal.md). La corrección
de regex inválida fue autorizada después de reproducirla también con el
candidato anterior. Estado: **implementación completada y verificada**. La autorización
no incluye commits, publicación ni una transición de SPI.

El gate de entrada es un cambio compatible ordinario: contrato y aceptación
constan en el encargo y la propuesta; el proyecto no exige una transición RFC.
El censo temporal fija base, fuentes, consumidores locales, exclusiones y gates.
Se conservan los cambios concurrentes de wire/driver; el cierre integrado exige
esperar a su estabilización y fijar juntos wheel, fuentes y pruebas.

## Implementación

- **R1:** la caché y el productor de semántica del cursor tienen tipo explícito.
  `_open_read_source(skip=..., limit=...)` concentra las dos aperturas previas;
  sustituye los helpers genéricos de overrides por una ventana tipada. Usa los
  DTO actuales y describe el callable de colección mediante un protocolo privado
  solo de tipos. Selecciona el proveedor una vez y acepta `AsyncIterable`, sin
  exigir un snapshot concreto a las fixtures. Conserva validación previa,
  contabilización, sesión, contexto, diferimiento del deadline, rewind y cleanup.
- **R2:** se retira `_iter_scan_documents_sync`, sin consumidores productivos.
  Sus 14 llamadas de tests pasan al `SQLiteScanReader` productivo, con semántica
  preparada y tracking activo. Conservan escenarios de stop, fallback, ventanas,
  sesión, conexión dedicada y cierre. Count recibe únicamente semántica preparada
  en su helper privado; desaparecen los argumentos crudos y los `None` posicionales
  de adaptación. Los entrypoints públicos conservan sus firmas.
- **Regex:** el compilador compartido traduce el error de sintaxis de Python a
  `OperationFailure` 51091 con documento de error completo. Los productores de
  filtros preparan patrones y valores regex de pertenencia antes de recorrer
  documentos. La evaluación defensiva de planes explícitos usa el mismo helper.
  El manejo existente de flags BSON de pertenencia se conserva y el objeto
  público `Regex.compile()` no cambia.

No se eliminan guardas de frontera, revalidación atómica, outcomes, snapshots,
outbox ni ownership. No se introduce caché global, request pública, nueva
capability, formato persistente o SPI v3. No se promete una mejora de latencia.

## Regresiones y oráculo nativo

Los fixtures de tipado verifican los retornos de operación, semántica y fuente,
y rechazan miembros inexistentes y ventanas inválidas desde el cursor. Se
mantienen los controles de las ocho entradas del adapter y del coordinador.

Las regresiones de apertura ejercitan proveedor async ordinario, lookup único,
primera fila, lotes, ventana original, fallback, contexto y renovación del
deadline. Los tests migrados y un control de stop verifican que el reader cierra
su conexión dedicada y sale del censo de recursos.

El corpus nuevo tiene 13 escenarios: ventanas, vacío, regex válida, collation,
proyección inválida y ocho variantes de regex inválida, incluidos literal BSON,
`$not`, `$in`, `$nin`, `$all`, no-match y colección vacía. Observa find, rewind,
clone, count, escrituras rechazadas, recuperación y estado final. Si el constructor
rechaza la sintaxis antes de devolver cursor, el intento siguiente reconstruye
su cursor; no se acredita identidad de la fase interna entre clientes.

El observador usa PyMongo real sync/async, comprueba una conexión física por
cliente y limpia su base privada. Fija la identidad antes de importar helpers y
la comprueba al terminar mediante el mecanismo compartido de captura. No importa
Mongoeco. El replay cruza Memory/SQLite, sync/async y acceso directo/wire, con
códigos, documentos, count y ausencia de efectos de las escrituras rechazadas.
Conserva el diagnóstico nativo completo en la fixture; compara familia, código,
`codeName`, `ok` y forma no vacía de `errmsg`, sin exigir que Python y PCRE
redacten igual el motivo de sintaxis.

También se recapturan los corpus existentes de errores, preparación y frontera;
las globales deben consumir sus referencias con hashes vigentes. Las fixtures
SQLite 4.5/4.10.1 comprueban compatibilidad sin exportación/reimportación.

## Hallazgos e intentos conservados

La [exploración inicial](evidence/engine-reads/exploratory-differences.json)
conserva las observaciones originales de regex y bindings. La regex se corrige
en este cambio. La otra diferencia es del recorrido de cliente: PyMongo 4.19
pierde `let` al clonar, mientras el candidato anterior lo conserva. No se elimina
la conservación de bindings de Mongoeco para imitar esa diferencia; clone con
`let` queda fuera de la acreditación de paridad de este corpus. No se identifica
ningún consumidor.

La primera sonda sync usó un context manager de cursor que Mongoeco no publica;
se corrigió el observador para cerrar explícitamente. También se conservan los
intentos focales fallidos: sustitución de test demasiado amplia, ausencia de
marca asyncio y una aserción que suponía un nodo diferido superior en cualquier
plan relaxed. Se corrigieron las pruebas y el observador sin relajar expectativas
de regresiones existentes, presupuestos o reglas globales.

## Validación final

El [recibo de cierre](evidence/engine-reads/validation.json) y su
[manifiesto de entradas](evidence/engine-reads/inputs.json) fijan el candidato
integrado, los comandos, entornos, resultados y hashes. Wheel y sdist se
construyeron dos veces con resultados idénticos y pasaron `twine check`.
Se verificaron 293 archivos runtime contra fuentes,
wheel, sdist y los cuatro entornos instalados. Wheel SHA-256:
`bf3d6b778b600a9b12913693eb98af4ff1f82b84c9e34f050a98533a2f2b2bab`.

| Gate | Resultado |
| --- | --- |
| Pytest completo Python 3.13 | 45203 passed, 31 skipped, 14463 subtests passed in 683.84s (0:11:23) |
| Pytest completo Python 3.14 | 45203 passed, 31 skipped, 14463 subtests passed in 746.24s (0:12:26) |
| Pytest completo Python 3.15 | 45203 passed, 31 skipped, 14463 subtests passed in 742.34s (0:12:22) |
| Cobertura Python 3.14 | 99.03731 %, mínimo 99 % conservado |
| Focales del wheel instalado | 1163 passed, 331 subtests passed in 35.09s |
| PyMongo 4.9.2 | 1033 passed in 325.47s (0:05:25) |
| Unittest completo | Ran 3628 tests in 105.080s; OK (skipped=3) |
| Properties deep | 9 passed in 39.51s |

Las tres globales se ejecutaron al final, con temporales aislados por versión,
PyMongo 4.19 y pytest 9.1.1. También pasan tipado público (dos fixtures positivos,
20/25 errores negativos esperados), mypy de frontera y módulos estrictos, API
manifest, canario independiente, ratchet completo sin cambiar baseline,
conformidad Memory/SQLite (10/10 cada uno), snapshots de compatibilidad,
smokes de búsqueda/vector y las instalaciones limpias de wheel, sdist y extra
pytest. Los contratos públicos protegidos y las fixtures SQLite conservan sus
hashes; el setup de SQLite conserva el mismo AST. Se ejecutan las regresiones de
compatibilidad 4.5/4.10.1 y no se modifica el formato persistente.

Las capturas independientes proceden de MongoDB 7.0.43, 8.0.32 y 9.0.2,
con su FCV correspondiente: 13 escenarios de lectura nuevos por versión,
además de recapturar los 1072 escenarios de errores, 26 de frontera y 16 de
preparación existentes sin deltas en las observaciones comparadas. Las referencias
fijan hashes vigentes de sus observadores; los servidores privados quedaron cerrados. El replay de
lectura cubre 24 celdas directas/wire, sync/async y Memory/SQLite.

La coordinación conservó el primer wheel, sustituido antes de la global por
una corrección diagnóstica del proxy. El unittest completo encontró después un
stub privado desactualizado en la prueba de cierre con una respuesta de 8 MiB.
Se actualizó su entrada y se limitó su espera, manteniendo la aserción de cierre.
Solo cambió esa prueba: el wheel siguió idéntico. Se refrescaron snapshot y
manifiestos; se verificó la reconstrucción idéntica de wheel/sdist y se repitieron
unittest completo y smoke del sdist.
Los gates de selecciones sin esa entrada se conservaron sobre el mismo runtime.
La primera matriz global encontró un único fallo del inventario de errores: dos
rechazos trasladados de owner y un rechazo legacy duplicado sin identidad. Se
corrigieron sin ampliar el baseline, se reconstruyó el candidato y se repitieron
los gates y las globales. La matriz fallida se conserva como antecedente. El nuevo
rechazo legacy conserva
`$err`, código 5739101 y flag 2 de `OP_REPLY`, con reutilización de conexión,
según los tres oráculos nativos; no añade `errmsg`/`codeName` a ese formato. Las
globales finales usan el corpus definitivo; el recibo conserva intentos y causas.

El diff de este bloque se revisó frente a las fuentes iniciales y la
documentación quedó alineada. Los cambios concurrentes conservan su propio
registro de revisión y están incluidos en el candidato y las globales. No se
realizaron commits ni publicación.

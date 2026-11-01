# B07: finalización de tandas, límites del driver y ownership de spill

Corte funcional aceptado en `dbe8257f`; protección del fixture en `caed66da`.
Base productiva `c1a7c4a5`; la revisión documental posterior `ce90b144` no cambia
sus fuentes productivas. [Receipt](receipt.json), [fuentes](runtime-sources.json)
y [wheel](wheel.json) fijan el material verificado. No cierra toda la unidad 5,
D06 ni los gates integrados D05.

## Comportamiento y causas

La formación de una respuesta del servidor y la lectura pública del driver
usaban el mismo límite. El contador de prelectura también se tomaba como prueba
de EOF, aunque podía haber filas pendientes de entregar. Esto alteraba el estado
del cursor y los callbacks incluso con documentos finales idénticos.

- La entrada de respuesta del servidor conserva el tamaño solicitado. La entrada
  del driver aplica su promoción inicial cuando batchSize coincide con limit y
  acota getMore al límite cliente restante; cierra tras decodificar la respuesta.
  Find raw utiliza esa entrada del driver. Los consumidores internos conservan
  la entrada del servidor, sin activar los ajustes del cliente.
- Las capturas distinguen límites de ejecución y EOF conocido: en 7/8 el sort
  bloqueante acotado puede necesitar una petición vacía adicional; en 9 conoce
  su cardinalidad real, incluso con fuente más corta que limit. La primera tanda
  de aggregate y los grupos sobre acceso distinct también tienen diferencias
  observadas por versión. Tres flags del dialecto expresan esas políticas; no se
  duplican comparadores ni ejecutores. La elegibilidad estructural de orden de
  índice se comparte con las guardas existentes. No pretende reproducir todas
  las elecciones de coste del optimizador nativo.
- La proyección de una fila posterior continúa evaluándose al formar su propia
  respuesta, aunque el sort haya materializado la fuente antes. Las capturas
  incluyen un error en la tercera fila después de dos respuestas correctas.
  Una fila pendiente por corte de bytes impide cerrar el cursor anticipadamente.
- Sort/group spill conservan cardinalidad y cleanup con un output owned. Cerrar
  antes de arrancar su iterador elimina sus archivos; el cursor registra el
  output al crearlo. El cierre sigue intentando liberar recursos independientes,
  conserva el error primario y permite reintentar el cierre de la fuente fallida.
  No hay otro registro de cursores ni otro ordenador de documentos.
- Find `singleBatch` acepta bool; otros tipos tienen error 14. True cierra tras
  la respuesta inicial, incluso batchSize cero. Las dos pruebas antiguas de
  comando ahora exigen el estado observado: tercer batch lleno abierto y cuarto
  vacío cerrado en el caso sin límite.

Los contratos propios de `Database.command` directo que materializan el resultado
completo permanecen como tales; los frames wire se contrastan separadamente.
No se cambia el significado local de `.limit(0)` por analogía con PyMongo.

## Evidencia diferencial

MongoDB standalone 7.0.43, 8.0.32 y 9.0.2, imágenes fijadas en el fixture existente:

| Corpus congelado | Acciones por versión | Observaciones por versión | Superficie |
| --- | --- | --- | --- |
| command_batch | 6 | 284 | Comando wire sync/async; formas, tamaños, límites, singleBatch, orden de índices y proyección fallida |
| query_batch_callbacks | 3 | 36 | Find/aggregate públicos y raw find; sobres, callbacks y liveness admitido |

Se conserva el `hello` íntegro. Captura, `--check` de la misma ejecución y segunda
comprobación con el guard del fixture coinciden exactamente en `source`,
`targetDialect`, `corpus`, `case_manifests` y `cases`. BuildInfo también coincide;
los bytes completos no son idénticos por hora/conexión de `hello`. Las copias
comprimidas y manifests nativos están en [native](native/); el receipt distingue
las rondas de caracterización de las comprobaciones finales frozen/guard-check.

Las referencias finales se congelaron tras corregir dos observadores nuevos:
`decode_all` se importa del módulo bson y la fábrica de sobres usa un dict
subclass soportado sin alterar búsquedas. No se modificaron observadores ni
referencias históricas para hacer pasar el producto. Los [407 archivos protegidos](protected.json)
conservan sus hashes.

## Gates y alcance

- [Baseline](baseline.txt.gz): fuente Git aislada `c1a7c4a5`, 104 fallos y 58
  correctas. [Candidato focal](focal-frozen.txt.gz): 162 correctas con los mismos
  observadores finales. Baseline no es un wheel instalado; esa distinción consta
  en su [origen](baseline-source.json) y [prueba de importación](baseline-imports.json.gz).
- [Replay instalado](replay.txt.gz): 10.660 correctas. [Cohorte instalada](unit.txt.gz):
  2.457 correctas, 4.709 subtests, cero fallos; incluye los dos nodos antes rojos.
  Wheel SHA-256 `54bd86d00b5b122b3eb8edad81fc08cb6d4928f6f49c47e4926dadc4804be2a1`;
  los 287 módulos y sus importaciones por worker están verificados en [installed](installed/).
- [Spill forzado](spill-feedback2.txt.gz): 37 correctas, mismos frames nativos,
  ambos motores/tres dialectos, archivos realmente creados y eliminados. Incluye
  cierre mientras se abre el output, antes de arrancar su delegado.
- [SDK-free](sdk-free.txt): 144 observaciones sobre Python 3.13.15, wheel instalado
  y `find_spec('bson') is None`. Funciones de observación y opciones conservadas;
  raw BSON se omite expresamente porque requiere la dependencia opcional.
- [API](api.txt): 18 correctas/13 subtests. [Typing](typing-installed.txt): positivos
  y negativos públicos/de frontera correctos. [Ratchet](lint-final.txt): 16 archivos
  Python. [Coste](cost.txt): dos pruebas/dos subtests deterministas; no acepta los
  presupuestos ni las matrices globales de D05.
- [Fixture](fixture-guard-final.txt): 47 correctas. El directorio se reclama de
  forma exclusiva antes de Docker y un manifest previo no se sobrescribe. La
  segunda comprobación nativa utiliza ese guard y referencias en solo lectura.

## Conservación y recursos

El [ledger de recursos](resources.json) prueba por ID la ausencia de 93 recursos
Docker de 31 manifests conservados; no quedan contenedores con el label del
fixture ni volúmenes anónimos creados desde el inicio del corte.

Tres manifests diagnósticos iniciales se sobrescribieron antes de añadir el
guard. Sus IDs originales no se pueden reconstruir. El inventario global es
prueba independiente de ausencia residual, no recuperación del ledger perdido.
Las capturas frozen y guard-check sí conservan manifests distintos y pruebas
por ID. Esta limitación no se oculta ni se atribuye a los goldens protegidos.

Siguen pendientes sobres/DTO, resultado bulk y otras combinaciones de codecs y
callbacks, contextos D06, disposición H53/D07 y aceptación integrada D05. Watch
conserva su contrato replica set; standalone no acredita ese bloque. No se
ha ejecutado CI remota por este corte.

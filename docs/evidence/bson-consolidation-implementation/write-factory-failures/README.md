# B07: fallos de fábrica, respuesta atómica y efectos confirmados

Corte funcional aceptado en `a8a81f0e`, base `1887ff80`. El
[receipt](receipt.json), las [fuentes](runtime-sources.json) y la
[wheel final](wheel.json) fijan la aceptación. La baseline instalada conserva
los 287 módulos de `05b3f655`; sus [fuentes](baseline-runtime-sources.json) y
[wheel](baseline-wheel.json) permanecen identificadas.

## Causa y refactorización

FindAndModify directo materializaba únicamente el documento devuelto. Omitía
las construcciones/asignaciones de `lastErrorObject/value/ok` que el SDK realiza
antes de devolver `value`. Después colocaba el documento público en otro
FindAndModifyOutcome: su copia defensiva podía repetir setters ya ejecutados.
La ruta servidor también aplicaba codecs antes de construir la respuesta,
aunque la decodificación correspondía al cliente.

- API y comando comparten `find_and_modify_response`, que proyecta el resultado
  atómico existente al DTO existente. La ruta servidor conserva valores sin
  codecs del usuario hasta entregar la respuesta.
- Se retiran los dos métodos privados `_find_one_and_update_outcome` y
  `_find_one_and_replace_outcome` de AsyncCollection, sin consumidores reales
  externos según el censo. La API llama al ejecutor ya existente y materializa
  después; el snapshot defensivo del resultado interno permanece. No se cambia
  SPI, el formato persistido ni el evaluador de mutaciones/proyecciones.
- `materialize_find_modify_response` conserva la política selectiva: con
  TypeRegistry crea el contenedor exterior, conserva metadata documental raw
  y decodifica el cuerpo al seleccionar `value`. Los callbacks de metadata de
  esa decodificación se ejecutan aunque se descarte su resultado transformado.
  Sin registry decodifica el cuerpo completo. El error sin `value` conserva
  su propio camino, fase y efectos de fábrica.
- Se reutilizan BSON.encode/decode y RawBSONDocument públicos del SDK, el
  materializador y el codec portable existentes. No se llama a APIs privadas
  del SDK. El perfil sin SDK comparte la recursión del codec: construcción y
  setter raíz propagan el error original; fallos al decodificar un elemento
  embebido pertenecen a InvalidBSON. No se envuelve todo error indiscriminadamente.
- Database.command diferencia opciones explícitas de opciones heredadas del
  Database. El corpus incluye ambos controles: heredar document_class no activa
  por sí solo la fábrica; `codec_options=` explícito sí lo hace. Los writes
  convencionales siguen ignorando fábricas/TypeDecoders de respuestas.

La reducción de capas conserva las decisiones de cada frontera: productor
atómico, DTO, selección de respuesta y conversión pública. La decodificación
no se convierte en una nueva validación de escritura ni un motivo de rollback.

## Diferenciales nativos

| Corpus | Topología | Acciones / observaciones por versión | Superficies |
| --- | --- | --- | --- |
| write_factory | Standalone | 2 / 672 | Directa sync/async y wire sync/async |
| write_factory_event | Replica set | 1 / 8 | Directa sync/async; watch wire conserva su exclusión vigente |

MongoDB 7.0.43, 8.0.32 y 9.0.2, con las imágenes/fixtures existentes. Las 24
rutas incluyen writes individuales/bulk, comandos con codec explícito/default,
findAndModify update/replace/delete, ausencia, upsert antes/después y rechazo.
Construcción en posiciones 0–4 y control correcto; setters de campos del sobre,
metadata y valor; con/sin registry. Se comparan tipo/resultado, familia/código/
mensaje, eventos de fábrica/decoder, objetos parcialmente construidos y estado.

La sonda RS añade barrera confirmada y comprueba que update/replace/delete
publican su evento una vez aunque falle la decodificación posterior. El write
rechazado no publica evento; no se infiere ausencia de un timeout. Reutiliza
el observador de eventos protegido, incluido contenido BSON y namespace
relacional. No acepta por asociación los callbacks de respuestas distribuidas:
su traza de construcción sigue usando el oráculo standalone.

Las [comprobaciones nativas](native/) coinciden exactamente en resultados,
corpus y manifiestos. Los campos estables de BuildInfo coinciden también.
`hello` y, en RS, `$clusterTime`/`operationTime` de BuildInfo se conservan como
metadata variable; no se afirma igualdad byte a byte. Cada ronda conserva
manifest y claim distintos. Los scripts de captura archivados son historia;
para comprobar referencias congeladas se usa `--check` y una salida distinta.

## Gates sobre la fuente final

- [Baseline instalada](baseline-combined.txt.gz): 36 fallos y 30 correctas con
  los testigos finales. [Focal standalone checkout](feedback.txt.gz): 51
  correctas antes de completar la rama portable; es feedback, no aceptación final.
- [Replay instalado final](replay-frozen.txt.gz): **10.825 correctas**, incluidos
  los 66 checks nuevos de ambas topologías y procedencia.
- [Cohorte instalada](unit-frozen.txt.gz): **2.457 correctas y 4.709 subtests**.
  [Identidad integrada](identity-integration.txt.gz): **12.068 correctas**.
- Wheel SHA-256 `d22c180abb5e4f0dbb9a54de9dc658b2333fc47f1ce21b836f0b8228f1cf097f`,
  287 módulos sin drift; [pruebas de importación por worker](installed/).
- [SDK-free](sdk-free-frozen.txt): 3.564 observaciones en Python 3.13.13 con
  `-I -S`, wheel instalado y ausencia comprobada de bson. Los cuerpos de los
  observadores no cambian. Se omiten 468 observaciones que exponen al callback
  la clase SDK RawBSONDocument, no disponible sin esa dependencia; no se fabrica
  un sustituto que prometa su transporte raw. No hay especímenes Code.
- Regresión portable en la misma wheel: [batching](sdk-free-batches.txt) 144,
  [DTOs](sdk-free-dtos.txt) 570 y [proyecciones](sdk-free-projections.txt) 1.488
  observaciones. Conservan sus omisiones previas y funciones de observación.
- [API](api.txt): 18 correctas/13 subtests; [typing](typing-installed.txt) y
  [ratchet](lint.txt) correctos. [419 archivos protegidos](protected.json)
  intactos; ninguna referencia histórica se modifica para aceptar la corrección.
- [Coste](cost.json): tres ejecuciones alternadas baseline/candidato, 300
  operaciones por motor/perfil y mismo checksum/estado. Medianas CPU entre
  0,888 y 1,028 en plain/factory/registry, con STANDARD explícito. Es diagnóstico
  acotado; no acepta los presupuestos integrados D05.
- [Recursos](resources.json): 36 recursos Docker ausentes por ID, doce manifests
  y sus doce claims retenidos, sin sobrescrituras.

Esto acredita los fallos de constructor/setter previstos en la unidad 5 B07,
junto con sus cortes previos de proyección, UUID, batching y DTOs. La unidad 6
conserva aceptación integrada en D05. Permanecen D06 (contextos, artefactos
preparados y otras transformaciones de respuesta), H53/D07 y las matrices,
cobertura, persistencia y costes de cierre. No se ha ejecutado CI remota.

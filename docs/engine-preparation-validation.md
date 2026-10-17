# Preparación interna: implementación T1–T6

## Alcance y autorización

El encargo del usuario de 2026-10-17 autoriza ejecutar T1–T6 de la
[propuesta](architecture/engine-preparation-proposal.md), conservando API, SPI v2
y archivos SQLite. No autoriza publicación ni una implementación de SPI v3.
El registro ADR-018 conserva su estado documental; no se simula una transición
formal como requisito de desarrollo que el proyecto no exige.

Base: 4.10.1, `738fd0eb5ece0adabf071e312ee0ff7b82b8589d`.
La documentación y evidencia previas forman parte del árbol inicial y se conservan.

## Roadmap de implementación

| Bloque | Estado | Evidencia que permite cerrarlo |
| --- | --- | --- |
| T1/T2 | Cerrado en el candidato fijado | Propagación dialecto/bindings corregida, normalización compartida y selección por identidad de un único productor; regresiones y manifiesto |
| T3 | Cerrado en el candidato fijado | Caracterización diferida, batches, rewind/clone; snapshots, cancelación y plazos existentes |
| T4 | Cerrado en el candidato fijado | Adapter tipado para planning/explain, sesión ligada, fallback mínimo y profiler best-effort |
| T5 | Cerrado en el candidato fijado | SQLite reutiliza semántica local, 2N→N sin cambiar SPI ni CAS; fallback de helper conservado |
| T6 | Cerrado en el candidato fijado | Corpus nuevo, captura real 7/8/9, replay sync/async Memory/SQLite direct/wire, wheel, SQLite histórico y global final |

Los fallos iniciales y resultados del candidato constan en
[validation.json](evidence/engine-preparation/validation.json). La evidencia
anterior es una línea base; no sustituye la verificación de este candidato.
El diff de T1–T6 y su documentación están revisados. Su implementación técnica
queda cerrada en la base y el candidato delimitados; el árbol combinado conserva
el límite indicado más adelante. No queda una transición formal obligatoria ni
una publicación incluida en este encargo.

## Cambios y garantías conservadas

- Las conversiones sin contexto heredan el dialecto de la operación y distinguen
  bindings ausentes de un mapping vacío explícito. Los casos ligados siguen
  usando su `OperationContext`; no recapturan `$$NOW` ni sesión.
- Update/delete construyen juntos filtro de identidad y plan. Se mantiene el
  selector original para su comprobación bajo lock. Las copias defensivas y
  los overrides legítimos de v2 siguen disponibles.
- Planning y explain pasan por métodos tipados del adapter existente. No se
  exige planner al motor mínimo y el profiler conserva sus resultados vacíos
  cuando no hay planner o este falla.
- SQLite pasa la semántica ya preparada a su helper privado para el mismo
  intento. El acceso directo al helper conserva su fallback de compilación.
  Los casos N=1/3 pasan de 2N a N preparaciones de selector; Memory conserva N.
  Esta cuenta no implica una mejora del 50 % en latencia.
- No se crean requests públicas, relojes, cachés globales, selector de versión,
  migración SQLite ni protocolo SPI v3.

## Corpus nativo y regresiones

Se añaden 16 escenarios a los 1.072 de errores existentes. Incluyen let/NOW,
lectura y count, update/replacement/pipeline, upsert con seed e ID null,
delete/proyección, no-match, errores previos a efectos y lotes ordered/unordered.
El observador conserva resultados, documentos de error, códigos, contadores,
estado tras cada intento y una escritura de recuperación. El replay cubre
sync/async, Memory/SQLite y clientes directos/wire; el último comprueba reutilizar
una sola conexión física de aplicación.

| Servidor real | FCV | SDK | Nuevos / existentes | Resultado nativo |
| --- | --- | --- | --- | --- |
| MongoDB 7.0.43 | 7.0 | PyMongo 4.19.0 | 16 / 1.072 | Sync/async equivalentes; referencias existentes sin delta |
| MongoDB 8.0.32 | 8.0 | PyMongo 4.19.0 | 16 / 1.072 | Sync/async equivalentes; referencias existentes sin delta |
| MongoDB 9.0.2 | 9.0 | PyMongo 4.19.0 | 16 / 1.072 | Sync/async equivalentes; referencias existentes sin delta |

Las capturas usan PyMongo sin importar Mongoeco. Cada cliente nativo reutiliza
una conexión para el corpus que captura. Las referencias nuevas fijan los hashes
de sus fuentes; las normalizaciones del observador existente se conservan.
La garantía es este corpus finito, no compatibilidad universal.

La fixture SQLite nueva procede del wheel oficial publicado 4.10.1, cuyo hash
se verifica contra todos los archivos instalados. Junto con la fixture 4.5,
prueba BSON, índices, búsqueda, outbox pendiente, checkpoint, reapertura,
coordinación de conexiones y rechazo de un schema futuro sin mutar el archivo.

## Identidad del candidato y aislamiento de la verificación

El wheel local de desarrollo conserva el número 4.10.1; no es una publicación.
SHA-256: `5f6c56ae3290c8fb334402b2613a612d2b00905f99c0196d8f1ac080153efc2e`.
Se instala en entornos Python 3.13/3.14/3.15 y se comparan los 289 archivos del
runtime con las fuentes del candidato.

Durante la global aparecieron cambios concurrentes ajenos en el kit pytest y
el proxy, incluidos módulos de diagnóstico. La primera ejecución quedó
invalidada: la suite descubría módulos de fuentes posteriores ausentes del
wheel ya fijado. Se conservan esos fallos y sus logs. Se repite la verificación
en una copia de la base indicada más los 20 archivos de código, pruebas,
herramientas y fixtures de T1–T6. Sus 289 archivos de runtime coinciden byte a
byte con el wheel. No se modifica ni absorbe el trabajo concurrente.

La evidencia de esta copia acredita T1–T6 sobre esa base. La integración del
árbol compartido con los cambios concurrentes requiere su propia global y no
se presenta como verificada aquí.

## Gates del candidato instalado

Todos los gates se ejecutan contra el wheel instalado y la copia de fuentes
fijada. `MONGOECO_TEST_INSTALLED_ARTIFACT=1` impide que pytest tome el runtime
del checkout. El registro JSON conserva comandos, versiones, hashes y límites.

| Gate | Resultado |
| --- | --- |
| Focales T1–T6 y SQLite | 253 pruebas correctas |
| Unittest completo | 3.628 pruebas; 3 omisiones previstas; sin fallos |
| Propiedades, perfil deep | 9 pruebas; hasta 300 ejemplos por prueba |
| PyMongo 4.9.2 | 293 pruebas: corpus nuevo, corpus wire existente y kit pytest |
| Conformance completa | 10 checks correctos por motor; canario independiente 5 correctos/5 no aplicables |
| API y tipado | Manifiesto idéntico; 1 caso positivo/20 errores esperados; mypy strict en 5 módulos |
| Lint | Ratchet correcto en 15 archivos Python modificados; baseline sin ampliar |
| Packaging | Wheel instalado: recursos/CXP/collation y smoke del kit correctos; wheel y sdist con 289 archivos de runtime idénticos a las fuentes |
| Pytest completo 3.13/3.14/3.15 | 44.229 correctas, 31 omitidas y 14.459 subtests por versión; sin fallos |
| Cobertura instalada, Python 3.14 | 99,01 %, por encima del mínimo del 99 % |

Las pruebas del kit ejercitan ambos motores, clientes síncronos/asíncronos,
xdist con 2/4 workers, fallos y cancelación. No se identifican consumidores ni
se cambian otros repositorios. Las 253 focales incluyen los trece checks de
compatibilidad SQLite.

## Incidencias retenidas

- Las regresiones dieron nueve fallos antes de corregir el runtime: dialectos
  8/9 en conversiones, bindings vacíos y preparación SQLite duplicada.
- Capturar los tres servidores en paralelo agotó archivos abiertos del host
  para MongoDB 7/8. Se repitieron secuencialmente, limpiando la base temporal
  cada 64 casos y conservando el cliente/conexión. No se aumentaron límites
  del sistema ni presupuestos del runtime. Los servidores propios se cerraron.
- Dos referencias iniciales quedaron con hashes anteriores al formateo del
  corpus; el control rechazó su replay. Se recapturaron contra las fuentes
  finales, sin ajustar observaciones para conseguir paridad.
- Faltaban pytest-cov y mypy en un entorno de prueba. Los intentos fallidos se
  conservan; tras provisionar las herramientas se repiten los gates afectados.
- La revisión detectó que la primera consolidación usaba truthiness para un
  mapping de bindings. Se restauró la distinción explícita con `None`, se
  añadieron seis regresiones de construcción ligada find/update/aggregate y se
  reemplazó el candidato. Sus ejecuciones incompletas se conservaron como
  evidencia superada; las globales finales usan el wheel corregido.

## Rendimiento y complejidad

La [medición reproducible](evidence/engine-preparation/benchmark.py) compara el
wheel oficial 4.10.1 y el candidato instalado, con Python 3.14.5 y el mismo
conjunto de dependencias. Usa siete procesos nuevos por artefacto, orden alterno
y calentamiento igual. Hay 36 celdas: find/count/update, N=1/100/1.000, ambas
fachadas y ambos motores. El tiempo usa `perf_counter_ns` sin tracing; otra
invocación mide pico de asignaciones Python con tracemalloc, **no RSS**. Se
comprueban también los efectos de la mutación fuera del tramo medido.

Los [datos completos](evidence/engine-preparation/benchmark.json) incluyen cada
muestra, mediana e IQR. Cuatro medianas de tiempo superan el +5 %, con IQR
solapado en los cuatro casos; ningún pico mediano de asignaciones supera el
+5 %. No se acredita una regresión temporal reproducible ni una mejora general
de latencia. La carga de fondo del host no está controlada. Los conteos de
preparación, medidos en regresiones separadas, sí acreditan 2N→N en SQLite.

El cambio productivo afecta nueve archivos: reutiliza contexto/DTO/adapter,
añade tres métodos al adapter existente y dos productores privados, y comparte
la normalización. No añade tipos públicos, copias, capturas de reloj, cachés,
versiones de protocolo ni migraciones. No elimina guardas de frontera o la
revalidación por candidato. La mejora de mantenimiento procede de concentrar
esas rutas y eliminar la segunda preparación, no de reducir el número neto de
líneas.

## Simplificación residual y posible v3

V2 sigue teniendo DTO con campos paralelos, overrides explícitos y dos métodos
de planning. Se conservan porque pertenecen a su contrato. El selector original
también se prepara por candidato para revalidarlo bajo lock; eliminar esa
preparación exigiría un contrato que transporte el programa correspondiente.

T1–T6 reducen duplicación interna sin añadir una segunda jerarquía de requests.
Una v3 futura necesita acreditar una garantía para autores de motores que no
pueda darse con v2, su coste de transición y un plan explícito de retirada.
Este cierre no acredita esa necesidad ni inicia una entrega 5.0. Tras completar
también S1/S2, la [decisión vigente](architecture/engine-spi-v3-proposal.md)
mantiene v2 y aparca v3; este registro conserva la evidencia de T1–T6.

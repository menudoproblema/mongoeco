# Preparación de Mongoeco 4.9.0: MongoDB 9.0 / PyMongo 4.18

Estado vigente: **recuperación de la entrega 4.9.0 tras la primera CI**. Contrato autorizado:
objetivo attachment `4896c5fd-4c6a-4dde-bb8e-f048d2d5c82b`, catorce bloques.
Este registro acredita el cierre técnico anterior a los commits. El encargo
posterior autoriza consolidar la release, commits semánticos y la etiqueta
local `v4.9.0`; su estado se recoge en [release-4.9.0.md](release-4.9.0.md).
La publicación sigue siendo posterior. Este registro conserva los antecedentes
inferiores; sus cierres y artefactos 4.8.1 no acreditan 4.9.0.

## Recuperación de la entrega

La etiqueta original `v4.9.0` apuntó a
`d85c4b7de313028b709cde2fce4cb89dae80d513` (objeto anotado
`6454a429ebeda70aa7b4a5a074962ce91f437513`). La
[primera CI](https://github.com/menudoproblema/mongoeco/actions/runs/37809790060)
pasó build, perfiles PyMongo, imports mínimos y Python 3.13/3.14. Los 29
casos obligatorios de paridad pasaron en cada servidor. La recaptura posterior
falló en dos errores de `$densify.range.step` en MongoDB 8/9: los builds
nativos enumeraban los mismos cuatro tipos numéricos en distinto orden.
La publicación quedó omitida por los gates; no se publicó este candidato.

La corrección compara exclusivamente esa enumeración sin orden. Conserva
tipos aceptados y su multiplicidad, tipo recibido, campo, formato completo y
el resto del error. Las pruebas negativas rechazan cambios de código,
codeName, labels, tipo recibido, tipos ausentes/extra/duplicados, otros campos
y texto adicional. Los goldens nativos permanecen intactos. Las seis capturas
descargadas de la CI para 8/9 (review, deltas y garantías semánticas) pasan la
comparación corregida; esa reutilización no sustituye recapturar las cuatro
familias, incluidos los cuatro casos de índices que la primera CI no alcanzó.

El job de contrato publicado de `main` intentaba instalar `4.9.0` desde PyPI
porque elegía el último tag Git. Ahora selecciona una vez la versión publicada
mediante metadata oficial de PyPI; su smoke limpio pasó con `4.8.1`.
La publicación requiere explícitamente el job de perfiles PyMongo, además de
build, imports mínimos, tests y matriz diferencial. No se eliminan gates ni
se convierten errores de red o servidores en éxitos.

La versión y el código del paquete permanecen en `4.9.0`. La CI corregida y
los artefactos finales siguen pendientes antes de sustituir la etiqueta
compartida y publicar. Los recibos del candidato original se conservan como
antecedentes; la evidencia nueva se guarda por separado en
`docs/evidence/mongoeco-4.9.0/ci-recovery/`.

Base efectiva: `aa626c51fea8a055510f89a2acf2f8ed1ce33761`; última release
`v4.8.1` (`ad90d846`). Workset inicial: 777 archivos, manifest SHA-256
`b99e89a7219c2f54abc2ab8f6f053e62b9078e18d789a5418ff9cca0681dab93`.
Identidades, copia del diff y censo fresco en `/private/tmp/mongoeco-490-20261008`.
Se conserva íntegramente la evidencia anterior.

## Plan vigente y aceptación

| Bloque | Requisitos del objetivo | Evidencia de cierre | Estado |
| --- | --- | --- | --- |
| A. Base, impacto y cobertura efectiva | 1–3 | Base/workset, owners actuales, matriz caso/garantía/runtime/FCV/SDK/perfil/engine/ruta/oráculo y consumidores efectivos; runner estricto | Cerrado: 852 filas; 431 caracterizaciones excluidas de paridad; capturas frescas y consumidores trazados. |
| B. Familias semánticas y fronteras | 4–7 | Capturas nativas y regresiones de densify, fechas, variables, preparación e índices; contexto completo, recursos, orden de errores y cleanup en todas las rutas anunciadas | Cerrado: familias corregidas con oráculos nativos, ambos engines/fachadas y rutas declaradas. |
| C. Propiedades | 8 | Generación/shrinking de multiplicidad, contenido, rangos, fechas y preparación/invalidation; ci y deep | Cerrado: nueve propiedades; perfiles ci/deep verdes, con shrinking. |
| D. Rendimiento | 9 | Paquete 4.8.1 identificado, mismo harness/dataset/deps; warmup y ≥5 rep; stream/materializado/spill/índices/cursores/preparación; comparación de resultados y planes sin relajar comparador | Cerrado: wheel publicado 4.8.1, 14 tareas comparables y medida streaming adicional; cinco repeticiones o más, planes/resultados iguales y límites explícitos. |
| E. Consumo real | 10 | Wheels aislados del provider sync y testkit async/failpoints, consumo gdynamics-testing, Memory/SQLite, configuración antigua y 9/4.18; resolución completa y cambios efímeros trazados | Cerrado: provider 21 por configuración, testkit 135 por configuración y gdt 39; pins/adaptaciones efímeros registrados. |
| F. Versión y documentación | 11–12 | Fuentes/derivados 4.9.0, changelog, defaults/límites/correcciones/adopción; deuda documental depurada y exclusiones conservadas | Cerrado: versión, derivados, changelog, defaults, límites y adopción alineados. |
| G. Candidato único | 13–14 | Todos los gates sobre el mismo código/deps/corpus/wheel/sdist 4.9.0; hashes reproducibles, recursos/avisos investigados, CI/documentación coherentes | Cerrado: gates del wheel final, cobertura real 99,0094427 %, builds reproducibles y sdist final limpio. |

El contrato del encargo y ADR-001/002/004/009/015/017 satisfacen la entrada.
No existe una transición RFC adicional exigida por Mongoeco. Defaults 7.0/4.9,
wire independiente, Engine SPI v2, search-v1, CXP y persistencia se conservan.
No se elimina validación directa de convert/group, SRV, deltas merge ni flags
semánticamente distintas. No se añaden las capacidades excluidas del bloque 12.

Los checkouts primarios de Cosecha y gdynamics-testing difieren de sus `main`.
Se ensayaron snapshots owner inmutables en copias temporales. Los repositorios
owner conservan HEAD y estado inicial. Sus pins, revisiones y cambios efímeros
quedan en la evidencia. La antigua revisión cutover del testkit con pin 4.6.0
no representa al owner vigente.

## Contraste crítico de los informes

Los informes revisan un workset anterior; sus recuentos y la cobertura citada
no acreditan este candidato. El primer informe declara el límite de su muestra;
el segundo llama «exhaustiva» a una revisión que no prueba las simplificaciones
propuestas. La ausencia de un fallo en suites locales no demuestra paridad.

| Idea agrupada | Decisión y garantía exigida |
| --- | --- |
| Densify común y correcciones 7/8 | Aceptada. Preservar originales, contenido y multiplicidad; bounds explícitos semiabiertos. Oráculo real por versión, sort explícito o multiconjunto, nunca conjuntos. |
| Ventanas fuera del rango Python | Corregir la comparación interna; no convertir un éxito nativo en OperationFailure por un límite de datetime. |
| Validación general y preparación | Política general independiente de arrays; namespace y contexto completos. Reutilización solo con contexto equivalente e invalidación demostrada. |
| Quitar guards convert/group | Rechazada: las entradas directas y físicas sin preparar siguen necesitando validación. Contar llamadas antes de retirar trabajo. |
| Agrupar todas las flags | Rechazada como regla general: coincidencia de valores en versiones actuales no prueba equivalencia de contratos. Conservar deltas observables y exports. |
| Quitar SRV u orden merge | Rechazada: utilidad estimada no deroga contratos. La supuesta pérdida TXT fue descartada; conservar precedencia URI/TXT y orden acreditado por versión. |
| Normalizar toda collation en engines | Rechazada sin equivalencia sobre storage compartido/SPI/metadata. Adaptación acotada en API; built-in id conserva su identidad. |
| WildcardProjection y snapshots | Límite de Mongoeco explícito; aceptación histórica 7/8 y rechazo previo al lote en 9. Snapshots actuales y deltas estructurados, historia intacta. |
| Evidencia y rendimiento | Captura con consumidor concreto y exclusiones. Wheel publicado 4.8.1, harness/deps iguales, warmup y cinco repeticiones; mismos resultados y planes. |

### Checkpoints intermedios conservados

El bloque siguiente conserva los hallazgos y fallos intermedios. Sus indicaciones
«Sigue» están sustituidas por el cierre final de 4.9.0 situado después; no son
pendientes vigentes ni evidencia del artefacto definitivo.

✅ Feedback focal: 2.729 pruebas pasan (`semantic-final-focused.log` en
`/private/tmp/mongoeco-490-20261008`); no es el gate global del artefacto final.
Incluye 54 casos semánticos nativos por servidor, 4 casos nuevos de índices,
ambos engines/fachadas, perfiles 4.9/4.18 donde se declara y propiedades ci.
La [matriz vigente](evidence/mongoeco-4.9.0/capture-coverage.json) tiene 852 filas:
431 quedan explícitamente como caracterización sin paridad ejercitada.

Capturas adicionales distinguen missing/null y particiones compuestas en
`$densify`; nombres y paths de variables conservan sus errores y precedencia.
Para `bounds: full` con un único float enorme o infinito, 7 conserva el original;
8/9 fallan con 5897900 cuando el step no avanza. Ese delta tiene flag y capturas
por versión; no se generaliza la salida de 7 a 8/9. Mongoeco detecta el bloqueo
numérico antes de agotar el límite de generación nativo.

El warning de GC del antecedente se investigó con trazas de creación del loop.
Su origen probado es una prueba que creaba un cliente sync sin cerrarlo;
se añadió cleanup explícito. Trece pruebas focales pasan sin el warning.
→ Sigue: comprobación global de recursos sobre el candidato final, sin filtros.

El contraste contra el wheel **publicado** 4.8.1 encontró una regresión de
trabajo en el dialecto por defecto: con `let`, 4.9.0 preparaba cuatro stages y
hacía 14 llamadas de copia frente a dos y siete. La frontera del cursor ahora
prepara con los bindings explícitos de su contexto de ejecución, incluidos los
heredados. No elimina namespace, scopes ni invalidación. El feedback ampliado
pasa 2.945 pruebas; el artefacto `e9610640…` y sus medidas quedan como diagnóstico
anterior y deben sustituirse por un wheel corregido y sus gates.

El gate source completo anterior pasa 8.738 tests / 30 skips esperados sin
warning de recursos, pero su cobertura **98,999884 %** falla el umbral real de
99 %. Se añadieron comprobaciones de `unique` mal tipado en la adaptación del
built-in y del rechazo standalone en `find($expr)`, sin retirar exclusiones ni
bajar el umbral. → Sigue: cobertura del wheel corregido y suites completas.

El testkit bruto reproduce una misma aserción obsoleta en 4.8.1 y 4.9.0
(134 passed / 1 failed). Adaptando esa aserción en ambas copias efímeras al
`provider_target.dependency_contract_ref` vigente, conserva la comprobación de
identidad y pasa 135 pruebas. Con SDK 4.18.2, default y selección 9/4.18 pasan
135 tests del testkit y 21 del provider. Gdynamics-testing pasa 39 pruebas
representativas desde su paquete instalado. Sus pins/adaptaciones quedan
trazados fuera de los repositorios owner; deben repetirse con el wheel corregido.
Forzar SQLite en la fábrica Memory del testkit sin abrir conexión falla los
mismos nueve casos con ambas versiones: ensayo de una combinación que esa
fábrica no ofrece, sin claim de soporte. El provider sí prueba sus rutas SQLite.
Los owners no incluyen `py.typed`; el canario estricto pasa analizando sus
anotaciones instaladas con `--follow-untyped-imports`, sin inventar stubs.

→ Sigue: propiedades deep, artefactos finales reproducibles, suites globales,
consumo real del wheel y comparación de rendimiento. Las mediciones y wheels
provisionales quedan invalidados por cambios posteriores; no son evidencia final.
La aserción owner contra `DependencyExportV1.contract_ref` se contrastó con
4.8.1: el fallo es preexistente, como acreditan los logs bruto y adaptado.
Ninguna declaración de cierre anterior se aplica al candidato 4.9.0.

## Cierre final de 4.9.0 — 8 de octubre de 2026

La última revisión detectó un keyword nuevo obligatorio para callbacks antiguos
del execute_request_pipeline público. El candidato final conserva esa firma,
asigna identidad a los command events sin cambiar lease/plan/request_id y no
reintenta un TypeError interno del callback. La firma se inspecciona una vez,
antes de adquirir recursos; un callable no inspeccionable conserva la llamada
histórica. Siete regresiones de identidad y 92 pruebas focales del driver pasan.
Los gates funcionales, consumidores, instalaciones y capturas se repitieron.

La [validación consolidada](evidence/mongoeco-4.9.0/validation.json) vincula
los gates, muestras, adaptaciones y hashes al wheel definitivo. La
[matriz vigente](evidence/mongoeco-4.9.0/capture-coverage.json) relaciona 852
observaciones nativas con consumidores y exclusiones; 431 siguen siendo
caracterización y no se cuentan como paridad. Las fixtures anteriores conservan
sus hashes. Los snapshots actuales se contrastan con 31 deltas estructurados.

| Gate local | Resultado del candidato |
| --- | --- |
| Pytest instalado Python 3.13.15 / 3.14.5 | 8.803 passed, 30 skips esperados y 3.990 subtests en cada versión; sin warning de recursos. |
| Cobertura real | 42.780 de 43.208 líneas: 99,0094426958 %; 428 sin cubrir, 882 excluidas. No se añadieron exclusiones ni se bajó el umbral. |
| Unittest / propiedades | 3.596 tests, dos skips esperados; nueve propiedades ci y deep. |
| Lint / typing / contratos | Ratchet contra v4.8.1 en 78 archivos, baseline intacto; typing positivo/negativo, exports, manifest y snapshots source/wheel verdes. |
| Conformance | Memory 10/10, SQLite 10/10; canario externo cinco pass y cinco no aplicables, sin fallos ni errores. |
| MongoDB nativo 7.0.43 / 8.0.32 / 9.0.2 | 29 casos de paridad por versión y 284 recapturas por versión; FCV estable exacta, SDK 4.18.2, cero omisiones inesperadas. |
| Perfiles PyMongo | 4.9.2, 4.11.3, 4.13.2, 4.17.0 y 4.18.2 aislados; snapshot exacto, ningún resultado indeterminado. |
| Consumo instalado | Provider 21 default + 21 explícito; testkit 135 + 135; gdynamics-testing 39; canario de anotaciones y resolución completa compatibles. |
| Persistencia / mínimo / installs limpios | SQLite 4.5: seis tests en copias, original intacto; entorno mínimo sin PyMongo/BSON/orjson/Hypothesis; wheel y sdist resueltos públicamente sin constraints internas. |
| Build / CI | Dos builds idénticos, twine verde; tres YAML, 75 bloques shell y cuatro Python validados. Workflows alojados no ejecutados aquí. |

El wheel final contiene 273 archivos de paquete idénticos al source corregido.
Los dos builds son reproducibles; wheel y sdist se instalaron en nuevos entornos
vacíos con resolución completa y sin constraints internas. El paquete instalado
desde sdist coincide byte a byte con el wheel. Los artefactos provisionales
`e9610640…` y `e6d60d98…` no se entregan como distribución final.

La [reutilización causal de rendimiento](evidence/mongoeco-4.9.0/performance-causal-reuse.json)
conserva la identidad del wheel medido `e6d60d98…`: 272 módulos permanecen
idénticos y solo cambia driver/execution.py. Una ejecución adicional del
candidato final a tamaño 20.000 bloquea cualquier llamada a ese módulo y
verifica que los cinco workloads de ambas engines no lo ejecutan. Preparación,
convert, índices y streaming se verifican por separado con el mismo guard.
Se reutilizan las muestras del mismo código efectivo; esa comprobación sin
comparación temporizada no sustituye los warmups ni las cinco repeticiones.

La recaptura compara éxitos completos y errores/manifiestos/corpus. Normaliza
únicamente el namespace temporal, los UUID del prefijo de construcción del
índice y su duración de scan incidental. Preserva código, labels, causa,
phase, número de registros y valores del documento. Pruebas negativas impiden
ampliar esa normalización. Las tres instancias propias se cerraron tras comprobar
que no quedaban bases de prueba y que sus dbPath eran los autorizados.

### Rendimiento y garantías de simplificación

La [comparación completa](evidence/mongoeco-4.9.0/performance-final-summary.json)
usa el wheel publicado 4.8.1, Python 3.14.5, 74 dependencias iguales aparte de
Mongoeco, harness/dataset idénticos, 20.000 documentos, warmup uno y cinco
repeticiones por tarea. El comparador existente y toda la metadata de resultados
y planes coinciden en las 14 tareas principales. Las medias wall medidas bajan
entre 15,1–29,5 % en Memory y 0,9–8,8 % en SQLite. CPU y RSS completos se conservan.

El workload principal llamado streamable no activa batches; una medida adicional
consume 20.000 documentos con batch_size=128 y acredita seis aperturas de stream
por paquete/engine (warmup y cinco ejecuciones), con planes y multiconjuntos
completos iguales. La primera ronda mostró +20/+36 %; las rondas inversa y
alternada dieron −1/−9 % y −11/−1 %. Se conservan todas las muestras, incluidas
las positivas. Las medianas agrupadas de 15 repeticiones fueron 265/254 ms
(Memory, baseline/candidato) y 319/331 ms (SQLite); CPU SQLite, 300/300 ms.
El perfilado diagnóstico mantiene 160 llamadas de preparación, 317 parses y
790 copias en ambos paquetes. No hay evidencia de una regresión estable
atribuible al cambio; tampoco se promete ausencia global de regresiones.

Las medidas deterministas de 1.000 documentos conservan dos parses y siete
copias para el pipeline con let en 7 y 9; las pruebas de reutilización también
acreditan 8. El candidato provisional duplicaba
ese trabajo en el default y se corrigió antes de entregar. En 9 se conserva
una validación estática convert y 1.000 validaciones directas, también en find;
los contadores del helper no existen en 4.8.1 y no se interpretan como cero.
La adaptación de índices en 9 conserva una lectura de catálogo por recreación
y tres para un lote de tres. El default no añade lecturas. Las micro-medidas
incluyen deltas positivos y negativos; la recreación Memory mediana pasa de
62 a 82 microsegundos, con resolución insuficiente para una promesa general.
No se retiraron guards, contexto ni autoridad de invalidación para mejorar tiempos.

Se observó realmente un spool group por engine, tanto en baseline como candidato,
con 20.000 documentos y threshold 10.000; no quedaron temporales. El muestreo
RSS a 5 ms puede perder transitorios, y ru_maxrss adicional es acumulativo:
ninguna medida establece un límite de memoria. Carga del host, caches y orden
pueden influir en los tiempos; las muestras no prueban aceleración causal.

### Adopción y artefactos

La [adopción trazada](evidence/mongoeco-4.9.0/consumer-adoption-final.json)
conserva revisiones owner, hashes y cambios exactos. Provider y testkit main
fijan 4.8.0; sus copias ensayadas fijan 4.9.0. Mochuelo fija 4.17.0 y solo su
copia eleva el SDK a 4.18.2 para el ensayo. La aserción CXP preexistente se
adapta en baseline y candidato conservando identidad. La selección 9/4.18 se
ensaya mediante inyección de constructores: los owners aún necesitan exponerla
en su configuración. Provider acredita sus engines Memory/SQLite; la fábrica
actual del testkit solo ofrece Memory. El ensayo forzado SQLite no es soporte
positivo. Ningún repositorio owner fue modificado.

Artefactos del cierre técnico anterior a los commits, conservados en
`dist/4.9.0-candidate/`. La preparación Git posterior genera distribuciones
del commit de release en `dist/4.9.0-release/`; sus identidades se registran
por separado y no sustituyen retrospectivamente estos hashes:

- Wheel SHA-256: `4d96655423e1bd83448f100514567a0c303dd3b5f1bd58fc490dc06a57022bdf`.
- Sdist SHA-256: `21e77eea54c09e21422d31a24acc03338bbc7aaffe6dcfff1d5f2dfe59a7a0e0`.

No queda trabajo requerido del encargo sin verificar. La adopción en owners,
la ejecución alojada y la publicación son acciones posteriores fuera de su
alcance. Los límites de metadata nativa, wildcard, conversiones aplazadas,
search-v2 y demás capacidades excluidas permanecen explícitos.

## Antecedente cerrado el 8 de octubre de 2026

El texto siguiente registra el alcance anterior de siete bloques y sus
artefactos de trabajo 4.8.1. Sus garantías correctamente implementadas se
reutilizan por causalidad, pero no sustituye los requisitos adicionales ni
la validación del candidato 4.9.0. Los límites de caracterización de índices,
propiedades, consumo real y rendimiento se revisan conforme al nuevo contrato.

### Registro del alcance anterior

Contrato autorizado: objetivo del usuario en `goal-objective.md`, attachment
`0decfad5-92c6-43f9-b714-3c0645e1b2c6`. Se implementan sus siete bloques de
principio a fin; commit, tag y publicación quedan fuera del encargo.

Base Git: `aa626c51fea8a055510f89a2acf2f8ed1ce33761`. El workset inicial
incluye el candidato previo, con SHA-256
`a21650b5dd417ec0b34d416d82899942bad44e1db402fb9962c46572da321c03`;
el diff inicial tiene SHA-256
`0780ca9f74800540ff9ad2b802cc1eb4718d2f2098d28ab959c95ebdef4e64bb`.
Se conservan sus archivos, capturas y
[validación anterior](evidence/mongodb9-pymongo418/validation.json).
Baseline de código y paquete de impacto temporales:
`/private/tmp/mongoeco-improvements-20261008`.

El encargo satisface el gate de entrada. ADR-002/009/015/017 siguen vigentes;
no hay AGENTS adicional ni transición RFC obligatoria localizada. Los defaults,
SPI v2, search-v1, CXP y formato persistente se conservan. La lista siguiente
separa trabajo técnico de publicación y no reduce el objetivo a los tests
actualmente verdes.

| Bloque | Hecho que permite cerrarlo | Estado |
| --- | --- | --- |
| 1. Evidencia activa | Matriz caso/dialecto/superficie/ruta y exclusiones; capturas reales nuevas con runtime/FCV; fixtures anteriores conservadas | Implementado y probado: 678 filas clasificadas y 72 casos nuevos por servidor. |
| 2. Densify | Semántica común sin pérdida de originales, delta real de bounds iguales y pruebas de vacíos/particiones/duplicados/fechas/deadlines/anidados/out/merge | Implementado y probado. `$merge` activo; `$out` y bounds `partition` continúan fuera del subset. |
| 3. Fechas y variables | Ventanas válidas fuera del rango Python correctas; offsets inválidos y nombres vacíos con clasificación real; literales y scopes intactos | Implementado y probado. Comparación entera interna, sin cambio persistente. |
| 4. Preparación | Validación general independiente de arrays/namespace; contexto completo y fast-path equivalentes, con let/joins/facet/comandos/wire/invalidation | Implementado y probado en API, comando y wire. |
| 5. Índices y derivados | Capturas de creación/recreación/opciones/metadata/lotes, adaptación de collation acreditada, nota wildcard honesta y snapshots estructurados con contratos históricos | Implementado y probado dentro del alcance documentado; diferencias históricas de `_id_` explícitas. |
| 6. Simplificación acreditada | Medidas antes/después; menos trabajo redundante sin retirar guards directos, SRV ni orden merge; introspección conservada | Implementado y medido; resultados y coste adicional de índices descritos abajo. |
| 7. Candidato final | Gates globales ligados a código, dependencias, corpus y wheel/sdist corregidos, documentación y diff revisados | Cerrado: todos los gates locales requeridos pasan; evidencia del candidato enlazada abajo. |

Gates finitos de entrega: suites Python 3.13/3.14 y unittest; property ci/deep;
cobertura real ≥99%; lint ratchet contra última etiqueta sin ampliar baseline;
typing/manifest/exports source y wheel; conformance Memory/SQLite/canario y
consumidor externo; diferenciales reales 7/8/9 sin omisiones y nuevos casos;
SQLite 4.5 sin mutar fixture; entorno mínimo; wheel/sdist limpios sin constraints
internas; dos builds reproducibles y hashes; CI/YAML y documentación alineados.

Los 709 tests focales de la revisión previa y su captura puntual de MongoDB 9
son antecedentes. No sustituyen la aceptación del candidato corregido.
No se retirará `validate_convert_spec` de `find($expr)` ni los guards de group
en entradas físicas sin prueba de preparación. Las correcciones de densify
7/8 se documentarán como cambios intencionales de fidelidad al servidor.

## Decisiones derivadas de la revisión

Los dos informes coinciden en exigir menos trabajo redundante y evidencia más
precisa. No todas sus propuestas conservaban las garantías:

- **Fidelidad observable.** Se corrigen la pérdida de originales en densify 7/8,
  el extremo superior, los vacíos y las ventanas válidas fuera del rango Python.
  Traducir automáticamente un overflow de Python a error MongoDB habría
  convertido éxitos reales en fallos: se comparan coordenadas temporales enteras.
  Los offsets inválidos conservan los códigos nativos 9, 5166406 y 5976500.
- **Fronteras de validación.** La validación general se independiza de arrays y
  del namespace. Se prepara con colección en la frontera pública y se reutiliza
  solo con contexto equivalente, incluyendo scopes, direcciones y registro.
  Se conservan los guards de conversión directa y las cuatro entradas de group.
- **Índices y almacenamiento.** La caracterización descarta que el engine local
  no listase `_id_` en una colección nueva. Se conserva su contrato histórico y
  se reconoce su diferencia frente al servidor. La adaptación simple de 9.0 se
  hace simétrica sin canonicalizar globalmente None/simple ni modificar SPI.
  Metadata nativa `v` y unicidad de `_id_` no se anuncian como paridad plena.
- **Contratos que deben mantenerse.** Las pruebas de precedencia SRV descartan
  la supuesta sobrescritura de URI por TXT. Se conserva el adaptador público,
  los guards directos, las flags exportadas y el orden merge real por dialecto.
  El rechazo de wildcardProjection se identifica como límite del producto.
- **Evidencia mantenible.** Snapshots actuales estructurados y 28 deltas
  históricos explícitos sustituyen la cirugía de texto. Las fixtures anteriores
  siguen verificándose semánticamente. CI recaptura los 72 casos nuevos y compara
  resultados, errores, manifiestos y corpus; solo normaliza el UUID del namespace
  en mensajes de error. La matriz distingue 437 capturas todavía sin paridad
  ejercitada de las comparaciones completas y parciales restantes.
  «Resultado completo» compara todos los valores bajo la normalización BSON
  existente; no acredita identidad de wrappers numéricos ni orden de mappings.
  Los errores se comparan por tipo, código, codeName y labels, sin exigir texto
  idéntico a Mongoeco. El orden de campos y la metadata tienen filas específicas.

## Coste y simplificación

La medición usa 250 documentos, ambas engines y 20 repeticiones. El pipeline de
dos stages pasa de cuatro validaciones a dos y de 14 llamadas de copia a siete.
La conversión pasa de dos validaciones estáticas a una; mantiene 250 validaciones
directas, necesarias para `find($expr)` y otras entradas sin preparación.

La recreación con collation simple explícita mantiene una lectura de índices;
un lote de tres mantiene tres. La recreación implícita de 9.0 añade una lectura
para conservar la definición ya almacenada cuando antes se creó simple explícita.
Ese coste evita un conflicto incorrecto y conserva el contrato compartido entre
dialectos, sesiones y SQLite. No se introduce caché sin una autoridad SPI que
garantice su invalidación. Los tiempos recogidos son diagnósticos locales, sin
calibración: la reducción de recorridos no acredita una mejora de throughput.

La revisión final añadió el paso enorme de fechas en densify: se conserva un
éxito real sin construir un timedelta que exceda el rango Python. También se
estabilizó una prueba heredada de timeout SQLite con un reloj controlado que
avanza durante la extracción SQL. Exige interrupción antes de procesar todos
los documentos y repite la consulta filtrada sin límite para acreditar que
el progress handler se retiró. No se aumentó el plazo ni se retiró una aserción.

## Cierre del candidato

[Validación y hashes](evidence/mongodb9-pymongo418-improvements/validation.json)
ligan cada gate al paquete, corpus, dependencias y workset finales. La evidencia
anterior permanece idéntica byte a byte. Commit, tag y publicación no se ejecutan.

| Gate | Resultado |
| --- | --- |
| Pytest Python 3.13 / 3.14, wheel instalado | 6.056 passed, 30 skips esperados y 3.990 subtests en cada versión. |
| Cobertura Python 3.14 | 42.664 / 43.092 statements = 99,00677619975866%; umbral 99% conservado. |
| Unittest discovery | 3.596 tests, 2 skips esperados, OK. |
| Property ci / deep | 4 passed por perfil; 30 / 300 ejemplos máximos. |
| Lint ratchet | 68 ficheros Python modificados; base v4.8.1; baseline sin ampliar. |
| Typing, manifest y exports | Source y wheel vigentes; exports iguales a los snapshots actuales; deltas históricos comprobados. |
| Conformance | Memory 10, SQLite 10 y consumidor externo 5 passed / 5 N/A por capacidades; cero failed/error. |
| MongoDB real | 29 casos estrictos por versión, cero omisiones; 72 recapturas por versión coinciden con sus expectativas. |
| PyMongo real | 4.9.2, 4.11.3, 4.13.2, 4.17.0 y 4.18.2; resumen coincide con la fixture; cero indeterminate. |
| SQLite 4.5 | 6 passed; fixture conserva SHA-256 e671484cd736c42eb3c4e11dbc5ca2f5564c35072f0e1a5fcc4dded172400efe. |
| Packaging | Wheel / sdist limpios sin constraints; imports mínimos sin PyMongo; dos builds idénticos y twine OK. |
| Benchmarks | Smoke de 5 engines y 4 workloads; medición secuencial de preparación/conversión/índices. Sin claim de throughput. |
| CI y documentación | 3 YAML, 74 scripts shell y 4 bloques Python parseados; gates nuevos incluidos. Los workflows alojados no se ejecutaron aquí. |

Las suites conservan un warning heredado de GC de un event loop; sus logs no
lo filtran. Las primeras ejecuciones paralelas compartieron TMPDIR y produjeron
interferencias en los tests de limpieza. Las repeticiones con temporales propios
pasan y se conserva el diagnóstico. El fallo intermitente de la prueba heredada
de timeout se corrigió mediante reloj controlado y una comprobación de limpieza
más exigente, sin modificar el comportamiento productivo de SQLite.

Artefactos reproducibles del candidato, versión de trabajo 4.8.1:

- Wheel: `6528f8ab8e4a8848b4f65a248786b18d52e6936bf65a17ced11da5895cd4a9c8`.
- Sdist: `a71f8253765ac2d51b457147dea3faca8776a55a2f6f0a02467b9b4b1d47739f`.

Los defaults 7.0 / 4.9, wire 20, dependencia PyMongo opcional, SPI v2,
search-v1, exchange CXP y almacenamiento se conservan. Las fronteras de `_id_`,
metadata nativa y capacidades aplazadas siguen expresamente fuera de la
paridad acreditada; 437 filas de la matriz son caracterizaciones sin paridad
ejercitada. No quedan bloques técnicos ni gates locales pendientes del encargo.

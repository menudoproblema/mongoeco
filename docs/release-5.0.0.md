# Candidato Mongoeco 5.0.0

Preparación iniciada el 2 de noviembre de 2026. No publicada, sin tag ni push.
Base publicada: v4.11.0. Guía: [migración](migrating-to-5.0.md).

Major por el cambio público del default UUID y la retirada de expresiones
anunciadas previamente. Incluye los cierres BSON, predicados, escrituras, fechas,
cursores, Search, comandos, parte I F00–F11 y los incrementos PCRE2/granularity/CXP.
Resto de parte II excluido. No se reabren los candidatos congelados de F11.

## Gates de preparación

R1 versión, narrativa y migración; R2 dos builds reproducibles y artefacto
inmutable; R3 tiempos comparativos aislados; R4 globales/cobertura/deep/matriz
contra wheel instalado; R5 consumidores, smokes, typing/SPI/SQLite/nativo;
R6 revisión final, evidencia y árbol limpio. Cada gate se registra cuando termina.
La aprobación para etiquetar o publicar es posterior y no forma parte del encargo.

## Preparación histórica anterior a Q00–Q06

Las secciones siguientes, hasta el incremento Q00–Q06, conservan la preparación
del artefacto anterior. El candidato aceptado actual y el plan de lanzamiento
figuran al final. No se deben usar sus hashes históricos para publicar.

### Protocolo de tiempos, fijado antes de medir

Comparar v4.11.0 con este candidato usando el harness actual sin modificar,
Python 3.14 y las mismas dependencias, datasets y máquina. Cuatro engines:
memory-sync, memory-async, sqlite-sync, sqlite-async. Tamaño 1000, calentamiento 1,
cinco repeticiones por tarea. Workloads: secondary_lookup_indexed,
cursor_consumption, simple_aggregation, sort_limit, search_diagnostics y
vector_search_diagnostics. Mantener JSON v2, hashes de resultados y skip sets.

Una secuencia A1→B→A2 (base/candidato/base), sin otras cargas propias. Presupuesto
máximo 20 minutos total; no repetir automáticamente. Errores o diferencias
semánticas conservan los datos y bloquean la atribución de ratios de esa tarea
hasta explicar su causa. No se eliminan muestras. A1/A2 caracteriza la variación
observada, no constituye un intervalo de confianza. Sin umbral porcentual
calibrado no se afirma aceptación estadística ni rendimiento universal. Si el
presupuesto se agota o el entorno no permite distinguir, se registra inconcluyente.
No se reinicia ni se añaden intentos al gate F11 histórico. Las capacidades sin
equivalente en 4.11 usan su evidencia focal aceptada, no un ratio inventado.

### Estado

R1–R6 registrados y verificados localmente. R3 conserva su resultado acotado
y A2 SQLite async inconcluyente; R5 usa el corpus sustitutivo autorizado.
Integración en main y limpieza propias realizadas. No publicada.

### Comparación de tiempos registrada

Datos completos y comandos: [comparación](evidence/mongoeco-5.0.0/benchmark-comparison.json)
y archivos comprimidos de cada tanda. Los 104 resultados base/candidato
conservan sus hashes y el contrato de comparación v2. No es una aceptación
estadística ni una promesa de ausencia de regresiones.

| Motor | Tareas | Mediana de ratios B/A1 | Rango de ratios | Segunda base |
| --- | ---: | ---: | ---: | --- |
| Memory sync | 26 | 1,350× | 1,037–2,277× | Completa |
| Memory async | 26 | 1,286× | 0,995–2,535× | Completa |
| SQLite sync | 26 | 1,461× | 0,880–11,638× | Completa |
| SQLite async | 26 | 1,336× | 0,948–11,678× | Inconcluyente: presupuesto agotado |

Son medianas de ratios por tarea, no un factor global de rendimiento de una
aplicación. Hay sobrecostes observados, no sólo ruido: el caso Search compound
should top-k pasa en SQLite sync de medianas 0,4304/0,4339 s (A1/A2) a 5,0090 s.
El plan anterior usaba postings de texto como presencia exacta y podaba de 1000
a 500 candidatos. El actual rechaza esa prueba de presencia —los postings omiten
null, arrays vacíos y objetos— y desactiva esa poda top-k insegura. Su diagnóstico
y `_sqlite_leaf_candidate_storage_keys` explican el cambio de estrategia; no se
atribuye cuantitativamente todo el sobrecoste a una sola función sin perfil.
No se restaura la optimización incorrecta para mejorar estas cifras.

La primera secuencia empleó un entorno reutilizado con USearch 2.25.3, inferior
al mínimo 2.26.4 declarado. No acredita la release y se conserva separadamente.
La corrección material fue crear entornos limpios constrained, validar pip check,
versiones e imports aislados, y ejecutar una secuencia nueva con USearch 2.26.4.
Ambas secuencias respetaron el máximo de 20 minutos. La segunda terminó A1 y B
en los cuatro motores y A2 en tres; no se reinicia la fase inconcluyente.
El presupuesto y resultado de F11 histórico no se alteran.

### Consumidor autocontenido de esta preparación

La fuente privada del canario histórico no se ha localizado por fingerprint ni
por inventario en los checkouts disponibles. Tras autorización expresa para
buscarlo o prepararlo, esta preparación usa
[`pytest_business_canary.py`](../tests/consumers/pytest_business_canary.py):
31 escenarios async de cuentas, pedidos y saldo, y siete de construcción SDK.
No reconstruye ni acredita la ejecución de las aserciones privadas históricas.
Su código queda conservado para evitar depender de una fuente irrecuperable.

El runner existente `run_pytest_consumer_canary.py` sustituye sólo `open_client`,
verifica el wheel y los hashes antes/después, y exige inventario íntegro sin skips.
Los 38 casos pasan contra Memory y SQLite en Python 3.13/3.14/3.15 y workers 0/2/4:
684 ejecuciones. SHA-256 de fuente:
`2e013f063ec8bbd0974f92db16e2f78a0506ee7257546d2ceeeb8c7475a3e61b`.
El límite histórico se conserva; el nuevo corpus no promete compatibilidad de
una aplicación privada no disponible.

### Artefacto y garantías locales verificadas

Runtime de distribución: `7dcb9df2087c4e8d0cad091bfdce1ad9d45c3699`.
Los cambios posteriores sólo afectan a tests, CI, documentación y evidencia;
los 310 archivos empaquetados siguen siendo idénticos. Las dos construcciones
con `SOURCE_DATE_EPOCH=1793647280` producen los mismos bytes:

- Wheel: `2ed1bd93385d759d7a7a61ebc7409ab74c544ba7d8cf58590f56405325974d2f`.
- Sdist: `4347a1ab421511c9dcbb9ac997ded0dcbb2d396697a3327538482a6569a0f927`.

Copias locales: `dist/release-5.0.0/`. Twine, fuentes y metadatos comprobados;
318 entradas versionadas del sdist conservan sus bytes. Los doce entornos
constrained de matriz contienen los mismos 310 archivos del wheel.

| Verificación | Evidencia aceptada |
| --- | --- |
| Pytest completo por Python 3.13/3.14/3.15 | 64.998 passed, 1.131 skips previstos y 14.946 subtests por perfil |
| Unittest por intérprete | 3.684 tests, OK, tres skips |
| Cobertura instalada, 289 módulos | 48.446/48.935 líneas: 99,000715 %, 489 sin cubrir |
| Matriz pytest 8.4.2/9.1.1 × PyMongo 4.9.2/4.19.0 × tres Python | Doce celdas, 3.587 passed por celda |
| Propiedades profundas | 66 passed |
| PCRE2 opcional por intérprete | Cinco tests focales y 339 contrastes con capturas MongoDB 7/8/9 |
| Consumidor preparado para esta release | 684 ejecuciones; fuente conservada, ningún skip |
| Captura nativa de revisión | 72 casos por MongoDB 7/8/9, fixtures sin delta; recursos propios cerrados |
| SDK sin extras y perfiles PyMongo | 14 tests en instalación mínima; seis perfiles SDK sin delta |
| Typing/API/SPI/snapshots | Gates conformes; comparación de snapshots actuales exacta |
| Packaging y conformance | Smokes wheel/sdist/pytest-kit y Memory/SQLite/engine externo conformes; conformance desde directorio ajeno |
| SQLite histórico | Fixtures 4.5/4.10 cubiertas por globales: BSON, índices, Search/outbox y hash conservado |
| Ruff ratchet | Conforme respecto a v4.11.0; sin ampliar baseline |

La primera cobertura global midió 98,9476 %. Faltaba observar el adaptador
PCRE2 opcional, ejecutado sólo en subprocess. Se conserva esa medición y se
completa con un test directo del adaptador nativo y los oráculos existentes,
acumulando las trazas sobre el mismo runtime instalado. El umbral sigue en
99,00 %. CI instala el extra únicamente en su celda de cobertura. No se usa
instrumentación general de subprocess: su prueba recogía copias de fixtures y
contaminaba el denominador; esa evidencia se conserva como no válida para
cobertura. No se ha rebajado ni desactivado ningún gate de cobertura.

El entorno reutilizado inicial contenía una instalación antigua física que los
subprocess aislados podían importar; se sustituyó por instalaciones limpias y
se conservan sus fallos como diagnóstico, sin acreditarlos para esta release.
Las dependencias soportadas, los comandos y los resultados están en
[los logs](evidence/mongoeco-5.0.0/validation-logs.tar.gz).

La integración en `main` incluye `fc9e8cea` y `bb72d17c`; las ramas propias
`fix/cxp-catalog-v2` y `release/5.0.0` se han retirado tras el fast-forward.
El worktree temporal de la base comparativa se retiró sin forzar. Se mantienen
los worktrees ajenos con commits no integrados y los candidatos congelados.
Se revisaron el diff final, whitespace y la coherencia documental. No hay
cambios de runtime desde la identidad probada y no se repiten sus suites.

### Límites y pasos posteriores de lanzamiento

La evidencia local corresponde a macOS arm64. No acredita los jobs alojados
de CI ni la configuración actual de Trusted Publisher en PyPI: comprobarlos
antes de autorizar el tag, con el protocolo del checklist. No hay autorización
para publicar, crear tags ni hacer push en esta preparación. La versión sigue
sin publicarse.

Los tiempos comparativos tienen los límites descritos arriba, incluido A2
SQLite async inconcluyente y el sobrecoste Search observado. No son el gate F11
histórico ni permiten afirmar ausencia de regresiones de coste. El canario nuevo
no demuestra la compatibilidad de la aplicación privada histórica no disponible.

Registro estructurado: [validación](evidence/mongoeco-5.0.0/validation.json).
[Logs completos de matriz](evidence/mongoeco-5.0.0/matrix-logs.tar.gz).

## Incremento Q00–Q06 aceptado e integrado (3 de noviembre)

El [plan de rendimiento](performance-5.0.0-plan.md) posee el alcance y cierre de
este incremento. Los registros anteriores de esta página corresponden a la
preparación histórica del artefacto previo; conservan sus identidades y resultados.
El nuevo runtime b5d9e803, empaquetado desde 95fa38c5, tiene wheel SHA-256
`5ac014b1643e5230d1e894828859f372b8e8870a4337dbe91c5c92044d45bb76` y sdist
`3ab93460dde32dc770e5f93b1ea0435762edf4bbf0f49e73a11fea270c78157d`.
Los dos builds son idénticos byte a byte. Los tres Python pasan pytest/unittest,
typing/API/SPI y smokes; la cobertura exacta es 99,0009806 %, con matriz 12/12,
deep y consumidores comprobados. Se conserva el canario sustitutivo autorizado,
que no acredita la aplicación privada histórica no disponible.

El único intento temporal completó veinte invocaciones en 844,9 s: 72 mejoras
observadas, 30 inconcluyentes y dos empeoramientos Memory async (+5,82 % y
+6,26 % de mediana en los dos shapes compound documentados). El usuario acepta
expresamente estos dos tradeoffs; la sonda causal de 44 consultas no identificó
su causa y no es una nueva aceptación temporal. No se promete ausencia de
regresiones ni mejora universal. [Datos y disposición](performance-5.0.0-plan.md#q06--aceptación-expresa-del-coste-y-gate-de-integración).

Q05 se difiere sin cambio productivo porque la reutilización del cierre idle
altera rewind; no se acredita ahorro de tránsitos. El incremento preserva los
cierres históricos y no añade el resto de la parte II. Integrado en main por
fast-forward hasta 5c4e0767, con perf/q00-q06 retirada.
Diff y documentación revisados; workset propio limpio, conservando
`benchmarks/execution.json` preexistente sin cambios. No hay tag, push ni publicación.


## Plan acotado de cierre antes del lanzamiento (3 de noviembre)

**Estado: preparación local cerrada; lanzamiento todavía no autorizado.**
Este apartado posee las obligaciones de prelanzamiento. No reabre Q00–Q06,
O1–O8, D05, P2 ni F11; no amplía la parte II. No requiere nuevos benchmarks,
perfiles, refactorizaciones ni canarios. Las dos reservas de rendimiento y los
30 inconcluyentes conservan su aceptación explícita. El canario sustitutivo
sigue siendo el aprobado, sin atribuirle la compatibilidad privada histórica.

### Estado externo comprobado en modo lectura

- Main local revisado: `ac282e0483c54f6b213c06159fb89354f0190b24`.
  Main remoto: `b38ef96764ce4a976883005ade83f82f7b3ef547`, antecesor del local,
  con 251 commits de diferencia antes de este plan documental. La API de rama
  indica main sin protección. No hay CI alojada
  del candidato Q06. La [última CI ordinaria](https://github.com/menudoproblema/mongoeco/actions/runs/39003582187)
  corresponde al SHA remoto antiguo, no a 5.0.0.
- El [último diferencial programado](https://github.com/menudoproblema/mongoeco/actions/runs/41445674695)
  terminó en failure sobre ese SHA antiguo. No demuestra una regresión del
  candidato actual ni sustituye su gate pendiente.
- La [API oficial de PyPI](https://pypi.org/pypi/mongoeco/json) devuelve 4.11.0
  como versión actual y no contiene 5.0.0. Tampoco existe tag local v5.0.0.
  Revalidar disponibilidad inmediatamente antes de una futura autorización.
- El entorno GitHub `pypi` existe, con `protection_rules: []` y sin política
  de ramas de despliegue. Eso **no acredita** el Trusted Publisher de PyPI.
  Su configuración privada no se ha comprobado en esta revisión.

### Huecos operativos observados

1. `dist/release-5.0.0/` contiene aún wheel 2ed1bd93… y sdist 4347a1ab…,
   la pareja anterior al incremento. El wheel Q06 aceptado es 5ac014b1… y su
   sdist 3ab93460…, hoy conservados en el temporal indicado por
   [el registro](evidence/mongoeco-5.0.0/q06-preflight.json). Ambos existen y
   sus hashes coinciden; los 318 inputs versionados de ese sdist siguen
   idénticos en el árbol actual. No publicar por nombre de archivo o por usar
   ese directorio histórico.
2. CI sólo llama al diferencial con el wheel cuando la ref es un tag. El
   workflow diferencial manual permite seleccionar casos, pero instala desde
   fuente: su input `distribution_artifact` sólo existe en `workflow_call`.
   Por tanto, el dispatch manual actual no cierra el gate del wheel pretag.
3. `publish` descarga el artefacto del mismo run y no reconstruye, pero no
   descarga ni comprueba `SHA256SUMS` como los jobs test/pytest-kit/differential.
   Conviene completar esa última comprobación con el mecanismo existente.
4. La cobertura de CI se presenta con precisión de dos decimales. El cierre
   local comprobó el cociente exacto, que debe conservarse también en CI:
   un 99,00 % redondeado no basta. El test adicional de bytearray ya está en main;
   no se reabre la corrección ni se rebaja el umbral.
5. Crear v5.0.0 activa el job de publicación cuando sus dependencias pasan.
   Como el entorno no tiene protección, **el tag no sirve como ensayo sin
   publicación**. Antes de autorizarlo debe estar verificado el publisher y
   debe existir autorización de publicación; si se quiere separar ambos actos,
   configurar antes aprobación manual en el entorno `pypi`.

### Bloques, dependencias y cierre observable

| Bloque | Prioridad / depende de | Trabajo acotado | Criterio de cierre |
| --- | --- | --- | --- |
| L00 — Identidad y conservación | P0 / ninguno | Conservar la pareja Q06 en una ubicación duradera distinta del directorio histórico, con hashes. Revisar versión 5.0.0 y fecha/narrativa antes de fijar el futuro SHA S. Si se ajusta una entrada empaquetada, declarar cuál y qué gate invalida | Pareja Q06 íntegra y recuperable sin depender sólo de un temporal; ninguna confusión con 2ed1bd93; inventario de payload/metadata que se reutiliza |
| L01 — Completar el circuito existente de CI | P0 / L00 | Cambios sólo en `.github/workflows/ci.yml`: ejecutar `release-differential` también en push a main, manteniendo build + `distribution_artifact`; comprobar manifiesto en publish; generar JSON de cobertura y exigir el cociente exacto ≥99 con el cálculo ya usado en Q06 | Diff mínimo revisado, YAML/expresiones válidos, sin nuevo runner/workflow; diferencial del wheel alcanzable antes del tag; publish sigue excluido en ramas; ninguna relajación de cobertura |
| L02 — CI alojada del candidato definitivo | P0 / L01 | Fijar S y su SOURCE_DATE_EPOCH; sincronizar main por fast-forward normal, sin tags, en la ejecución que se autorice. Consumir una sola ejecución CI de S, incluidos los tres Python, matriz de doce celdas, SDK/core mínimo, smokes y MongoDB 7/8/9 con las particiones/corpus existentes | Jobs obligatorios verdes sobre S, sin selección vacía ni skips obligatorios; build doble reproducible; todos los consumidores usan el wheel de ese run y su manifiesto; cobertura exacta ≥99 y diferencial real verde |
| L03 — Publisher y disponibilidad | P0 / paralelo a L01–L02 | Comprobar en PyPI Publishing la tupla `menudoproblema / mongoeco / ci.yml / pypi`; revisar permisos OIDC ya declarados, acceso del mantenedor y disponibilidad de 5.0.0. No crear publisher ni cambiar protecciones sin autorización para esas acciones | Configuración real coincidente, con evidencia fechada; 5.0.0 libre; explícito si un tag autorizará publicar automáticamente o si habrá aprobación del entorno |
| L04 — Decisión de lanzamiento | P0 / L02 + L03 | Descargar la pareja y SHA256SUMS del run aprobado a un destino final distinto de los artefactos históricos; registrar SHA S, run, hashes y reservas. Verificar árbol propio limpio y referencias congeladas; presentar el resultado | Ningún gate de prelanzamiento pendiente; artefactos finales recuperables; commit y manifiesto inequívocos. Terminar aquí, **sin tag, push de tags ni publicación** |

Los bloques anteriores son **planificados, no ejecutados**. En esta revisión sólo
se consultaron archivos/APIs y se comprobó identidad de los artefactos existentes;
no se cambió CI ni se copiaron/promovieron distribuciones. Tampoco se hizo push,
se dispararon workflows ni se ejecutaron suites o nuevas mediciones.

### Identidad de artefacto y reutilización de evidencia

CI obtiene SOURCE_DATE_EPOCH del commit que construye. Q06 usó 95fa38c5
(epoch 1793658789); ac282e04 tiene otro timestamp y S incluirá los cambios de
prelanzamiento. Por eso el SHA-256 del archivo de CI **puede diferir** del wheel
local 5ac014b1 aun con el mismo runtime. No prometer igualdad de archivos entre
revisiones distintas. Comparar los 310 archivos de paquete y la metadata, además
de los hashes de archivo. La pareja que se autorizará publicar será la **de CI
probada para S**, no la de un temporal ni la del directorio histórico.

Se reutilizan los cierres locales, capturas nativas sin cambios, historial
SQLite, tests de consumidores y aceptación del coste. La ejecución alojada es
el gate Linux todavía pendiente, no una repetición local por comodidad. Sus
smokes de benchmark comprueban ejecución funcional: no constituyen otra campaña
comparativa ni reabren R3/F11. Contract fuzzing programado no es una condición
adicional de publicación: deep ya forma parte del job test exigido.

Si falla un job, corregir sólo su causa y repetir los controles afectados;
reutilizar el build aprobado cuando el material no cambie. Si cambian runtime,
metadata empaquetada, tooling o fixtures, identificar los hashes y gates
invalidados antes de repetir. No trasladar al nuevo wheel la aceptación de otro
artefacto sin demostrar identidad del material pertinente.

El workflow actual no promueve un binario entre el run pretag y el run de tag:
el segundo vuelve a construir y probar. No se atribuye a ese mecanismo una
promoción por hash que no tiene. Antes de lanzar, elegir explícitamente el modo:
si se exige aprobar exactamente los hashes pretag, usar la aprobación del
entorno `pypi` para comparar ambos manifiestos antes de liberar publish; si se
mantiene publicación automática, la autorización futura de tag/publicación debe
alcanzar la pareja probada del run de tag para S. No sustituir esa decisión por
crear un tag de prueba. No hace falta añadir otro sistema de promoción ahora.

El futuro tag debe apuntar exactamente a S. Registrar resultados en los
artefactos/run de Actions existentes, sin cambiar después el commit elegido
sólo para añadir un recibo y provocar otra construcción. El pipeline de tag
volverá a ejecutar los gates que actualmente exige CI: esa ejecución no se
omite por el plan. El job publish debe consumir su pareja comprobada, sin
reconstruirla, y conservarse la vinculación con el manifiesto aprobado. La
retención actual de siete días exige descargar la evidencia antes de caducar;
si cambia el material no se reutiliza un manifest anterior.

No queda trabajo productivo conocido exigido para este cierre: quedan gates de
entrega externa y estas mejoras mínimas de su circuito de validación. Si aparece
un fallo del candidato en L02, pasa a ser una corrección obligatoria, con impacto
focal; no autoriza una auditoría general ni ampliar el alcance de la versión.


### Ejecución de L00–L04 iniciada el 3 de noviembre

El usuario autoriza ejecutar el plan con el modo de publicación más seguro,
conservando el límite sin tags/publicación. L00 comprobado: la pareja Q06 se
conserva también en `dist/release-5.0.0-q06/`, con SHA256SUMS verificado. El
directorio histórico `dist/release-5.0.0/` no se sobrescribe.

L01 implementado: diferencial del wheel también en push a main; publish sigue
siendo exclusivo de tags y ahora comprueba el manifiesto; cobertura JSON con
cociente exacto ≥99 y denominador no vacío, conservada como artifact. actionlint
1.7.12 del release oficial, checksum verificado, valida el workflow. El cálculo
inline acepta 48.459/48.948 y rechaza tanto 48.458/48.948 (99,00 redondeado) como
el denominador vacío. No cambian runtime, metadata empaquetada ni fixtures.

Protección GitHub L03 aplicada y reconsultada: `pypi` exige aprobación manual
del propietario `menudoproblema` (id 976187) y sólo permite deployments de tags
`v*`. Se permite que el propietario apruebe su propio job: es el control manual
de publicación del mantenedor, no una garantía de revisión por dos personas.
No se conceden nuevas credenciales ni se crean publishers. El futuro tag no
liberará automáticamente el paso protegido en el flujo normal.

La configuración privada del Trusted Publisher de PyPI sigue pendiente de
confirmación del propietario. El control del equipo no autorizó Firefox y el
usuario indica continuar por CLI; no se vuelve a intentar el navegador. La
protección GitHub no se presenta como prueba de la configuración PyPI.

L02 se acredita con la ejecución alojada del commit que contiene estos ajustes:
se conservan los artifacts/manifiestos/run de Actions y se descargan al destino
`dist/release-5.0.0-final/`. La evidencia terminal de ese run, con head SHA y
conclusiones, determina L02/L04; escribir un recibo no debe cambiar el candidato
ni provocar otra suite completa. La comprobación privada L03 permanece separada
hasta recibir su evidencia. Esta nota registra el inicio, no adelanta un verde.

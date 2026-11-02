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

## Protocolo de tiempos, fijado antes de medir

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

## Estado

R1–R6 registrados y verificados localmente. R3 conserva su resultado acotado
y A2 SQLite async inconcluyente; R5 usa el corpus sustitutivo autorizado.
Integración en main y limpieza propias realizadas. No publicada.

## Comparación de tiempos registrada

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

## Consumidor autocontenido de esta preparación

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

## Artefacto y garantías locales verificadas

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

## Límites y pasos posteriores de lanzamiento

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

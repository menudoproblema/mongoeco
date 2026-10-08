# Implementación de MongoDB 9.0 y PyMongo 4.18

Este registro describe el candidato anterior y conserva su evidencia. La
revisión posterior encontró huecos que se corrigen y verifican en
[el roadmap de mejoras](mongodb9-pymongo418-improvements-progress.md).
Los resultados y hashes siguientes no acreditan el workset corregido.

Contrato autorizado: encargo del usuario de implementar de principio a fin
[el plan](plan-mongodb9-pymongo418.md), conservando todas sus garantías.
Base Git: `aa626c51fea8a055510f89a2acf2f8ed1ce33761`. No hay instrucciones
`AGENTS.md` adicionales ni régimen RFC obligatorio localizado. ADR-002,
ADR-009, ADR-015 y ADR-017 siguen vigentes. El plan previo se conserva.

✅ Cerrado: el soporte autorizado está implementado y sus gates técnicos pasan.
El estado contractual continúa aprobado; el gate de entrada está satisfecho,
el corte funcional está cerrado y `phase_result` es `completed`.
La [guía de soporte](mongodb9-pymongo418.md) explica selección y límites.
El [registro de validación](evidence/mongodb9-pymongo418/validation.json)
identifica el candidato mediante hashes y conserva sus informes y logs.

| Bloque | Hecho necesario para cerrarlo | Estado y evidencia |
| --- | --- | --- |
| Instrumentos de prueba | Configuración rechazada nunca contada como aceptación; integración real sin verdes por vacío/skips/conexión/FCV incorrecta | Cerrado: clasificador estricto y pruebas del runner; 29 casos reales en cada servidor 7.0.43/8.0.32/9.0.2, cero skips. Matriz oficial aislada de cinco versiones completada, sin indeterminados. |
| Capturas reales 8.0→9.0 | Casos reproducibles de fechas, agregaciones, conversiones, índices y errores, con versiones/FCV efectivos | 154 casos por versión en 7.0.43/8.0.32/9.0.2, FCV efectiva exacta y PyMongo 4.18.2; fixtures reales `mongodb_version_deltas_*` conservan runtime, manifest y SHA-256 del corpus. |
| Perfil 4.18 | Normalización común, ConfigurationError, superficies y detección exacta verificadas | Implementado y verificado en 40 combinaciones de perfil/superficie/motor; detección patch exacta y defaults conservados. Snapshots históricos preservados. |
| Driver/BSON/wire | Identidad por ejecución/retry, cleanup, SRV público y buffers verificados | Cerrado: identidad y correlación, cancelación, resolver público oficial y límites de simulación. BSON/RawBSON y buffers verificados con ambos motores y PyMongo 4.18.2 real; revisión y suite global correctas. Codec y versión wire conservados. |
| Dialecto 9.0 | Semántica y rechazos explícitos, con paridad y oráculo real | Cerrado: catálogo, arrays, conversiones escalares/base y fronteras numéricas, fechas/ventanas, densify, group/trim, CLUSTER_TIME standalone, metadata de índices y orden de merge. Rutas comunes y validación temprana en entrada vacía/anidada. Diferencial real 29/29 y suite global correctos. Extensiones aplazadas rechazadas explícitamente. |
| Dependencias/CI | Pins, extra y lanes 7/8/9 ejecutables y aisladas | Cerrado: extra `mongodb9`, constraints 4.18.2 y lock; GitHub/GitLab con matriz aislada de perfiles, targets 7/8/9 por digest, comprobación efectiva de FCV y wheel de entrega. Configuración válida y gates equivalentes locales ejecutados. |
| Contratos/documentación | Capabilities, exports, typing, manifest y snapshots vigentes coherentes | Cerrado: exports, typing, manifest y catálogo vigentes alineados; snapshots históricos intactos. CSV de conservación, README, COMPATIBILITY, DIALECTS, changelog, guía y checklist actualizados. |
| Candidato de entrega | Checklist completo: suite/property/99 %/lint/typing/conformance/real/persistencia/artefactos reproducibles | Cerrado: todos los gates de la tabla siguiente correctos sobre el candidato final. Diff revisado, documentación alineada, sin cambios ajenos ni gates requeridos pendientes. |

Cada bloque reutiliza los mecanismos registrados en el plan. No hay tags,
publicación, cambios de SPI, `search-v1` ni migración del formato persistente
en este alcance. Los artefactos de infraestructura temporales se mantienen
en `/private/tmp/mongoeco-support-20261008`; la evidencia contractual necesaria
para reproducir resultados se conserva en el repositorio. Las tres instancias
temporales propias (29017/29018/29019) se cerraron después de verificar sus
dbPath; el registro de cleanup se conserva con la evidencia.

Capturas reproducibles: el runner exige versión y FCV exactas, crea solo bases
UUID de prueba y las elimina en `finally`. Las tres fixtures comparten el SHA-256
`b0a224e2177ee7cfc70f5c854d5fb3a0d55aba7af7d6e7e3082b1abeb2f050d5`
del corpus, sin expectativas obtenidas de Mongoeco. Binarios descargados de
`fastdl.mongodb.org`, verificados contra los `.sha256` oficiales:

- 7.0.43: `764137ddb0eada62eee1a23129e8c32cb197eb2d7f64b63cc89a56a96d7e3cc7`.
- 9.0.2: `9a30e942877445dc143d83671eba6cda2ec93a99160c8a8de57595853dc76de3`.

La matriz 4.18 confirma rechazos nuevos para `aggregate` y los helpers de
agregación; los argumentos Python duplicados continúan como `TypeError`.
Los buffers y el proxy no requieren cambios de producción ni aumentar
`maxWireVersion=20`. Los resultados globales del candidato final son:

| Gate | Resultado final |
| --- | --- |
| Pytest, Python 3.13.15 y 3.14.5 | 5.462 passed, 30 skipped y 3.990 subtests passed en cada entorno; imports desde `site-packages` del wheel final. |
| Cobertura, Python 3.14 | 42.606/43.033 sentencias: **99,007738 %** real; 99,01 % mostrado, supera el mínimo 99,00 %. |
| Unittest discovery | 3.596 tests, OK, 2 skips. |
| Property `ci` / `deep` | 4 pruebas por perfil; 30/300 ejemplos, respectivamente. |
| Lint ratchet | 57 archivos Python modificados, base `v4.8.1` derivada de `HEAD^`; baseline sin ampliación. |
| Typing PEP 561 | Source y wheel: contrato positivo y 14 errores negativos esperados. |
| Manifest y exports | Manifest vigente en source y wheel; exports actuales byte a byte iguales. Fixtures históricas intactas. |
| Conformance | Memory 10 passed; SQLite 10 passed; canario externo 5 passed y 5 not-applicable conforme a sus capacidades; cero failed/error. Consumidor SPI externo correcto. |
| Persistencia SQLite 4.5 | 6 pruebas sobre copias: lecturas/índices/BSON/Search y entrega idempotente del outbox. Fixture original byte a byte intacta; no requiere migración. |
| Entorno mínimo | Wheel final en Python 3.13 sin PyMongo: 11 pruebas de imports/exports correctas. |
| Diferenciales reales | 29/29 en cada servidor 7.0.43, 8.0.32 y 9.0.2; cero errores, fallos u omisiones; versión y FCV verificadas. |
| Matriz PyMongo | 4.9.2/4.11.3/4.13.2/4.17.0/4.18.2 aislados; resumen coincide con la fixture vigente. El timeout solo acredita aceptación de argumentos. |
| Instalación | Wheel y sdist en entornos limpios sin constraints de CI; smokes y conformance básica correctos. |
| Reproducibilidad | Dos checkouts y entornos de build independientes; wheel y sdist normalizado idénticos. `SOURCE_DATE_EPOCH=1790852290`; `twine check` correcto. |
| Benchmark | Informe `mongoeco-benchmark-report/v2`, cuatro motores/superficies y mongomock, cuatro workloads, tamaño 250. Smoke correcto; sin comparación acreditada de regresión al no disponer de baseline calibrada. |

Las 30 omisiones ordinarias corresponden a suites reales opcionales y clases
base heredadas. El diferencial obligatorio separado no omite ningún caso.
Las suites globales emiten una advertencia de GC de un event loop, conservada
en sus logs. Sus temporales se aíslan por proceso para evitar interferencias
en las aserciones existentes de archivos de spill. Los workflows alojados de
GitHub/GitLab no se han ejecutado desde esta sesión: la evidencia acredita
los gates locales y la validación de su configuración.

Las distribuciones se conservan en `/private/tmp/mongoeco-support-20261008/dist-first`:

- Wheel: `1ea246c26ed65a754ec157cc44aaf7c93e9db6baae27bbf4cf7643b9c1f649db`.
- Sdist: `a090c7a5abccaf0e149055f0549f89b2cf20864d15965a1d3df27f7d6f8c0c09`.

No queda trabajo técnico del alcance autorizado. Commit, tag, publicación y
adopción en otros repositorios no forman parte de este cierre.

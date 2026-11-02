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

R1 en preparación; R2–R6 pendientes. Los cierres de implementación son válidos,
pero no acreditan todavía las distribuciones versionadas 5.0.0 de esta release.

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

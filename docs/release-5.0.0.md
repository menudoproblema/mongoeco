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

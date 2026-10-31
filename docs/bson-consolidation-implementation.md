# Implementación de consolidación BSON

Contrato autorizado por el usuario: implementar el
[plan refinado](bson-semantics-consolidation-plan.md), revisando cada bloque y
commiteando semánticamente. Base: `eef9c09`. Estado: implementación en curso.
No cambia SPI v2 ni declara implementados los pilotos opcionales.

## D00 — Activo

Se reutiliza el runner/capturador/replay existente. El corpus
`tests/differential/semantic_binary_cases.py` añade selección por operadores,
eq/in, deduplicación pública por subtipo/contenido y no-op clásico/pipeline.
Sus observaciones son independientes del comparador y claves de producción.

Primera [caracterización anterior al arreglo](evidence/bson-consolidation-implementation/d00-initial/receipt.json):
MongoDB 9.0.2, FCV 9.0, standalone desechable; los tres actions difieren en
Memory y SQLite con dialecto 9.0. Se comprobaron selección, valores distinct
por contenido/subtipo y contadores no-op. Es evidencia inicial, todavía fuera
del registro/capturador/replay, no un cierre de bloque. El corpus usa 16 bytes
para subtipo 5: la ejecución inicial nativa rechazó su literal de expresión de
dos bytes, un rechazo que los motores locales no aplicaron. No se admite esa
ejecución fallida como referencia de igualdad; su caracterización de gramática
queda pendiente dentro del dominio Binary.

Pendientes antes de aceptar D03a: integrar el corpus en el registro, capturas
nativas 7/8/9, fallo previo del replay, propiedades generadas de coherencia en
fronteras reales y regresión pública. Un corpus preparado no cierra D00.

## Trabajo restante

- D03a: clasificación/vistas Binary, comparación, claves y guarda SQL.
- D01a/D01b: identidad BSON y consecuencias públicas.
- D02: milisegundos exactos, ventanas y TTL compatible.
- D03b: tipos/SQL bajo prueba de equivalencia.
- D06a/D06b/D06c: gramática, preservación BSON wire y consumidores DBRef.
- D04: preparación/contexto de watch, referencia con replica set.
- H53: caracterización y disposición de observabilidad conforme al plan.
- D07: piloto opcional con criterio de continuidad, sin migración obligatoria.
- D05: revisión y gates integrados, matrices y commits de cierre.

Ningún bloque funcional se declara cerrado todavía.

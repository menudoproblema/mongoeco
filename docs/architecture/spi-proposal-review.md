# Revisión agrupada de los tres informes de preparación/SPI

Fecha: 2026-10-17. Base productiva sin modificar: 4.10.1,
`738fd0eb5ece0adabf071e312ee0ff7b82b8589d`. Es revisión de autoría y nueva
propuesta, **no una auditoría independiente ni una aceptación** de ADR-018.

Se denominan R1 al primer informe adjunto (H1–H8), R2 al segundo (apartados A–G)
y R3 al informe incluido en el mensaje (F1–F4, sondas y observaciones P2).
No se publican identidades, enlaces ni documentos privados de sus fuentes.
Sus resultados declarados no se presentan como ejecuciones propias.

La salida es [A mínima](engine-preparation-proposal.md), con SPI v2 intacto.
Se preserva el [borrador original](../evidence/spi-v3-proposal/revision-1.json).

## Agrupación, contraste y decisión

| Grupo y aportaciones | Contraste propio | Disposición y destino |
| --- | --- | --- |
| G01 — Coste A/B y dualidad. R1 H3; R2 A; R3 F1 | B mantiene la compatibilidad A y añade frontera. La fórmula anterior no distingue coste de transición y estado final; no acredita beneficio neto | **Incorporar:** A mínima como objetivo; B fuera del alcance. No se concluye que cualquier sucesor sea imposible ni se declara v2 definitivo |
| G02 — Control mínimo y shapes. R1 H7a; R2 E; R3 F3 | Dos tipos están reexportados, pero FindOperation figura en el protocolo de planning. No se pueden cambiar libremente tres tipos solo por no estar en __all__ | **Incorporar con corrección:** consolidar productores e incluir AggregateOperation; conservar todos los shapes que cruza el SPI, también transitivos (T1/T2) |
| G03 — Fronteras duales, firmas y protocolos. R1 H1/H2/H5/H6; R2 B/C/D; R3 P2 | El borrador deja sin fijar composición, payloads incompatibles con nombres iguales y declaración privada de versión. También mezcla CRUD conservado y redefinido | **Eliminar la necesidad ahora:** sin B no hay selector, clases frontera v3, protocolo ni renombres. Las decisiones se exigen si se reabre B; no quedan a cargo de implementar A |
| G04 — Semántica útil para externos. R3 F2; precedente Search de R1 | El canario reinterpreta BSON e ignora contexto en varias primitivas; __all__ no ofrece un evaluador general. Los objetos compilados sí exponen datos/métodos accesibles, pero ello no acredita una API general soportada ni que todo engine necesite tal evaluador | **Incorporar el límite:** no usar ese canario como prueba del beneficio B. **Descartar** que todo motor externo sea incapaz de ejecutar planes o que haya que añadir un evaluador universal. Reabrir v3 requiere garantía/uso real |
| G05 — Deadline y rewind. R1 H4; R2 B/F; R3 F4 | Sonda confirma find diferido/cacheado; rewind renueva semántica y plazo, conserva contexto; clone crea otro. Deadline no es campo de OperationContext | **Incorporar:** T3 y censo por ruta. **Descartar** mover todos los plazos al despacho o preservar el mismo deadline en rewind |
| G06 — Ramas privadas de SQLite y shape-sniffing. R1 hechos/H4 | _iter_scan_documents_sync discrimina filtro/semántica en helper privado; solo se encontraron callers en tests. _count_matching_documents_sync tiene otra rama cruda; que compile no demuestra renovación pública. La inferencia de SPI por versión sigue eliminada | **Matizar:** registrar rama y alcance; no confundir unión privada con shim de versión ni llamar defecto público a una renovación no reproducida. Caracterizar antes de preservar/corregir |
| G07 — Guardas, plan ajeno y dialecto. R1 H7b; R3 sondas | Se reprodujeron ausencia permitida de duplicados en Operation, guardas más fuertes en Semantics, plan ajeno aceptado y 8→7 en conversión de helper sin ligar. Los casos tienen alcances distintos y no prueban un fallo de fachada ligada | **Incorporar:** T1/T2; corregir propagación y controlar productores internos. **Descartar** tokens/procedencia absoluta o prohibición general de overrides v2 legítimos |
| G08 — Preparación por documento y overrides. R1 H7c; R2 G; R3 sondas | Nuevas sondas en 8 celdas: N preparaciones de selector en Memory; 2N en SQLite, N=1/3 y ambas fachadas. El coordinador SQLite y su helper compilan lo mismo | **Incorporar:** T5 reutiliza objeto ya preparado para 2N→N. **Conservar** evaluación/CAS por candidato. No se promete compilar todo una sola vez ni se añade caché global |
| G09 — Planning/explain y profiler. R1 hechos; R2 B/C/G; R3 P2 | Explain llama al motor directamente; profiler usa getattr y best-effort. El contrato mínimo no exige planner. Callable presente no demuestra corrección de su cuerpo antes de conectar | **Incorporar:** T4 centraliza invocación, sesión y typing manteniendo SPI y fallback. **Descartar** validar semántica completa por nombre/firma o exigir nuevo planner a un engine mínimo |
| G10 — Ownership y congelación. R1 H7b; R2 C | frozen/slots o un proxy superficial no congelan BSON profundo. Outcomes y snapshots ya poseen mecanismos defensivos | **Incorporar:** pruebas de aislamiento y reutilización de fronteras. **Descartar** que disciplina o MappingProxyType solos prueben inmutabilidad, y evitar congelación universal sin medir (T2) |
| G11 — Piloto y coste proyectado. R1 H3; R2 G; R3 F4 | Find y $set de un documento no ejercitan borrado, planning, overrides ni repetición N. P2 no puede medir código aún no implementado | **Incorporar:** delete, planning/explain, update_many, upsert/replacement y checkpoints P2a/P2b; costes no ejecutados quedan como estimación. La comparación deja de ser A/B |
| G12 — Reloj y medición. R2 F | La sonda de plazo sustituye solo la función local, sin tocar asyncio. No hace falta proveedor público nuevo para ese caso. Subir tolerancia de CI al 10 % no elimina ruido | **Incorporar** aislamiento de reloj y estadística explícita. **Descartar** proveedor anticipado y verde por umbral más laxo; compilaciones/paridad son gates deterministas, timing ruidoso es inconcluso |
| G13 — Trazabilidad y autoridad. R1 H8; R3 P2 | ADR-017 menciona timeout, pero el objeto no lo contiene; ADR-016 declara valor histórico. El diff anterior suavizaba una promesa de 5.0 sin clasificarla en ADR-018 | **Incorporar:** aclaración editorial sin cambiar el estado de ADR-017, traza expresa en ADR-018 y ninguna retirada autorizada por ADR-016 |
| G14 — Fortalezas y alcance. R1 aciertos; R2 A/G; R3 conclusión | Evidencia P0 y mecanismos existentes útiles; no se necesita Search v2, nuevas requests por simetría, IR ni backends para la mejora local | **Conservar:** evidencia previa íntegra, anonimato, control 4.10.1, outcomes/snapshots/diagnóstico y pruebas nativas como oráculo acotado. V2 estable, sin proclamarlo definitivo |

La sugerencia de comparar con tecnologías de otros ecosistemas no se utiliza
como prueba: el criterio es el contrato y ejecución de este repositorio. No se
añaden dependencias, recomendaciones de librerías ni analogías normativas.

## Qué se ha validado ahora

- Lectura de operaciones, semántica, adapter, protocolos, exports, coordinadores,
  motores, helper SQLite, límites, cursor wire y ADRs vigentes.
- Reproducción de las sondas publicadas en
  [probe_review.py](../evidence/spi-v3-proposal/probe_review.py): guardas, plan,
  dialecto, cursor/rewind y ocho celdas de compilación. Se comprueban resultados
  y contadores de escritura, sin medir latencia ni abrir sockets.
- Verificación de hashes de fuentes de P0 y de las fuentes adicionales frente
  a la base Git. La revisión no introduce fixes productivos.
- Gates focales y mecánicos ejecutados en esta revisión: resultados y límites en
  [review-validation.json](../evidence/spi-v3-proposal/review-validation.json).

La primera versión de la sonda de multi-update asumía N también en SQLite y
falló. Se inspeccionaron las dos llamadas reales; la sonda final distingue N y
2N, sin cambiar expectativas de pruebas del producto. Se registra ese intento
como corrección del observador, no como defecto del runtime ni ocultándolo como
una ejecución verde original.

No se ejecuta ni aprueba A, no se construye otro wheel, no se recaptura MongoDB,
no se repite una suite global ni se valida adopción externa en este encargo.
Los resultados P0 de 140 tests/1.080 subtests y 12.880 tests contra referencias
nativas conservan su identidad; no se atribuyen de nuevo a esta revisión.

Este apartado corresponde a la revisión previa a la autorización de desarrollo.
La ejecución posterior de T1–T6 tiene su propio
[registro y evidencia](../engine-preparation-validation.md); no reescribe estos
resultados históricos.

Tras completar también S1/S2, la
[decisión posterior](engine-spi-v3-proposal.md) mantiene SPI v2 y aparca v3.
Su evaluación actual no altera las sondas ni los resultados de esta revisión.

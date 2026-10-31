# Finalización del promedio por contexto nativo

El estado `NumericSum` sigue siendo único. El corte añade una decisión de
finalización a `PreparedStage`, no otro acumulador ni otro evaluador. Se calcula
antes del pushdown y se conserva en las vistas físicas; una reubicación lógica
con contexto distinto la vuelve a calcular. El bucket conserva la decisión al
pasar por spill y el ejecutor con provenance usa la misma preparación.

En el motor clásico, el promedio no decimal divide la parte double del total.
La finalización SBE convierte primero el estado en la suma BSON y después divide
por el conteo. Para `[Int64(max), 1, Int64(-max)]`, la parte double puede ser cero
mientras la suma int64 final es uno. No se sustituye esta diferencia por una
identidad matemática ni por una tolerancia.

Fuentes primarias consultadas en MongoDB r8.0.32:

- [Finalización de acumuladores SBE](https://github.com/mongodb/mongo/blob/r8.0.32/src/mongo/db/query/sbe_stage_builder_accumulator.cpp),
  `buildFinalizeAvg`: SHA-256
  `41c1831e19e9745a32ae842ff3ae8e4880a9f52394e024c8a81376f58c87bc04`.
- [Fronteras del lowering de expresiones](https://github.com/mongodb/mongo/blob/r8.0.32/src/mongo/db/query/sbe_stage_builder_expression.cpp):
  SHA-256 `2079573e06406da6decabebfdc6afe25ca47c9022a396d235dd7d6e10c853861`.
- [Promedio clásico](https://github.com/mongodb/mongo/blob/r8.0.32/src/mongo/db/pipeline/accumulator_avg.cpp).

`group_average.py` posee exclusivamente esta selección numérica. Su tabla de
expresiones clásicas no acepta/rechaza operadores: soporte y gramática siguen
perteneciendo al catálogo y preparador existentes. Se excluyen las extensiones
locales del selector nativo. No se añade configuración pública, caché, formato
persistido ni opción para escoger dos intérpretes locales.

Las capturas `semantic_average_routes_{7,8,9}_0.json` comprueban 31 contextos,
incluidos prefijos, expresiones y scopes. No se extrapola la captura de 8 a 9:
las exclusiones, set/addFields/unset, reemplazo de raíz y match/expr con in de la
matriz cambian de resultado en 9 y tienen expectativas propias. Facet y lookup
anidados conservan el promedio clásico; unionWith anidado conserva la semántica
de su pipeline independiente. Los planes iniciales root/unwind/facet se guardan
en `sum-route-{7,8,9}.json`.

Se conservan los límites de la evidencia: servidores/FCV identificados en las
capturas, configuración nativa por defecto y operadores del subconjunto local.
No se anuncia un emulador general del planificador SBE ni del parámetro interno
`internalQueryFrameworkControl`.

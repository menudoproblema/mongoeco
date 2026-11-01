# Contraste sobre cb77581 y refinamiento del plan

1 de noviembre de 2026. Árbol inicial limpio. El adjunto conserva el SHA-256
`a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`:
es el mismo texto contrastado desde revisión 3. El
[plan vigente](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
conserva una sola secuencia de trabajo y los IDs existentes.

## Disposición del informe

| Afirmación | Contraste actual | Disposición |
| --- | --- | --- |
| Interno/local/SDK cruzan fronteras diferentes; cambiar rangos solo cubría un subcaso. | `binary_parts`/`binary_order_key` reconocen las representaciones, incluido subtipo 2. Las propiedades contrastan formas y framing SDK; distinct conserva subtipo/payload. | Conservar D03a y sus leyes. No otra tabla de clases ni helper universal de igualdad. |
| SQL eq/in por tag podía omitir Binary 0 equivalente a bytes. | `_is_translatable_equality_value` declina binarios; los tests ejecutan SQL, observan matcher y comparan con traducción desactivada, incluyendo índices. | Mantener fallback; optimizar no es condición de cierre. |
| Literal DBRef y wire eran causas independientes. | `is_query_expression_document` distingue la gramática por contexto; la aceptación básica de parser/raw ya está registrada. | No reabrir el literal básico ni usar el predicado del decoder SDK como gramática. D06 conserva contextos pendientes. |
| Watch requiere clase/código/fase y contexto propio. | Compila PreparedPipeline con dialecto/scope y captura contexto. El corpus nativo observa fase, clase y código. | Conservar D04; su aceptación no implica admisión BSON completa B07. |
| TTL solo podría retrasarse un milisegundo, sin importancia. | El cutoff actual es entero; hay pruebas de frontera, reconstrucción de auxiliares, reapertura y rollback. | No adoptar esa cota. Candidatos, residual y cadencia son garantías diferentes. |
| Las 167 pruebas y las fuentes no estaban verificadas por el autor. | Las 167 son históricas; esta revisión ejecuta 115 + 38 regresiones locales. Se descargan las 16 fuentes fijadas y coinciden sus hashes, incluidas las 14 originales. | Distinguir la evidencia propia del autor, la histórica del repositorio y la nueva de esta revisión. H53 mantiene C03/B08. |

El [parser oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp)
explica la excepción gramatical DBRef. El
[contrato de comparación](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
separa comparación de consulta/expresión y ordena BinData por longitud/subtipo/bytes.
Estas fuentes explican mecanismos; no reemplazan capturas nativas por versión.

## Refinamiento aplicado

La conversión escalar y el codificador de lecturas tienen aceptaciones posteriores
a revisión 18. Se actualiza el orden activo: B07 escrituras/comandos → UUID
coordinado, con positivos/respuestas en su corte propio; después contextos D06
pendientes y cierre D05. H53 y la decisión opcional D07 pueden avanzar de forma
independiente. El detalle de B07 mantiene las seis unidades y sus garantías.

La simplificación consiste en reutilizar autoridades existentes: vistas de valor,
identidad física, gramática/raw, contexto y codificación cliente. El defecto común
es decidir después de perder información o en una fase equivocada. No se resuelve
con un único comparador, un modo universal o endureciendo el codec interno.

En escrituras hay dos riesgos concretos por inspección: la preparación bulk
normaliza antes de admitir y la guarda de `_id` se ejecuta antes de codificar.
El plan exige conservar originales y contrastar errores simultáneos antes de
fijar precedencias. No se presentan aquí como nuevas divergencias reproducidas.
Los lotes con errores cliente y los efectos parciales por errores servidor deben
seguir teniendo oráculos separados. No validar anticipadamente todo un bulk.

## Verificación y límites

- [Focales](focal.txt): 115 pruebas Binary/SQL/TTL/contexto watch y 38 del
  codificador. Ejecutadas sobre checkout, no gate de wheel ni cobertura global.
- [Sonda](representation-probe.py.txt), derivada de revisión 18 sin cambiar las
  aserciones: [seis pares y 18 trazas](representations.json), IDs eq/in y contenido
  completo de distinct. Los nombres de clases privadas no se hacen contrato.
- [Verificación de fuentes](sources-check.json): 16 descargas r8.0.0, 16 hashes
  coincidentes y correspondencia con las 14 entradas de `sources.json`.
  [Script reproducible](sources-check.py.txt). El primer intento dentro del
  sandbox no resolvió DNS; la descarga autorizada posterior terminó correctamente.
- Sin cambios de runtime, goldens, observadores ni receipts anteriores. Sin nuevos
  servidores MongoDB, capturas nativas, wire replay, matrices, wheel o benchmarks.
  Los 96 fallos B07 registrados por la aceptación de lecturas siguen pendientes;
  no se ha repetido ni aceptado ese gate amplio en esta revisión.

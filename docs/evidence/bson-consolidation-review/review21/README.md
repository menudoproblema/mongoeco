# Autocrítica contrastada con el estado actual

1 de noviembre de 2026. Base `a7d27fc9`, con candidato de proyecciones
preexistente sin commitear. El [receipt](receipt.json) identifica el adjunto y
los archivos de ese candidato. El adjunto es idéntico por SHA-256 al ya
contrastado desde revisión 3. Esta revisión modifica documentación y evidencia;
no corrige ni acepta el candidato.

## Disposición del informe

| Afirmación | Contraste y decisión |
| --- | --- |
| La causa original H48 era correcta; interno/local y SDK son mecanismos diferentes. | Confirmado como explicación histórica. `binary_parts` y `binary_order_key` reconocen las representaciones actuales. Conservar propiedades y resultados por subtipo, sin una nueva tabla de clases. |
| `$in` y distinct ya figuraban; faltaba un testigo concreto de subtipos. | Correcto. La sonda repetida conserva IDs eq/in y distinct con payload idéntico y subtipos 0/5, observando contenido además de cardinalidad. No crear otro bloque para el mismo consumidor. |
| La igualdad SQL de bytes excluye Binary 0 por tag. | Defecto histórico: `_is_translatable_equality_value` y `_translate_membership` ahora declinan binarios. Conservar fallback y pruebas de ruta; una traducción optimizada nueva exige equivalencia y coste acreditados. |
| Falta reconocer el literal DBRef; wire podría ocultarlo. | La gramática común actual ya distingue primer campo y presencia de campos DBRef. El parser oficial r8.0.0 comprueba nombres, no tipos. Las variantes de contexto/orden/raw permanecen en D06; éxito básico no acredita transporte completo. |
| Watch necesita clase/código/fase. | Garantía D04 vigente. Conservar apertura/consumo, contexto y efectos por separado; la inspección de código no sustituye el diferencial. |
| Falta observar ambos lados del matcher. | Se repite la sonda existente: seis pares motor/dialecto y 18 trazas. Los nombres privados describen este diagnóstico; el contrato permanente es equivalencia, subtipo/contenido y resultado público. |
| TTL solo puede retrasarse un milisegundo y es irrelevante. | No adoptar. El código actual usa enteros tanto para auxiliares como para cutoff, con residual y migración propios. El residual no puede recuperar un vencido excluido en la preselección. No se demuestra esa cota ni se acepta una tolerancia nueva. |
| Las 167 pruebas, H53 y los hashes requieren cautela. | Las 167 son históricas. H53 conserva disposición C03/B08 pendiente. En esta revisión se descargan las 16 fuentes fijadas: todos sus hashes coinciden, incluidos los 14 del manifiesto original. |

El [parser oficial](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp)
explica `isDBRefDocument` e `isExpressionDocument`. La lectura del servidor
explica el mecanismo; los contratos por versión siguen requiriendo las
referencias nativas congeladas correspondientes.

## Plan refinado

Se actualiza el [único orden activo](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución):
B07 positivos/respuestas, resto D06, disposición H53/D07 e integración D05.
El default UUID ya está aceptado y deja de describirse como dependencia futura.
Se conservan IDs y entregas aceptadas, sin otra fila duplicada de D01/D03.

La simplificación común es preservar información hasta el consumidor que
decide: vistas BSON para tipos, gramática por contexto, contexto preparado,
identidad física y claves según la política existente. No unificar todas las
igualdades en un comparador ni crear otro runner.

Para el candidato de proyecciones, la aceptación debe separar disponibilidad
de metadatos, valor por fila y materialización pública; conservarlos a través
de optimizaciones y copias sin persistirlos como campos. Reusar el evaluador
existente, ejecutarlo una vez por fila y probar contexto, subpipelines,
callbacks y fase de errores con sus efectos. Estas son obligaciones de
aceptación; las verificaciones focales siguientes no prueban su cumplimiento.

## Evidencia y límites

- [Focales](focal.txt): 78 pruebas correctas sobre checkout; comando completo
  registrado, sin aceptación de wheel instalada.
- [Representaciones](representations.json) y [sonda](representation-probe.py.txt):
  seis pares, 18 trazas, eq/in y contenido/subtipo de distinct. Adaptación del
  diagnóstico de revisión 19, sin cambiar sus aserciones.
- [Fuentes](sources-check.json) y [verificador](sources-check.py.txt):
  16 descargas y hashes coincidentes, con correspondencia al manifiesto original.
- No se ejecutan nuevos servidores, capturas nativas ni gates integrados. No
  se cambian runtime, tests, goldens u observadores. El candidato preexistente
  conserva sus pendientes y requiere sus propios gates tras congelar la fuente.

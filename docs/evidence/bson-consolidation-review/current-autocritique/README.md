# Autocrítica: contraste sobre el candidato de batching

1 de noviembre de 2026. Base `c1a7c4a5`, con cambios preexistentes de batching,
spill, respuestas, fixture y sus tests sin commitear. El [receipt](receipt.json)
fija sus hashes. Esta revisión modifica documentación y evidencia; conserva
intactos esos cambios y no los acepta como entrega.

El adjunto tiene SHA-256
`a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`:
es el mismo texto ya contrastado desde revisión 3. No abre otros IDs ni reabre
D01/D02/D03/D04 por repetir diagnósticos históricos.

| Afirmación del informe | Contraste actual y disposición |
| --- | --- |
| H48 cruzaba operando interno, documento decodificado y salida pública; Binary SDK tenía otro mecanismo de fallo. | Correcto históricamente. `binary_parts` reconoce bytes y wrappers con subtipo; `binary_order_key` añade longitud física, incluso los cuatro bytes internos del subtipo 2. Comparación/rango/claves delegan en las primitivas actuales. No basta probar la herencia de una clase ni volver a añadir tablas de tipos. |
| Eq/in y distinct ya existían en las familias; faltaba un testigo específico de subtipos. | Mantener payload idéntico, bytes/Binary 0 equivalentes y Binary 5 distinto. La sonda repetida comprueba IDs y contenido/subtipo de distinct, no solo cardinalidad, en seis pares motor/dialecto. |
| SQL de igualdad omite Binary 0 por su tag distinto de bytes. | Defecto histórico corregido mediante fallback: `_is_translatable_equality_value` declina formas binarias. No se exige un pushdown nuevo para simplificar. Conservar controles de SQL realmente ejecutado y matcher/fallback, también con tags persistidos antiguos. |
| El parser no admite el literal DBRef y wire podría ocultarlo. | El parser actual comparte `is_query_expression_document`; presencia de campos y primer campo deciden su gramática. El literal básico tiene aceptación propia; orden/extras y los contextos restantes conservan D06a/b/c. La decodificación SDK no puede recuperar orden perdido. |
| Watch necesita fase, clase y código, además de dialecto/contexto. | Ya son garantías D04. La preparación usa scope watch y dialecto; la transformación pasa el contexto capturado a ejecución compilada o interpretada. El replay focal de contexto pasa, sin presentarlo como nueva prueba nativa de todas las fases. |
| TTL solo puede retrasarse un milisegundo y es irrelevante. | No adoptar esa cota ni tolerancia. Cutoff y auxiliares SQLite actuales usan milisegundos enteros; el residual exacto sigue separado. Un residual no recupera candidatos omitidos. D02 conserva migración/reapertura y fronteras, sin exigir cadencia inmediata al monitor. |
| Propiedad igualdad/clave, trazabilidad D01/D03 y 167 tests. | D00 acompaña cada consumidor; las leyes dependen de su política. La secuencia activa ya diferencia entregas únicas D01a/b y D03a/b. Las 167 pruebas son históricas y no se suman a esta ejecución. |
| H53 y fuentes no se habían comprobado completamente. | H53 mantiene disposición C03/B08 pendiente: no inferir warning desde aceptación de escritura ni crear logging implícito. Se descargan y verifican las 16 fuentes fijadas, incluidas las 14 originales. No acredita por sí solo ninguna ejecución de mongod. |

El [parser oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp)
distingue `isDBRefDocument` de `isExpressionDocument` y comprueba nombres de
campos, no tipos, para esta decisión. Eso explica el mecanismo de gramática;
los diferenciales por versión siguen teniendo su propio oráculo nativo.

## Refinamiento aplicable

Se modifica el [único orden vigente](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
sin añadir una revisión numerada ni otro backlog. Permanece: B07 positivos y
respuestas; resto D06; disposición H53/D07; D05 integrado. La entrada documental
actual distingue la aceptación de proyecciones del candidato posterior: los
dos fallos históricos de batching no se atribuyen automáticamente al checkout.

Tres precisiones quedan dentro de esos bloques:

1. D06 debe contrastar el artefacto que recibe cada consumidor. Por inspección,
   `_materialize_pipeline(pipeline)` admite un prefijo alternativo, pero la fuente
   de `_load_pushdown_source_documents` vuelve a construirse desde el pipeline
   completo del cursor mediante `_pushdown_find_operation`. No hay aquí un
   testigo que demuestre resultado incorrecto. Caracterizar la ruta antes de
   corregirla; si falla, pasar el pushdown existente ya resuelto, sin otro owner.
2. B07 debe comparar finalización, fase y callbacks además de documentos: índice
   frente a sort/group bloqueantes, fuente corta, initial/getMore, EOF y spill
   cerrado sin primera lectura. Las garantías siguen al lector y spool actuales.
   El candidato existente conserva sus verificaciones y aceptación pendientes;
   esta revisión no incorpora como gates los logs de otras ejecuciones en `/tmp`.
3. Cada captura debe reservar evidencia propia y conservar su ownership. Una
   ejecución posterior no repara un ledger sobrescrito. Esta obligación refina
   el fixture existente, sin otro runner ni restauraciones inventadas.

La refactorización conjunta conserva información hasta el consumidor autorizado:
vistas BSON, gramática y contexto preparados, identidad física y claves por
política. Compartir esas primitivas reduce autoridades duplicadas. Un único
comparador o contexto universal borraría diferencias necesarias.

## Verificación y límites

- [Focales](focal.txt): 78 correctas sobre checkout; no wheel instalada.
- [Sonda](representation-probe.py.txt) y [trazas](representations.json): seis
  pares Memory/SQLite × 7/8/9, 18 filas documento/operando; mismos asserts que
  revisión 21, solo cambian destino y etiqueta de la fuente inspeccionada.
- [Verificador](sources-check.py.txt), [resultado](sources-check.txt) y
  [hashes](sources-check.json): 16 descargas coincidentes, 14 entradas originales.
- Ninguna nueva captura MongoDB, alteración de goldens/tests/runtime ni aceptación
  integrada. Los cambios de implementación preexistentes permanecen intactos.
- Validación documental: enlaces locales de los documentos afectados y
  `git diff --check`. No se ha encontrado un validador Markdown específico.

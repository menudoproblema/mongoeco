# Relectura del informe sobre 79d5a62 y candidato de mutaciones

1 de noviembre de 2026. El [receipt](receipt.json) identifica el adjunto y cada
archivo preexistente modificado/nuevo de runtime/tests. No se modifica ese
candidato ni se acepta por esta revisión. El SHA-256 del adjunto coincide con
el texto ya contrastado desde revisión 3.

## Disposición

| Afirmación | Contraste y decisión |
| --- | --- |
| La causa H48 era correcta, pero cruzaba representaciones distintas. | `binary_parts` y `binary_order_key` ya reconocen bytes, SDK/local/interno y subtipo. Las seis combinaciones motor/dialecto conservan IDs eq/in, contenido y subtipo de distinct; 18 trazas observan ambos lados del matcher. Conservar D03a; no otra tabla de clases. |
| Eq/in SQL por tag puede descartar Binary 0 equivalente a bytes. | `_is_translatable_equality_value` y `_translate_membership` declinan binarios. Las regresiones de SQL y almacenamiento pasan. Conservar fallback hasta demostrar una traducción exacta; no convertir su optimización en requisito del arreglo. |
| El parser confunde literal DBRef y operadores. | La gramática común actual distingue primer campo y campos DBRef por contexto. El parser oficial r8.0.0 comprueba nombres, no tipos; no usar la conversión SDK de respuestas como gramática de consulta. El defecto básico es histórico, D06 conserva sus contextos pendientes. |
| TTL solo podría retrasarse un milisegundo y es irrelevante. | No aceptar la cota. SQLite actual usa milisegundos enteros y residual exacto; las pruebas de migración/reapertura pasan. Separar conservación de candidatos, decisión exacta y cadencia. |
| Watch necesita clase/código/fase; las fuentes y 167 pruebas no estaban verificadas por el autor. | D04 ya posee preparación/contexto y aceptación propia. Las 167 son historia, no resultado nuevo. Se comprueba integridad de seis artefactos de revisión 19 y correspondencia de sus 16 registros de fuentes con las 14 entradas originales; no se repiten esas descargas. H53 sigue en C03/B08. |

Fuentes releídas: [parser oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp),
funciones `isDBRefDocument`/`isExpressionDocument`, y
[contrato de orden BSON](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/),
que distingue comparaciones de consulta/expresión y orden longitud/subtipo/bytes.
Estas lecturas explican mecanismos; no sustituyen capturas por versión.

## Refinamiento aplicado

El [único orden activo](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
se actualiza al estado real: lectura e inserciones aceptadas; candidato
update/delete/findAndModify pendiente de revisión/aceptación; después bulk por
familia y comandos, UUID coordinado, positivos/respuestas y contextos D06.
H53 y decisión opcional D07 conservan independencia; D05 integra el candidato final.
No se añaden bloques ni se reabren arreglos aceptados.

La simplificación reutiliza encoder, vistas, gramática, contexto y ejecutores
existentes. La validación cliente conserva el orden propio de cada operación y
perfil SDK: no unificarlo mediante una secuencia universal. El cambio de bulk
al ejecutor interno evita doble admisión, pero todavía no demuestra la admisión
completa de cada grupo. Se mantienen testigos de error del segundo elemento,
prefijo de grupos previos, estado, índices y barrera de eventos.

## Verificación reproducible y límites

- [Focales](focal.txt): `python -m pytest -q` con
  `tests/property/test_bson_binary_coherence.py`,
  `tests/unit/core/test_binary_storage_paths.py`,
  `tests/integration/api/test_ttl_exact_millisecond_migration.py`,
  `tests/integration/api/test_watch_context_contract.py`,
  `tests/unit/core/test_sqlite_query.py` y `tests/unit/api/test_bson_input.py`:
  186 pruebas correctas sobre checkout.
- [Candidato](mutation-candidate.txt):
  `python -m pytest -q tests/integration/api/test_bson_mutation_boundary.py`:
  49 pruebas correctas sobre checkout, sin convertirlas en aceptación instalada.
- [Representaciones](representations.json): se reutiliza sin cambiar aserciones
  la [sonda de revisión 19](../review19/representation-probe.py.txt), cambiando
  solo directorio de salida y etiqueta de procedencia; seis pares y 18 trazas.
- No se ejecutan nuevos servidores MongoDB, capturas, replay wire ni gates de
  wheel/matrices/cobertura/coste. No se cambia ningún golden u observador.
  Los errores B07 del gate amplio histórico siguen visibles; no se afirma aquí
  su resultado actual ni el cierre del objetivo.

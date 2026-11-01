# Contraste sobre b49d521 y refinamiento del plan

1 de noviembre de 2026. El adjunto es idéntico al de revisiones anteriores
(SHA-256 a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca).
El [plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
conserva el único orden vigente. Esta revisión no implementa ni acepta conversiones.

## Contraste

- Binary usa binary_parts/binary_order_key para bytes, SDK/local/interno y UUID
  canónico; SQL eq/in declina formas binarias. No reintroducir pushdown por tag.
- query_operators distingue literal DBRef por primer campo, contexto y nombres;
  el [parser oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp)
  consultado de nuevo confirma que su reconocimiento no exige tipos de campos.
  No sustituir esta gramática por las reglas del decoder SDK.
- SQLite TTL usa bson_datetime_milliseconds para cutoff y residual exacto.
  La migración tiene test permanente. No asumir retraso máximo de un milisegundo:
  un residual no recupera candidatos omitidos, y cadencia es otra garantía.
- Watch compila PreparedPipeline con dialecto y scope watch; cursor recibe el
  contexto de ejecución. Clase/código/fase siguen siendo garantías D04.
- Grupo/conjuntos y Search ya tienen aceptaciones 332d07a/fbc7cde posteriores
  al último contraste. Conservarlas sin declarar D06/D05 completos.
- H53 sigue pendiente C03/B08. Las 167 pruebas del informe son historia.
  sources.json tiene 14 entradas; sources-check de revisión 15 añade parser y
  pipeline_executor: 16 correspondencias correctas, comprobadas documentalmente.
  No se han descargado otra vez esos 16 archivos.

## Verificación y límites

Se repite exactamente el comando focal de revisión 17: **97 passed in 1.56s**.
La [sonda](representation-probe.py.txt), reutilizada sin cambiar aserciones,
verifica seis pares motor/dialecto y 18 trazas de documento/operando; resultados
[representations.json](representations.json). Observa eq/in y subtipo/payload de
distinct, además de tipos/rangos internos; no fija clases privadas como API.

El [workset preexistente](preexisting-workset.json) identifica producción/corpus
sin commit conservados intactos. Se precisa su aceptación futura (tipo público,
fase y controles numéricos); no se aceptan goldens nuevos por leerlos. Esta
revisión no ejecuta servidores ni captura MongoDB, wheel, cobertura, matrices
completas o benchmarks. La regresión focal no es un gate global.

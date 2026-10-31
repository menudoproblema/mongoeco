# D03b: tipos de consulta y predicados SQL

Owner: [registro](../../../bson-consolidation-implementation.md).
Contrato: [plan refinado](../../../bson-semantics-consolidation-plan.md).

Se reutilizan capturador/registro/replay. Las referencias nuevas están en
`tests/fixtures/semantic_type_query_{7_0,8_0,9_0}.json`. Cada una incorpora
selección, paginación/negación/ausentes, escrituras, argumentos y almacenamiento
numérico. Los argumentos incluyen los 21 códigos y nombres, variantes numéricas
válidas y rechazos nativos por mayúsculas, espacios, tipo o valor.

Los logs separan fallos anteriores, gates finales y diagnósticos. El hash del
corpus se refrescó tras una anotación de lint, verificando primero que todos
los resultados y manifiestos de casos permanecían idénticos. La ampliación
posterior de argumentos fue recapturada y contrastada nuevamente con --check.
No se modifican fixtures históricos, observers ni snapshots protegidos.

`paths.json` observa conexiones reales de lectores dedicados. Las 90 pruebas
permanentes además comprueban archivos reabiertos, códigos/aliases, índices y
traducción desactivada. Un fallback correcto no se cuenta como SQL exacto.
La propiedad SQL usa bytes de tipo emitidos por el SDK BSON como expectativa;
no importa el clasificador del matcher para construir su oráculo.

El primer benchmark se ejecutó con otros gates activos y motivó una repetición
secuencial sin ellos. Se conservan ambos. Son cinco muestras por carga con
checksum idéntico, no las cargas/matrices finales de D05. El receipt declara
la fuente y alcance de cada comprobación y los pendientes del programa.

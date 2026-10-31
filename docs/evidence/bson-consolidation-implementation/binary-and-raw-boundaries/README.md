# Binary y frontera raw compartida

Owner: [registro de implementación](../../../bson-consolidation-implementation.md).
Contrato: [plan, revisión 5](../../../bson-semantics-consolidation-plan.md).

Las capturas nuevas `semantic_binary_edges_*` y `semantic_dbref_grammar_*` viven
en tests/fixtures y se verifican contra MongoDB 7.0.43, 8.0.32 y 9.0.2, FCV de
su major. El receipt identifica las referencias y los gates, incluidos fallos
previos del runtime f69759a. No se cambian goldens históricos ni observers.

Los logs distinguen candidatos intermedios de la aceptación final. La primera
pasada integrada detectó reemplazos inmutables; se detuvo tras identificar su
proceso, al quedar invalidada por la corrección. Las capturas intermedias que
abortaron con un rechazo legítimo de `$pull` no se aceptan como goldens. El
corpus final observa ese rechazo y el estado conservado.

El benchmark pequeño usa cinco muestras secuenciales, checksums idénticos y
semillas fijas. Incluye parseo raw y peticiones directas; se ejecutó con otros
gates activos y no sustituye cargas ni matrices de D05. Los scripts y las
trazas de caminos reales se conservan como evidencia diagnóstica.

D03a y la dependencia compartida no acreditan todo D06: quedan consumidores de
claves, subpipelines y aceptación física restante según el registro. Tampoco
se cierra la frontera general BSON B07 ni la política de UUID legacy.

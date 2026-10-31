# Revisión 9: autocrítica frente al estado entregado

Owner de obligaciones: [plan](../../../bson-semantics-consolidation-plan.md).
Owner de estado/gates: [registro](../../../bson-consolidation-implementation.md).
Base inspeccionada: `163ccb4`, runtime y tests limpios. El adjunto tiene el
mismo SHA-256 que revisiones 3–8; no crea hallazgos nuevos por repetición.

Se mantienen las retractaciones sobre H48, in/distinct y D00. La inspección
actual confirma vistas Binary compartidas, fallback SQL y decisión gramatical
DBRef por contexto. Pasan 88 checks de propiedades/fronteras/contexto y 408 de
replay Binary/DBRef/tipos/watch sobre capturas existentes, incluidos directa
sync/async y wire donde el corpus lo soporta. La sonda registra ambos lados
del matcher: 18 filas en seis pares motor/dialecto. Los 16 hashes de fuentes
r8.0.0 vuelven a coincidir al descargar sus URLs históricas.

Los intentos iniciales bajo sandbox fallaron al abrir sockets wire y descargar
fuentes; se repitieron con los accesos necesarios y terminaron correctamente.
No fueron discrepancias semánticas. El primer replay tuvo 177 checks verdes
y 168 fallos wire; la repetición incluye todo ese corpus más watch. La prueba
wire aislada identifica PermissionError al hacer bind en 127.0.0.1. El intento
de fuentes conserva sus errores en sources-sandbox-attempt.json. No hubo rechazo
de aprobación automática ni cambios para acomodar los resultados.

El plan corrige una ambigüedad temporal: preparación/contexto e imágenes CRUD
de watch tienen aceptación propia, aunque D04 sigue abierto. No reabre D03a/b
ni D02. El cutoff TTL actual usa milisegundos enteros; no se acepta como
contrato la supuesta irrelevancia de un retraso en selección de candidatos.

La refactorización siguiente comparte la coordinación de entrega existente
con DDL de base/cliente, conserva el registro atómico/outbox y mantiene aparte
ámbito/cierre y forma pública. No añade un normalizador universal, otro runner,
un metadato de encarnación ni una modificación de SPI v2. Las tres garantías
D04 permanecen dentro de su bloque, con aceptación separada.

No se ejecutan nuevas sondas MongoDB físicas, TTL, H53, costes, wheel ni matriz
global. Los datos nativos de aceptación conservan los receipts de sus cortes.
No cambia producción/tests ni se sobrescriben observadores o goldens históricos.

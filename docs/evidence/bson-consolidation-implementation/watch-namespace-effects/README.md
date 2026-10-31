# D04: efectos de namespace y scopes

Owner de obligaciones: [plan](../../../bson-semantics-consolidation-plan.md).
Owner de progreso: [registro](../../../bson-consolidation-implementation.md).
Base de produccion: 1a4e28b. No cierre completo D04/D05/B07.

El corpus watch_lifecycle usa el registro/capturador/replay existentes: siete
acciones por cinco scopes (coleccion, origen inexistente, destino, base y
cliente). Comprueba drop/rename API, drop/dropDatabase comando, dropDatabase
cliente y ausencia de efecto. Cada escenario termina con escritura barrera o
invalidate/cierre; no infiere ausencia solo de esperar un timeout. Las capturas
7/8/9 se repiten con --check en replica sets temporales propios, bases UUID
limpiadas en finally. No se modifica la instancia habitual ni la topologia.

El observador retiene todos los campos salvo framing variable de token, reloj
de cluster y wallTime. Canonicaliza solo el orden de miembros del sobre de
evento y el nombre efimero de base en ns/to; conserva arrays, presencia/ausencia
de campos y valores BSON ordenados. No cambia el observador protegido ni
goldens anteriores. Las formas/campos del evento importan, no solo sus tipos.

## Garantias aceptadas

- Mutacion y efectos DDL comparten publication scope/outbox. La entrega API
  comparte preparacion, hooks y finally; publish_committed conserva secuencia
  y publicacion bajo el mismo guard del hub. SPI v2/sinks anteriores conservan
  fallback. No hay otro contexto, secuenciador, esquema SQL ni token nuevo.
- Scopes distinguen drop de coleccion/base, rename origen/destino y coleccion
  inexistente. El journal conserva el destino opcional; replay publico prueba
  continuidad y no fabrica ns/documentKey para invalidate.
- Los tests de fallo comprueban restauracion de datos/catalogo/indices/opciones,
  ausencia de eventos y reintento. El rollback Memory de rename agrupa ambos
  namespaces; drop de base restaura roots sin copiar documentos. SQLite registra
  los payloads antes de terminar su write scope.
- Commit/abort local es control del contrato existente, no promesa de DDL
  transaccional nativo/distribuido. Un SQLite/journal producido por la base real
  prueba lectura/rename/reapertura, esquema y claves estables; no downgrade.

## Evidencia y limites

El receipt fija comandos, resultados, hashes y candidato. acceptance-* son
gates finales; feedback-* conserva el fallo de rollback del candidato intermedio,
el mock SQL obsoleto sustituido por conexion real y el test de invalidacion
global que se interrumpio para darle scope de coleccion y timeout acotado.
Los intentos iniciales de captura usaban renameCollection fuera de admin y un
seed vacio que el replay no admite: se corrigio el testigo, no el servidor ni
sus respuestas. La captura aceptada no incluye esa entrada de rename comando;
rename-routing.json conserva el defecto H56 como pendiente explicito.

No se acredita wire watch, rename entre bases, dropTarget atomico, protocolo
completo del cursor, reanudacion tras invalidate, cancelacion ni interacciones
adicionales de imagenes con DDL. Preparacion e imagenes CRUD conservan sus
receipts anteriores. RS de CI, D06 y matrices/costes/globales D05 siguen abiertos.
La medida ABBA se ejecuta secuencialmente sin otros gates propios activos y
observa CRUD default/updateLookup, no coste DDL ni garantia de rendimiento general.

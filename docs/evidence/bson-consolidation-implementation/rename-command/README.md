# D04/H56: entrada admin y rename atomico

Owner de obligaciones: [plan](../../../bson-semantics-consolidation-plan.md).
Owner de estado: [registro](../../../bson-consolidation-implementation.md).
Base: 6e6d31e. H56 se acepta funcionalmente; D04/D05/B07 siguen abiertos.

## Refactorizacion y garantias

La gramatica raw se integra en api/_command_bson: admin antes de admitir
campos, validacion de campos desconocidos/tipos/requeridos antes de semantica
de namespace, y booleanos estrictos. La clasificacion comun distingue Code de
string. Los motores validan existencia/conflicto bajo su propia unidad atomica.

La limpieza fisica de namespace es reutilizable por drop y overwrite; su caller
conserva locks, rollback y registro. SQLite separa remove_collection_contents
del write scope; Memory conserva ambos snapshots de namespace. El overwrite
emite rename, sin un drop intermedio. Se preservan origen/destino y sus indices,
opciones y recursos Search ante fallo. No hay esquema SQL, token, contexto ni
protocolo SPI v2 nuevo. El rename sin overwrite conserva el metodo publico;
el recurso enriquecido se exige para overwrite y falla antes de mutar cuando
el proveedor no lo implementa.

## Referencias y comprobaciones

Dos corpora registrados en el runner existente: 17 escenarios de comandos y
24 de eventos por version, 7/8/9, con captura y --check. Comandos por directa
sync/async y wire sync/async; eventos por scopes locales sync/async. Errores,
datos tras rechazo, origen/destino ocupados/ausentes/iguales, metadata e indices.
Solo se canonicaliza el orden de miembros del sobre/opciones de catalogo y
el nombre efimero de base en eventos; se conserva el BSON ordenado del validator
y de los documentos. No se alteran observadores ni referencias historicas.

Tests adicionales inyectan fallo tras limpiar destino o preparar efectos;
comprueban rollback, reintento y Search previamente usado. El proveedor con
override del metodo publico protege la delegacion ordinaria; otro proveedor
sin recurso enriquecido prueba rechazo de overwrite sin perdida de datos.
Un proxy con cliente prestado demuestra entrega de escrituras wire en streams
locales publicos y continuidad despues de cerrar el proxy. No es watch wire.

Un SQLite/journal producido por el archivo real de 6e6d31e prueba overwrite,
clave del origen e indices/opciones heredados, esquema central y reapertura.
La medida ABBA de rename ordinario se ejecuta sin otros gates propios activos,
con diez muestras por runtime/caso y resultados iguales; no acredita todos
los costes de D05 ni coste de overwrite.

## Feedback y limites

acceptance-* corresponde al source congelado final. feedback-* incluye
expectativas antiguas de CollectionInvalid sustituidas por OperationFailure
contrastado, orden incidental de opciones y el replay interrumpido al
reproducir el bypass del override publico. Ese candidato no se acepta.
El intento de caracterizar temp en create fallo con 72: no acredita soporte
local de temporales ni extiende el subset de rename. Los probes de argumentos,
precedencia y eventos quedan separados de los corpora permanentes.

El subset es rename dentro de una base. Protocolos completos de cursor,
reanudacion tras invalidate, cierre filtrado/cancelacion, mas interacciones
de imagenes/DDL y RS CI siguen en D04. D06, H53/conversion, D07 y la matriz
global/wheel/entornos/rendimiento D05 conservan su pendiente. No se acredita
durabilidad distribuida, downgrade ni atomicidad de proveedores sin recurso.

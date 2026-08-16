# ADR-013 - Observabilidad Search con estados y dominios tipados

## Contexto

Un candidato, un match de query, un hit emitido y una salida final pertenecen a
dominios distintos. Campos planos opcionales permiten representar trazas
contradictorias y confunden planificacion sin ejecucion con metricas no
disponibles.

## Decision

La observabilidad Search usa estados discriminados para planificacion,
ejecucion completada y ejecucion no disponible o rechazada. Cada metrica
declara valor, dominio, exactitud, origen y disponibilidad. Los constructores
validan relaciones entre dominios, collectors, residuals y backend. Las fases
`query`, `residual-filter` y `collector` hacen imposible publicar evidencia de
un residual o collector que el trace no declare como ejecutado. Cuando existe
un `SearchRequest`, `operationId` correlaciona plan y ejecucion sin introducir
otra fuente de contexto. `collectorCount` describe la configuracion;
`collectorDocumentCount` mide trabajo del collector y `pipelineOutputCount`
mide la salida final. Todas las metricas canonicas se representan incluso
cuando no estan disponibles.

Una traza interna ejecutada declara contexto ligado y snapshot capturado. La
forma legacy 4.x que no aporta esa evidencia se conserva como compatibilidad
deprecada, pero no se usa en Memory, SQLite ni el adapter canonico y se
retirara en 5.0.0.

Fallbacks y degradaciones usan codigos estables con un mensaje humano. Los
detalles fisicos se aislan bajo `engineDetails`. Los campos planos y previews
publicados en 4.x se derivan del modelo canonico como aliases deprecados; no
son otra fuente de verdad.

## Consecuencias

- `queryPlanner` declara explicitamente que no ejecuto.
- `executionStats` solo puede serializar una ejecucion real y coherente.
- Una metrica ausente se distingue de cero y de un valor no exacto.
- Memory y SQLite comparten validacion y serializacion comunes.

## Alternativas descartadas

- Anadir mas booleanos y campos opcionales a una unica traza.
- Fabricar una traza vacia para indices pendientes o ejecuciones rechazadas.
- Publicar directamente diccionarios de diagnostico propios de cada engine.

# ADR-017 - SPI v2 exclusivo en MongoEco 4.7

## Estado

Aceptada para MongoEco 4.7.

## Contexto

MongoEco mantenia dos caminos de engine: el contrato tipado SPI v2 y una capa
que inferia implementaciones anteriores por su shape. La convivencia duplicaba
la resolucion de capabilities, traducía retornos y callbacks, mantenia
primitivas paralelas y permitia que el momento de destruccion de objetos
alcanzase rutas de sincronizacion distintas.

El producto acepta expresamente que la retirada sea incompatible dentro de una
minor. Retrasarla hasta 5.0 conservaria durante mas tiempo estados imposibles de
acreditar con el kit de conformidad v2.

## Decision

MongoEco 4.7 publica SPI v2 como unico engine SPI estable:

- todo engine declara `EngineCapabilities(spi_version=2)`;
- no se infiere version ni capacidad mediante shape, firma o flags privados;
- la API, conformance, Memory y SQLite cruzan la misma frontera v2;
- no existen adapters, aliases, entrypoints ni fallbacks de version;
- las variantes de batch, snapshots, change delivery y Search se expresan
  exclusivamente mediante capabilities v2;
- la ruptura se documenta y el manifiesto publico declara `engineSpi: [2]`.

Esta decision no publica ni define un contrato sucesor. Cualquier evolucion
posterior requiere autoridad arquitectonica independiente.

## Invariantes

- no cambia la semantica publica aceptada de SPI v2;
- `OperationContext` conserva identidad, timeout, sesion y reloj ligados;
- outcomes y snapshots se validan antes de hacerse observables;
- cierre inmediato y async siguen siendo idempotentes y acotados;
- un finalizador no reentra en un lock no reentrante que ya posee el hilo;
- el engine externo conserva el ownership de sus recursos;
- cancelacion o fallo parcial no abandona cleanup ni sustituye el primer error.

## Consecuencias

Los engines que no hayan migrado dejan de importarse o construirse con 4.7.
Los consumidores v2 no cambian de contrato y ganan una frontera sin
negociacion ambigua. El catalogo de deprecaciones deja de anunciar simbolos ya
retirados y la guia de migracion pasa a ser la unica referencia operativa para
la superficie eliminada.

## Alternativas descartadas

- conservar un shim sin exportarlo: mantendria la doble semantica y haria falsa
  la afirmacion de que v2 es exclusivo;
- aumentar timeouts: no corrige reentrada ni elimina estados ambiguos;
- esperar a 5.0: contradice la decision explicita de producto para 4.7;
- publicar simultaneamente otro SPI: amplia el contrato sin una decision
  aceptada y queda fuera de esta release.

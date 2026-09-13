# Frontera del Engine SPI

## Responsabilidad

MongoEco 4.7 dispone de una sola frontera estable para engines: SPI v2.
`EngineSpiAdapter` es un componente interno que conecta la API y el kit de
conformidad con ese contrato; no selecciona versiones ni traduce shapes
alternativos.

```text
API publica / conformance
    -> EngineSpiAdapter
        -> declaracion EngineCapabilities v2
        -> validacion de outcomes y snapshots
        -> coordinacion de publicacion
        -> contrato Search tipado
        -> engine v2
```

La declaracion se resuelve una vez al construir la frontera. Una declaracion
ausente, de otro tipo o con una version distinta de `2` falla de inmediato. La
API no consulta metodos antiguos ni intenta deducir capacidades mediante
introspeccion de firmas.

## Invariantes compartidas

- `OperationContext` es la autoridad de una operacion ligada;
- los outcomes se validan antes de publicar cambios;
- un snapshot rechazado se descarta y su cleanup queda supervisado;
- cada cursor posee y cierra exactamente un snapshot;
- la entrega secuenciada confirma el checkpoint despues del callback;
- el engine externo conserva el ownership de sus propios recursos y MongoEco
  solo ejecuta el lifecycle declarado;
- los timeouts, cancelaciones y errores de cierre siguen siendo observables.

El lock que protege la preparacion del consumidor de cambios es reentrante: el
registro puede liberar objetos y disparar un finalizador en el mismo hilo. Los
callbacks y el cleanup externo no se ejecutan bajo un lock no reentrante de la
frontera.

## Variantes admitidas dentro de v2

Las variantes son capabilities, no versiones implicitas:

- `batch_inserts=False` usa la primitiva individual con ordinales derivados;
- `explicit_read_snapshots=False` usa `scan_find_semantics` y lo envuelve en un
  `ReadSnapshot` estable;
- `change_delivery` selecciona entrega inmediata, secuencia de commit u outbox;
- `search` habilita exclusivamente el request y los outcomes Search tipados.

## Ratchets

- el manifiesto publico declara solamente `engineSpi: [2]`;
- los engines integrados no exponen primitivas retiradas;
- una declaracion por shape se rechaza en el constructor del cliente;
- el canario externo importa solo `mongoeco.engines` y pasa conformance v2;
- las busquedas de compatibilidad forman parte del cierre de release.

La migracion incompatible esta documentada en
[Migracion a MongoEco 4.7](../migrating-to-4.7.md).

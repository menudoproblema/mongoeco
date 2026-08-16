# Arquitectura versionada de adapters de engine

## Problema

El adapter 4.x concentra seleccion de SPI, traduccion legacy, validacion de
outcomes, snapshots, Search y publicacion. Extraer SPI v1 eliminando tambien
validaciones comunes duplicaria invariantes en API o engines. Mantener ramas
`if spi_version` dentro de cada operacion haria que cada nueva version
multiplicara caminos.

## Solucion propuesta

La frontera se divide conceptualmente en cinco capas:

```text
API publica
    -> CanonicalEngineBoundary
        -> AdapterRegistry selecciona una vez
            -> SpiV1Adapter | SpiV2Adapter | SpiV3Adapter
        -> OutcomeValidator
        -> SnapshotValidator
        -> ChangePublicationCoordinator
        -> SearchContractAdapter
```

`CanonicalEngineBoundary` posee las invariantes que sobreviven a todas las
versiones: ownership, atomicidad observable, identidad de operacion,
validacion de outcomes/snapshots y publicacion. Los adapters traducen shapes,
pero no vuelven a implementar esas garantias.

## Reglas

- la version se resuelve una vez al construir la frontera;
- cada instancia usa un solo adapter;
- SPI v1 es la unica version que puede inferirse por shape;
- SPI v2/v3 requieren declaration tipada;
- la API publica no consulta metodos legacy;
- ningun adapter publica cambios antes de validar el outcome;
- Search se selecciona por `contract_version`, no por presencia de metodos;
- retirar un adapter no retira validators ni coordinadores comunes.

## Estructura futura

```text
mongoeco/engines/boundary.py
mongoeco/engines/validation/outcomes.py
mongoeco/engines/validation/snapshots.py
mongoeco/engines/publication.py
mongoeco/engines/adapters/v1.py
mongoeco/engines/adapters/v2.py
mongoeco/engines/adapters/v3.py
mongoeco/engines/adapters/registry.py
```

La estructura es objetivo, no una orden de mover archivos durante 4.6. La
extraccion debe hacerse por comportamiento, con tests caracterizadores, y no
como un cambio masivo de nombres.

## Ratchets

- allowlist AST exacta de vocabulario SPI v1;
- prohibicion de nombres v1 en `mongoeco.api`;
- conformance por adapter;
- fixture de engine defectuoso que demuestre que todas las versiones atraviesan
  los validators comunes;
- graph test que impida imports desde API hacia `adapters.v1`;
- diff de API publica antes y despues de cada extraccion.

## Secuencia de extraccion

1. congelar comportamiento actual con conformance v2 y tests negativos;
2. extraer validators sin cambiar imports publicos;
3. extraer publicacion y snapshots;
4. mover traduccion v1 a su modulo;
5. crear registry y adapter v2 explicito;
6. introducir v3 solo despues de que v2 siga verde sin ramas legacy;
7. retirar v1 en 5.0 eliminando una entrada del registry y su modulo.

Cada paso debe ser reversible y no debe modificar formatos SQLite.

# ADR-016 - Transicion cohesiva y observable hacia 5.0

## Estado

Aceptada como direccion de diseno. SPI v3 y `search-v2` siguen siendo
propuestas y no forman parte del contrato ejecutable de 4.6.

## Contexto

La serie 4.x estabilizo SPI v2, `OperationContext`, outcomes, snapshots y
`search-v1`, pero conserva tres fronteras de transicion:

- SPI v1 se detecta por shape y se traduce mediante `LegacyEngineAdapter`;
- los tipos `FindOperation`, `UpdateOperation` y `AggregateOperation` pueden
  existir ligados o no ligados y duplican dialecto, collation y `let` respecto
  a `OperationContext`;
- Search conserva aliases y previews 4.x junto al modelo canonico de metadata,
  planning y observabilidad.

Retirar solo nombres legacy reduciria superficie, pero mantendria estados
invalidos representables. Reescribir al mismo tiempo planner, backends o
producto aumentaria el riesgo de una major sin mejorar la frontera contractual.

## Alternativas evaluadas

### Major minima

Retirar SPI v1 y aliases Search, manteniendo SPI v2 y los tipos de operacion
actuales. Tiene menor coste inmediato, pero obliga a otra major para separar
specifications y operaciones ligadas. No elimina las fuentes duplicadas de
contexto.

### Major cohesiva

Introducir antes de la major SPI v3 y `search-v2` de forma opt-in, mantener una
ventana dual de conformidad y usar 5.0 para retirar solo contratos con una ruta
de migracion probada. Resuelve la causa estructural y permite medir la adopcion,
a costa de mantener adapters simultaneos durante la transicion.

### Major de expansion

Combinar la limpieza contractual con Atlas remoto, nuevos backends, Rust o una
reescritura general del planner. Puede producir mas novedades visibles, pero
mezcla riesgos, dificulta aislar regresiones y hace que rollback contractual y
rollback de producto sean inseparables.

## Decision

Se adopta la major cohesiva con una minor 4.x de convivencia si SPI v3 y
`search-v2` necesitan exposicion publica antes de 5.0:

1. 4.6 cierra compatibilidad, inventario, fixtures y herramientas de
   conformidad sin retirar contratos.
2. Una minor posterior puede publicar SPI v3 y `search-v2` como opt-in, sin
   modificar SPI v2 ni cambiar el default `search-v1`.
3. 5.0 retira SPI v1 y aliases Search 4.x solo si sus gates de migracion estan
   demostrados.
4. SPI v2 permanece estable y soportado mediante un adapter propio. Su retirada
   requiere otra decision, deprecacion observable y al menos un ciclo estable
   con SPI v3 disponible.
5. La major no incorpora nuevos backends, Atlas remoto, Rust ni un planner
   universal.

## Invariantes

- una version de SPI nunca cambia silenciosamente de significado;
- el engine recibe exclusivamente operaciones ligadas al contrato que declara;
- `OperationContext` es la unica autoridad semantica de una operacion ligada;
- `search-v1` y `search-v2` no mezclan shapes en un mismo resultado;
- cada adapter tiene conformance independiente;
- formatos SQLite 4.x siguen siendo legibles o disponen de una migracion
  explicita, transaccional y probada;
- el manifest de API y el catalogo de deprecaciones son gates, no documentacion
  informativa.

## Criterios de aceptacion

- SPI v3 y `search-v2` disponen de contratos normativos y fixtures consumidoras;
- Memory, SQLite y un canario externo pasan conformance de la version declarada;
- existe diff semantico de API entre la ultima 4.x y 5.0;
- ninguna referencia SPI v1 nueva aparece fuera de su frontera allowlisted;
- los diferenciales reales y la migracion SQLite estan verdes;
- la retirada se anuncia en el catalogo antes de ejecutarse.

## Consecuencias

La transicion dura mas que una retirada puntual, pero reduce el riesgo de
convertir 5.0 en una segunda fase de descubrimiento. El coste temporal de los
adapters duales compra evidencia de migracion y mantiene SPI v2 como contrato
honesto para engines externos.

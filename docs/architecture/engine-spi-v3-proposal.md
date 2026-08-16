# Propuesta de Engine SPI v3

## Estado y alcance

Este documento especifica una propuesta para una minor 4.x de transicion y
MongoEco 5.0. No publica SPI v3, no cambia SPI v2 y no autoriza retirar SPI v1.

## Causa raiz

Los tipos de operacion 4.x combinan specification, compilacion y binding. Un
`FindOperation` puede tener `context=None` o estar ligado, mientras conserva
campos `dialect`, `collation` y `let` que tambien viven en `OperationContext`.
Eso obliga a validaciones de coherencia, recompilacion condicional y parametros
opcionales en la frontera del engine. El problema no es un metodo concreto del
adapter: es que el type system no distingue estados del lifecycle.

## Alternativas

### Mantener un unico tipo con un flag

Es compatible, pero conserva estados invalidos y mueve el error a runtime.

### Hacer generico `Operation[Unbound | Bound]`

Reduce nombres, pero Python pierde parte de la discriminacion al serializar,
inspeccionar protocolos o implementar engines sin generics avanzados.

### Tipos distintos por estado

Hace imposible entregar una specification al engine por accidente y permite
protocolos simples. Tiene mas tipos publicos, pero la frontera queda explicita.

## Naming seleccionado

Los nombres conceptuales evaluados fueron `UnboundFindSpecification`,
`FindTemplate` y `FindSpecification`. Se seleccionan:

- `FindSpecification`;
- `UpdateSpecification`;
- `AggregateSpecification`;
- `BoundFindOperation`;
- `BoundUpdateOperation`;
- `BoundAggregateOperation`.

`Specification` ya expresa que el valor no esta ligado; el prefijo `Unbound`
seria redundante. La documentacion y tests deben usar tambien los nombres
largos `Unbound*Specification` como terminologia de estado, pero no como aliases
publicos. `Bound*Operation` conserva el estado en el nombre porque es la unica
forma que puede cruzar el engine.

## Modelo propuesto

```python
@dataclass(frozen=True, slots=True)
class FindSpecification:
    filter_spec: Filter
    projection: Projection | None
    sort: SortSpec | None
    skip: int
    limit: int | None
    hint: HintSpec | None
    planning_mode: PlanningMode

    def bind(self, context: OperationContext) -> BoundFindOperation: ...


@dataclass(frozen=True, slots=True)
class BoundFindOperation:
    specification: FindSpecification
    context: OperationContext
    selector_plan: QueryNode
    planning_issues: tuple[PlanningIssue, ...]
```

Update y aggregate siguen el mismo patron. Los datos que alteran compilacion
pertenecen a la specification sin normalizar o al contexto, nunca a ambos. Los
planes, array filters normalizados y pipelines compilados pertenecen solo al
valor ligado.

## Frontera de binding

La API publica realiza exactamente una vez:

1. validacion de argumentos y ownership defensivo;
2. creacion de `OperationContext` con reloj capturado;
3. normalizacion BSON, collation y bindings;
4. compilacion de filtro, update o pipeline;
5. construccion de la operacion ligada.

`bind()` es idempotente solo sobre la misma specification y el mismo contexto
por identidad. Una operacion ligada no ofrece `bind()`, `with_overrides()` ni
campos opcionales de contexto. Cambiar contexto exige volver a la specification
y producir otra operacion.

## OperationContext v3

`OperationContext` sigue siendo frozen y posee:

- dialecto;
- contexto de expresiones, reloj y `let`;
- codec;
- sesion;
- collation;
- politica de publicacion;
- identidad y ordinal de efecto.

Los mappings internos deben ser inmutables en profundidad. El engine no
consulta relojes, no normaliza argumentos y no reconstruye el contexto desde
campos del operation DTO.

## Capabilities y protocolo

SPI v3 usa una capability discriminada, no `spi_version >= 3` distribuido:

```python
EngineContract(version=EngineSpiVersion.V3, ...)
```

La seleccion de adapter se realiza una vez en la frontera de cliente mediante
un registro `{version: adapter_factory}`. El protocolo v3 recibe:

- `BoundFindOperation` para snapshots y lecturas;
- `BoundUpdateOperation` para CAS/update;
- `BoundAggregateOperation` para aggregation;
- outcomes tipados existentes o sus sucesores versionados;
- `ReadSnapshot` con el mismo `operation_id`.

No se permiten unions de retornos, `_return_outcome`, flags privados, `hasattr`
semantico ni callbacks legacy.

## Compatibilidad SPI v2

SPI v2 no se modifica. `SpiV2Adapter` traduce sus operaciones actuales hacia
la frontera canonica interna, valida outcomes y snapshots y conserva sus
capabilities. SPI v3 no hereda protocolos v2 ni monkeypatches sus clases.

Un engine puede publicar dos factories independientes durante la migracion,
pero una instancia declara una sola version. No se negocia metodo por metodo.

## Conformance v3

Perfiles propuestos:

- `spi-v3-binding`;
- `spi-v3-crud`;
- `spi-v3-snapshots`;
- `spi-v3-change-delivery`;
- `spi-v3-search-v1` o `spi-v3-search-v2`.

Las pruebas deben demostrar:

- no existe operacion ligada sin contexto;
- binding captura el reloj una vez;
- ningun engine renormaliza BSON;
- operation y snapshot comparten identidad;
- cancelacion y cleanup son exactos una vez;
- outcomes imposibles se rechazan en la frontera comun;
- capabilities parciales producen `not-applicable`, nunca falsos pass.

## Migracion de engines

1. pasar primero SPI v2 y su CLI de conformidad;
2. separar DTOs de entrada de los planes ejecutables internos;
3. implementar primitivas v3 sobre `Bound*Operation`;
4. declarar capabilities v3 sin modificar la instancia v2;
5. ejecutar perfiles v2 y v3 en paralelo;
6. retirar la factory v2 solo despues de una deprecacion posterior.

## Gates de implementacion

- ADR aceptada para el contrato exacto;
- snapshot de API revisado;
- fixtures mypy positivas y negativas;
- canario externo sin imports privados;
- adapters por version sin ramas distribuidas;
- paridad completa Memory/SQLite y sync/async;
- documentacion de migration y rollback.

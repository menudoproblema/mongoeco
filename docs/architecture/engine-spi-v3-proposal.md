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

La seleccion de adapter y la validacion del contrato se realizan una vez en la
frontera de cliente mediante un registro `{version: adapter_factory}`. El
resultado es un `EngineRuntime` estable, ligado a la identidad de la instancia
del engine y al lifecycle del cliente; no se reconstruye por coleccion ni por
operacion. El protocolo v3 recibe:

- `BoundFindOperation` para snapshots y lecturas;
- `BoundUpdateOperation` para CAS/update;
- `BoundAggregateOperation` para aggregation;
- outcomes tipados existentes o sus sucesores versionados;
- `ReadSnapshotV3` con el mismo `operation_id`.

No se permiten unions de retornos, `_return_outcome`, flags privados, `hasattr`
semantico ni callbacks legacy.

### Runtime preparado e identidad

SPI v3 distingue el contrato declarado, el runtime validado y el binding de
namespace. Son lifecycles diferentes:

```python
class EngineRuntime(Protocol):
    engine: object
    contract: EngineContract
    lifecycle_id: str

    def bind_namespace(
        self,
        namespace: CollectionNamespace,
    ) -> BoundEngineNamespace: ...

    async def aclose(self) -> None: ...


class BoundEngineNamespace(Protocol):
    runtime: EngineRuntime
    namespace: CollectionNamespace
```

- `EngineContract` es inmutable y describe capacidades, no estado operativo;
- `EngineRuntime` valida una sola vez la pareja engine/contrato y posee
  admision, observabilidad, entrega de cambios y obligaciones de cleanup;
- `BoundEngineNamespace` conserva identidad y metadatos de acceso reutilizables,
  pero no retiene por si mismo un snapshot, una sesion o una operacion;
- `Bound*Operation` sigue siendo inmutable y especifico de cada invocacion.

La cache de bindings pertenece al runtime o al cliente, usa como clave el
namespace y solo opciones que cambian la frontera del engine. `codec`, read y
write concern, preferencia y planning mode siguen perteneciendo a la API o a la
operacion salvo que la futura RFC los declare parte del contrato del engine.
No se usa una cache global por `id(engine)`, atributos inyectados en engines
externos ni weak references como sustituto de ownership: esos mecanismos no
definen de forma fiable cierre, reutilizacion entre clientes ni reciclado de
identidades.

El runtime se invalida de forma monotona al comenzar el cierre del cliente.
Ningun binding nuevo puede crearse desde ese punto; los bindings existentes no
prolongan el engine mas alla del runtime. `aclose()` agrega el cierre de leases,
outbox y recursos del engine y termina una sola vez. Dos clientes solo pueden
compartir runtime si una futura factory explicita devuelve el mismo owner y
declara esa politica; recibir la misma instancia de engine no basta para
inferirlo.

En 4.x puede reutilizarse internamente un `AsyncCollection` inmutable desde su
wrapper sync y evitar un `with_options()` redundante. Eso no constituye el
runtime v3, no se expone a engines externos y no cambia el ownership de SPI v2.

## Lifecycle, admision y presion de snapshots

SPI v3 debe separar tres conceptos que SPI v2 concentra en `ReadSnapshot`:

- la vista estable que retiene el cursor;
- la obligacion de liberar sus recursos fisicos;
- el permiso de capacidad que autoriza abrirla.

No se amplia la clase publica `ReadSnapshot` de SPI v2. SPI v3 introduce un
contrato sucesor, `ReadSnapshotV3`, y un coordinador de liberacion separado,
`SnapshotRelease`. La distincion es de ownership, no solo de forma: el cursor
puede solicitar cierre y esperar su resultado, pero la obligacion fisica y su
lease pertenecen al `EngineRuntime`.

```python
class SnapshotPurpose(StrEnum):
    READ = "read"
    AGGREGATION = "aggregation"
    TRANSACTION = "transaction"


class SnapshotRelease(Protocol):
    lifecycle: SnapshotLifecycle
    first_error: BaseException | None

    def request_close(self) -> None: ...

    async def wait_closed(
        self,
        *,
        timeout_seconds: float | None = None,
    ) -> None: ...


class ReadSnapshotV3(Protocol, AsyncIterator[Document]):
    operation_id: str
    policy: SnapshotPolicy
    purpose: SnapshotPurpose
    release: SnapshotRelease

    async def aclose(self) -> None: ...
```

`request_close()` es idempotente y no espera capacidad, locks ni I/O. En modo
`IMMEDIATE` puede completar la liberacion antes de retornar solo si el engine
acredita que no suspende, no ejecuta callbacks y no reentra en coordinacion del
runtime. En modo `ASYNC` entrega la obligacion al supervisor del
`EngineRuntime`. `wait_closed()` observa siempre esa misma obligacion; cancelar
o agotar el timeout del waiter no cancela el cleanup. `aclose()` es la
conveniencia comun que solicita y espera, no crea un segundo owner.

La implementacion MUST compartir una unica transicion y un unico resultado
entre `request_close()`, `aclose()`, cierre de cliente y finalizacion de cursor.
Dos llamadas concurrentes no ejecutan dos cleanups. Un finalizador best-effort
solo puede solicitar cierre; nunca espera ni ejecuta cleanup externo mientras
el caller conserva un lock de ejecucion. Esta separacion elimina la reentrada
que SPI v2 no puede prohibir sin cambiar su contrato publico.

El engine propietario mantiene un `SnapshotLease` desde antes de adquirir el
primer recurso fisico hasta que ese recurso queda realmente liberado. El lease
no se libera cuando vence el timeout del consumidor, cuando se cancela quien
espera ni cuando el lifecycle cambia a `FAILED`. Un cleanup fallido sigue
contando hasta que el engine acredita su recuperacion o termina su lifecycle.
Esto hace que el numero de obligaciones `OPEN + CLOSING + FAILED` quede acotado
por la admision, sin cancelar una limpieza ya iniciada para cuadrar un registro.

La capability forma parte del `EngineContract`; los limites efectivos
pertenecen a la instancia del engine. De este modo el adapter conoce la
semantica sin convertir una configuracion de despliegue en una version nueva
del SPI:

```python
class SnapshotSaturationMode(StrEnum):
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class SnapshotAdmissionPolicy:
    max_inflight_snapshots: int
    max_retained_bytes: int | None
    saturation: SnapshotSaturationMode = SnapshotSaturationMode.REJECT


class SnapshotCloseMode(StrEnum):
    IMMEDIATE = "immediate"
    ASYNC = "async"


@dataclass(frozen=True, slots=True)
class SnapshotLifecycleContract:
    close_mode: SnapshotCloseMode
    admission: SnapshotAdmissionPolicy


EngineContract(
    version=EngineSpiVersion.V3,
    snapshots=SnapshotLifecycleContract(...),
)
```

Cada lease identifica el proposito observable de la retencion mediante
`SnapshotPurpose`. La enumeracion no expone el algoritmo de almacenamiento.
Una copia completa,
una raiz persistente o un snapshot nativo del backend son implementaciones
validas si producen la misma estabilidad y contabilizan la retencion que
mantienen viva. HAMT, paginas WAL y conexiones pertenecen al engine, no al SPI.

`max_inflight_snapshots` debe ser positivo y finito. `max_retained_bytes` solo
se declara cuando el engine puede reservar una estimacion conservadora antes de
abrir; `None` significa que el contrato no acredita una cota por bytes, no que
el coste sea cero. Un pool fisico compartido puede aplicar una segunda admision
en su owner, pero no se crea un gestor global que mezcle engines y lifecycles.
La capability publicada contiene la politica efectiva y es inmutable durante
el lifecycle de la instancia. Cambiar limites exige crear otra instancia; no
puede alterar la semantica de cursores ya abiertos.

La admision incluye toda vista estable que pueda prolongar una generacion:
cursores de lectura, la vista raiz de una agregacion y snapshots de transaccion.
Los snapshots hijos de una misma `AggregationReadView` cargan su reserva al
lease raiz y no inflan el contador de vistas por separado. Una transaccion
reserva antes de fijar su generacion y conserva el lease hasta commit o abort
terminal. Un engine que no soporte transacciones no anuncia ni reserva ese
proposito.

Cuando se declara `max_retained_bytes`, la reserva conservadora incluye el coste
incremental atribuible a mantener la generacion viva: raices, caminos
versionados, buffers, cursores y recursos nativos que no se reclamarian sin el
snapshot. No se usa el tamaño logico completo de una base para penalizar dos
veces estructuras compartidas, ni se presenta el sharing como coste cero. Si el
engine no puede estimarlo antes de abrir, declara `None` y conserva admision por
numero. La telemetria separa reservas por `SnapshotPurpose` y expone total,
high-watermark y rechazos, sin revelar namespaces o payloads.

La reserva es atomica respecto a la capacidad y precede a la apertura. Si no
cabe, el primer inicio efectivo del cursor falla con
`SnapshotCapacityExceeded`; construir `find()` sigue siendo lazy. El error no
ha adquirido recursos, no consume el lease y es reintentable por el llamador.
Un fallo durante la apertura cierra lo ya adquirido antes de devolver el lease.
La cola o fairness de esperas no se oculta dentro del cursor. SPI v3 inicial
solo admite `REJECT`: la saturacion falla de forma inmediata y observable. Una
estrategia `WAIT`, su cancelacion y su fairness requeririan una capability
posterior; no pueden aparecer como otro valor aceptado por un adapter v3 que no
la conozca.

`SnapshotCapacityExceeded` forma parte del contrato publico v3 y expone, sin
referencias al recurso retenido:

- `operation_id` de la apertura rechazada;
- dimension limitante (`snapshots` o `retained_bytes`);
- limite configurado y reserva observada;
- `retryable=True`.

El error no promete un instante de reintento: solo cerrar o recuperar una
obligacion real puede liberar capacidad. Tampoco se reutilizan errores de
timeout, porque la admision no ha iniciado I/O ni ha esperado por capacidad.

El modo de cierre es discriminado, no inferido mediante `hasattr`:

- `IMMEDIATE` permite que `SnapshotRelease.request_close()` complete la
  liberacion y la transicion terminal antes de retornar. No puede ejecutar I/O,
  esperar un worker, adquirir el lock de ejecucion ni delegar cleanup posterior.
- `ASYNC` hace que `request_close()` transfiera la obligacion al
  `EngineRuntime`; `wait_closed()` y `ReadSnapshotV3.aclose()` solo esperan su
  resultado. El timeout limita cuanto espera el consumidor, no la
  responsabilidad del engine. Tras timeout el snapshot permanece `CLOSING` y
  el cleanup continua supervisado.

Ambos modos conservan cierre idempotente y exactamente una liberacion fisica.
Un error de cierre mantiene el primer error observable, transiciona a `FAILED`
y no se presenta como recurso liberado. Disconnect intenta cerrar todas las
obligaciones y agrega diagnosticos sin confirmar datos ni ocultar errores. El
engine expone contadores de leases por estado, rechazos, bytes reservados cuando
proceda, high-watermark y fallos de recuperacion; no expone objetos o payloads
retenidos.

Esta capacidad no se incorpora retroactivamente a SPI v2. La correccion 4.x
compatible conserva sus timeouts y supervision, deja de cancelar cleanups bajo
presion y puede usar un fast path interno solo para fuentes cuyo cierre
inmediato controla el propio engine. Los adapters v2 no inventan capacidad ni
prometen cotas que el engine externo no declara.

### Incorporacion propuesta en SPI v3

La incorporacion se divide en una unica capability gobernable, con cuatro
superficies coordinadas:

1. `EngineContract.snapshots` declara modo de cierre y politica de admision;
2. `open_read_snapshot()` reserva el lease y devuelve un snapshot que conserva
   su ownership hasta la liberacion fisica mediante `SnapshotRelease`;
3. `open_aggregation_read_view()` y el inicio transaccional cargan sus
   retenciones al mismo owner con su `SnapshotPurpose`;
4. la conformidad observa estados y contadores, sin acceder a tareas, locks ni
   payloads internos.

La primera entrega v3 debe incluir estas cuatro superficies juntas. Publicar el
error sin lease permitiria rechazos que no acotan obligaciones reales; publicar
el lease sin observabilidad impediria acreditar la cota; inferir el modo de
cierre conservaria la ambiguedad de SPI v2. No se crea un protocolo separado
de admision ni un manager global.

La entrega se ordena en cinco slices que no cambian el contrato v2:

1. publicar tipos v3, adapters por version y fixtures de typing, manteniendo
   `ReadSnapshot` y `ReadSnapshotV3` sin herencia entre ellos;
2. implementar `EngineRuntime`, `SnapshotLease` y `SnapshotRelease` con
   conformidad de reentrada, cancelacion, timeout, fallo y disconnect;
3. acreditar Memory y SQLite en `IMMEDIATE` o `ASYNC` segun su liberacion real,
   incluida la contabilidad por proposito;
4. incorporar `AggregationReadView` sobre el mismo owner y demostrar una unica
   generacion para fuente y namespaces foreign;
5. ejecutar canario externo, wheel instalado, diff de API y matrices de
   convivencia antes de habilitar una factory v3 fuera del repositorio.

Las slices 1 y 2 forman el minimo publicable: exponer tipos sin ownership
ejecutable permitiria un contrato nominal que conserva la causa de reentrada.
La slice 4 puede permanecer como capability `MATERIALIZED`, pero no puede
simular `STABLE_QUERY` mediante snapshots independientes.

Los defaults concretos de Memory y SQLite, y si ambos pueden acreditar una cota
por bytes, quedan como decisiones de la futura RFC normativa. La propuesta no
autoriza escogerlos durante la implementacion. El limite por numero, el modo
`REJECT` y la inmutabilidad por instancia si forman una unidad contractual.

## Vista estable de agregacion y acceso foreign

El core 4.x resuelve las colecciones referenciadas por `$lookup`, `$facet` y
`$unionWith` como listas completas. Sustituir esa lista por consultas SPI v2
independientes no es correcto: cada consulta puede abrir otra generacion y una
misma agregacion observaria estados incompatibles. SPI v3 debe transportar la
capacidad de consulta junto con el ownership de una unica vista estable.

La capacidad es explicita y discriminada en `EngineContract`; no se detecta
con `hasattr` ni se simula desde el adapter v2:

```python
class AggregationForeignAccess(StrEnum):
    MATERIALIZED = "materialized"
    STABLE_QUERY = "stable-query"


@dataclass(frozen=True, slots=True)
class AggregationReadContract:
    foreign_access: AggregationForeignAccess


class AggregationReadView(Protocol):
    operation_id: str

    def open_snapshot(
        self,
        namespace: CollectionNamespace,
        operation: BoundFindOperation,
    ) -> ReadSnapshotV3: ...

    async def aclose(self) -> None: ...
```

`open_aggregation_read_view()` recibe la `BoundAggregateOperation` y el
inventario de namespaces derivado del pipeline antes de adquirir recursos. La
vista debe cumplir estas invariantes:

- fuente principal y foreign reads observan la misma generacion confirmada o
  el mismo snapshot transaccional;
- cada `BoundFindOperation` hija conserva dialecto, collation, reloj, sesion y
  deadline del contexto padre; solo añade namespace, filtro y proyeccion;
- el engine decide si usa indice, scan o plan hibrido y devuelve documentos
  propios segun el contrato de `ReadSnapshot`;
- cerrar un snapshot hijo libera su cursor, pero no la generacion retenida por
  la vista; ningun hijo puede sobrevivir al cierre de la vista;
- el cierre de la vista es idempotente, cierra hijos restantes y libera una
  sola vez su lease fisico;
- admision contabiliza la vista raiz y toda reserva de cursores/buffers hijos
  antes de abrirlos. Un fallback no puede escapar del mismo lease;
- el planner puede usar `STABLE_QUERY` solo para formas cuya igualdad pueda
  expresar sin perdida. Collation, correlacion o tipos no cubiertos degradan a
  scan/materializacion sobre la misma vista.

El modo `MATERIALIZED` sigue siendo una capacidad v3 valida y explicita para
engines que no ofrecen consultas foreign estables. El adapter SPI v2 declara
siempre ese modo: no puede fabricar `STABLE_QUERY` abriendo snapshots
independientes. El core conserva entonces el hash acotado y el nested loop
vigentes. Esta degradacion cambia coste, no resultados, orden ni errores.

`explain()` debe separar capacidad y eleccion: publica el acceso foreign que
declara el engine, el plan candidato (`indexed-query`, `bounded-hash` o
`nested-loop`) y el motivo de fallback. No expone buckets, conexiones,
generaciones ni payloads privados.

Los defaults, la estimacion de reservas y si una vista multi-namespace puede
admitirse por bytes pertenecen a la futura RFC normativa. La propuesta fija la
coherencia y el lifecycle, pero no autoriza limites o errores nuevos en 4.x.

### Simulacion de estados

```text
capacity available -> RESERVED -> OPEN -> CLOSING -> CLOSED -> lease released
                            |         |          |
open failure ---------------+         |          +-> FAILED -> recovery/disconnect
                                      |
waiter timeout/cancel ----------------+  (cleanup keeps ownership)

capacity unavailable -> SnapshotCapacityExceeded (no resource, no lease)
```

Camino feliz, close repetido, cancelacion antes/durante de apertura, timeout,
fallo de cleanup, disconnect y reintento deben pasar conformidad. Una prueba de
presion debe abrir hasta el limite, demostrar el rechazo siguiente, cerrar una
obligacion realmente y solo entonces admitir otra. Ninguna prueba puede cerrar
el gate observando solo que una tarea desaparecio de un registro.

### Disposicion de piezas

| Pieza | Requisito | Mecanismo vigente | Disposicion |
| --- | --- | --- | --- |
| Lifecycle y primer error | Terminalidad observable | `ReadSnapshot` SPI v2 | `derive` en `ReadSnapshotV3`; no heredar la clase |
| Timeout del waiter | Espera finita | `close_timeout_seconds` | `reuse` |
| Cancelacion del cleanup por overflow | Limitar tareas visibles | Registro interno acotado | `remove`: rompe liberacion |
| `SnapshotLease` | Acotar obligaciones reales antes de adquirir | No existe | `justify` |
| `SnapshotRelease` | Separar solicitud, espera y ownership fisico | `ReadSnapshot.aclose()` concentra los tres | `justify` |
| `SnapshotPurpose` | Contabilizar lectura, agregacion y transaccion sin exponer internals | No existe | `justify` |
| Persistencia estructural publica | Evitar copias O(N) en un engine concreto | Raices HAMT privadas en Memory | `remove`: el SPI gobierna coste observable, no estructuras |
| `SnapshotSaturationMode.REJECT` | Evitar espera oculta y no acotada | Timeout de cleanup, no de admision | `justify` |
| Modo de cierre discriminado | Evitar task para fuentes realmente inmediatas | Siempre se crea task | `justify` |
| Protocolo de admision separado | Exponer capacidad | `EngineContract` ya agrega capabilities | `consolidate` en lifecycle |
| Consultas foreign SPI v2 independientes | Evitar materializar foreign | Cada scan puede observar otra generacion | `remove`: viola coherencia |
| `AggregationReadView` | Consulta filtrada sobre una vista multi-namespace | No existe | `justify` |
| Gestor global de recursos | Unificar capacidad | No existe | `remove`: owners y lifecycles distintos |

La incorporacion normativa requiere una RFC `Proposed` y su auditoria antes de
publicar SPI v3. Mongoeco aun no dispone de catalogo registrado en el sistema
RFC comun; este documento conserva por tanto el diseño como propuesta no
aceptada y no sustituye esa materializacion ni su autorizacion humana.

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
- `spi-v3-aggregation-read-view`;
- `spi-v3-change-delivery`;
- `spi-v3-search-v1` o `spi-v3-search-v2`.

Las pruebas deben demostrar:

- no existe operacion ligada sin contexto;
- binding captura el reloj una vez;
- ningun engine renormaliza BSON;
- operation y snapshot comparten identidad;
- todos los caminos de cierre comparten un unico `SnapshotRelease` y el mismo
  primer error;
- solicitar cierre desde un finalizador o mientras existe coordinacion activa
  no espera locks ni ejecuta cleanup externo inline;
- cancelacion y cleanup son exactos una vez;
- la admision limita obligaciones reales y rechaza antes de adquirir recursos;
- lectura, agregacion y transaccion contabilizan su retencion en el owner y
  proposito correctos;
- dos vistas estructuralmente compartidas reservan el coste incremental de
  retencion sin presentarlo como cero ni duplicar el tamaño logico completo;
- el error de saturacion identifica dimension, limite, reserva y operacion sin
  filtrar el recurso retenido;
- fuente y foreign queries de una agregacion comparten generacion y contexto;
- un engine sin `STABLE_QUERY` usa materializacion y no abre consultas
  independientes como falsa emulacion;
- cerrar la vista cierra hijos y libera una sola vez su lease;
- un timeout o cancelacion del waiter no cancela el cleanup;
- `IMMEDIATE` no crea tareas ni suspende; `ASYNC` conserva supervision en el
  runtime hasta terminalidad;
- cambiar configuracion exige otra instancia y no altera leases abiertos;
- outcomes imposibles se rechazan en la frontera comun;
- capabilities parciales producen `not-applicable`, nunca falsos pass.

## Migracion de engines

1. pasar primero SPI v2 y su CLI de conformidad;
2. separar DTOs de entrada de los planes ejecutables internos;
3. implementar `ReadSnapshotV3` y `SnapshotRelease` sin reutilizar subclases v2;
4. implementar primitivas v3 sobre `Bound*Operation`;
5. declarar capabilities v3 sin modificar la instancia v2;
6. ejecutar perfiles v2 y v3 en paralelo;
7. retirar la factory v2 solo despues de una deprecacion posterior.

## Gates de implementacion

- ADR aceptada para el contrato exacto;
- snapshot de API revisado;
- fixtures mypy positivas y negativas;
- canario externo sin imports privados;
- adapters por version sin ramas distribuidas;
- paridad completa Memory/SQLite y sync/async;
- documentacion de migration y rollback.

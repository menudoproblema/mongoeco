# Evolución del Engine SPI: mantener v2 y aparcar v3

## Decisión vigente

**2026-10-17: mantener SPI v2 y aparcar SPI v3**, por acuerdo explícito del
usuario tras completar T1–T6 y S1/S2. Este documento posee la decisión actual,
su justificación y las condiciones para volver a estudiarla. No se fija una
entrega 5.0, una deprecación ni una ventana de retirada de v2.

SPI v2 sigue siendo el único contrato publicado. Se conservan API, protocolos,
exports, capacidades y archivos SQLite. No se añaden requests paralelas,
traducciones v2/v3, selección de versión ni un programa de migración anticipado.
Las mejoras internas se eligen por su valor para v2; no son fases obligatorias
para llegar a un sucesor.

Los trabajos ya completados tienen sus propios owners de diseño y evidencia:

- [Preparación T1–T6](engine-preparation-proposal.md) y su
  [validación](../engine-preparation-validation.md).
- [Frontera S1/S2](spi-v2-boundary-proposal.md) y su
  [validación](../engine-boundary-validation.md): selección atómica acreditada,
  canario corregido y ocho entradas del adapter tipadas hasta su coordinador.

## Beneficio residual frente a coste de transición

Una representación canónica podría retirar campos públicos paralelos y la
libertad de suministrar un plan que no corresponda a su filtro. Ese beneficio
requiere controlar la construcción y el ownership, no solo agrupar los mismos
argumentos en una request. Retirar constructores, campos u overrides actuales
rompería v2; mantenerlos mediante traducción conservaría parte de la complejidad.

La conversión normal de lectura ya reutiliza plan y contexto. Suprimir el DTO
intermedio no equivale a evitar otra compilación. Las dos entradas de planning
de Memory/SQLite ya convergen en el mismo planner. Los resultados T1–T6
acreditan menos preparación en SQLite, sin una mejora general de latencia.

El estado final tras retirar v2 debe compararse por separado de la convivencia:
esta necesita despacho, traducciones, conformidad y documentación para ambos
contratos. Una retirada directa exige migración de motores, cuyo impacto externo
no está censado suficientemente. Tampoco desaparecen las guardas de outcomes,
imágenes, secuencias, snapshots, cleanup o selección atómica.

La preparación y la ejecución tienen vidas distintas: `rewind()` conserva la
operación y renueva la semántica que contiene el deadline. Un objeto preparado
único no autoriza a fusionar esas fases ni a cambiar el inicio de los errores.

## Simplificaciones de v2 revisadas

La [sonda reproducible](../evidence/spi-v2-maintenance/observe.py) y sus
[resultados](../evidence/spi-v2-maintenance/observations.json) fijan las fuentes
actuales. El censo AST distingue llamadas directas de referencias a métodos
pasados como callback; no demuestra ausencia de consumidores externos ni de
dispatch dinámico. Son observaciones previas a la implementación. El encargo
posterior autoriza R1/R2 y la corrección de regex; su
[registro de implementación](../engine-read-validation.md) posee estado y gates.
Las demás candidatas continúan diferidas, sin un trabajo de transición a v3.

| Candidata | Evidencia previa y valor propio | Disposición y aceptación necesaria |
| --- | --- | --- |
| Tipar el recorrido de lectura del cursor | `_base_operation()` devuelve `FindOperation`, pero `_operation_with_overrides`, `_base_semantics` y `_semantics_with_overrides` devuelven `Any` al consumidor estático. La apertura de lectura se repite en `_scan` y `_fetch_batch` | **R1 implementada; gates en su registro.** Productores, caché y apertura tipados; elección de ruta compartida con lookup único. Conserva proveedor de colección, fallback, momento de validación y `AsyncIterable`. Regresiones de ventana, lotes, cleanup, contexto y renovación de deadline; fixtures negativos desde el cursor |
| Retirar el wrapper de scan y la entrada cruda de count SQLite | `_iter_scan_documents_sync` solo se referencia desde 14 llamadas de tests. Count recibe semántica preparada desde su única referencia productiva; tiene tres llamadas directas desde tests | **R2 implementada; gates en su registro.** Las 14 llamadas de tests ejercitan el reader productivo con tracking y conservan cierre, interrupción y ownership. Wrapper de scan y entrada cruda de count retirados, sin cambios de SPI ni formato SQLite |
| Reducir campos redundantes de `EngineUpdateSemantics` | Existe un único constructor productivo. Las trazas anteriores no consumen sus dos filtros crudos; los planes sí se utilizan | **Diferir.** El ahorro previsto son referencias, no copias profundas. Antes de retirarlos, revisar accesos dinámicos y cubrir update/pipeline/replacement/upsert y delete por separado. Conservar el contexto y sus guardas |
| Preparar el selector original una vez por operación | El selector crudo sigue viajando por v2 y se prepara por candidato | **Aparcar hasta medir coste.** Una capability aditiva también exige contrato, fallback y dos rutas. No añadir caché global, estado lateral ni mover la revalidación fuera del lock |
| Publicar un evaluador para motores | El matcher de lectura ya es alcanzable por `compiled_query`, pero no cubre toda la ejecución | **Aparcar hasta acreditar uso.** Aclarar opcionalidad y estabilidad del contrato transitivo antes de ampliar exports. No exige por sí mismo v3 |

El primer bloque prolonga el beneficio de S2 en un recorrido que perdía
tipos. El segundo retira una adaptación mantenida para tests. Ninguno necesita
una nueva jerarquía de operaciones ni promete una mejora de rendimiento. Su
implementación no compromete las candidatas diferidas.

En el cursor, las derivaciones actuales solo ajustan `skip` y `limit`. Un helper
privado con esos parámetros tipados cierra esos puntos de llamada sin restringir
el `with_overrides` público ni añadir otro DTO. El retorno de apertura conserva
`AsyncIterable[Document]`, ya declarado por la colección; no se impone que todas
las fixtures devuelvan un `ReadSnapshot` concreto.

La sonda inicial de tipado muestra un miembro inexistente rechazado al recibir
`FindOperation` y aceptado al recibir `Any` con la comprobación ordinaria.
Con `--strict`, llamar a `_base_semantics` produce `no-untyped-call`, pero los
helpers de overrides, con parámetros anotados y retorno sin anotar, siguen
permitiendo el miembro inexistente sobre `Any`. La aceptación exige tipos de
retorno comprobados y fixtures negativos; activar strict en el consumidor no
basta. No se ha acreditado un error de resultados de aplicación en esas rutas.

## Condiciones para reconsiderar un sucesor

Exigir conjuntamente, mediante una propuesta nueva:

1. Una garantía necesaria para un autor de motor que no pueda darse de forma
   razonable con v2 y sus mecanismos aditivos.
2. Uso real reproducible documentado anónimamente; el canario no acredita demanda.
3. Un prototipo que mida estados, autoridades y conversiones eliminados frente
   al v2 ya consolidado, incluyendo el coste de sus nuevas piezas. Separar
   convivencia y estado final; no atribuir velocidad a la reducción de DTOs.
4. Censo de impacto y migración/retirada explícitas, con conformidad independiente
   y conservación de errores, plazos, atomicidad, ownership y datos SQLite.

No hay un compromiso de completar más refactorizaciones antes de reabrir v3.
La ausencia de esa evidencia mantiene vigente la decisión de continuar con v2.

## Antecedentes y alcance de la actualización

La revisión 1 no fue aceptada ni implementada. La
[síntesis de los tres informes](spi-proposal-review.md) llevó a A mínima;
el [texto original y sus hashes](../evidence/spi-v3-proposal/revision-1.json)
se conservan sin modificación.

[ADR-017](decisions/ADR-017-spi-v2-exclusivo-en-4.7.md) sigue siendo la autoridad
del SPI exclusivo. [ADR-018](decisions/ADR-018-propuesta-operaciones-preparadas-spi-v3.md)
conserva su ID y estado documental de propuesta revisada como antecedente;
la decisión posterior registrada aquí no es una aceptación retrospectiva de su
revisión original ni una auditoría independiente. El acuerdo inicial actualizó
documentación y análisis; la autorización posterior de R1/R2 consta en su
registro. No se publica una versión ni se retoma v3 mediante esos bloques.

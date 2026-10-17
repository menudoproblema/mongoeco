# Propuesta: acreditar y tipar la frontera SPI v2

## Estado y recomendación

**S1/S2 aprobados, implementados y verificados.** Fecha:
2026-10-17. El cuerpo conserva el análisis previo y sus criterios de aceptación;
el [registro de implementación](../engine-boundary-validation.md) distingue los
resultados actuales de las sondas históricas.
La [decisión posterior](engine-spi-v3-proposal.md) mantiene v2 y aparca v3;
las siguientes mejoras candidatas se evalúan allí por su valor para el contrato
actual, sin convertir S1/S2 en una transición de versión.
La búsqueda parte del árbol con T1–T6, no de un HEAD limpio. La base Git sigue
siendo `738fd0eb5ece0adabf071e312ee0ff7b82b8589d`; las
[sondas y hashes](../evidence/spi-v2-next/probes.json) identifican las fuentes
inspeccionadas y las contrastan con el wheel T1–T6 instalado, SHA-256
`5f6c56ae3290c8fb334402b2613a612d2b00905f99c0196d8f1ac080153efc2e`.

Recomiendo **dos bloques compatibles con v2**: acreditar la selección por
identidad más selector, y tipar ambos extremos del adapter existente. Son
mejoras de garantías y mantenibilidad que también servirían a un SPI sucesor.
Su valor no depende de llegar a publicar v3.

No recomiendo otra jerarquía de requests ni una extensión de ejecución
preparada en esta entrega. T1–T6 ya redujeron preparación sin acreditar una
mejora general de latencia. Añadir otra frontera exige una garantía diferencial
y evidencia de uso que todavía no tenemos.

## Búsqueda y hechos comprobados

Se recorrieron productores de operaciones, contexto, protocolos y exports,
adapter, coordinador, lecturas/planning, ambas mutaciones de Memory y SQLite,
helpers SQL, conformance y canario. El censo incluye referencias desde
`src`, `tests`, `scripts`, `benchmarks` y `examples`, mediante búsqueda textual
y AST. El paquete de impacto es temporal; no se inspeccionan otros repositorios
ni se identifican consumidores.

Las [sondas reproducibles](../evidence/spi-v2-next/probe.py) usan el artefacto
instalado y bases en memoria. La
[validación de autoría](../evidence/spi-v2-next/validation.json) registra comandos,
resultados y límites. No constituye una auditoría independiente.

| Hallazgo | Evidencia y alcance | Consecuencia |
| --- | --- | --- |
| H1 — Conformance no acredita la selección completa | El canario obtiene 5 checks correctos y 5 no aplicables. En cuatro casos adicionales, update/delete ignoran la identidad `b` al recibir un selector y afectan a `a`. Los ocho controles equivalentes de Memory/SQLite pasan | La siguiente mejora prioritaria es del contrato probado. No es un defecto demostrado de las fachadas de Memory/SQLite |
| H2 — Ocho entradas variádicas | Update, delete, insert, batch insert, merge, snapshot, get y count aceptan `*args` o `**kwargs`. Se buscan payloads por nombre o posición y se transportan opciones como `object` | La frontera valida resultados en runtime, pero permite errores evitables de wiring antes de llegar al motor |
| H3 — El tipado se pierde también en el consumidor | `_engine_spi` del coordinador se infiere como `Any \| None`; tras excluir None queda `Any`. Una llamada imposible al planner pasa mypy a través de ese atributo, aunque directamente el método tipado produce tres errores | Tipar solo las firmas del adapter deja abierta la otra mitad de la frontera |
| H4 — Hay deuda estática concreta | Una comprobación adicional de adapter/coordinador encuentra 23 errores en dos módulos. No es el gate público que ya pasó en T1–T6 | El bloque de tipado debe cerrar esa deuda y tener controles negativos; añadir casts sin comprobar llamadas no basta |
| H5 — Existe un matcher de lectura accesible | `EngineFindSemantics.compiled_query.match(...)` discrimina correctamente tenant y collation. El consumidor puede acceder por el DTO recibido, sin importar el evaluador privado | La ausencia de un evaluador general no equivale a imposibilidad técnica de reutilizar toda lectura. Este tipo transitivo merece revisión contractual |
| H6 — Hay limpieza privada posible | `_iter_scan_documents_sync` tiene 14 referencias, todas en tests. Count conserva una unión filtro/semántica, pero su único consumidor productivo pasa semántica preparada. Dos filtros de `EngineUpdateSemantics` no se leen en los escenarios trazados | Son opciones de limpieza local; no justifican un protocolo nuevo ni prometen reducir copias profundas |
| H7 — Persisten asimetrías auxiliares | La operación declara dialecto 8. En ambos motores, una llamada directa de borrado sin el `operation_context` exigido por v2 borra un valor undefined si omite el override de dialecto; con 8 explícito no lo borra | Reproducción retenida. No acredita un fallo de un caller v2 válido ni una divergencia de la fachada. Requiere resolver el contrato de esa conveniencia antes de cambiarlo |

H1 se produce porque el canario emplea
`selector_filter or operation.filter_spec`: el selector sustituye al objetivo.
La garantía que debe comprobarse es que **ambos** correspondan al documento
modificado bajo la misma frontera atómica. El adapter actual comprueba imágenes,
contexto y contadores; no vuelve a interpretar la selección para verificar al
motor, lo que duplicaría el intérprete.

Fuentes principales: [canario](../../tests/consumer/engine_canary.py),
[conformance](../../src/mongoeco/conformance/runner.py),
[adapter](../../src/mongoeco/engines/adapter.py),
[coordinador](../../src/mongoeco/api/_async/_collection_runtime.py),
[semánticas](../../src/mongoeco/engines/semantic_core.py) y
[matcher](../../src/mongoeco/core/compiled_query.py).

H5 también corrige una lectura demasiado fuerte de los informes iniciales:
un símbolo no reexportado no es automáticamente ajeno al contrato si resulta
alcanzable a través de un campo público. `compiled_query` puede ser None y su
matcher solo filtra: no aplica proyección, sort, paginación, plazos ni mutaciones.
No se acredita aquí un ejecutor completo ni la estabilidad de cada método del
objeto transitivo. Tampoco se propone retirarlo como si fuera inaccesible.

## Bloque S1 — Conformidad de identidad y selector

Ampliar el check **existente** `spi-v2-atomicity:compare-and-set`; conservar ID,
perfil, esquema de informe, capacidades y firmas. Los casos añadidos usarán
igualdad simple y `$set`, ya ejercitados por el núcleo. No impondrán una gramática
nueva a todos los motores.

Probar update y delete con dos documentos:

1. Ambos cumplen el selector; solo la identidad `b` puede resultar afectada.
2. `a` cumple el selector y `b` no: resultado cero, sin imágenes ni efectos.
3. Selector ausente y selector vacío: identidad conservada.
4. Tras cambiar la condición o repetir una eliminación: no-match, sin saltar a `a`.
5. Competidores sobre identidad y condición separadas: un ganador, contadores e
   imágenes del ganador y ningún efecto sobre el otro documento.

Corregir el canario combinando las dos condiciones. Mantener su intérprete
mínimo, capacidades y límites. Añadir un motor deliberadamente incorrecto en
los tests del runner: debe fallar ese check. Así se prueba que el nuevo caso
detecta el defecto, además de conseguir que el fixture correcto pase.

**Compatibilidad:** un motor que ya cumple la selección conserva el resultado.
Un motor antes verde que ignore la identidad podrá fallar la conformidad nueva;
se documentará como detección de incumplimiento de la garantía existente. No se
presentará ese cambio de veredicto como una nueva versión del SPI ni se
suavizarán expectativas para conservar todos los verdes anteriores.

**Aceptación:** los cuatro fallos reproducidos del canario desaparecen; los
controles incorrectos fallan por la causa prevista; Memory/SQLite pasan;
identidades, imágenes, contadores y ausencia de efectos se comprueban por estado
final independiente. El canario sigue siendo un consumidor del subconjunto
documentado, no una acreditación de compatibilidad MongoDB completa.

## Bloque S2 — Firmas explícitas y tipos de extremo a extremo

Modificar las ocho entradas del **mismo** `EngineSpiAdapter`. Sustituir sus
argumentos abiertos por nombres y tipos del SPI vigente y conservar los callbacks
locales del adapter. No mantener una segunda entrada variádica de compatibilidad
privada ni introducir requests para empaquetar los mismos argumentos.

- Namespace, operación/semántica, documentos, flags y callback quedan tipados.
- `operation_context` es un keyword requerido; su entrada puede admitir None
  para validar los DTO v2 opcionales, pero se rechaza en runtime antes del motor,
  como ahora. El motor recibe siempre el contexto validado.
- `_engine_spi` del coordinador se declara `EngineSpiAdapter`, reutilizando el
  adapter del cliente. No queda `Any` ni una unión con None en ese atributo.
- Reutilizar los protocolos existentes para CRUD y batch. La selección de
  fallback sigue dependiendo de capacidades declaradas. Batch conserva entradas
  secuenciales; solo adapta el contenedor a lista cuando lo exige la primitiva,
  reutilizando las listas existentes y sin renormalizar el BSON.
  Las llamadas CRUD quedan comprobadas contra esos protocolos; no se tipa solo
  la firma para después despachar todo por `Callable[..., Any]`.
- Las guardas de contexto reciben el payload explícito. Se puede retirar
  `_call_argument` cuando ya no tenga consumidores; no se sustituye por otro
  extractor genérico de diccionarios.
- Mantener guardas runtime de presencia, outcomes, imágenes, secuencias,
  cardinalidad, snapshots y cleanup. El tipado no acredita el cuerpo de un
  motor externo. Conservar las llamadas y fases válidas del SPI v2.
- Mantener publicación posterior al retorno para `change_delivery='none'`,
  las rutas secuenciadas/outbox y los ordinales del fallback batch. No reenviar
  callbacks locales a primitivas cuyo protocolo no los declara.

La comprobación de los dos módulos se incorpora sin relajar la ya existente:

```bash
python -m mypy --follow-imports=silent --ignore-missing-imports \
  -m mongoeco.engines.adapter \
  -m mongoeco.api._async._collection_runtime
```

**Aceptación estática:** 23 errores de línea base → cero, con la misma
comprobación y sin nuevos ignores. No se exige convertir toda la biblioteca a
strict. Los fixtures positivos cubren las ocho entradas y sus callbacks; los
negativos rechazan operación equivocada, keyword desconocido, contexto de tipo
incorrecto, contexto omitido, flags y replacement inválidos, tanto en el adapter
como a través del campo del coordinador. None explícito se cubre como error
runtime. No se añaden `Any` o casts para ocultar esos parámetros de control;
los valores BSON conservan sus tipos de documento vigentes.

**Aceptación de comportamiento:** los motores observan los mismos argumentos
válidos; los errores de firma y contexto se rechazan antes de invocar al motor;
callbacks se emiten una vez en su fase actual. La validación de outcomes sigue
siendo posterior al retorno del motor, como en v2. No se cambian planes,
capturas de reloj, deadlines,
coerciones de resultados, diagnósticos públicos ni selección bajo lock.

## Adopción crítica y piezas descartadas

| Pieza / opción | Garantía y mecanismo actual | Beneficio frente a coste; disposición |
| --- | --- | --- |
| Conformance de selección | Primitivas atómicas, dos filtros y outcomes existentes | **consolidate/reuse, adoptar S1.** Cierra un hueco reproducido; no crea protocolo ni gramática |
| Adapter y protocolos actuales | Validación y despacho existentes, con ocho entradas abiertas | **consolidate, adoptar S2.** Prevención estática observable; añade detalle de firmas y deuda de anotaciones. No se promete menos líneas ni más velocidad |
| Contexto, guardas, snapshots y outcomes | Ya protegen identidad, recuperación, imágenes y ownership | **reuse.** Retirarlos por tener firmas tipadas perdería garantías ante motores dinámicos |
| Nuevas requests internas o v3 detrás de un wrapper v2 | Los DTO existentes ya llegan a la frontera | **remove de este plan.** Mantendría los DTO y añadiría traducciones; los estados externos v2 seguirían siendo representables |
| Capability de selector preparado | V2 transmite el selector crudo; se prepara por candidato para revalidarlo | **remove del corte; justificar antes de reabrir.** Sería una extensión pública, con fallback y dos rutas que mantener. No tenemos demanda de autor de motor ni ganancia de latencia acreditada que compense su coste |
| Meter ambos selectores en el plan actual | Ya existen plan de identidad y selector independiente | **remove, no adoptar.** Debe preservar también BSON observado por motores que lo interpretan, selección SQL, orden de errores, hints y CAS. La equivalencia lógica no acredita equivalencia de ejecución ni compatibilidad del subconjunto |
| Caché por operation_id o estado lateral en contexto | Semánticas locales y contextos ya delimitan cada intento | **remove.** Añadiría invalidación, retención y un transporte implícito para evitar ampliar el contrato explícito |
| Publicar un evaluador o `matches` nuevo | Existe acceso transitivo al matcher de lectura | **derive del mecanismo existente; justificar una garantía pública nueva antes de adoptarla.** Primero acreditar uso y aclarar alcance, opcionalidad y contrato transitivo. Un helper aditivo podría resolverlo en v2; no es por sí mismo argumento para v3 |
| Retirar campos paralelos públicos | Constructors/overrides v2 y sus guardas los admiten | **reuse.** Retirarlos rompe v2; convertirlos en propiedades sin conservar constructor, replace y overrides también puede romperlo |
| Quitar dos referencias internas de filtros | `EngineUpdateSemantics` tiene un único productor; las trazas no consumen esos campos | **reuse por ahora.** Ahorra dos referencias, no copias profundas ni un intérprete. No merece mezclar otra modificación de shape con S1/S2 |
| Retirar helpers/overloads privados de lectura SQLite | El reader productivo ya recibe semántica preparada | **reuse por ahora; limpieza independiente diferida.** Es compatible y potencialmente útil, pero exige migrar 14 usos de tests conservando sus pruebas de cierre, interrupción y ownership. Eliminar esos tests para ahorrar código sería una pérdida |
| Uniformar borrados sin contexto | Hay conveniencias adicionales a la firma obligatoria v2 | **reuse hasta decidir su contrato.** La reproducción H7 no autoriza a cambiar clocks o defaults de llamadas válidas por analogía |

S1 aporta evidencia del contrato; S2 reduce la posibilidad de introducir errores
de wiring. Ninguno prueba la corrección de toda expresión o de todo motor.
Una versión nueva conservaría los mismos huecos si transportase operaciones por
`Any` y se acreditase solo con el canario actual.

## Plan acotado y cierre

1. **S1 primero:** runner de conformance, canario y regresiones focales. Añadir
   oráculos de selección equivalentes con MongoDB 7/8/9 al corpus; observar
   documentos e identidades además de códigos y contadores. La primitiva SPI de
   dos filtros se contrasta con la condición combinada del comando MongoDB,
   no con una firma de servidor inexistente.
2. **S2 después:** adapter/coordinador, fixtures de tipado y gate CI de esos dos
   módulos. Limitar las anotaciones adicionales a cerrar ese recorrido; no
   iniciar una conversión general de tipos ni otro sistema de validación.
3. **Cierre sobre un candidato único:** wheel instalado, sync/async,
   Memory/SQLite, replay del corpus existente y nuevo, errores, efectos parciales
   y reutilización; conformance, API/typing/lint, fixtures SQLite 4.5/4.10.1 y
   global final de Python 3.13/3.14/3.15 con el mínimo de cobertura vigente.
   El trabajo concurrente de proxy/pytest se conserva y su integración debe
   constar en la identidad del candidato. La global aislada de T1–T6 no acredita
   por sí sola ese árbol combinado.

El régimen local no exige una RFC nueva para este corte: conserva ADR-017,
firmas y capacidades v2, IDs y esquema de conformance, API y almacenamiento.
El resultado de autoría fue `no_rfc_required`; el encargo posterior del usuario
autoriza la implementación ordinaria de S1/S2. Su estado y evidencia se mantienen
en el registro de implementación, sin cambiar el lifecycle de ADR-018. Una
extensión pública de ejecución preparada requeriría su propia decisión
contractual.

## Condiciones para volver a considerar v3

Después de verificar S1/S2, el usuario acuerda continuar con v2. La
[decisión vigente](engine-spi-v3-proposal.md) posee la justificación, las
candidatas compatibles pendientes y las condiciones para reabrir v3 mediante
una propuesta nueva. La implementación de estos bloques queda cerrada y no
compromete más refactorizaciones ni una entrega 5.0.

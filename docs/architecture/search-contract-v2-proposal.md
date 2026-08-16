# Propuesta de contrato local `search-v2`

## Estado

Propuesta no ejecutable. `search-v1` sigue siendo el unico contrato y el default
de toda la serie 4.x hasta que una minor de transicion publique un opt-in
explicito.

## Causa raiz

`search-v1` estabilizo metadata, collectors, planning y explain, pero conserva
aliases de la evolucion experimental: inyeccion automatica de
`searchHighlights`, previews y contadores planos como `matchedCount`. Mantener
esas formas dentro del modelo canonico impide hacer obligatoria la provenance y
permite trazas ejecutadas sin evidencia de contexto o snapshot.

El problema no se resuelve ocultando campos en serializacion: v1 y v2 necesitan
contratos discriminados para que un resultado nunca mezcle shapes.

## Alternativas

### Limpiar `search-v1` in place

Romperia 4.x y haria que capabilities iguales describieran resultados
distintos. Se descarta.

### Parametros booleanos por alias

Produce combinaciones no testeables y fragmenta conformance. Se descarta.

### Contrato versionado completo

Mantiene una unica semantica por version y permite convivencia y diff. Es la
opcion seleccionada.

## Shape v2

`search-v2` conserva:

- metadata fuera del documento BSON;
- acceso explicito mediante `$meta`;
- `RuntimeDocumentState` interno;
- planner inmutable y estrategia reference;
- metricas tipadas por dominio, exactitud y disponibilidad;
- degradaciones con codigo estable;
- `engineDetails` para evidencia fisica;
- paridad Memory/SQLite y sync/async.

Retira:

- inyeccion automatica de `searchHighlights`;
- `countPreview`;
- `facetPreview`;
- `highlightPreview`;
- `matchedCount`;
- trazas ejecutadas sin `operationId`;
- trazas ejecutadas sin snapshot capturado.

Highlight solo aparece cuando una expresion o proyeccion solicita
`$meta: "searchHighlights"`. Copiar ese valor lo convierte en BSON ordinario;
no solicitarlo no crea un campo virtual publico.

Collectors se serializan bajo una unica seccion estable. Count distingue total
y lower bound mediante un discriminante. Facets usan nombres, buckets tipados y
orden BSON determinista. Ningun preview actua como alias adicional.

## Observabilidad v2

`queryPlanner` puede representar una operacion no ejecutada. Toda traza con
estado ejecutado exige:

- contrato `search-v2`;
- `operationId` no vacio;
- identidad de snapshot o evidencia `snapshotCaptured=true`;
- fases coherentes con residual y collectors;
- todas las metricas canonicas, incluso como `not-available`;
- ausencia de campos planos legacy.

`queryMatchedCount` sustituye `matchedCount` y conserva dominio `query`.
`returnedHitCount`, `collectorDocumentCount` y `pipelineOutputCount` no son
intercambiables. Los validadores rechazan relaciones imposibles.

## Convivencia en 4.x

- `search-v1` permanece default;
- v2 se selecciona por una opcion publica y tipada, nunca por inspeccion de
  fields;
- capabilities declaran exactamente un contrato por instancia;
- `explain()` muestra `contractVersion`;
- el adapter rechaza request y capability de versiones distintas;
- conformance ejecuta perfiles v1 y v2 por separado;
- una misma operacion no puede emitir aliases v1 y shape v2.

Memory y SQLite pueden compartir el semantic core, pero serializadores,
validadores y fixtures de contrato permanecen versionados.

## Migracion

| v1 | v2 |
| --- | --- |
| campo virtual automatico `searchHighlights` | proyeccion `$meta` explicita |
| `countPreview` | collector `count` discriminado |
| `facetPreview` | collectors `facets` nombrados |
| `highlightPreview` | metadata highlight solicitada |
| `matchedCount` | metrica `queryMatchedCount` |
| trace legacy sin contexto | trace ligada a operation y snapshot |

Los consumidores deben activar v2 en tests, ejecutar ambos perfiles de
conformidad y comparar sus expectativas de forma intencional. No deben usar un
normalizador que borre diferencias.

## Gates

- schema o manifest versionado del resultado y explain;
- fixture de migracion v1/v2 con diferencias esperadas;
- tests de no mezcla de shapes;
- properties optimized/reference;
- matrices de provenance por stage;
- Memory/SQLite y sync/async;
- canario externo;
- una minor estable de convivencia antes de retirar aliases.

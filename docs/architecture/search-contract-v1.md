# Contrato local `search-v1`

## Alcance

`search-v1` define la semantica estable del runtime local de `$search`,
`$searchMeta` y highlight. La sintaxis es Atlas-like dentro del subset
declarado, pero el resultado y la ejecucion pertenecen a MongoEco.

La fuente de verdad de operadores sigue siendo
`core/_search_contract.py`. Este documento fija los invariantes que no deben
depender del backend fisico.

La propuesta [search-v2](search-contract-v2-proposal.md) no altera este
contrato ni esta activa en 4.6. Una futura minor debera ofrecer convivencia
opt-in antes de que 5.0 pueda retirar aliases v1.

## Frontera de ejecucion

Una operacion Search se compila una vez y cruza el engine como un request
inmutable. El request conserva:

- query compilada e indice solicitado;
- modo `hits` o `metadata`;
- ventana de resultados y filtro downstream seguro;
- el mismo `OperationContext` que posee el cursor.

El outcome separa hits, metadata, trace y estadisticas. Un modo `metadata` no
necesita construir documentos de salida. Ningun engine puede volver a leer el
reloj, reconstruir `let`, sustituir collation ni abrir otro snapshot para los
collectors de la misma operacion.

Los operadores textuales con varias palabras usan matching any-term en todo el
runtime: ranking, matching semantico y traduccion FTS5 comparten esa regla. Un
backend solo puede declarar un candidato exacto cuando conserva esa semantica.

## Count

`total` devuelve un total exacto. `lowerBound` devuelve:

- el total exacto cuando no supera el threshold;
- el threshold solicitado como cota inferior cuando existe al menos un match
  adicional;
- `exact`, `threshold` y `cappedByThreshold` como metadata local explicita.

Sin otros collectors, el engine puede detenerse tras `threshold + 1` matches.
Una facet que requiera el conjunto completo impide esa terminacion temprana.

## Facets

Las facets de `search-v1` son facets de terminos tipados:

- missing y `null` no forman bucket;
- arrays y rutas anidadas se recorren con la misma semantica de paths Search;
- un valor repetido en un mismo documento cuenta una sola vez;
- datetime se normaliza a UTC con precision BSON de milisegundos;
- ObjectId y UUID conservan sus tipos BSON publicos;
- tipos incompatibles con la facet se ignoran;
- los buckets se ordenan por count descendente y despues por una clave BSON
  canonica estable;
- `numBuckets` se aplica despues de ordenar.

La facet simple legacy y el collector de facets nombradas comparten la misma
definicion tipada. `includeMeta` anade cobertura local; no cambia los buckets.

MongoEco no declara todavia facets numericas o temporales por intervalos
Atlas. `number` y `date` siguen siendo facets locales por valor.

## Highlight

Highlight representa pasajes derivados del valor original, no una copia
truncada desde su inicio. Los offsets se expresan en indices de code points
Python sobre ese valor.

Generan spans las clausulas positivas `text`, `phrase`, `autocomplete`,
`wildcard` y `regex`. En `compound`, `filter` y `mustNot` nunca generan
highlight. Los spans solapados se fusionan antes de seleccionar pasajes.

La metadata viaja en un `RuntimeDocumentState` separado del documento BSON.
`$meta: "searchHighlights"` resuelve esa metadata tipada sin depender de un
nombre privado. Durante 4.x, `searchHighlights` se resuelve como campo virtual
cuando no existe un campo real con ese nombre. El valor virtual se materializa
solo en la salida publica; no se inserta en el payload persistible ni puede
ocultar datos del usuario.

Los nombres legales usados por implementaciones anteriores, incluidos los
prefijos `__mongoeco_*`, son datos ordinarios del usuario y nunca se eliminan
al leer, proyectar o ejecutar `$merge`.

La provenance se transforma explicitamente aunque un stage anide, divida o
combine documentos. `$unwind` rebasa el estado; `$lookup` y `$unionWith`
mantienen scopes independientes; `$group`, buckets y expresiones explicitas
materializan solo los valores que producen. `$merge` descarta el alias
generado, conserva una sobrescritura real y persiste una copia explicita bajo
otro nombre. `$unset` elimina tambien el campo virtual.

El namespace privado con NUL solo permanece en el adapter SPI v1 deprecado. Se
convierte al entrar y salir de esa frontera y nunca es la representacion
canonica del runtime ni alcanza persistencia.

## Orden y optimizaciones

Toda optimizacion del primer stage Search debe demostrar que conserva el
dominio observable de la operacion completa:

- `$searchMeta` materializa metadata y nunca recibe limites o filtros del
  dominio posterior de hits;
- el `filter` interno de `$vectorSearch` es pre-top-k, pero un `$match`
  posterior sigue siendo post-top-k y no puede cambiar los candidatos;
- highlight, count y facets se calculan sobre los hits Search anteriores al
  pipeline; un filtro downstream no se adelanta cuando puede alterar esa
  metadata;
- un limit puede convertirse en hint solo si los stages intermedios preservan
  cardinalidad y no existe un collector que necesite el conjunto completo;
- `$merge` desactiva shortcuts de hits para no escribir un conjunto parcial.

Estas decisiones se construyen una vez en el cursor y se reutilizan al
ejecutar, explicar y materializar prefijos. El plan inmutable declara efectos
de cardinalidad, orden, metadata y writeback, reglas aplicadas, rechazos y
ownership por fase. Cada stage declara tambien su dominio:

- `document` para filtros y transformaciones independientes por documento;
- `stream` para ventanas que dependen de la posicion;
- `full-set` para sort, group, facets, buckets y stages que necesitan observar
  el conjunto completo;
- `writeback` para `$merge`;
- `unknown` para shapes que obligan a ejecucion completa.

Las dependencias de metadata se detectan por referencias semanticas exactas,
incluido `$meta`, nunca por substrings del nombre de un campo. El plan distingue
ejecucion completa, ventana directa, prefijo iterativo y salida vacia. Memory y
SQLite no pueden reinterpretar por separado la posicion semantica de un filtro
downstream.

Las pruebas de equivalencia pueden seleccionar un modo `reference` interno que
desactiva todos los shortcuts y ejecuta el pipeline completo. El oracle usa
semillas reproducibles y compara salida, orden, metadata, collectors, errores,
writeback y eventos. Las metricas se comparan solo dentro del mismo dominio:
un top-k puede reducir candidatos e hits emitidos sin cambiar matches de query
ni la salida final.

## Explain

`queryPlanner` describe compilacion, backend, candidatos, collectors,
highlight, residuals y degradaciones sin ejecutar la consulta.
`executionStats` ejecuta exactamente una vez y anade conteos observados. Un
indice `PENDING` puede explicarse con `queryPlanner`, pero `executionStats`
falla en vez de fabricar una traza. `queryMatchedCount`, `returnedHitCount`,
`downstreamFilteredCount`, `candidateCount`, `documentsScanned`,
`collectorDocumentCount` y `pipelineOutputCount` representan dominios
distintos. `collectorCount` cuenta collectors configurados; no cuenta
documentos procesados por ellos. Cada metrica canonica esta siempre
representada y declara dominio, exactitud, origen y disponibilidad; ausencia
de evidencia se serializa como `unavailable`, nunca como cero ni omision.

Cada ejecucion declara las fases `query`, `residual-filter` y `collector` que
realmente ocurrieron, el `operationId` y `executionContext.bound` /
`snapshotCaptured`. Las rutas internas ejecutadas no pueden publicar una traza
sin contexto ni snapshot. El constructor legacy 4.x admite que callers
anteriores omitan esa evidencia; queda deprecado y se retirara en 5.0.0.
`matchedCount` se conserva como alias 4.x del match de query cuando ese valor
puede observarse. Las previews 4.x se derivan de la misma ejecucion. El explain
conserva dialecto, collation, `let` y reloj del `OperationContext`.

El shape comun incluye `contractVersion="search-v1"`. Los detalles fisicos
propios de Memory o SQLite viven bajo un nodo del engine. Toda degradacion que
afecte coste o exactitud es observable; no existe fallback silencioso.

`countPreview`, `facetPreview` y `highlightPreview` son aliases deprecados de
compatibilidad 4.x. No forman parte del modelo extensible y se retiraran en
5.0.0.

## Matriz normativa

| Caso | Resultado requerido |
| --- | --- |
| Campo missing o `null` en facet | No crea bucket |
| Valor repetido dentro de un array | Cuenta una vez por documento |
| Empate entre buckets | Orden BSON canonico estable |
| Datetime con offset equivalente | Mismo bucket UTC/milisegundo |
| `lowerBound` sin facets | Puede terminar en `threshold + 1` |
| `lowerBound` con facets | Recorre lo necesario para completar facets |
| Highlight Unicode | Offsets sobre code points del valor original |
| Highlight en `mustNot` o `filter` | No genera spans |
| Campo real `searchHighlights` | Nunca se sobrescribe |
| Campo legal con prefijo `__mongoeco_*` | Se preserva como dato del usuario |
| `$vectorSearch` seguido de `$match` | El match se aplica despues de top-k |
| Metadata runtime atravesando un stage estructural | Sigue reglas tipadas de provenance |
| Highlight generado seguido de `$merge` | No persiste el alias inyectado |
| Highlight proyectado y copiado a otro campo | Persiste solo la copia explicita |
| `$limit: 0` despues de Search | Valida Search y retorna cero hits |
| Indice pendiente con `executionStats` | Falla sin publicar traza sintetica |
| Query textual con varios terminos | Coincide si aparece cualquier termino |
| Explain `queryPlanner` | Cero ejecuciones y cero materializacion |
| Explain `executionStats` | Una ejecucion, un snapshot y contadores tipados |

## Diferencias conscientes con Atlas

- el backend es local y no un deployment Atlas;
- las facets actuales son por valor, no collectors completos por intervalos;
- las extensiones `exact`, `threshold`, `cappedByThreshold` e `includeMeta`
  son metadata local;
- las previews existen solo por compatibilidad 4.x;
- las capacidades reales se exponen en compat y explain, sin inferir soporte
  a partir de similitud sintactica.

# Contrato local `search-v1`

## Alcance

`search-v1` define la semantica estable del runtime local de `$search`,
`$searchMeta` y highlight. La sintaxis es Atlas-like dentro del subset
declarado, pero el resultado y la ejecucion pertenecen a MongoEco.

La fuente de verdad de operadores sigue siendo
`core/_search_contract.py`. Este documento fija los invariantes que no deben
depender del backend fisico.

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

La metadata viaja como sidecar interno. `$meta: "searchHighlights"` es la
frontera sin colisiones. Su namespace contiene NUL y por tanto no puede
colisionar con un campo BSON persistido. Durante 4.x se conserva la inyeccion
automatica legacy cuando el documento no contiene ya `searchHighlights`; si el
campo existe, se preserva el valor del usuario, se omite solo la inyeccion
legacy y la metadata generada sigue disponible mediante `$meta`.

Los nombres legales usados por implementaciones anteriores, incluidos los
prefijos `__mongoeco_*`, son datos ordinarios del usuario y nunca se eliminan
al leer, proyectar o ejecutar `$merge`.

## Explain

`queryPlanner` describe compilacion, backend, candidatos, collectors,
highlight, residuals y degradaciones sin ejecutar la consulta.
`executionStats` ejecuta exactamente una vez y anade conteos observados. El
explain conserva dialecto, collation, `let` y reloj del `OperationContext`.

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
| Query textual con varios terminos | Coincide si aparece cualquier termino |
| Explain `queryPlanner` | Cero ejecuciones y cero materializacion |
| Explain `executionStats` | Una unica ejecucion y un unico snapshot |

## Diferencias conscientes con Atlas

- el backend es local y no un deployment Atlas;
- las facets actuales son por valor, no collectors completos por intervalos;
- las extensiones `exact`, `threshold`, `cappedByThreshold` e `includeMeta`
  son metadata local;
- las previews existen solo por compatibilidad 4.x;
- las capacidades reales se exponen en compat y explain, sin inferir soporte
  a partir de similitud sintactica.

# TODO

Estado actual del repo:

* fases históricas cerradas;
* capítulo `mongomock` cerrado y sin backlog operativo abierto;
* runtime local de `search` cerrado en su perímetro actual;
* backlog restante ya no es de cierre de fases, sino de evolución del producto.
* ya no se mantiene un `FOLLOWUPS.md` separado: los huecos de producto viven en
  `MISSING_FEATURES.md` y el backlog amplio/roadmap vive aquí.

La deuda arquitectónica activa y las fronteras que se vigilan ya no se
describen aquí. Su referencia canónica vive ya repartida en
`docs/architecture/`, especialmente en `api-surfaces.md`,
`storage-engines.md` y `testing-and-compatibility.md`.

## 1. Search: siguiente nivel

Objetivo: ampliar búsqueda local solo donde el siguiente salto aporte valor real, sin desdibujar el cierre del tier textual local ya documentado.

Pendientes:

* decidir si merece la pena ir más allá del tier textual local ya cerrado hacia features Atlas-like avanzadas, sobre todo:
  * `facet`
  * `highlight`
  * `count`
  * semántica más rica de `autocomplete`
  * semántica más rica de `wildcard`
  * opciones/flags más ricos de `regex`
* ampliar mappings locales más allá de:
  * `string`
  * `token`
  * `autocomplete`
  * `number`
  * `date`
  * `boolean`
  * `objectId`
  * `uuid`
  * `embeddedDocuments`
* decidir si el backend ANN local actual de `vectorSearch`, ya con
  ampliacion adaptativa de candidatos antes del fallback exacto, es suficiente
  o si necesita una siguiente fase mas ambiciosa;
* mejorar `explain()` de search si aparecen nuevos backends u operadores.

## 2. Rendimiento y planificación

Objetivo: mejorar throughput y coste operativo sin reabrir arquitectura base.

Pendientes:

* seguir mejorando pushdown/planning en SQLite;
* revisar si compensa otra vuelta sobre fallback/materialización en agregación;
* estudiar mejoras adicionales de concurrencia/throughput en SQLite si el objetivo pasa de paridad funcional a ambición más cercana a producción;
* optimizar `vectorSearch` si deja de ser suficiente el backend ANN local
  actual o si los filtros post-candidato pasan a exigir una politica mas rica;
* seguir endureciendo el planner físico si aparecen consultas mixtas donde el pushdown parcial actual no baste.

## 3. Consolidación selectiva

Objetivo: subir señal y confianza en las zonas todavía más débiles.

Pendientes:

* subir cobertura selectiva en:
  * `src/mongoeco/core/search.py`
  * `src/mongoeco/engines/virtual_indexes.py`
  * `src/mongoeco/driver/transports.py`
  * `src/mongoeco/engines/sqlite.py`
* seguir apurando fidelidad BSON rara y semántica fina de comparación solo cuando haya casos reales o diferenciales que lo justifiquen;
* reforzar contraste diferencial con MongoDB real si se quiere aumentar confianza observable más allá de la paridad práctica actual.

## 4. Publicación y producto

Objetivo: dejar el proyecto preparado para una primera release pública, si se decide publicar.

Pendientes:

* revisar packaging y metadatos de distribución;
* seguir endureciendo documentación pública de uso, alcance y posicionamiento;
* decidir política de compatibilidad y de versiones;
* seguir enriqueciendo la metadata pública por operación en CXP/compat,
  especialmente en:
  * `read`
  * `write`
  * `aggregation`
* decidir si compensa publicar un adaptador CXP live opcional sobre `mongoeco`
  ahora que:
  * capabilities, operations y profiles ya salen del catálogo canónico
  * existe proyección de telemetría del driver al vocabulario `db.client.*`
  * y la responsabilidad de negotiation / lifecycle no tiene por qué vivir en
    el runtime base;
* decidir qué hacer con:
  * `benchmarks/`
  * `mongoeco-rs/`
  * `MEMORIES.md`
* seguir ampliando ejemplos públicos reales si aparecen nuevos flujos
  representativos del producto.

## Anotaciones de estado

Ya no forman parte del backlog pendiente básico:

* pipeline-style updates en su subset documentado;
* `$merge` como stage terminal local de agregación;
* `$densify` y `$fill` en su subset local documentado;
* `currentOp` / `killOp` locales;
* `vectorSearch` local con backend ANN `usearch`, fallback exacto, `filter` y
  similitudes `cosine`, `dotProduct` y `euclidean`.
* projection avanzada de `find` en el subconjunto diario (`$slice`,
  `$elemMatch`, proyección posicional, `$meta: "textScore"`);
* `$collStats` como stage inicial de agregación local;
* índices `hidden` como metadata administrativa local.

## 5. Backends y extensibilidad

Objetivo: abrir líneas nuevas de producto, ya fuera del backlog histórico.

Pendientes:

* valorar si tiene sentido un SDK o contrato más explícito para backends terceros;
* estudiar si un backend SQL adicional (`DuckDB`, `PostgreSQL`) compensa ahora que el contrato interno está más limpio;
* evaluar si el runtime de driver debe seguir creciendo como producto propio o quedarse como soporte local suficiente.
* valorar si el provider CXP live debe vivir:
  * dentro de `mongoeco` como capa opcional muy separada
  * en un paquete/adaptador externo
  * o no existir mientras baste con catálogo, profiles y telemetría proyectada

## 6. Referencia futura: Rust

Esto queda explícitamente fuera del alcance actual, pero sí debe seguir visible como dirección futura.

Pendientes futuros:

* evaluar un backend Rust a grano grueso, nunca documento a documento;
* priorizar, si se aborda, fronteras como:
  * `scan/filter/sort` completos
  * comparación BSON por lotes
  * ejecución de runtime intensivo sobre colecciones grandes
* evitar una integración Python/Rust con llamadas por documento, porque el coste de cruce puede anular la mejora;
* decidir si `mongoeco-rs/` será:
  * backend opcional
  * librería separada
  * o núcleo futuro del motor embebido

## 7. Criterio de prioridad

Orden recomendado a partir de aquí:

1. ampliar `search` solo si abre valor real de producto;
2. subir cobertura selectiva en `search` / `virtual_indexes` / `sqlite` / `transports`;
3. preparar publicación si el objetivo es sacar versión;
4. dejar Rust como línea futura, no como trabajo inmediato.

## 8. Pensando 4.0

No es objetivo inmediato, pero sí conviene ir observando qué justificaría una
`4.0.0` de verdad.

Líneas a vigilar:

* consolidar `mongoeco` como runtime claramente CXP-first, no solo alineado;
* decidir si la metadata pública por operación llega al punto de sustituir
  parte del relato legacy de `compat`;
* evaluar si `$search` y `vectorSearch` pasan de subset útil a superficie de
  producto claramente diferenciada;
* decidir si un provider CXP live opcional merece vivir como adaptador externo
  o como capa separada dentro del proyecto;
* reservar una `4.0.0` solo para un cambio real de centro de gravedad, no para
  seguir ampliando subset local dentro de `3.x`.

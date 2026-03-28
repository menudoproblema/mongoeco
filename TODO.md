# TODO

Estado actual del repo:

* fases históricas cerradas;
* capítulo `mongomock` cerrado:
  * `872` casos inventariados
  * `73` `covered`
  * `701` `equivalent`
  * `98` `outside-scope`
  * `0` `review-needed`
* runtime local de `search` cerrado en su perímetro actual;
* backlog restante ya no es de cierre de fases, sino de evolución del producto.

## 1. Search: siguiente nivel

Objetivo: ampliar el runtime local de búsqueda más allá del perímetro actual sin degradar la honestidad del contrato.

Pendientes:

* ampliar el subset de `$search` más allá de `text` y `phrase`;
* decidir si merece la pena soportar operadores adicionales como:
  * `autocomplete`
  * `wildcard`
  * `near`
* ampliar mappings locales más allá de:
  * `string`
  * `token`
  * `autocomplete`
* decidir si `vectorSearch` sigue siendo suficiente en modo experimental o si necesita backend dedicado;
* mejorar `explain()` de search si aparecen nuevos backends u operadores.

## 2. Rendimiento y planificación

Objetivo: mejorar throughput y coste operativo sin reabrir arquitectura base.

Pendientes:

* seguir mejorando pushdown/planning en SQLite;
* revisar si compensa otra vuelta sobre fallback/materialización en agregación;
* estudiar mejoras adicionales de concurrencia/throughput en SQLite si el objetivo pasa de paridad funcional a ambición más cercana a producción;
* optimizar `vectorSearch` si deja de ser suficiente la fuerza bruta local;
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
* preparar documentación pública de uso y alcance;
* decidir política de compatibilidad y de versiones;
* decidir qué hacer con:
  * `benchmarks/`
  * `mongoeco-rs/`
  * `MEMORIES.md`
* revisar si conviene añadir una guía corta de:
  * alcance soportado
  * diferencias deliberadas frente a MongoDB real
  * diferencias deliberadas frente a `mongomock`

## 5. Backends y extensibilidad

Objetivo: abrir líneas nuevas de producto, ya fuera del backlog histórico.

Pendientes:

* valorar si tiene sentido un SDK o contrato más explícito para backends terceros;
* estudiar si un backend SQL adicional (`DuckDB`, `PostgreSQL`) compensa ahora que el contrato interno está más limpio;
* evaluar si el runtime de driver debe seguir creciendo como producto propio o quedarse como soporte local suficiente.

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

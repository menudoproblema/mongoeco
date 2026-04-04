# Missing Features

Este fichero lista solo capacidades de producto que siguen pendientes. La deuda
arquitectonica y las decisiones de endurecimiento del diseno viven ya en
`docs/architecture/`, no aqui.

| Prioridad | Nombre                                                            | Impacto     | Esfuerzo    | Refs                                                              |
| --------- | ----------------------------------------------------------------- | ----------- | ----------- | ----------------------------------------------------------------- |
| 7         | Ampliar `$text` clasico mas alla del subset local actual          | Alto        | Alto        | `search.py:252`<br>`core-runtime.md:82`                           |
| 10        | Opciones avanzadas de indice adicionales a `hidden`              | Medio-Alto  | Medio       | `admin_parsing.py:92`<br>`types.py:1492`                          |
| 11        | Ampliar `$search` mas alla de `text`/`phrase`/`autocomplete`/`wildcard`/`exists`/`in`/`equals`/`range`/`near`/`compound` | Medio-Alto  | Medio       | `search.py:126`<br>`TODO.md:10`                                   |
| 12        | Updates con posicional legado `$`                                 | Medio-Alto  | Medio       | `update_paths.py:135`<br>`compat_catalog_snapshot.md:27`          |
| 14        | Llevar `vectorSearch` mas alla del backend ANN local actual y su fallback exacto | Medio-Alto  | Alto        | `README.md:20`<br>`search.py:187`<br>`TODO.md:25`                 |
| 18        | `codec options` avanzadas                                         | Medio       | Medio       | `types.py:1244`                                                   |
| 19        | Window functions avanzadas en `$setWindowFields`                  | Medio       | Medio-Alto  | `test_aggregation_expression_basics.py:684`                       |
| 20        | `$redact` en agregacion                                           | Medio-Bajo  | Medio-Alto  | `stages.py:256`                                                   |
| 21        | Stages admin/introspeccion en agregacion                          | Bajo        | Medio       | `stages.py:256`                                                   |
| 23        | Query top-level `$where`                                          | Bajo        | Bajo-Medio  | `test_query_plan.py:72`<br>`test_query_plan.py:243`               |

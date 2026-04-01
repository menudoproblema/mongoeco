# Missing Features

| Prioridad | Nombre                                                            | Impacto     | Esfuerzo    | Refs                                                              |
| --------- | ----------------------------------------------------------------- | ----------- | ----------- | ----------------------------------------------------------------- |
| 6         | Query geoespacial e indexacion asociada                           | Alto        | Alto        | `test_query_plan.py:259`<br>`compat_catalog_snapshot.md:25`       |
| 7         | Full-text clasico: `$text` y `$meta` / `textScore`                | Alto        | Alto        | `compat_catalog_snapshot.md:25`<br>`test_projections.py:139`      |
| 8         | Updates por pipeline de agregacion                                | Alto        | Medio-Alto  | `operators.py:136`<br>`test_async_api.py:4336`                    |
| 9         | Proyeccion avanzada en `find`: posicional y operadores de proyeccion | Medio-Alto  | Medio       | `projections.py:13`<br>`test_projections.py:129`                  |
| 10        | Opciones avanzadas de indice                                      | Medio-Alto  | Medio       | `admin_parsing.py:92`<br>`types.py:1492`                          |
| 11        | Ampliar `$search` mas alla de `text` y `phrase`                   | Medio-Alto  | Medio       | `search.py:126`<br>`TODO.md:10`                                   |
| 12        | Updates con posicional legado `$`                                 | Medio-Alto  | Medio       | `update_paths.py:135`<br>`compat_catalog_snapshot.md:27`          |
| 13        | `$merge` en agregacion                                            | Medio-Alto  | Alto        | `stages.py:256`                                                   |
| 14        | Hacer `vectorSearch` algo mas que un runtime experimental         | Medio-Alto  | Alto        | `README.md:20`<br>`search.py:187`<br>`TODO.md:25`                 |
| 15        | `$collStats` como stage de agregacion                             | Medio       | Bajo-Medio  | `database_commands.py:51`<br>`stages.py:256`                      |
| 16        | Stages analiticos `$densify` y `$fill`                            | Medio       | Medio-Alto  | `stages.py:256`<br>`compat_catalog_snapshot.md:29`                |
| 18        | `codec options` avanzadas                                         | Medio       | Medio       | `types.py:1244`                                                   |
| 19        | Window functions avanzadas en `$setWindowFields`                  | Medio       | Medio-Alto  | `test_aggregation_expression_basics.py:684`                       |
| 20        | `$redact` en agregacion                                           | Medio-Bajo  | Medio-Alto  | `stages.py:256`                                                   |
| 21        | Stages admin/introspeccion en agregacion                          | Bajo        | Medio       | `stages.py:256`                                                   |
| 22        | `$geoNear` como stage de agregacion                               | Bajo-Medio  | Muy Alto    | `stages.py:256`                                                   |
| 23        | Query top-level `$where`                                          | Bajo        | Bajo-Medio  | `test_query_plan.py:72`<br>`test_query_plan.py:243`               |

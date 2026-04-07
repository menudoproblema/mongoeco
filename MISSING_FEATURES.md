# Missing Features

Este fichero lista solo capacidades de producto que siguen pendientes. La deuda
arquitectonica y las decisiones de endurecimiento del diseno viven ya en
`docs/architecture/`, no aqui.

| Prioridad | Nombre                                                            | Impacto     | Esfuerzo    | Refs                                                              |
| --------- | ----------------------------------------------------------------- | ----------- | ----------- | ----------------------------------------------------------------- |
| 7         | Ampliar `$text` clasico mas alla del subset local actual (weights, `\$language`, flags sensibles y compound text indexes mixtos) | Alto        | Alto        | `search.py:252`<br>`core-runtime.md:82`                           |
| 10        | Opciones avanzadas de indice mas alla del subset local actual (`hidden`/`partialFilterExpression`/TTL) | Medio-Alto  | Medio       | `_types/indexes.py:109`<br>`admin_parsing.py:84`                 |
| 11        | Ir mas alla del subset avanzado local de `$search` hacia paridad Atlas-like real (`searchMeta`, collector `facet`, highlighting mas rico, count meta, semantica mas rica de `autocomplete`/`wildcard`/`regex`) | Medio-Alto  | Medio-Alto  | `search.py:126`<br>`TODO.md:10`                                   |
| 11.5      | Ampliar mappings locales de `$search` mas alla de `string`/`token`/`autocomplete`/`number`/`date`/`boolean`/`objectId`/`uuid`/`document`/`embeddedDocuments` | Medio-Alto  | Medio       | `search.py:29`<br>`TODO.md:29`                                    |
| 14        | Llevar `vectorSearch` mas alla del retrieval hibrido local actual | Medio-Alto  | Alto        | `README.md:20`<br>`search.py:187`<br>`TODO.md:25`                 |
| 18        | `codec options` avanzadas                                         | Medio       | Medio       | `types.py:1244`                                                   |
| 19        | Window functions avanzadas restantes en `$setWindowFields` (`derivative`, `integral`) | Medio       | Medio-Alto  | `test_aggregation_expression_basics.py:745`                      |
| 20        | `$redact` en agregacion                                           | Medio-Bajo  | Medio-Alto  | `stages.py:256`                                                   |
| 21        | Stages admin/introspeccion en agregacion                          | Bajo        | Medio       | `stages.py:256`                                                   |

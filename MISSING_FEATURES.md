# Missing Features

Este fichero lista solo capacidades de producto que siguen pendientes. La deuda
arquitectonica y las decisiones de endurecimiento del diseno viven ya en
`docs/architecture/`, no aqui.

| Prioridad | Nombre                                                            | Impacto     | Esfuerzo    | Refs                                                              |
| --------- | ----------------------------------------------------------------- | ----------- | ----------- | ----------------------------------------------------------------- |
| 11        | Ir mas alla del contrato local estable `search-v1` hacia paridad Atlas-like remota/distribuida (analyzers, collectors y highlighting de backend) | Medio-Alto  | Alto  | `docs/architecture/search-contract-v1.md`<br>`TODO.md:10` |
| 11.5      | Ampliar mappings locales de `$search` mas alla de `string`/`token`/`autocomplete`/`number`/`date`/`boolean`/`objectId`/`uuid`/`document`/`embeddedDocuments` | Medio-Alto  | Medio       | `search.py:29`<br>`TODO.md:29`                                    |
| 14        | Llevar `vectorSearch` mas alla del retrieval hibrido local actual | Medio-Alto  | Alto        | `README.md:20`<br>`search.py:187`<br>`TODO.md:25`                 |

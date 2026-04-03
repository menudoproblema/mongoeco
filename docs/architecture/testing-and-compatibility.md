# Testing y compatibilidad

## Dos ejes de compatibilidad

`mongoeco` separa explicitamente:

- `mongodb_dialect`: semantica del servidor;
- `pymongo_profile`: superficie de la API Python.

Esta separacion es parte de la arquitectura, no una opcion secundaria. Permite:

- modelar deltas semanticos sin mezclar API y servidor;
- fijar contratos reproducibles;
- explicar por que una feature puede depender del perfil, del dialecto o de
  ambos.

## `planning_mode` como parte del contrato

La compatibilidad no se limita a "si funciona o no". Tambien incluye como se
visibiliza la degradacion:

- `STRICT`: fallo temprano cuando el shape no es ejecutable de forma coherente;
- `RELAXED`: conservacion de metadata y `planning_issues`.

Esto se prueba y documenta como parte del contrato observable.

## Estrategia de testing

La suite se apoya en varias capas:

- unit tests para helpers y semantica local;
- integration tests para la superficie publica;
- contract tests para comportamientos compartidos entre engines;
- parity tests para superficies async/sync;
- smoke de distribuciones y extras opcionales.

## Politica de parity tests

Toda feature publica compartida debe entrar acompanada por tests que congelen su
paridad:

### Async/sync

Si la feature existe en ambas superficies:

- se prueba en async y sync;
- se fijan errores publicos equivalentes;
- se comprueba preservacion de opciones heredadas cuando haya reconstruccion de
  fachadas.

### Cross-engine

Si la semantica se promete igual para `MemoryEngine` y `SQLiteEngine`:

- se anaden tests cruzados de parity;
- se fijan errores publicos, explain shapes o resultados compartidos segun el
  caso.

## Contratos publicos y tests

La suite no se usa solo para detectar bugs. Tambien congela contratos:

- shapes de errores;
- `planning_issues`;
- mensajes publicos importantes;
- capabilities expuestas;
- limites conscientes del producto.

## Compatibilidad como documentacion ejecutable

Los catalogos y resoluciones de `compat` convierten decisiones de producto en
datos ejecutables:

- catalogos de dialectos y perfiles;
- resoluciones explicitas;
- soporte por opcion u operacion;
- introspeccion de capacidades.

Internamente, `compat` ya no concentra todo esto en un unico fichero:

- modelos y estados de soporte;
- datos versionados de dialecto;
- datos versionados de perfiles PyMongo;
- matrices de soporte por operacion/opcion;
- exportadores del catalogo serializado.

La API publica y los snapshots siguen siendo los mismos; la separacion es
arquitectonica para facilitar futuras versiones de MongoDB y PyMongo.

Algo parecido ocurre con `mongoeco.types`: la superficie publica sigue siendo
`mongoeco.types`, pero su implementacion se reparte ya por dominios internos
para que nuevos tipos o resultados no obliguen a seguir creciendo en un
monolito unico.

La misma regla se aplica ya al subset local de `search`:

- `core/_search_contract.py` fija el inventario declarativo de operadores
  textuales soportados;
- `core/search.py` materializa compilacion, matching y shape de explain sobre
  ese contrato;
- `SearchIndexDefinition.to_document()` reutiliza ese mismo inventario al
  exponer capacidades;
- los tests estructurales congelan esa alineacion para que runtime, docs y
  snapshots no diverjan cuando entre un operador nuevo.

La capa `compat` fija ahora dos matrices separadas:

- `database_commands`, para declarar el inventario de comandos soportados, su
  familia administrativa, si forman parte tambien de la surface wire local y
  si tienen explain declarado;
- `operation_options`, para la API publica estilo coleccion;
- `database_command_options`, para la surface cruda de comandos
  `database.command(...)` y su adaptacion wire.

Parte de ese catalogo se proyecta tambien en runtime a traves de
`listCommands`, que ya expone `adminFamily`, `supportsWire`,
`supportsExplain`, `supportsComment`, `supportedOptions` y `note`
por comando para tooling e introspeccion local.

La regla operativa resultante es que un comando admin nuevo no se considera
cerrado solo porque funcione en runtime. Debe entrar por el contrato
declarativo y quedar alineado en las tres vistas que hoy forman el contrato
real del subsistema:

- `compat`;
- runtime observable (`listCommands`, `serverStatus`, explain);
- tests y snapshots.

Por eso esta capa debe leerse junto con los tests y no solo junto con las docs.

## Tests estructurales de mantenimiento

Ademas de parity y contratos funcionales, la suite fija varias fronteras
arquitectonicas para que no reaparezcan monolitos donde ya se ha invertido en
separacion:

- `mongoeco.types` debe seguir siendo una fachada agregadora publica;
- `compat/catalog.py` debe seguir siendo un compositor fino del catalogo;
- `DriverRuntime` debe seguir coordinando sobre helpers de resolucion y
  lifecycle, no reabsorber toda la logica de intentos;
- `WireCommandExecutor` debe seguir coordinando sobre helpers de parseo,
  validacion temprana, handlers especiales, passthrough y errores, no volver a
  mezclar toda la surface wire en un unico archivo;
- `_executor_support.py` no debe volver a concentrar la validacion por comando
  de `wire/admin`; esa frontera vive ya en `_executor_validation.py`;
- `database_admin.py` debe seguir siendo una fachada y punto de compatibilidad,
  no volver a concentrar el routing y la ejecucion por familias que ya viven en
  `_database_admin_routing.py` y servicios auxiliares;
- `database_commands.py` no debe volver a fabricar por su cuenta la metadata
  publica de comandos que ya sale del contrato declarativo compartido en
  `_database_command_contract.py`;
- `SQLiteEngine` no debe volver a concentrar el runtime de sesion o la capa
  administrativa ya extraida.
- `sqlite.py` no debe volver a concentrar el lifecycle de search/vector
  (materializacion, rebuild, ejecucion y explain); esa frontera vive ya en
  `_sqlite_search_runtime.py`.
- `sqlite.py` no debe volver a concentrar la traduccion de fallback a
  `planning_issues` y `pushdown_hints`; esa frontera vive ya en
  `_sqlite_explain_contract.py`.
- `sqlite.py` tampoco debe volver a decidir en paralelo la capacidad real del
  backend local de `$search`; esa frontera vive ya en
  `_sqlite_search_backend.py`.
- un subset local complejo (`geo`, `vectorSearch`, `$merge`, ops locales) no
  debe darse por soportado si no coincide el mismo significado entre runtime,
  compat, docs y tests.

Estos tests no sustituyen a los funcionales, pero ayudan a detectar deriva de
responsabilidad antes de que vuelva a traducirse en regresiones.

## Regla operativa para nuevas features

La regla vigente del proyecto es:

- nueva feature publica compartida -> parity async/sync;
- semantica compartida entre engines -> cross-engine parity;
- degradacion publica -> shape fijado en tests;
- reconstruccion de fachada -> preservacion de opciones heredadas fijada en
  tests;
- frontera arquitectonica ya extraida -> test estructural ligero que detecte
  reabsorcion de responsabilidades.

En la fase actual, eso incluye ya explicitamente:

- parity async/sync y cross-engine para `$densify`, `$fill` y `$merge`;
- parity async/sync y cobertura cruzada para el subset geoespacial local
  (`$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere` y `$geoNear`);
- parity async/sync, cross-engine y surface wire para el subset local de
  `$text` clasico con `textScore`;
- parity async/sync, cross-engine y `explain()` coherente para el subset local
  de `$search` (`text`, `phrase`, `autocomplete`, `wildcard`, `compound`);
- parity async/sync y cross-engine para projection avanzada de `find`
  (`$slice`, `$elemMatch` y proyeccion posicional);
- parity async/sync y cross-engine para `$collStats` como stage inicial de
  agregacion local;
- coverage observable para indices `hidden` como metadata administrativa local,
  incluyendo rechazo estable de `hint` contra indices ocultos;
- tests de surface administrativa local para `currentOp` y `killOp`;
- regresiones sobre `vectorSearch` local con similitudes adicionales, `filter`,
  backend ANN `usearch` y `explain()` con degradacion observable;
- snapshots de compatibilidad regenerados automaticamente cuando cambia el
  catalogo declarado.

# Superficies publicas y fachadas

## Estructura general

La superficie publica se organiza alrededor de dos familias de objetos:

- familia async:
  - `AsyncMongoClient`
  - `AsyncDatabase`
  - `AsyncCollection`
  - cursores async
- familia sync:
  - `MongoClient`
  - `Database`
  - `Collection`
  - cursores sync

La familia sync no reimplementa la semantica completa. Actua como una capa
adaptadora sobre la familia async y reutiliza su comportamiento.

## Patron `Facade`

La `api` usa el patron **Facade** de forma intensiva:

- el cliente expone concerns, session handling, runtime del driver y acceso a
  bases de datos;
- la base de datos expone administracion, comandos y acceso a colecciones;
- la coleccion expone CRUD, agregacion, indices, search indexes y watch;
- los cursores encapsulan carga, iteracion, explicacion y clonacion.

El objetivo es dar una forma de uso cercana a PyMongo sin exponer directamente
la complejidad de `core`, `engines`, `driver` o `_change_streams`.

Internamente, estas fachadas ya no concentran toda la orquestacion:

- `AsyncCollection` delega parte de su runtime local en
  `_collection_runtime.py`;
- `AsyncDatabaseAdminService` separa el namespace admin en
  `_database_admin_namespace.py`;
- `AsyncDatabaseAdminService` reparte la compilacion y ejecucion de comandos en
  `_database_admin_command_compiler.py`,
  `_database_admin_read_commands.py` y
  `_database_admin_write_commands.py`;
- la superficie sync sigue adaptando sobre la semantica async en vez de
  duplicarla.

## Patron `Adapter`

La superficie sync adapta la async a traves de:

- `_SyncRunner`, que mantiene un loop estable;
- wrappers sync de cursores materializados y perezosos;
- adaptadores de servicios de base de datos y coleccion.

Esto permite:

- que la semantica viva en una sola ruta principal;
- que los fixes funcionales entren primero en async;
- que la superficie sync quede alineada por adaptacion y parity tests.

La consecuencia practica es que una nueva feature publica no deberia entrar ya
añadiendo semantica propia a la capa sync, sino ampliando la ruta async o los
helpers compartidos y fijando parity tests.

## Normalizacion y compilacion

La `api` separa tres pasos:

### 1. Shape publico y aliases

`public_api.py` define el catalogo de operaciones publicas:

- argumentos permitidos;
- aliases;
- dependencias de perfil;
- required arguments.

`argument_validation.py` valida piezas transversales como `sort`, `hint`,
`batch_size` y `max_time_ms`.

### 2. Parsing administrativo

`admin_parsing.py` resuelve shapes de comandos y opciones de admin:

- `listCollections`
- `listDatabases`
- `findAndModify`
- normalizacion de indexes, insert/update/delete batches y namespaces

### 3. Compilacion de operaciones

`operations.py` compila las operaciones publicas a objetos internos:

- `FindOperation`
- `UpdateOperation`
- `AggregateOperation`

Esos objetos concentran metadata semantica, planning issues y payload ejecutable
sin atar la logica al engine concreto.

El runtime de la coleccion se reparte ahora entre:

- fachada publica y normalizacion en `collection.py`;
- lectura/modificacion/indexacion en modulos `_collection_*` ya extraidos;
- coordinacion de profiling, seleccion, publicacion de eventos y acceso al
  engine en `_collection_runtime.py`.

En la administracion de base de datos ocurre algo parecido:

- `database_admin.py` queda como fachada y punto de compatibilidad para el
  router de comandos;
- `_database_admin_routing.py` hace explicita la frontera entre
  compilacion/routing y ejecucion por familias de comandos reutilizables desde
  `database.command(...)` y desde la surface `wire`;
- el compilado de operaciones administrativas vive en
  `_database_admin_command_compiler.py`;
- la lectura/listado/explain vive en `_database_admin_read_commands.py`;
- la orquestacion de `insert/update/delete/findAndModify` e indices vive en
  `_database_admin_write_commands.py`;
- snapshots y namespace admin documental siguen concentrados en
  `_database_admin_namespace.py`.

La consecuencia importante es que la misma familia de comandos administrativos
puede exponerse ahora de forma coherente por tres rutas:

- `database.command(...)`;
- el router interno de `database_commands.py`;
- la surface `wire` cuando el proxy local delega esos comandos como
  passthrough.

Ademas, la metadata publica de esos comandos ya no se recompone solo en
`database_commands.py`. `_database_command_contract.py` actua como fuente de
verdad compartida para:

- `listCommands`;
- recuentos por familia y surface visible en `serverStatus`;
- opciones soportadas y metadata de explain/wire derivadas del catalogo
  declarado.

`AsyncDatabaseCommandService` refleja ya esa separacion de forma explicita:

- `_compiler` resuelve el shape semantico y compila operaciones internas;
- `_routing` decide la familia del comando y lo enruta a la ejecucion correcta;
- los wrappers historicos del admin se mantienen como capa de compatibilidad,
  pero ya no son la unica frontera real del subsistema.

La debilidad arquitectonica que sigue viva aqui es explicita: `database_admin.py`
sigue siendo una fachada relativamente gruesa porque conserva wrappers
historicos, helpers de compatibilidad y muchos puntos de entrada publicos. La
regla vigente no es seguir troceandola por inercia, sino impedir que vuelva a
reabsorber routing, validacion o shape publica que ya viven en servicios
internos.

## Reconstruccion de objetos y opciones heredadas

Una decision importante del proyecto es que la reconstruccion de fachadas no
puede perder metadata runtime ni concerns heredados. Esto afecta a:

- `with_options()`;
- `database`;
- `get_collection()`;
- `rename()`;
- obtencion de subcolecciones via `__getattr__` y `__getitem__`.

La documentacion debe dejar claro que estas rutas preservan:

- `mongodb_dialect` y su resolucion;
- `pymongo_profile` y su resolucion;
- concerns de lectura/escritura;
- `codec_options`;
- configuracion local de change streams.

## Cursores y formas de lectura

La API tiene varios tipos de cursor porque el contrato observable no es unico:

- `Cursor` / `AsyncCursor` para `find`;
- `AggregationCursor` / `AsyncAggregationCursor` para pipelines;
- `ListingCursor` para listados administrativos;
- `IndexCursor` y `SearchIndexCursor` para metadata de indices;
- `RawBatchCursor` para superficies de batches crudos;
- `ChangeStreamCursor` y `AsyncChangeStreamCursor` para `watch`.

Cada cursor es una fachada especializada sobre una fuente de datos y mantiene
sus propias invariantes de mutabilidad, explicacion, carga y cierre.

## Contrato publico frente a runtime interno

La `api` no es un simple passthrough al engine. Tambien coordina:

- profiling;
- concerns;
- sessions;
- public error shapes;
- degradacion por `planning_mode`;
- public exports y paridad async/sync.

Por eso la documentacion debe presentar la `api` como frontera contractual, no
como capa trivial de wiring.

Eso incluye una regla de implementacion interna: cuando una responsabilidad
empieza a mezclar concerns publicos, profiling, seleccion y llamada al engine,
debe extraerse a un servicio interno antes de crecer dentro de la fachada.

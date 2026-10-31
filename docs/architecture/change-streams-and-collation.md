# Change streams y collation

## Por que van juntos en la documentacion

Ambos subsistemas comparten una caracteristica arquitectonica importante:

- tienen un contrato publico visible;
- su perimetro es conscientemente parcial;
- exponen capacidades y limites via APIs de introspeccion;
- priorizan honestidad contractual frente a fingir una compatibilidad total.

## Arquitectura de change streams

La fachada publica vive en `change_streams.py`, pero la implementacion real se
reparte en `_change_streams`:

- `models.py`: tipos y contratos de backend/estado;
- `pipeline.py`: validacion y compilacion de pipelines de change stream;
- `hub.py`: almacenamiento retenido, offsets, publicacion y reanudacion;
- `journal.py`: persistencia local incremental;
- `cursor.py`: cursores async/sync y reanudacion.

## Modelo operativo

El modelo actual es:

- local al proceso o al entorno local donde vive el journal;
- con retencion en memoria acotada;
- con persistencia opcional a fichero;
- reanudable dentro de ese entorno local;
- no distribuido entre nodos.

Eso se expone explicitamente con:

- `change_stream_backend_info()`;
- `change_stream_state()`.

## Hub, journal y compaction

`ChangeStreamHub` coordina:

- retencion de eventos;
- offsets reanudables;
- lectura por cursores;
- journal incremental;
- compaction de snapshot;
- metadata observable de estado.

Los engines locales entregan cambios por secuencia de commit. El adaptador usa
`publish_committed` del hub para comprobar secuencia y publicar bajo el mismo
guard, y acepta replay idempotente: una fila anterior a
`next_token` ya esta aplicada. En SQLite, el outbox es atomico con la escritura;
en Memory, la secuencia se asigna al commit MVCC. Cuando hay journal, su ultimo
token es el checkpoint durable desde el que un proceso reiniciado reanuda el
outbox pendiente.

Las escrituras CRUD cuya politica no captura eventos registran un hueco de
secuencia en vez de inventar una imagen recuperable. Los efectos de mantenimiento
TTL y de catalogo conservan su payload en el registro atomico existente.
Los resume tokens no pueden atravesar huecos silenciosamente.

El contenido de los cambios de actualización se construye en
`core/change_events.py`, compartido por Memory y SQLite. Los planes clásicos
conservan las rutas realmente aplicadas; los pipelines conservan la imagen
calculada cuando difiere de la guardada por la política de `_id`. Ese contenido
no se publica hasta confirmar la escritura: scopes transaccionales y outbox
siguen siendo responsables de la entrega.

Drop, rename y dropDatabase de Memory/SQLite registran el efecto de catalogo y
su invalidacion en esa misma unidad atomica. La coordinacion API en
`api/_change_delivery.py` comparte preparacion, hooks de commit y despacho tras
la operacion, incluso si esta falla despues de confirmar efectos parciales.
El productor decide si hubo un cambio real: drop inexistente no publica.

El selector de ambito conserva los destinatarios concretos. Drop invalida la
coleccion afectada; dropDatabase invalida sus colecciones existentes y el stream
de base, manteniendo el de cliente y el de una coleccion que nunca existio.
Rename entrega el evento e invalida streams de origen y destino. El destino se
conserva como metadata opcional en journal; el evento publico rename expone
`to`. Invalidate omite `ns`/`documentKey`, y dropDatabase omite `ns.coll`.
El registro de [implementacion BSON](../bson-consolidation-implementation.md)
posee la aceptacion y los pendientes de protocolo, reanudacion, entradas de
comando y CI; esta descripcion no cierra todo D04.

El journal conserva orden documental y anchos numéricos BSON al reanudar.
Las entradas incrementales nuevas usan versión 2, cuyo checksum incluye el
orden; el lector sigue aceptando las entradas versión 1 con su checksum
canónico anterior. Los snapshots conservan su formato versión 1 y dejan de
ordenar recursivamente los documentos. No se cambian tokens ni esquema del
outbox SQLite. El orden o ancho ya perdido en journals antiguos no puede
reconstruirse a partir del fichero.

La arquitectura persigue robustez local, no durabilidad de cluster. Por eso se
admiten opciones como:

- tamano de retencion;
- `journal_path`;
- `journal_fsync`;
- maximo de bytes del log incremental.

## Collation

`core/collation.py` define el contrato de collation soportado hoy.

### Surface soportada

- `locale="simple"` y `locale="en"`
- strengths `1`, `2` y `3`
- `caseLevel` y `numericOrdering` para el subset Unicode soportado

### Backends

- `PyICU`: backend preferido y opcional
- `pyuca`: fallback para collation Unicode basica
- `simple`: comparador BSON/Python base, sin tailoring Unicode

### Reglas importantes

- `simple` rechaza knobs Unicode como `numericOrdering` o `caseLevel`;
- opciones avanzadas como `backwards`, `alternate`, `maxVariable` y
  `normalization` requieren `PyICU`;
- cuando no se pueden soportar, se falla explicitamente en vez de ignorarlas.

## Capability exposure

La collation expone dos capas de introspeccion:

- `collation_backend_info()`
- `collation_capabilities_info()`

Esto convierte un detalle de runtime en un contrato documentable e inspeccionable.

## Tradeoff de producto

La decision vigente no es "soportar toda collation de MongoDB". La decision es:

- soportar bien el subset declarado;
- permitir un backend avanzado opcional;
- no mentir sobre lo que no se implementa.

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

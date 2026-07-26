# Release 4.1.0

Status: preparada para publicación desde la versión etiquetada `4.1.0`.

## Reloj inyectable para pruebas deterministas

La versión 4.1 añade `now_factory` a `AsyncMongoClient` y `MongoClient`. Es un
parámetro opcional y no cambia el comportamiento por defecto: sin factory,
mongoeco continúa usando la hora UTC real con precisión BSON de milisegundos.

```python
from datetime import UTC, datetime
from mongoeco import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine

clock = [datetime(2026, 1, 2, 3, 4, 5, 987654, tzinfo=UTC)]
client = AsyncMongoClient(MemoryEngine(), now_factory=lambda: clock[0])
```

Cada comando real o lote lógico captura una sola vez el valor. Por tanto,
`$$NOW`, `$currentDate` de tipo fecha y la limpieza TTL ven el mismo instante;
al avanzar `clock[0]`, la siguiente operación observa el tiempo nuevo. El valor
se convierte a UTC naïve y se trunca a milisegundos.

El factory se conserva al derivar bases, colecciones y objetos creados con
`with_options()`. El facade síncrono ofrece el mismo parámetro.

## Límites

El reloj inyectable sólo se permite en engines con
`supports_injected_clock=True`, actualmente Memory y SQLite. Se rechaza para
un backend externo o real, porque mongoeco no puede imponerle su reloj. No
controla `ObjectId`, handshakes, telemetría, perfiles ni métricas.

## Verificación de rendimiento

`benchmarks/sync_hot_paths.py` incluye un caso guardián sin factory y otro con
reloj inyectado. Ejecuta siete repeticiones y muestra mediana y dispersión;
antes de publicar se acepta como máximo una regresión del 5% en la mediana del
caso común sin factory.

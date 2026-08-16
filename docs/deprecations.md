# Catalogo publico de deprecaciones

MongoEco distribuye el recurso
`mongoeco/compat/resources/deprecations-v1.json` y su JSON Schema. La API
publica devuelve valores tipados u owned documents:

```python
from mongoeco.compat import (
    deprecation_catalog,
    deprecation_catalog_schema,
    deprecation_entries,
)

entries = deprecation_entries()
document = deprecation_catalog()
schema = deprecation_catalog_schema()
```

## Semantica de estados

- `decision-pending`: existe deuda identificada, pero no una retirada decidida;
- `planned`: existe direccion de retirada, aun sin deprecacion activa;
- `deprecated`: la sustitucion esta disponible y la retirada tiene version;
- `removed`: el contrato ya no existe y el registro queda como historial.

Un ID no se reutiliza ni cambia de significado. La evolucion normal es
`decision-pending -> planned -> deprecated -> removed`; se pueden omitir pasos
solo cuando no se afirma una deprecacion inexistente. Toda entrada conserva
sustituto, impacto, migracion y referencias.

## Versionado

El schema `mongoeco-deprecations/v1` puede recibir campos opcionales aditivos.
Cambiar campos requeridos, estados admitidos o significado exige otro
`schemaVersion`. Los consumidores seleccionan por schema y no por la version
del paquete.

La fixture `tests/fixtures/deprecations_v1.json` protege lectura futura del
shape v1. Los cambios de contenido normal del catalogo se revisan mediante el
manifest de API y changelog; regenerar una fixture no sustituye esa revision.

## Politica de retirada

Un contrato no se retira hasta que:

1. existe sustituto publico y documentado;
2. typing y conformance cubren la sustitucion;
3. hubo una ventana de migracion observable;
4. la guia de major contiene comandos de verificacion;
5. el diff semantico clasifica la retirada esperada.

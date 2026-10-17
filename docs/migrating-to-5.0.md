# Guia de migracion hacia MongoEco 5.0

## Estado

MongoEco 5.0 y sus contratos no se han publicado. La linea actual 4.10.1 expone SPI v2
como unico engine SPI estable y mantiene `search-v1`. Los engines anteriores se
migran mediante la [guia de 4.7](migrating-to-4.7.md), no mediante una futura
release.

La [decisión posterior a T1–T6 y S1/S2](architecture/engine-spi-v3-proposal.md)
mantiene SPI v2 y aparca v3. No hay migración a un sucesor, retirada de v2 ni
cambio SQLite aprobados. Esta guía conserva material futuro e histórico, sin
una migración SPI que deba preparar el usuario actual.

## Inventario previo

```python
from mongoeco.compat import deprecation_entries, public_api_manifest

for entry in deprecation_entries():
    print(entry.identifier, entry.status, entry.replacement)

print(public_api_manifest()["contracts"])
```

Conserva el manifest de tu version actual y comparalo durante la actualizacion:

```bash
python scripts/update_public_api_manifest.py --compare path/to/baseline.json
```

No anticipes contratos de engine no publicados. Una evolucion posterior debe
tener decision, tipos, conformance y ventana de migracion propios sin cambiar
silenciosamente el significado de SPI v2.

## Search v1 a v2

Esta sección conserva los cambios contemplados por la
[propuesta Search v2](architecture/search-contract-v2-proposal.md), no una
receta ejecutable. `search-v1` sigue siendo el único contrato disponible;
no existe actualmente una opción pública para activar v2.

- solicita highlight con `$meta: "searchHighlights"`;
- sustituye previews por collectors estables;
- usa `queryMatchedCount`, no `matchedCount`;
- lee metricas por dominio y disponibilidad;
- exige `contractVersion` en explain;
- elimina normalizadores que mezclen shapes v1/v2.

Si se aprueba y publica ese contrato, su guía deberá activar v2 primero en un
entorno de test y conservar fixtures separadas. Comparará documentos, metadata,
collectors, orden y explain, no solo el numero de hits.

## SQLite

Haz una copia antes de migrar. Abre la copia con la nueva version y ejecuta
lectura, indices, Search y outbox antes de promoverla. MongoEco rechaza schemas
futuros y no debe rebajarlos. La fixture oficial 4.5 del repositorio cubre BSON,
indices, Search, checkpoint y replay del sufijo pendiente.

No exportes/reimportes JSON para migrar: perderia precision BSON y estado de
outbox. Si 5.0 requiere un cambio de schema, debe usar una migracion
transaccional versionada.

## Typing y artefacto instalado

Ejecuta mypy contra el wheel, no contra un checkout accidental:

```bash
MONGOECO_TEST_INSTALLED_ARTIFACT=1 python scripts/check_public_typing.py
```

Ejecuta tambien el canario y el CLI desde un directorio ajeno al proyecto. Una
migracion no esta cerrada si solo funciona con imports privados o editable
installs.

## Checklist de salida

- catalogo revisado sin elementos `decision-pending` relevantes;
- conformance de cada contrato declarado;
- diff de API aprobado;
- base SQLite copiada y verificada;
- ausencia de warnings deprecados durante la suite consumidora;
- rollback documentado;
- ningun adapter local en el consumidor para ocultar diferencias.

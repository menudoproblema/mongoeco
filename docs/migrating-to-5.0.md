# Guia de migracion hacia MongoEco 5.0

## Estado

Esta guia prepara la migracion, pero 5.0 y sus contratos sucesores aun no se
han publicado. En 4.6 siguen operativos SPI v1, SPI v2 y `search-v1`.

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

## SPI v1 a SPI v2

1. declara `EngineCapabilities(spi_version=2, ...)`;
2. sustituye `put_document` y `put_documents_bulk` por
   `insert_document`/`insert_documents`;
3. acepta `OperationContext` en cada primitiva;
4. devuelve `InsertOutcome`, `MutationOutcome`, `DeleteOutcome` y
   `MergeOutcome`;
5. implementa snapshot estable o declara el fallback v2;
6. declara change delivery y sus primitivas;
7. elimina flags `capture_document(s)` y callbacks `on_commit` de la semantica
   nativa.

Verificacion:

```bash
python -m mongoeco.conformance package.engine:factory \
  --format json \
  --output conformance-spi-v2.json \
  --require-success
```

## SPI v2 a SPI v3

Cuando SPI v3 exista, no cambies una clase v2 in place. Publica una factory v3
que:

- reciba solo `BoundFindOperation`, `BoundUpdateOperation` y
  `BoundAggregateOperation`;
- use exclusivamente el `OperationContext` ligado;
- no normalice BSON ni recapture el reloj;
- mantenga outcomes y snapshots tipados;
- declare capabilities v3.

Ejecuta perfiles v2 y v3 por separado hasta retirar deliberadamente la factory
antigua. Una instancia no debe negociar version metodo por metodo.

## Escrituras, outcomes y snapshots

Los booleanos o documentos opcionales de SPI v1 deben convertirse en outcomes
al cruzar el adapter, no dentro del consumidor. Captura before/after dentro de
la misma seccion atomica. Un no-match no lleva secuencia ni evento.

Todo cursor debe cerrar el snapshot que posee. No conserves iteradores del
engine fuera de `ReadSnapshot` ni reconstruyas identidad de operacion.

## Search v1 a v2

- solicita highlight con `$meta: "searchHighlights"`;
- sustituye previews por collectors estables;
- usa `queryMatchedCount`, no `matchedCount`;
- lee metricas por dominio y disponibilidad;
- exige `contractVersion` en explain;
- elimina normalizadores que mezclen shapes v1/v2.

Activa v2 primero en un entorno de test y conserva fixtures separadas. Compara
documentos, metadata, collectors, orden y explain, no solo el numero de hits.

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
- conformance de cada SPI/Search declarada;
- diff de API aprobado;
- base SQLite copiada y verificada;
- ausencia de warnings deprecados durante la suite consumidora;
- rollback documentado;
- ningun adapter local en el consumidor para ocultar diferencias.

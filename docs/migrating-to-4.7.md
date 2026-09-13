# Migracion a MongoEco 4.7

## Ruptura deliberada

MongoEco 4.7 elimina el engine SPI v1 dentro de una version minor por decision
explicita de producto. No hay periodo dual, shim, alias ni deteccion por shape.
SPI v2 es el unico engine SPI estable publicado; esta release no publica SPI
v3.

La migracion afecta a engines externos y a tooling que importase directamente
la capa de compatibilidad. No cambia el formato SQLite ni requiere migrar datos.

## Sustituciones

| Antes, SPI v1 | MongoEco 4.7, SPI v2 |
| --- | --- |
| capabilities ausentes o inferidas por shape | `EngineCapabilities(spi_version=2, ...)` explicito |
| `put_document(...) -> bool` | `insert_document(...) -> InsertOutcome` |
| `put_documents_bulk(...) -> list[bool]` | `insert_documents(...) -> tuple[InsertOutcome, ...]` |
| update/delete con retornos union | `MutationOutcome` / `DeleteOutcome` |
| flags privados de captura | imagenes `before`/`after` dentro del outcome |
| callback del engine para confirmar | `change_delivery` y secuencia/outbox declarados |
| contexto, dialecto y reloj separados | un `OperationContext` ligado |
| iterable de scan sin ownership | `ReadSnapshot`, o fallback v2 `scan_find_semantics` declarado |
| metodos Search basados en argumentos sueltos | `execute_search(SearchRequest)` y outcomes tipados |
| import del adapter de compatibilidad | tipos publicos desde `mongoeco.engines` |

## Declaracion minima

```python
from mongoeco.engines import EngineCapabilities, InsertOutcome


class CustomEngine:
    capabilities = EngineCapabilities(
        spi_version=2,
        batch_inserts=False,
        explicit_read_snapshots=False,
        change_delivery="none",
    )

    async def insert_document(
        self,
        db_name,
        coll_name,
        document,
        *,
        operation_context,
        **options,
    ):
        # Persistir y capturar la imagen dentro de la misma frontera atomica.
        return InsertOutcome(applied=True, document=document)
```

El engine debe completar las primitivas CRUD v2. Si
`batch_inserts=False`, MongoEco deriva la operacion individual por elemento. Si
`explicit_read_snapshots=False`, el engine implementa `scan_find_semantics` y
MongoEco conserva el ownership mediante un `ReadSnapshot` estable. No añadas
wrappers con los nombres retirados: su presencia no activa compatibilidad.

## Lifecycle y concurrencia

- propaga el mismo `OperationContext` a toda suboperacion;
- captura imagenes antes/despues bajo la misma atomicidad de la escritura;
- cierra cada snapshot exactamente una vez ante agotamiento, error o
  cancelacion;
- no ejecutes cleanup externo dentro de un finalizador que mantenga un lock no
  reentrante;
- conserva timeouts y el primer error mientras finalizas el resto de recursos;
- no cierres un engine externo que el cliente no posea según su lifecycle
  declarado.

## Verificacion

Ejecuta primero el perfil publico:

```bash
python -m mongoeco.conformance package.engine:factory \
  --profile spi-v2-core \
  --format json \
  --output conformance-spi-v2.json \
  --require-success
```

Después valida la aplicacion con cancelacion, cierre concurrente, timeout y un
cursor parcialmente consumido. La ausencia de error en CRUD no sustituye estas
pruebas de ownership.

Para inventariar la ruptura antes de actualizar:

```python
from mongoeco.compat import public_api_manifest


assert public_api_manifest()["contracts"]["engineSpi"] == [2]
```

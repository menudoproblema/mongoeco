# Pytest con clientes PyMongo reales

El kit se incorpora en Mongoeco **4.10.0**, con el extra `pytest`.
Para adaptar una aplicación existente,
consulta la [guía de pruebas](use-cases/test-runtime.md) o la
[entrada desde mongomock](migrating-from-mongomock.md).

Instala `mongoeco[pytest]` y carga el plugin explícitamente desde el
`conftest.py` raíz. Mongoeco inicia un servidor wire local por prueba, con
puerto dinámico y un motor nuevo. Los clientes conservan los tipos, imports,
resultados y excepciones de PyMongo.

```python
pytest_plugins = ("mongoeco.pytest_plugin",)


def test_insert(mongoeco_client):
    coll = mongoeco_client.app.items
    coll.insert_one({"_id": "a", "value": 1})
    assert coll.find_one({"_id": "a"})["value"] == 1
```

## Clientes async

Con pytest-asyncio >=1.4,<2, carga `mongoeco.pytest_asyncio` en lugar del
plugin común. Lo incluye automáticamente. Funciona en strict y auto, con
loops de función; no compartas el cliente entre loops o pruebas.
El adaptador valida el scope efectivo, incluido el default global
`asyncio_default_test_loop_scope` y la opción heredada `scope` del marcador.
Un marcador explícito `loop_scope="function"` permite usar el kit aunque el
default global sea más amplio; las combinaciones incompatibles fallan antes
de iniciar el servidor.

```python
import pytest

pytest_plugins = ("mongoeco.pytest_asyncio",)


@pytest.mark.asyncio
async def test_insert(mongoeco_async_client):
    await mongoeco_async_client.app.items.insert_one({"_id": "a"})
    assert await mongoeco_async_client.app.items.count_documents({}) == 1
```

Con AnyIO >=4.11,<5, carga `mongoeco.pytest_anyio` y conserva tu configuración
AnyIO con backend `asyncio`. Usa la misma fixture `mongoeco_async_client` y
la marca `pytest.mark.anyio`. El kit no sustituye `anyio_backend` globalmente.
No admite Trio. Si también está instalado pytest-asyncio, debe estar en modo
strict. Carga un solo adaptador async por ejecución.

El extra instala pytest >=8.4,<10 y PyMongo >=4.9,<5. El runner async se instala
por separado: el consumidor conserva el que ya utiliza. No hay autoload ni
fixtures autouse en el paquete. La instalación base no importa pytest.

## Motores, perfil y configuración

```sh
pytest --mongoeco-backend=memory
pytest --mongoeco-backend=sqlite
pytest --mongoeco-backend=both -n 4
```

El último comando requiere pytest-xdist. `both` ejecuta cada test dependiente
del kit dos veces, con IDs `memory` y `sqlite`; los demás tests se ejecutan
una sola vez. El default es `memory`.

El dialecto por defecto es `7.0`, seleccionable con
`--mongoeco-mongodb-dialect=7.0|8.0|9.0`. El perfil de PyMongo usa
`strict-auto-installed`: exige un perfil exacto para el SDK instalado.
`--mongoeco-pymongo-profile` permite elegir explícitamente un perfil del
catálogo existente. Estos defaults del kit no cambian los del cliente Mongoeco.
La integración conserva los límites semánticos y wire de Mongoeco; no simula
un clúster ni extiende el soporte de opciones del servidor.
Las [escrituras no confirmadas](unacknowledged-writes.md) conservan sus resultados
y efectos, y permiten reutilizar la misma conexión después de un `w=0`.

Para configurar los clientes administrados, redefine una fixture de función:

```python
import pytest


@pytest.fixture
def mongoeco_client_options():
    return {"tz_aware": True, "uuidRepresentation": "standard"}
```

Se pasan estas opciones a PyMongo. El timeout de selección por defecto es
3000 ms y puede reemplazarse. `host` y `port` están reservados para el endpoint
administrado. Las demás opciones las valida PyMongo; no se ignoran en silencio.

## Diagnóstico en el informe

Al terminar, el plugin añade una sección al resumen de pytest, también con
`-q`. Con pytest-xdist aparece una sola vez, en el proceso coordinador:

```text
================ Mongoeco configuration ================
Mongoeco 4.10.0; PyMongo 4.19.0
Selected backends: memory, sqlite; MongoDB dialect: 8.0
PyMongo profile: 'strict-auto-installed' -> 4.19
Mongoeco async adapter: pytest_asyncio
```

Las versiones del ejemplo dependen de la instalación. El informe muestra la
selección de motores, el dialecto solicitado, el perfil solicitado y resuelto,
y el adaptador async de Mongoeco cargado (`pytest_asyncio`, `pytest_anyio` o
`none`). `none` no impide construir clientes async propios desde `mongoeco_uri`.
La selección describe la configuración de la ejecución; no cuenta qué motores
han utilizado realmente las pruebas.

El diagnóstico solo consulta configuración y metadatos: no evalúa fixtures ni
abre clientes, motores, archivos SQLite, sockets o hilos. No imprime URIs ni
opciones del cliente. Si falta PyMongo, indica `not installed`; si el perfil no
puede resolverse, indica `invalid`. Esto no cambia el resultado de pruebas que
no utilizan el kit. Las que solicitan sus fixtures conservan el error de setup
original. Un motor o dialecto inválido sigue produciendo el error de argumentos
de pytest antes de que pueda generarse el resumen.

## Conservar el constructor del consumidor

Desde Mongoeco 4.11.0, para registrar metadatos del servidor y mostrarlos solo
cuando fallen, activa `--mongoeco-wire-diagnostics`. El informe se genera después
del teardown, incluye el estado de cierre y funciona también con xdist. La
[guía de diagnóstico wire](wire-diagnostics.md) detalla los datos retenidos,
límites y una receta con `failCommand` para provocar errores controlados.

La fixture `mongoeco_uri` permite cambiar solo la conexión en una preparación
existente, conservando sus imports, genéricos y opciones:

```python
import pytest
from pymongo import MongoClient


@pytest.fixture
def database_client(mongoeco_uri):
    with MongoClient(mongoeco_uri, tz_aware=True) as client:
        yield client
```

Para async, utiliza `async with AsyncMongoClient(...)` en la fixture del runner
que ya emplee tu suite. El kit cierra los clientes que crea mediante
`mongoeco_client` y `mongoeco_async_client`; el consumidor cierra los clientes
que construye a partir de la URI.

## Aislamiento y cierre

URI y clientes de una prueba comparten el mismo servidor. La siguiente prueba
recibe otro motor completo: documentos, índices y catálogo independientes,
incluso si conserva nombres de base de datos y colección. No hay scopes de
sesión o módulo en esta entrega.

El servidor tiene un hilo y loop propios; no bloquea con el cliente sync el
loop del test. SQLite usa un directorio y executor propios. Los finalizadores
cierran clientes, conexiones activas, servidor, motor, loop, executor e hilo,
y retiran los archivos temporales, también tras fallos y cancelaciones.
El cierre capturado al crear el cliente sigue disponible si un test instala
un spy sobre `close`. Un error de cleanup se informa como error de teardown;
no convierte en éxito un fallo del test. Arranque y join tienen límite de
10 segundos. No se promete recuperación después de terminar el proceso por
la fuerza.
El proxy completa también un cierre cancelado mientras espera a que termine
un arranque concurrente.

Mongoeco requiere Python >=3.13. Un consumidor que también pruebe versiones
anteriores necesita una dependencia condicionada y preparación alternativa
en esas celdas; añadir el extra incondicionalmente elevaría su mínimo.

## Aceptación de esta entrega

El [contrato y corpus K01–K20](architecture/pytest-integration-proposal.md)
separa las pruebas sync/async del kit del canario de negocio async.
`scripts/run_pytest_consumer_canary.py` ejecuta el canario contra el wheel
instalado, comprueba sus archivos y el hash de la suite, y solo sustituye su
helper de preparación. Sus entradas se suministran por argumentos; los
informes públicos contienen IDs anónimos, hashes y contadores.

El [cierre de implementación](pytest-integration-validation.md) recoge las
versiones efectivamente comprobadas, límites y evidencia del candidato.
La [revisión previa a publicación](pytest-review.md) registra las correcciones
posteriores y su validación.
La [evidencia del diagnóstico](evidence/pytest-report/validation.json) registra
las regresiones del informe, su comprobación sin recursos y la matriz del
wheel candidato.

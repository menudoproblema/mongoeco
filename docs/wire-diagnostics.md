# Diagnóstico de pruebas y protocolo wire

El diagnóstico opcional ayuda a distinguir una petición recibida, un error de
ejecución, una respuesta suprimida por el protocolo y un cierre de conexión.
La captura está desactivada por defecto.

```sh
python -m pytest --mongoeco-backend=both --mongoeco-wire-diagnostics
python -m pytest --mongoeco-backend=both --mongoeco-wire-diagnostics -n 4
```

El segundo comando requiere pytest-xdist. Se conserva el adaptador pytest de la
suite: `mongoeco.pytest_plugin`, `mongoeco.pytest_asyncio` o
`mongoeco.pytest_anyio`.

## Qué se captura

Cada servidor de prueba conserva los últimos **128 eventos**, con un contador de
eventos descartados, y una copia adicional del primer error. Ambas reservas están
acotadas; el primer error sobrevive a los pings y a la limpieza posteriores. Los eventos incluyen apertura y cierre de conexión,
recepción y finalización de peticiones, respuestas enviadas y cierre del servidor.
Retienen identificadores locales de conexión y petición, opcode, nombre conocido
del comando normalizado, indicador booleano `mirrored`, expectativa de respuesta,
resultado, fase, código numérico de error y
duración medida con reloj monotónico. Los nombres desconocidos se representan
como `<unknown>`.

Estas mediciones pertenecen a la telemetría del host y están clasificadas en
`tests/fixtures/clock_call_allowlist.json`. No modifican el reloj BSON de la
operación ni sus presupuestos de ejecución.

No se conservan documentos BSON, filtros, nombres de bases o colecciones,
credenciales, URI, mensajes de excepción ni respuestas completas. Una respuesta
`ready` significa que terminó su codificación; `response_sent` acredita que terminó el
envío al transporte local, sin afirmar que el cliente la haya procesado.

Cuando falla el setup, el cuerpo o el teardown de una prueba que usa el kit,
aparece una sección `Mongoeco wire diagnostics (after teardown)`. Con xdist,
los workers entregan los metadatos al coordinador, que muestra una sola sección.
Las pruebas correctas no muestran sus capturas. Un perfil inválido que impide
crear el servidor aparece como `not_started`.

El resumen de recursos muestra listener, handlers, conexiones, cursores,
sesiones y conexión del motor al terminar el teardown. Los eventos de espera y
recepción del cuerpo distinguen una trama incompleta de una operación lenta.
Los fallos de envío y de handler conservan conexión, fase y familia de error
(`connection`, `protocol` o `internal`), sin el texto de la excepción. El evento
`shutdown_started` conserva también cuántas conexiones, sesiones y cursores
quedaban al comenzar el cierre. Los errores de cleanup conservan su resultado de
teardown original; el diagnóstico no los oculta ni cambia el resultado del test.

El informe solo lee una copia de la captura: no abre clientes, motores,
archivos, sockets o hilos. La captura debe activarse antes de ejecutar el test;
no reconstruye actividad anterior. El buffer puede haber descartado contexto
anterior, aunque conserva el primer evento clasificado como error. Si el proceso termina por la fuerza, no se garantiza
el informe de teardown.

## Proxy utilizado directamente

```python
from mongoeco.wire import AsyncMongoEcoProxyServer

async with AsyncMongoEcoProxyServer(diagnostic_capacity=128) as proxy:
    uri = proxy.address.uri
    # Construir y cerrar los clientes de la aplicación usando esta URI.

snapshot = proxy.diagnostics
```

`diagnostic_capacity=0` desactiva la captura; admite enteros hasta 10000.
`diagnostics` devuelve una copia de metadatos ya registrados, también después
del cierre, sin consultar recursos vivos desde el hilo de pytest.
El formato es diagnóstico local y puede ampliarse; no es un reemplazo de los
eventos de monitorización de PyMongo ni un protocolo de réplica.

## Provocar un timeout controlado

Mongoeco ya admite el failpoint local `failCommand`. Esta receta comprueba el
código 50, la ausencia de escritura en el intento fallido y la recuperación
posterior. Los clientes son PyMongo reales.

```python
import pytest
from pymongo.errors import ExecutionTimeout

pytest_plugins = ("mongoeco.pytest_plugin",)

@pytest.fixture
def mongoeco_client_options():
    return {"retryReads": False, "retryWrites": False, "maxPoolSize": 1}

def test_timeout_recovery(mongoeco_client):
    client = mongoeco_client
    items = client.example.items
    items.insert_one({"_id": "a", "value": 0})
    client.admin.command({
        "configureFailPoint": "failCommand",
        "mode": {"times": 1},
        "data": {"failCommands": ["findAndModify"], "errorCode": 50},
    })
    try:
        with pytest.raises(ExecutionTimeout) as caught:
            items.find_one_and_update({"_id": "a"}, {"$inc": {"value": 1}})
        assert caught.value.code == 50
        assert caught.value.details["codeName"] == "MaxTimeMSExpired"
        assert items.find_one({"_id": "a"})["value"] == 0
        items.find_one_and_update({"_id": "a"}, {"$inc": {"value": 1}})
        assert items.find_one({"_id": "a"})["value"] == 1
    finally:
        client.admin.command({"configureFailPoint": "failCommand", "mode": "off"})
```

Con `mongoeco.pytest_asyncio` o `mongoeco.pytest_anyio`, usa
`mongoeco_async_client` y espera con `await` cada operación del cliente. La receta
se comprueba en sync/async y Memory/SQLite. El failpoint provoca un error
determinista; no reproduce el agotamiento temporal de un presupuesto ni permite
atribuir la causa de un timeout observado en MongoDB real. Para ese caso, usar
la monitorización de comandos de PyMongo permite correlacionar nombre de comando,
request ID, duración y código del fallo sin volcar el documento del comando.

## Evidencia del contrato de respuestas

`tests/support_wire_reads.py` observa dieciséis combinaciones: lectura ordinaria o
`mirrored`, flag `moreToCome` activado o desactivado y filtro documento, array,
null o escalar.
El observador envía un `ping` por el mismo socket inmediatamente después de la
lectura y verifica otra escritura y lectura sobre esa conexión.

`scripts/capture_wire_read_parity.py` captura el oráculo de MongoDB
7.0.43/8.0.32/9.0.2 sin importar Mongoeco y comprueba versión, FCV y limpieza.
Las capturas `tests/fixtures/wire_reads_mongodb_*.json` incluyen hashes de las
fuentes. `tests/integration/wire/test_read_reply_parity.py` contrasta ambas
implementaciones locales con los errores, efectos y respuestas nativos. La CI
nativa vuelve a capturar y comparar los tres servidores.

El corpus acredita ese contrato finito de mensajes. No simula la generación de
lecturas espejo entre miembros de un replica set.

La [evidencia inicial](evidence/wire-diagnostics/validation.json)
identifica el wheel de esa primera incorporación, las matrices, los resultados
y los intentos previos conservados. El [cierre de la ampliación](wire-hardening-validation.md)
identifica el candidato posterior y sus comprobaciones integradas.

## Sobres y fases del protocolo

`tests/support_wire_envelopes.py` añade catorce testigos independientes: flags
opcionales y requeridos desconocidos, checksums CRC32C válidos, incorrectos,
ausentes o truncados, secciones desconocidas, longitudes fuera de rango y namespace legacy inválido. Los
clientes de socket sync/async observan códigos y presencia de `errmsg`, contadores,
efectos de la escritura, ping por la misma conexión y lectura de recuperación.
Se ejecutan sobre Memory/SQLite con captura activada y desactivada.

MongoDB 7/8 devuelve ciertos errores de parseo (22, 40432 y 51252) y conserva
la conexión; MongoDB 9 cierra esas conexiones. Los checksums incorrectos, flags
requeridos desconocidos y tamaños inválidos cierran en las tres versiones.
Los flags opcionales desconocidos se ignoran. Estas reglas pertenecen al codec
compartido por proxy y transporte cliente; se validan antes de ejecutar el comando. El namespace OP_QUERY no admitido
devuelve el código nativo 5739101 con `$err`, sin `errmsg` ni `codeName`, y flag
OP_REPLY de error 2; la conexión continúa utilizable en las tres versiones.
La disposición de respuesta se conserva aunque falle la decodificación BSON.

El contexto normalizado se prepara una sola vez y alimenta ejecución y captura,
incluido el wrapper legacy `$query`. La clasificación distingue errores de
comando, escritura y write concern. Las regresiones de write concern y fallo del
encoder incluyen condiciones controladas y no acreditan que el failpoint local
simule una réplica ni un problema real de durabilidad.

El observador fija hashes antes de importar los helpers y los comprueba al
terminar; incluye su propio script, el helper de identidad y el inventario de
casos. Si una fuente cambia durante la ejecución, rechaza el recibo. El wheel,
las pruebas y sus fuentes deben congelarse juntos antes de la comprobación global.
La identidad de los oráculos nativos no sustituye la identidad del candidato.

# Propuesta de integración oficial con pytest

Estado: **contrato aprobado por el usuario e implementación completada**,
15 de octubre de 2026. La aprobación consta en el encargo de ejecutar el plan.
La [guía de uso](../pytest.md) describe el kit; la evidencia del prototipo
se conserva como antecedente, sin atribuirle los gates de implementación.

## Objetivo y perímetro

Permitir adoptar Mongoeco en una suite que usa PyMongo cambiando su preparación
de base de datos: dependencia de pruebas, carga del plugin y fixture o URI.
Se conservan los imports y tipos PyMongo, las aserciones y el código consumidor.
La entrega comprende el kit, sus regresiones y un canario consumidor contra el
wheel instalado. No incorpora nuevos operadores, formato SQLite, SPI, soporte
de clúster ni modificación de repositorios consumidores.

La ampliación de soporte de Python 3.13 hasta octubre de 2028 está aprobada
y aplicada en la [política](../python-support.md); es independiente de esta
propuesta. El kit conserva el mínimo Python 3.13 del paquete.

## Hechos inspeccionados

- `wire/proxy.py` ya crea un endpoint de loopback con puerto dinámico y permite
  clientes PyMongo reales. Solo ofrece ciclo de vida asíncrono.
- `conformance/pytest.py` contiene una aserción de conformidad; no proporciona
  fixtures. El extra `engine-testing` sirve a ese contrato diferente.
- `wire/connections.py` registra contexto de conexión, pero no posee ni espera
  el cierre de sus transportes y handlers.
- Reproducción sobre el wheel previamente validado: `close()` no termina con
  un socket cliente abierto; termina cuando el peer cierra. Otra reproducción
  muestra `connect=1`, `disconnect=0` después de fallar el bind por puerto ocupado.
  Ambas pruebas liberaron después los recursos de la reproducción.
- Un prototipo privado con loop e hilo propios pasa **7 pruebas con dos workers**:
  clientes nativos sync/async, Memory/SQLite, AnyIO sobre asyncio,
  pytest-asyncio strict y cierre tras una excepción. Usa PyMongo 4.19.0,
  pytest 9.1.1, pytest-asyncio 1.4.0 y AnyIO 4.15.1 en CPython 3.14.5.
  No acredita todavía fallos de setup/cancelación, toda la matriz ni adopción real.
  El [registro de evaluación](../evidence/pytest-evaluation/evaluation.json)
  conserva reproducciones, hashes y resultado. Sus fuentes `*.py.txt` se
  restauran en un directorio temporal antes de ejecutarlas; no se instalan
  como plugin ni entran en el descubrimiento normal de pytest.
- La suite consumidora inspeccionada usa un helper de construcción y AnyIO
  sobre asyncio. Sus casos de negocio son async; sus casos sync son de
  construcción. El canario no debe presentarlos como cobertura de CRUD sync.
- Su packaging admite Python anteriores a 3.13. Una dependencia de pruebas
  incondicional elevaría su mínimo: la adopción debe limitarse a entornos 3.13+
  o mantener preparación alternativa en intérpretes anteriores.

## Contrato propuesto

### Instalación y activación

Añadir **un extra `mongoeco[pytest]`** con `pytest>=8.4,<10` y el mismo rango
PyMongo del extra wire. Activación explícita mediante `pytest_plugins` en el
`conftest.py` raíz o `pytest -p`; sin autoload `pytest11` en esta primera entrega.
La instalación base y los tests que no usan las fixtures conservan su coste y
comportamiento. No se modifica `conformance.pytest` ni se fusionan sus extras.

Módulos propuestos:

| Módulo | Contrato |
| --- | --- |
| `mongoeco.pytest_plugin` | Fixtures comunes, clientes sync y opciones |
| `mongoeco.pytest_asyncio` | Carga el común y ofrece la fixture async en pytest-asyncio >=1.4,<2 |
| `mongoeco.pytest_anyio` | Carga el común y ofrece la fixture async en AnyIO >=4.11,<5, backend asyncio |

El consumidor conserva su runner y lo instala como dependencia de pruebas.
Se admite un adaptador async por ejecución. Cargar ambos o pedir un backend
Trio produce un error de configuración claro antes de crear recursos. El
adaptador AnyIO no cambia globalmente `anyio_backend` ni el modo del runner.
La coexistencia con pytest-asyncio exige strict; no se acepta la captura de
fixtures AnyIO por su modo auto. El adaptador pytest-asyncio admite strict y
auto, con loop de función para el cliente y el test.

### Fixtures y configuración

| Fixture, siempre de ámbito función | Valor y ownership |
| --- | --- |
| `mongoeco_uri` | URI de un servidor exclusivo de esa prueba y variante de motor |
| `mongoeco_client` | `pymongo.MongoClient` real, administrado por el kit |
| `mongoeco_async_client` | `pymongo.AsyncMongoClient` real, administrado por el adaptador elegido |
| `mongoeco_client_options` | Diccionario redefinible en `conftest.py`, pasado a los constructores nativos |

URI y ambos clientes de una prueba comparten servidor y datos. Otra prueba
obtiene otro servidor y motor completos, incluidos índices, usuarios y catálogo:
no basta borrar una colección o cambiar el nombre de base de datos.
Los clientes administrados añaden `serverSelectionTimeoutMS=3000` salvo que el
consumidor lo configure. No se descartan opciones desconocidas: se dejan validar
por PyMongo. `host` y `port` no pueden sustituir el endpoint administrado.
No se convierten errores BSON, resultados o excepciones nativas en wrappers.

Opciones propuestas:

- `--mongoeco-backend=memory|sqlite|both`, default `memory`.
- `--mongoeco-mongodb-dialect=7.0|8.0|9.0`, default `7.0`.
- `--mongoeco-pymongo-profile`, default `strict-auto-installed`, reutilizando
  el resolver público existente y rechazando SDKs sin perfil exacto.

`both` parametriza únicamente los tests que dependen directa o indirectamente
del kit, con IDs `memory` y `sqlite`. No duplica tests ajenos. La elección de
motor se realiza por esta opción, sin otro sistema de parametrización público.
Los defaults globales de Mongoeco no cambian.

Uso implementado en el candidato, pendiente de publicación:

```python
pytest_plugins = ("mongoeco.pytest_plugin",)


def test_write(mongoeco_client):
    result = mongoeco_client.app.items.insert_one({"value": 1})
    assert mongoeco_client.app.items.find_one({"_id": result.inserted_id})["value"] == 1
```

Quien conserva su propio constructor puede consumir solo `mongoeco_uri` y
seguir usando `with MongoClient(...)` o `async with AsyncMongoClient(...)` en
su fixture. **El kit cierra los clientes que crea; el consumidor cierra los
clientes que crea desde la URI.** No se parchean constructores globales para
adivinar su ownership. Los timeouts del kit no reemplazan la configuración
de clientes construidos por el consumidor.

### Ciclo de vida y fallos

Un hilo no daemon y un loop propios por servidor permiten atender al cliente
sync sin bloquear al servidor. El cliente async se usa y cierra en el loop
del test; nunca se traslada al hilo del servidor. No se exige que la aplicación
use `asyncio.to_thread` ni se ejecuta un loop anidado en su test.

Memory se crea por prueba. SQLite usa un directorio temporal exclusivo y un
executor propio (`executor_workers=1`); así el kit puede cerrar los recursos
que crea sin depender de un pool global. No se abre un SQLite del consumidor.

Orden de finalización: clientes administrados; detener admisión; cerrar
conexiones activas y esperar/cancelar sus handlers; desconectar motor propio;
cerrar loop y executor; unir hilo; retirar directorio SQLite, WAL y SHM.
Al cerrar se abortan transportes con bytes pendientes: no se espera a que un
peer consuma una respuesta. Las aceptaciones ya encoladas se rechazan si
terminan después de iniciar el cierre.
Un cliente prestado al proxy conserva su ownership actual. Capturar el cierre
de cada cliente al crearlo evita que un spy posterior deje recursos abiertos.

Registrar finalizadores en cuanto existe cada recurso, también antes del
`yield`. Fallos de bind o arranque deben revertir el motor conectado. Cleanup
idempotente y protegido frente a cancelación; si falla una fase, se intenta
el resto y pytest conserva tanto el fallo original como el error de teardown.
Arranque y join tienen un límite de 10 segundos: excederlo falla el gate;
un hilo daemon o un timeout silenciado no cuentan como limpieza correcta.
No se promete recuperación después de SIGKILL o caída del intérprete.

## Corpus finito y aceptación

| ID | Garantía observable |
| --- | --- |
| K01 | Cliente sync nativo: escritura, lectura, duplicate code 11000 y cierre |
| K02 | Cliente async nativo: mismas operaciones y errores en su loop |
| K03 | Solo URI: constructor y configuración propios, BSON y tipos conservados |
| K04 | Repetir nombres de DB, colección, IDs e índices entre pruebas sin contaminación |
| K05 | `both` duplica solo consumidores del kit, con IDs estables |
| K06 | Configuración del kit inválida falla antes de abrir sockets, hilos o SQLite |
| K07 | Cierre con peer abierto termina, cierra su transporte (EOF/reset) y no deja handlers |
| K08 | Bind fallido desconecta el motor ya conectado; puerto ocupado ajeno sigue intacto |
| K09 | Fallo parcial de arranque libera cada recurso creado antes del fallo |
| K10 | Aserción fallida conserva el fallo del test y cierra clientes y servidor |
| K11 | Fallo de otra fixture, antes y después del arranque, no deja recursos |
| K12 | Cancelación durante operación async o setup ejecuta finalizadores |
| K13 | Un cierre que falla o un spy de `close` no impide los otros cierres |
| K14 | Cierre repetido y proxy con cliente prestado preservan su ownership |
| K15 | SQLite cierra executor y archivos; elimina su directorio sin tocar archivos ajenos |
| K16 | Repeticiones con `xdist -n 2` y `-n 4`: datos independientes, puertos simultáneos distintos |
| K17 | Async: ambos adaptadores en sus modos admitidos, errores claros para combinaciones incompatibles |
| K18 | Imports/instalación base sin extra, carga explícita y ausencia de efectos sobre tests ajenos |
| K19 | Wheel instalado, typing y contratos públicos correctos; ninguna importación desde checkout |
| K20 | Canario consumidor: solo cambia preparación; aserciones y lógica originales intactas |

K01–K16 se ejercitan en cada motor y fachada aplicables. K17 cubre ambos
runners por separado. El contrato del kit se ejecuta en CPython 3.13/3.14/3.15
con pytest 8.4 y 9.x; los extremos SDK 4.9.2 y 4.19.0 se comprueban sin ampliar
silenciosamente el rango de perfiles. Los probes SDK existentes se conservan.
Las combinaciones async que no existan en un extremo se documentan como
incompatibilidad y bloquean el rango prometido; no se convierten en skips verdes.

Además de sus asserts, los subprocesos de fallos deben mostrar que el endpoint
rechaza conexiones, desaparecen hilos y tareas propios, el SQLite puede
reabrirse sin bloqueo antes de retirarlo y no hay warnings de recursos ni
excepciones de tareas abandonadas. Un exit code esperado por el fallo inducido
no sustituye esas comprobaciones. Se permite reutilización de puertos después
de cerrar un servidor; solo se exige unicidad entre servidores vivos.

El canario externo ejecuta la suite de negocio inspeccionada, con Memory y
SQLite, contra el mismo wheel que los gates. Se fija revisión e inventario;
no se recortan tests fallidos ni cambian expectativas para admitir Mongoeco.
Su preparación recibe la fixture async o la URI. Las pruebas de contrato sync
son una selección separada y deben contabilizarse como tal. Se ejecuta el
canario en los tres Python soportados; su pytest debe cumplir también los
requisitos del consumidor. Si la sustitución descubre semántica nueva fuera
del corpus, se registra el impedimento y se vuelve a decidir el alcance.

## Piezas, alternativas y garantía diferencial

| Pieza | Disposición | Requisito y consecuencia de retirarla |
| --- | --- | --- |
| Proxy y executor wire actuales | **reuse** | Mantener clientes nativos; un API simulado pierde ese contrato |
| Transporte y handlers en cierre | **consolidate** en el proxy existente | Su cierre actual no garantiza finalización con peers abiertos |
| Loop/hilo de servidor | **justify**, helper privado | Atender clientes sync mientras el test bloquea su propio hilo |
| Motores y temporales | **reuse / derive** | Aislamiento de todo el catálogo y liberación del SQLite por prueba |
| Fixtures y hook de parametrización | **justify** | Integración pytest y matriz opt-in sin preparación repetida |
| Dos adaptadores async delgados | **justify** | Respetar el runner existente y el loop del cliente; no duplicar lifecycle |
| Resolver de perfil y opciones PyMongo | **reuse** | Validar el SDK realmente instalado sin cambiar defaults del producto |
| Conformance assertion/extra actuales | **reuse** sin fusionar | Prueban motores externos, no preparación de consumidores PyMongo |
| Autoload, cliente wrapper, servidor compartido de sesión, factories públicas adicionales | **remove del diseño** | No añaden garantía al alcance; constructores PyMongo y URI ya cubren configuración |
| Canario real e informe anónimo | **derive** | Los tests propios no prueban adopción ni distribución instalada |

Mongoeco posee plugin, proxy, packaging y gates. El consumidor posee su
preparación y código. Se descartan reemplazar imports, monkeypatch global de
PyMongo y un servidor compartido con borrado parcial entre tests: no conservan
las garantías solicitadas. El SPI y la persistencia no necesitan otra decisión.

## Plan acotado

1. Corregir lifecycle del proxy con K07–K09/K14 y regresiones wire existentes.
2. Añadir helper privado, extra, fixtures y adaptadores; K01–K06/K10–K18.
3. Construir wheel e instalarlo fuera del checkout; K19/K20 y paralelismo.
   El launcher existente de escrituras permanece como regresión independiente;
   no sustituye este canario de adopción mediante fixtures.
4. Ejecutar matriz, suite completa, cobertura, typing, manifiesto, SPI,
   smokes wheel/sdist y ratchet habituales. Documentar uso y límites y revisar
   el candidato. El número de versión y la publicación siguen sin decidirse.

El encargo aprueba este contrato completo. Los cuatro bloques están completados;
el [cierre y evidencia](../pytest-integration-validation.md) conserva el candidato
final y sus resultados. El prototipo sigue siendo un antecedente, sin sustituir
las ejecuciones de aceptación. K06 valida las opciones propias del kit antes del
arranque; los demás kwargs los valida el constructor PyMongo con la URI creada
y, si los rechaza, la regresión exige el cierre de todos los recursos del setup.

## Confidencialidad y evidencia

Ningún archivo destinado a commit debe identificar proyectos consumidores,
enlaces a ellos, imports propios, rutas de checkout ni nombres de distribución.
Fuentes, revisión y mapeos privados permanecen fuera del repositorio. La
evidencia pública conserva IDs anónimos, hashes, versiones de herramientas,
inventario y resultados; un gate revisa también logs y metadatos de wheel/sdist.
La [anonimización del trabajo previo](../evidence/consumer-privacy/redaction.json)
conserva los resultados observados y la identidad hash de los originales.

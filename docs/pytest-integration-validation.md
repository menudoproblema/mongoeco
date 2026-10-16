# Validación de la integración pytest

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

Este registro conserva el candidato inicial. La [revisión posterior](pytest-review.md)
documenta las correcciones adicionales de lifecycle y scope, con la identidad
y evidencia del candidato que lo sustituye.

Implementación completada el **15 de octubre de 2026**, conforme al
[contrato K01–K20](architecture/pytest-integration-proposal.md).
La [guía](pytest.md) describe instalación, fixtures y configuración.
La publicación y el número de la siguiente versión siguen pendientes de decisión.

El candidato es el workset sin commit sobre
`3a05d7daa62115bf68d28d721aa2d3fe9b5c0eaa`. Conserva provisionalmente la versión
de paquete 4.9.1; **no es el artefacto publicado con ese número**. Su wheel tiene
SHA-256 `5aa7f4bc54877b1c2dfcc6ad9d88669c9ea0b88b75fd9b7ddb367a58e3b0b8c5`.
Wheel y sdist son reproducibles byte a byte; sus 280 archivos de runtime coinciden
con el checkout, el sdist y las tres instalaciones verificadas.

## Cambios y regresiones

El proxy conectaba el motor antes del bind y no lo desconectaba si este fallaba.
Además, su cierre esperaba a los peers abiertos sin cerrar primero sus
transportes y handlers. Ahora revierte el arranque parcial, aborta conexiones,
espera a los handlers y desconecta el motor propio, con cleanup protegido frente
a cancelaciones y respetando clientes prestados.

Las regresiones incluyen aceptaciones encoladas después del cierre, respuestas
grandes que el peer no consume y handlers ya terminados cuyo callback de retirada
sigue pendiente. Este último caso podía bloquear el loop durante el cierre.
Los dos últimos tienen reproducciones que fallan antes del arreglo y pasan
contra el candidato.

El extra opt-in `mongoeco[pytest]` incorpora URI y clientes PyMongo nativos,
adaptadores para pytest-asyncio y AnyIO sobre asyncio, y selección Memory/SQLite.
Cada prueba tiene motor, puerto y recursos propios. Las regresiones comprueban
aislamiento, configuración, errores nativos, fallos de setup y aserción,
cancelación, teardown y ausencia de recursos propios abiertos, también con
xdist de dos y cuatro workers. Los motores, el SPI y el formato SQLite no cambian
en este bloque; se preserva el trabajo anterior autorizado.

## Resultados del candidato

| Gate | Resultado |
| --- | --- |
| Suite completa, en cada Python 3.13/3.14/3.15 | 22.397 passed, 14.450 subtests passed, 31 skips previstos |
| Unittest, en cada intérprete | 3.625 tests, OK, 3 skips previstos |
| Kit y lifecycle: 3 Python × 2 pytest × 2 PyMongo | 12 celdas, 38 passed por celda; 456 ejecuciones |
| Canario consumidor contra el wheel instalado | 684 ejecuciones, sin fallos, errores ni skips |
| Cobertura, Python 3.14 | 99,04557 %; cinco módulos nuevos al 100 % |
| Instalaciones limpias | 9 con extras: wheel y sdist con engine-testing, y wheel con pytest, por intérprete; además, instalación base sin extras en Python 3.13 |
| Conformance | 6 ejecuciones: ambos motores en los tres Python |
| Propiedades profundas | 9 passed |
| Benchmarks de validación | Memory y SQLite completados |
| Typing, manifiesto público, consumidor SPI y snapshots | Correctos en los tres intérpretes |
| Ruff ratchet, twine y reproducibilidad | Correctos |

Los intérpretes ejecutados son CPython 3.13.15, 3.14.5 y 3.15.0. La matriz cruza
pytest 8.4.2/9.1.1 y PyMongo 4.9.2/4.19.0. El canario usa pytest 9.1.1 y PyMongo
4.19.0: sus 38 casos originales se ejecutan con ambos motores, en los tres Python,
sin workers y con dos y cuatro workers. Se sustituye únicamente su helper de
preparación; fuentes, lógica y aserciones originales permanecen intactas.

El inventario consumidor contiene 31 casos de negocio async y siete de
construcción sync. La cobertura de CRUD sync procede del contrato público del
kit, contabilizado por separado. Los informes contienen identificadores anónimos,
hashes y contadores; las fuentes y el mapeo del consumidor permanecen privados.
La instalación base ejecuta además el gate de imports, exportaciones y BSON
opcional, comprobando la ausencia de pytest, PyMongo y los runners async.

## Evidencia y límites

El [registro de validación](evidence/pytest-kit/validation.json) enlaza comandos,
resultados, hashes y manifiestos del candidato. Se revisaron el diff final y su
documentación; los gates locales requeridos se ejecutaron sobre el runtime final.
Los cambios posteriores se limitan a documentación, evidencia y configuración
CI, verificados sin atribuirles una nueva ejecución de las suites de runtime.

La ejecución local corresponde a macOS arm64. CI incorpora como obligatorio el
gate de doce celdas contra el wheel y la instalación limpia del extra; los jobs
alojados no se han ejecutado aquí. Este bloque no recaptura MongoDB real: las
suites completas reproducen las referencias nativas existentes. Los probes de
firmas SDK anteriores conservan sus entradas y se reutilizan como evidencia
independiente; la nueva matriz ejecuta realmente los dos extremos del SDK.

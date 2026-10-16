# Soporte de PyMongo 4.19

Disponible desde Mongoeco **4.10.0**. La versión publicada 4.9.1 no incluye
este perfil. No se cambia el mínimo Python 3.13 ni los
defaults MongoDB 7.0 / PyMongo 4.9.

## Selección y contrato

El perfil `4.19`, `PyMongoProfile419` y `PYMONGO_PROFILE_419` están disponibles en
`mongoeco.compat`. `auto-installed` y `strict-auto-installed` reconocen la serie
4.19 como coincidencia exacta; una serie futura desconocida sigue usando fallback
solo en el modo flexible. El dialecto MongoDB se selecciona por separado.

```python
from mongoeco import MongoClient

with MongoClient(pymongo_profile="4.19", mongodb_dialect="8.0") as client:
    client.example.records.insert_one({"_id": "example"})
```

El uso local sin URI SRV no requiere instalar PyMongo. Las operaciones y errores
de agregación conservan el contrato de 4.18. La CI principal instala PyMongo
4.19.0, y la matriz aislada conserva 4.9.2 / 4.11.3 / 4.13.2 / 4.17.0 / 4.18.2 y
añade 4.19.0. El extra `mongodb9` mantiene su mínimo 4.18.2: el dialecto del
servidor y la versión del SDK son ejes independientes.

## Descubrimiento SRV

Ambas fachadas aceptan el argumento `srv_host_validator: Callable[[str], bool]`.
Solo se permite con perfil 4.19 y una URI `mongodb+srv://`. La resolución inicial
usa `pymongo.uri_parser.parse_uri` de PyMongo >=4.19; no se usan APIs privadas.

```python
client = MongoClient(
    uri="mongodb+srv://cluster.example.net/",
    pymongo_profile="4.19",
    srv_host_validator=lambda host: host.endswith(".trusted.internal"),
)
```

La construcción del cliente conserva el descubrimiento inicial inmediato de
Mongoeco, incluida su fachada async. El callback sustituye la política de dominio
por defecto y debe validar todos los dominios permitidos. Cada host devuelto se
valida antes de aplicar `srvMaxHosts`; `False`, cualquier resultado que no sea un
booleano o una excepción del callback rechazan el descubrimiento completo con
`ConfigurationError`. No se admite un callback asíncrono.

El callback y `srvAllowedHostsSuffix` son mutuamente excluyentes. Un valor no
callable falla con `ValueError`. El callback no puede expresarse en la URI, ni
combinarse con `srv_records` o `srv_resolver` de simulación. Las credenciales,
TXT y precedencia de opciones siguen el resolver público. Los sufijos privados
de una sola etiqueta, como `internal`, `test` o `localhost`, se validan con el
driver 4.19; los sufijos públicos y los hosts fuera de la política se rechazan.

El perfil 4.19 rechaza percent-encoding en hostnames con `InvalidURI`, subclase
local de `ConfigurationError` y compatible con la excepción homónima de PyMongo
cuando está instalado. La validación precede a DNS y también se aplica a URI
`mongodb://`. Los límites preexistentes de IPv6 y sockets Unix no se amplían.

## Cambios upstream y alcance

El análisis usa el [changelog del tag 4.19.0](https://github.com/mongodb/mongo-python-driver/blob/4.19.0/doc/changelog.rst)
y el [diff oficial desde 4.18.2](https://github.com/mongodb/mongo-python-driver/compare/4.18.2...4.19.0).
La documentación web y Context7 aún describían 4.18 al realizar el análisis;
la fuente de esta entrega es el tag publicado el 14 de octubre de 2026.
Las sondas distinguen el delta declarado del observado: `srv_host_validator`
se acepta a partir de 4.19; el sufijo privado y el rechazo de hostnames
codificados ensayados también pasan con el artefacto 4.18.2. No se anuncian
como novedades exclusivas de 4.19.

Los cambios internos de cursor, bulk y sesión se contrastan usando ambas APIs
del driver real contra el proxy, con paginación `getMore`, escrituras y sesiones
explícitas. El codec BSON sigue delegado al driver opcional; las pruebas de la
frontera wire rechazan elementos que consumen el terminador del documento y
conservan la propiedad de buffers y RawBSON. No se eleva la versión wire anunciada.

No se añade CSFLE/QE, callbacks KMS, `append_metadata`,
subintérpretes ni polling SRV al runtime local. La disponibilidad de estas
funciones en PyMongo no implica soporte de Mongoeco. DNS se prueba con el
resolver público real y respuestas controladas; no acredita una instalación DNS
o TLS externa.

El soporte oficial del intérprete Python 3.15 se incorpora en un bloque
posterior de la misma entrega, con su propia
[política y validación](python-support.md).

El consumidor externo observado ya fija PyMongo 4.19.0; el testkit observado
mantiene una restricción de Mongoeco anterior a 4.9. Sus manifests no se cambian
en esta implementación ni se acredita su adopción permanente.

## Verificación del candidato histórico

Esta sección conserva los resultados previos al cierre. La identidad y los gates
de la entrega están en [release-4.10.0.md](release-4.10.0.md).

Las pruebas protegen selección explícita y automática, defaults, errores de
configuración, callback por host, rechazo completo, sufijos privados y públicos,
la frontera BSON y CRUD/bulk/cursor/sesiones con clientes reales. El corpus de
114 actualizaciones posicionales se repite con perfiles 4.9 y 4.19 en ambos
motores y fachadas, conservando códigos y documentos persistidos.

Los resultados finales se registran en
[evidence/pymongo419/validation.json](evidence/pymongo419/validation.json).

Sobre el wheel candidato instalado, Python 3.13 y 3.14 pasan 8.955 pruebas,
31 skips esperados y 4.913 subtests por versión. La cobertura es del 99,01 %
con el floor original del 99 %. `unittest` pasa sus 3.606 tests con tres skips
esperados; las nueve propiedades pasan también con el perfil deep.

La matriz SDK ejecuta 324 sondas sin resultados indeterminados. Los clientes
4.19 pasan bulk, getMore y sesiones a través del proxy, y once tests de imports
mínimos pasan sobre el wheel sin PyMongo instalado. Typing público, manifiesto,
snapshots, canario SPI y lint ratchet también pasan. Los smokes Search/vector
de Memory y SQLite son comprobaciones funcionales; no acreditan una mejora de
rendimiento ni una comparación porcentual con la entrega anterior.

La matriz real usa MongoDB 7.0.39 / 8.0.32 / 9.0.2 con FCV exacta y PyMongo
4.19.0. Comprueba 1.368 observaciones posicionales: 114 casos × dos motores ×
dos perfiles × tres servidores, incluidos códigos 2, 28, 40 y 65. También pasa
los 29 casos anteriores y 284 capturas por servidor. Las instancias temporales
propias terminan y sus directorios se eliminan al cerrar la ejecución.

Esta evidencia es local. No se ha creado una etiqueta, ejecutado CI remota ni
publicado una nueva versión como parte de este cambio.

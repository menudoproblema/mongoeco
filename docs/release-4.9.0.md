# Release 4.9.0

Fecha: 8 de octubre de 2026. Release menor de compatibilidad y correcciones
sobre Mongoeco 4.8.1.

Estado: **publicada y verificada en [PyPI](https://pypi.org/project/mongoeco/4.9.0/)**.
La etiqueta `v4.9.0` identifica el commit
`7612872b5052c25de223825cd881445cc6f106f1`. La
[CI de entrega](https://github.com/menudoproblema/mongoeco/actions/runs/37816027502)
pasó todos los gates requeridos antes de publicar. El
[registro de recuperación](mongodb9-pymongo418-improvements-progress.md#recuperación-de-la-entrega)
conserva la primera ejecución fallida y la sustitución autorizada de la etiqueta.

## Alcance y selección

Mongoeco incorpora el dialecto explícito MongoDB `9.0` y el perfil PyMongo
`4.18` dentro de su subset local documentado. Los defaults continúan siendo
MongoDB `7.0` y PyMongo `4.9`; la versión del servidor, el perfil de API y las
capacidades wire siguen siendo ejes independientes.

```python
from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine

with MongoClient(
    MemoryEngine(), mongodb_dialect="9.0", pymongo_profile="4.18"
) as client:
    collection = client.example.documents
```

El extra `mongodb9` instala `pymongo>=4.18.2,<5`. La guía de
[soporte y límites](mongodb9-pymongo418.md) detalla el comportamiento de arrays,
conversiones escalares, ventanas, variables, índices y orden de `$merge`.
SRV usa la API pública de PyMongo 4.18 y conserva validación de sufijo, TXT y
precedencia URI. Los retries comparten identidad de operación y mantienen una
identidad de request por intento, con cleanup al cancelar.

## Cambios observables y migración

- `$densify` conserva todos los originales, contenido y duplicados, incluso
  fuera de bounds explícitos; solo genera valores dentro del intervalo
  semiabierto. Corrige también los dialectos 7/8 y conserva las diferencias
  nativas acreditadas para bounds iguales, particiones y pasos que no avanzan.
- Las ventanas temporales aceptan los límites válidos acreditados más allá del
  rango de `datetime`, sin cambiar precisión BSON ni fechas persistidas.
  Variables y paths inválidos usan códigos y precedencia nativos.
- La recreación de índices conserva identidad, opciones y metadata. Los
  conflictos de nombre usan `86/IndexKeySpecsConflict`; un lote inválido
  conserva el catálogo preexistente. `_id_` mantiene su definición única e
  inmutable en el SPI.
- La preparación reutilizada conserva namespace, bindings, ámbitos, posiciones,
  recursos e invalidación. Las entradas directas de `$convert` y `$group`
  mantienen sus controles. Los callbacks públicos antiguos del driver siguen
  siendo válidos y un `TypeError` de su implementación no provoca una segunda
  adquisición.
- El perfil 4.18 rechaza opciones reservadas de agregación antes de crear
  cursores o ejecutar I/O.

No hay migración de datos, cambios del Engine SPI v2, search-v1, contrato CXP,
import roots ni formatos persistentes. Las salidas incorrectas corregidas en
7/8 pueden cambiar resultados o errores; son correcciones de fidelidad, no un
cambio automático de defaults.

`wildcardProjection` es un límite del subset de Mongoeco: se conserva la
aceptación histórica sin efecto en 7/8 y el rechazo previo al lote en 9.
Database.aggregate, Search v2, semántica Atlas remota y las conversiones fuera
del subset aprobado siguen excluidos. Una captura almacenada no acredita por
sí sola soporte.

La [adopción en consumidores](evidence/mongoeco-4.9.0/consumer-adoption-final.json)
registra las copias efímeras ensayadas: pins de provider/testkit, restricción
PyMongo del framework, migración de una aserción CXP preexistente y selección
explícita de perfiles. Los repositorios owner no se modificaron; la adopción
permanente requiere esos ajustes y su configuración propia.

## Verificación y rendimiento

La [evidencia del cierre técnico](evidence/mongoeco-4.9.0/validation.json)
identifica el candidato anterior a los commits y sus gates:

| Gate local | Resultado |
| --- | --- |
| Pytest, Python 3.13 y 3.14 | 8.803 passed, 30 skips esperados y 3.990 subtests en cada versión. |
| Cobertura | 99,0094427 %, sin nuevas exclusiones. |
| Unittest / propiedades | 3.596 tests; nueve propiedades en ci y deep, con shrinking. |
| MongoDB real 7.0.43 / 8.0.32 / 9.0.2 | FCV exacta, 29 casos obligatorios por servidor sin omisiones; 284 capturas por versión. |
| Consumidores aislados | Provider 21 y testkit 135 por configuración; consumo indirecto 39; typing y resolución completa. |
| Contratos y packaging | Lint ratchet, typing, manifests, conformance, SQLite 4.5, imports mínimos, instalaciones limpias y dos builds reproducibles. |

La [matriz efectiva](evidence/mongoeco-4.9.0/capture-coverage.json) tiene 852
filas; 431 caracterizaciones siguen excluidas de paridad y no se anuncian como
capacidades protegidas. Los servicios propios se cerraron sin bases de prueba
restantes. La primera CI alojada pasó build, perfiles SDK, imports mínimos y
ambas versiones de Python, pero falló la comparación de capturas semánticas
en MongoDB 8/9 por el orden de enumeración de tipos BSON. La CI de entrega
del commit corregido pasó 8.830 pruebas, 30 skips esperados y 3.990 subtests
en cada versión de Python, con cobertura 99,01 % y nueve propiedades deep.
La matriz requerida repitió los 29 casos de paridad y las 284 capturas por
servidor contra el wheel inmutable, con FCV exacta y sin skips inesperados.
La [evidencia de publicación](evidence/mongoeco-4.9.0/ci-recovery/published/published-verification.json)
añade descarga desde PyPI, identidad de artefactos, instalación limpia sin
constraints internas, contrato público, manifests, ambos engines/fachadas y
conformance del perfil core. Esa evidencia separa los gates del artefacto
publicado de los antecedentes locales.

La [comparación de rendimiento](evidence/mongoeco-4.9.0/performance-final-summary.json)
usa el wheel publicado 4.8.1, harness y dependencias equivalentes, calentamiento
y al menos cinco repeticiones. Los workloads principales tuvieron menores
tiempos medios; streaming dio resultados mixtos. En las quince muestras
agrupadas, SQLite mostró +3,9 % de mediana de tiempo real y CPU prácticamente
igual. No se afirma una mejora general ni ausencia global de regresiones.
El RSS muestreado tampoco acredita un límite de memoria.

## Artefactos y entrega Git

Los cambios se agrupan en commits semánticos de implementación, CI,
documentación y preparación de 4.9.0. La versión canónica reside en
`src/mongoeco/_version.py`; metadata dinámica, lock y changelog quedan alineados.

Los dos builds locales, la CI previa de `main`, la CI de la etiqueta y los
archivos descargados de PyPI producen la misma pareja de artefactos:

| Artefacto | SHA-256 |
| --- | --- |
| `mongoeco-4.9.0-py3-none-any.whl` | `4214ea98d80389929a43e0f59177c3fc88535f93cb6be16178f911a64df42d3f` |
| `mongoeco-4.9.0.tar.gz` | `ad940c391dccb2ce6262257da99c1d59283bba3aa09e6dfb8b51956306705e85` |

La publicación promovió los artefactos comprobados sin reconstruirlos. Los
273 archivos del paquete y los requisitos de dependencias son idénticos al
candidato técnico original. `dist/4.9.0-recovery/` conserva los builds y el
recibo consolidado; `dist/4.9.0-release/` conserva la pareja anterior fallida.
Los hashes y recibos históricos no se reescriben.

La etiqueta anotada final tiene el objeto
`dbe2eea4b2d5c1efb1634e9dec678b5e41156378`. Sustituyó con autorización directa
el objeto `6454a429ebeda70aa7b4a5a074962ce91f437513`, que apuntaba a
`d85c4b7de313028b709cde2fce4cb89dae80d513`, usando un lease sobre ese objeto.
Se conserva el original en la referencia local
`refs/codex/recovery/v4.9.0-original` y en la evidencia histórica.

Trusted Publishing OIDC terminó correctamente para owner `menudoproblema`,
repositorio `mongoeco`, workflow `ci.yml` y environment `pypi`. No hubo lectura
autenticada de la configuración privada de PyPI; el resultado de publicación
y los hashes oficiales acreditan la entrega efectiva. El primer intento de
instalación encontró el índice simple aún sin propagar; después de comprobar
que anunciaba ambos archivos, una instalación en otro entorno limpio pasó.

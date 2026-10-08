# Release 4.9.0

Fecha: 8 de octubre de 2026. Release menor de compatibilidad y correcciones
sobre Mongoeco 4.8.1.

Estado: en recuperación de la entrega. La primera etiqueta `v4.9.0` se envió
al remoto; su CI bloqueó correctamente la publicación. La versión permanece
en `4.9.0`. El [registro de recuperación](mongodb9-pymongo418-improvements-progress.md#recuperación-de-la-entrega)
separa esa ejecución de las verificaciones de la corrección.

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
en MongoDB 8/9 por el orden de enumeración de tipos BSON. Sus resultados no
acreditan el commit de recuperación; este requiere su propia CI verde.

La [comparación de rendimiento](evidence/mongoeco-4.9.0/performance-final-summary.json)
usa el wheel publicado 4.8.1, harness y dependencias equivalentes, calentamiento
y al menos cinco repeticiones. Los workloads principales tuvieron menores
tiempos medios; streaming dio resultados mixtos. En las quince muestras
agrupadas, SQLite mostró +3,9 % de mediana de tiempo real y CPU prácticamente
igual. No se afirma una mejora general ni ausencia global de regresiones.
El RSS muestreado tampoco acredita un límite de memoria.

## Artefactos y preparación Git

Los cambios se agrupan en commits semánticos de implementación, CI,
documentación y preparación de 4.9.0. La versión canónica reside en
`src/mongoeco/_version.py`; metadata dinámica, lock y changelog quedan alineados.

El wheel y sdist reconstruidos desde el commit de release se conservan en
`dist/4.9.0-release/`. `SHA256SUMS` y `release-verification.json` identifican
commit, epoch, builds reproducibles, equivalencia del código probado y smokes
de instalación. El cierre técnico y sus hashes anteriores se conservan como
antecedentes; la nueva identidad de distribución no se presenta como una nueva
ejecución de las suites completas.

Enviar `v4.9.0` activa el workflow de publicación. Antes de enviarla, comprobar
el Trusted Publisher vigente de PyPI: owner `menudoproblema`, repositorio
`mongoeco`, workflow `ci.yml`, environment `pypi`. La publicación OIDC de 4.8.1
está [acreditada](release-4.8.1.md); en esta sesión no hay acceso autenticado a
la configuración privada actual de PyPI. Esa comprobación y los gates alojados
pertenecen a la entrega remota posterior.

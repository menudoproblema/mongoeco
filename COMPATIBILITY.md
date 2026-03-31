# Compatibility Guide

Esta guía resume cómo configurar `mongoeco` cuando quieres controlar:

* la semántica objetivo de MongoDB (`mongodb_dialect`)
* la superficie pública objetivo de PyMongo (`pymongo_profile`)

## 1. Dos ejes distintos

`mongoeco` separa dos conceptos:

* `mongodb_dialect`
  * controla semántica observable del servidor
  * ejemplos: comparación con `null`, tratamiento de `undefined`, validaciones y deltas de MQL
* `pymongo_profile`
  * controla compatibilidad de la API Python
  * ejemplos: parámetros aceptados por métodos públicos o diferencias pequeñas de superficie

La versión instalada de `pymongo` **no** decide la semántica del servidor MongoDB.

## 1.1 Baseline soportado

`mongoeco` no persigue compatibilidad hacia atrás por debajo de estos mínimos:

* MongoDB `7.0`
* PyMongo `4.9`

Consecuencias prácticas:

* no se aceptan como objetivo de diseño semánticas específicas de MongoDB `6.x` o anteriores
* no se aceptan como objetivo de diseño firmas o comportamientos específicos de PyMongo anteriores a `4.9`
* cuando se amplía superficie pública o semántica, la referencia es siempre PyMongo `4.9+` sobre dialectos MongoDB `7.0+`

## 2. Configuración explícita recomendada

La forma más estable y reproducible es fijar ambos ejes explícitamente:

```python
from mongoeco import AsyncMongoClient

client = AsyncMongoClient(
    mongodb_dialect="7.0",
    pymongo_profile="4.9",
)
```

También puedes usar los objetos oficiales:

```python
from mongoeco import AsyncMongoClient, MongoDialect70, PyMongoProfile411

client = AsyncMongoClient(
    mongodb_dialect=MongoDialect70(),
    pymongo_profile=PyMongoProfile411(),
)
```

La misma idea se aplica a ambos ejes: `mongoeco` resuelve y conserva metadata
de la decisión tomada.

## 3. Dialectos MongoDB disponibles

Hoy el catálogo oficial incluye:

* `7.0`
* `8.0`

Regla práctica:

* `7.0` es la baseline de desarrollo
* `8.0` se trata como compatibilidad adicional con deltas explícitos
* la selección del dialecto es explícita; `mongoeco` no autodetecta servidor en el flujo normal
* no existe catálogo oficial para versiones anteriores a `7.0`

## 3.1 Resolución del dialecto MongoDB

La API pública ya expone una resolución estructurada equivalente a la de
`pymongo_profile`:

```python
from mongoeco import resolve_mongodb_dialect_resolution

resolution = resolve_mongodb_dialect_resolution("8.0")

print(resolution.resolved_dialect.key)
print(resolution.resolution_mode)
```

Campos disponibles:

* `requested`
* `detected_server_version`
* `resolved_dialect`
* `resolution_mode`

Modos posibles hoy:

* `default`
* `explicit-alias`
* `explicit-instance`

## 4. Perfiles PyMongo disponibles

Hoy el catálogo oficial incluye:

* `4.9`
* `4.11`
* `4.13`

Regla práctica:

* `4.9` es la baseline de API pública
* `4.11` activa el primer delta real: `update_one(sort=...)`
* `4.13` queda disponible como perfil posterior compatible
* no existe catálogo oficial para perfiles anteriores a `4.9`

## 5. Autodetección de PyMongo instalada

Puedes pedir a `mongoeco` que resuelva el perfil según la versión instalada del
paquete `pymongo`.

### Modo flexible

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="auto-installed")
```

Política:

* si la versión instalada coincide con un perfil conocido, usa ese perfil
* si aparece una minor nueva dentro de la misma major conocida, cae al último
  perfil compatible de esa major
* si aparece una major nueva no registrada, falla

Ejemplos actuales:

* `4.8.x` -> error explícito
* `4.10.x` -> `4.9`
* `4.12.x` -> `4.11`
* `4.14.x` -> `4.13`
* `5.x` -> error explícito

### Modo estricto

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="strict-auto-installed")
```

Política:

* solo acepta versiones instaladas que encajen exactamente en un perfil
  registrado
* si aparece una minor nueva todavía no modelada, falla

Este modo es el recomendable para CI o validación contractual estricta.

## 6. Inspeccionar la resolución aplicada

Si quieres conocer exactamente qué política se ha aplicado, usa la API pública
de resolución:

```python
from mongoeco import resolve_pymongo_profile_resolution

resolution = resolve_pymongo_profile_resolution("auto-installed")

print(resolution.installed_version)
print(resolution.resolved_profile.key)
print(resolution.resolution_mode)
```

Campos disponibles:

* `requested`
* `installed_version`
* `resolved_profile`
* `resolution_mode`

Modos posibles hoy:

* `default`
* `explicit-alias`
* `explicit-instance`
* `auto-exact`
* `auto-compatible-minor-fallback`

También puedes inspeccionar la resolución ya aplicada en el cliente:

```python
from mongoeco import MongoClient

client = MongoClient(pymongo_profile="auto-installed")

print(client.pymongo_profile.key)
print(client.pymongo_profile_resolution.installed_version)
print(client.pymongo_profile_resolution.resolution_mode)
```

Y de forma simétrica para el dialecto:

```python
from mongoeco import MongoClient

client = MongoClient(mongodb_dialect="8.0")

print(client.mongodb_dialect.key)
print(client.mongodb_dialect_resolution.resolution_mode)
```

## 7. Recomendación operativa

Para trabajo diario:

* `mongodb_dialect="7.0"`
* `pymongo_profile="auto-installed"`

Para CI y suites de compatibilidad:

* `mongodb_dialect` fijado explícitamente
* `pymongo_profile` fijado explícitamente, o `strict-auto-installed`

## 8. Modo de planning

La compatibilidad semántica y la compatibilidad de API no sustituyen al modo de
planning.

`mongoeco` expone dos políticas:

* `PlanningMode.STRICT`
  * es la baseline recomendada
  * falla en compilación cuando el shape recibido no es ejecutable de forma
    coherente
* `PlanningMode.RELAXED`
  * conserva metadata de la operación y deja visibles `planning_issues`
  * no convierte documentos inválidos o no soportados en no-ops silenciosos
  * es útil para explain, tooling y superficies que prefieren degradación
    explícita frente a error inmediato

## 9. Alcance actual de collation

La implementación actual no intenta exponer toda la superficie de collation de
MongoDB.

Hoy el contrato soportado y testeado es:

* locales `simple` y `en`
* `strength` `1`, `2` y `3`
* `numericOrdering`
* `caseLevel`

Para collation Unicode:

* `mongoeco` prefiere `PyICU` cuando está disponible
* si `PyICU` no está instalado, usa `pyuca` como backend runtime de base
* ambas rutas quedan cubiertas por tests, pero pueden existir diferencias
  menores en tailoring avanzado fuera de este subconjunto soportado

## 10. Verificación contractual contra PyMongo real

La ampliación de superficie pública no debe decidirse por memoria ni por lectura
aislada de firmas.

El repositorio incluye un arnés repetible:

* [scripts/run_pymongo_profile_matrix.py](scripts/run_pymongo_profile_matrix.py)
* [tests/fixtures/pymongo_profile_matrix.json](tests/fixtures/pymongo_profile_matrix.json)

Uso recomendado:

```bash
python3 scripts/run_pymongo_profile_matrix.py
```

El script crea entornos aislados para `PyMongo 4.9`, `4.11` y `4.13`, ejecuta
una sonda de aceptación de parámetros reales y devuelve un JSON con los
resultados.

El JSON versionado en `tests/fixtures/` actúa como snapshot contractual del
último contraste validado y debe actualizarse cuando cambie la matriz real.

Regla de mantenimiento:

* cualquier parámetro nuevo en la API pública debe contrastarse primero con este
  arnés
* solo se añade un hook nuevo a `PyMongoProfile` cuando la matriz real detecta
  un delta observable entre perfiles

Matriz ya verificada:

* baseline común en `4.9/4.11/4.13`:
  * `hint`, `comment` y `let` en `update_*`, `replace_one`, `delete_*`
  * `comment` y `let` en `bulk_write`

## 9. Superficie aceptada frente a semántica efectiva

No toda opción aceptada por la API pública tiene ya un efecto real en los
engines locales.

El proyecto distingue ahora entre:

* `effective`
  * la opción ya participa en la semántica observable
* `accepted-noop`
  * la opción se acepta y valida por compatibilidad, pero todavía no cambia el
    comportamiento real del motor

API pública disponible:

```python
from mongoeco import (
    OPERATION_OPTION_SUPPORT,
    OptionSupportStatus,
    get_operation_option_support,
    is_operation_option_effective,
)

support = get_operation_option_support("aggregate", "let")
assert support is not None
assert support.status is OptionSupportStatus.EFFECTIVE

assert is_operation_option_effective("find", "hint")
```

Casos relevantes hoy:

* `aggregate(let=...)` -> `effective`
* `find(hint=...)` -> `effective`
* `find(comment=...)` -> `effective`
* `find(max_time_ms=...)` -> `effective`
* `find(batch_size=...)` -> `effective` con batching local del cursor
* `aggregate(batch_size=...)` -> `effective` en pipelines streamables; stages globales siguen materializando completo
* `update_one(let=...)` -> `effective` cuando el filtro usa `$expr`
* `replace_one(let=...)` -> `effective` cuando el filtro usa `$expr`
* `bulk_write(comment=...)` -> `effective`
* `bulk_write(let=...)` -> `effective` cuando las operaciones usan filtros con `$expr`

Regla de mantenimiento:

* no se debe promocionar una opción a `effective` sin test observable
* no se debe aceptar una opción nueva sin registrarla en esta matriz
  * `max_time_ms` en `find_one_and_*`
  * `hint`, `comment`, `let`, `batchSize/maxTimeMS` en `aggregate`
* delta real desde `4.11+`:
  * `sort` en `update_one`
  * `sort` en `replace_one`
  * `sort` en `UpdateOne(...)` y `ReplaceOne(...)` para `bulk_write`
* explícitamente no soportado en `4.9+`:
  * `max_time_ms` en `update_one`, `update_many`, `replace_one`,
    `delete_one` y `delete_many`

## 9. Qué no hace `mongoeco`

`mongoeco` no:

* infiere la semántica del servidor MongoDB a partir de la versión instalada de
  `pymongo`
* acepta silenciosamente majors nuevas de `pymongo`
* mezcla dialecto de servidor y perfil de driver en una sola opción

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

## 8. Qué no hace `mongoeco`

`mongoeco` no:

* infiere la semántica del servidor MongoDB a partir de la versión instalada de
  `pymongo`
* acepta silenciosamente majors nuevas de `pymongo`
* mezcla dialecto de servidor y perfil de driver en una sola opción

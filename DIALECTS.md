# Dialectos y Compatibilidad

Este documento define cómo debe evolucionar `mongoeco` para soportar
versiones futuras de MongoDB y distintas superficies de compatibilidad con
PyMongo sin duplicar la suite ni dispersar `if version == ...` por todo el
core.

Para una guía de uso orientada a configuración de aplicación, testing local y
CI, ver [COMPATIBILITY.md](/Users/uve/Proyectos/mongoeco2/COMPATIBILITY.md).

---

## 1. Principio General

La compatibilidad futura se gestiona en **dos ejes separados**:

* **Dialecto de servidor MongoDB**:
  define la semántica de MQL y del comportamiento observable del motor.
* **Perfil de API PyMongo**:
  define la superficie pública y los matices de compatibilidad del driver
  Python.

No deben mezclarse.

`pymongo` instalado en el entorno **no** determina la semántica del servidor.
Un mismo `pymongo` puede hablar con MongoDB 7.0, 8.0 o versiones posteriores.
Por tanto:

* la semántica del core debe depender del **dialecto MongoDB**
* la compatibilidad de la API pública debe depender del **perfil PyMongo**

Ambos ejes ya exponen resolución estructurada y metadata pública en cliente,
base de datos y colección.

---

## 2. Dialectos de MongoDB

### Objetivo

Evitar una arquitectura de "baseline + parches ad hoc" repartidos por todo el
código.

### Modelo

Se define una familia de dialectos semánticos, por ejemplo:

* `MongoDialect70`
* `MongoDialect80`
* `MongoDialect90`

La resolución pública actual del dialecto ya usa una estructura explícita
(`MongoDialectResolution`) para reflejar baseline, alias explícito o instancia
explícita.

Cada dialecto encapsula solo los puntos de decisión que realmente pueden
cambiar entre versiones:

* truthiness de expresiones
* comparación de valores y edge cases BSON
* soporte o rechazo de operadores
* validación de payloads
* diferencias entre error y no-op
* reglas sutiles de proyección, update y agregación

Además del comportamiento ejecutable, el catálogo oficial puede registrar
**flags de comportamiento versionados** para diferencias reales documentadas.

Esto sirve para:

* fijar el contrato en tests y documentación
* preparar un hueco de diseño antes o durante su conexión al core
* evitar introducir un delta artificial solo para “usar” el dialecto

Primer ejemplo registrado:

* `MongoDialect70.null_query_matches_undefined() -> True`
* `MongoDialect80.null_query_matches_undefined() -> False`

Este delta también vive en `MONGODB_DIALECT_BEHAVIOR_FLAGS` como dato
inmutable de módulo. `mongoeco` ya modela un valor `UNDEFINED` propio y este
delta ya está conectado al runtime en el perímetro actualmente soportado:

* igualdad por `null` y `$eq`
* `$in` que incluye `null`
* `$ne` / `$nin` frente a `null` cuando el campo está ausente
* `$lookup` con `localField` / `foreignField`

La suite local fija explícitamente la diferencia entre `7.0` y `8.0` en esos
caminos.

### Deltas activos hoy

| Caso | MongoDialect70 | MongoDialect80 | Cobertura |
|---|---|---|---|
| `null_query_matches_undefined()` | `True` | `False` | `tests/unit/test_compat.py` |
| Igualdad query `{"field": null}` sobre `UNDEFINED` | match | no match | `tests/unit/core/test_filtering.py` |
| `"$in": [null]` sobre `UNDEFINED` | match | no match | `tests/unit/core/test_filtering.py` |
| `"$ne": null` con campo ausente | no match | match | `tests/unit/core/test_filtering.py` |
| `"$nin": [null]` con campo ausente | no match | match | `tests/unit/core/test_filtering.py` |
| `$lookup` simple con `null` vs `UNDEFINED` | iguala | no iguala | `tests/unit/core/test_aggregation.py`, `tests/differential/*` |

### Regla de implementación

El resto del core **no debe conocer números de versión**.

En vez de:

* `if version == "8.0": ...`
* `if version >= "9.0": ...`

el core debe preguntar a una abstracción de dialecto:

* `dialect.expression_truthy(value)`
* `dialect.values_equal(left, right)`
* `dialect.compare_values(left, right)`
* `dialect.supports_query_operator(name)`
* `dialect.supports_expression_operator(name)`
* `dialect.validate_projection(spec)`
* `dialect.validate_update(spec)`

### Baseline de desarrollo

La baseline semántica actual del proyecto es:

* **MongoDB 7.0**

Esto implica:

* la suite principal diaria se ejecuta contra la baseline 7.0
* las divergencias futuras se expresan como **deltas de dialecto**
* MongoDB 8.0 y posteriores se incorporan como compatibilidad adicional, no
  como redefinición inmediata de toda la base

---

## 3. Perfiles de Compatibilidad con PyMongo

### Problema

La compatibilidad con PyMongo no debe modelarse como "adaptarse a la versión
instalada de `pymongo`" de forma implícita y global.

Eso sería frágil porque mezcla:

* semántica del servidor
* superficie del cliente Python
* entorno local del desarrollador

### Propuesta

Introducir una segunda abstracción, separada del dialecto MongoDB:

* `PyMongoProfile49`
* `PyMongoProfile411`
* `PyMongoProfile413`
* perfiles futuros si aparece una divergencia real adicional

Este perfil controla:

* nombres y disponibilidad de métodos públicos
* shape de resultados
* validaciones de API
* mensajes y tipos de error compatibles
* superficie de cursor y colecciones

Pero **no** controla:

* comparación BSON
* truthiness
* semántica de operadores MQL
* reglas del servidor

### Recomendación de configuración

La aplicación debería poder fijar ambos ejes de forma explícita:

* `mongodb_dialect="7.0"`
* `pymongo_profile="4.9"`

o bien:

* `mongodb_dialect=MongoDialect70()`
* `pymongo_profile=PyMongoProfile49()`

### Qué no se recomienda

No se recomienda que `mongoeco` "se case" automáticamente con la versión
instalada de `pymongo` como única fuente de verdad.

Eso puede servir solo como conveniencia opcional para seleccionar un perfil de
API, pero nunca como fuente de verdad de la semántica del servidor.

### Prioridad de resolución recomendada

1. valor explícito dado por la aplicación
2. fallback por defecto del proyecto

Para el perfil PyMongo:

1. valor explícito dado por la aplicación
2. autodetección opcional de la versión instalada de `pymongo`

### Estado actual del catálogo

El catálogo oficial hardcoded del repo modela hoy:

* `4.9` como baseline de API pública
* `4.11` como primer perfil con delta activo: `update_one(sort=...)`
* `4.13` como perfil posterior compatible, hoy sin un segundo delta activo adicional

La autodetección `pymongo_profile="auto-installed"` mapea:

* `4.9` a `4.10` -> `PyMongoProfile49`
* `4.11` a `4.12` -> `PyMongoProfile411`
* `4.13+` -> `PyMongoProfile413`

Versiones anteriores a `4.9` y series mayores desconocidas no se aceptan silenciosamente.

También existe `pymongo_profile="strict-auto-installed"` para entornos donde la
instalación local debe encajar exactamente en un perfil registrado y cualquier
minor nueva debe fallar de forma explícita.
3. fallback por defecto documentado

---

## 4. API Recomendada

La forma más sana de evolucionar esto es permitir una API explícita y estable.

### Estado actual

Este repositorio ya implementa la **infraestructura mínima pública**:

* `mongoeco.compat` expone dialectos y perfiles base
* el catálogo oficial vive en constantes inmutables de módulo
* `AsyncMongoClient` y `MongoClient` aceptan `mongodb_dialect` y
  `pymongo_profile`
* `Database` y `Collection` propagan la configuración efectiva

Lo que **aún no se ha hecho** de forma masiva es mover cada decisión semántica
del core a objetos de dialecto. Esa es la siguiente fase del refactor.

Ejemplo conceptual:

```python
AsyncMongoClient(
    engine,
    mongodb_dialect="7.0",
    pymongo_profile="4.9",
)

MongoClient(
    engine,
    mongodb_dialect="7.0",
    pymongo_profile="4.9",
)
```

También puede aceptarse una forma más avanzada:

```python
AsyncMongoClient(
    engine,
    mongodb_dialect=MongoDialect70(),
    pymongo_profile=PyMongoProfile49(),
)
```

### Modo `auto`

Se puede admitir, pero debe ser **opt-in** y con alcance acotado:

* `pymongo_profile="auto-installed"`

Restricciones:

* `auto-installed` solo debe ajustar la capa de API pública
* ningún modo `auto` debe cambiar silenciosamente la semántica del core sin que
  quede claro en logs o configuración efectiva

### Catálogo hardcoded e inmutable

El registro oficial de dialectos y perfiles vive en código como datos
inmutables de módulo:

* instancias singleton oficiales por versión
* alias oficiales de resolución
* pequeñas matrices de capacidades
* flags de comportamiento versionados

Eso da:

* tipado fuerte
* refactor seguro
* tests directos
* cero dependencia de configuración dinámica externa

Lo que no debe existir es un "dialecto activo global" mutable a nivel de
proceso. La selección efectiva sigue perteneciendo al cliente o a su contexto.

---

## 5. Estrategia de Tests

### Suite principal

La suite actual se conserva como suite principal de baseline.

Se divide conceptualmente en:

* **tests invariantes**:
  casos que deben comportarse igual en todos los dialectos soportados
* **tests versionados**:
  solo para zonas donde realmente exista una divergencia entre versiones
* **tests de rechazo explícito**:
  casos fuera de perímetro que deben fallar de forma clara y estable

### Regla clave

No se debe duplicar toda la suite para cada versión.

La estrategia correcta es:

* una suite principal compartida
* pocos tests parametrizados por dialecto
* runner diferencial contra Mongo real por versión

### Testing diferencial real

El repositorio ya incluye una capa opcional compartida:

* `tests/differential/_real_parity_base.py`
* `tests/differential/mongodb7_real_parity.py`
* `tests/differential/mongodb8_real_parity.py`
* `scripts/run_mongodb_real_differential.py`
* `scripts/run_mongodb7_differential.py`
* `scripts/run_mongodb8_differential.py`

Esta capa compara `mongoeco` contra un servidor real de MongoDB 7.0 u 8.0 en
casos de alto riesgo semántico.

La evolución prevista es:

* matriz explícita de divergencias por dialecto

### Filosofía de cobertura

Los tests no deben modelar una versión entera del producto como una copia
completa. Deben modelar:

* invariantes
* divergencias
* frontera de soporte

---

## 6. Flujo de Desarrollo Recomendado

### Día a día

* trabajar sobre la baseline `MongoDialect70`
* ejecutar la suite local normal
* usar tests versionados solo cuando una diferencia entre versiones ya exista o
  sea deliberada

### Al introducir una diferencia nueva

1. reproducirla con test diferencial contra Mongo real
2. decidir si es:
   * invariante
   * delta de 8.0
   * delta de 9.0
3. encapsular la decisión en el dialecto
4. añadir el test versionado mínimo necesario

### CI recomendada

* **CI normal**:
  suite local principal
* **CI extendida**:
  diferencial contra MongoDB 7.0 real
* **CI opcional o nocturna**:
  diferencial contra MongoDB 8.0 real

Esto evita convertir cada PR en una batería pesada de integración externa.

---

## 7. Qué Evitar

* dispersar `if version == ...` por módulos del core
* inferir semántica del servidor a partir de la versión instalada de `pymongo`
* duplicar suites enteras por versión
* mezclar compatibilidad de API Python con semántica MQL
* introducir diferencias de versión sin dejar test diferencial o test
  versionado asociado

---

## 8. Estado Actual

En el punto actual del proyecto:

* la baseline efectiva es **MongoDB 7.0**
* la suite local principal está verde y con cobertura completa
* existe un arnés diferencial opcional compartido para MongoDB 7.0 y 8.0
* la arquitectura de dialectos y perfiles de PyMongo ya existe como capa de
  ejecución pública y como punto de extensión del core
* ya hay un primer delta versionado registrado en el catálogo oficial y
  conectado a comportamiento activo del motor

Esto permite cerrar la fase actual sin rediseñar la suite, y a la vez deja una
dirección clara para soportar MongoDB 8/9 y compatibilidad de API con PyMongo
sin perder control del desarrollo.

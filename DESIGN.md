# Diseño de Arquitectura: mongoeco 2.0 (Async Reboot)

Guías complementarias:

* [DIALECTS.md](/Users/uve/Proyectos/mongoeco2/DIALECTS.md)
* [COMPATIBILITY.md](/Users/uve/Proyectos/mongoeco2/COMPATIBILITY.md)

Este documento define la visión técnica para la reescritura de **mongoeco**, enfocándose en un diseño moderno, asíncrono nativo y desacoplado.

---

## 1. Objetivos de Diseño
* **Async-First**: Implementación nativa con `async/await`.
* **Arquitectura Hexagonal**: Separación total entre lógica de MongoDB, almacenamiento y API de cara al usuario.
* **Persistencia Profesional**: Uso opcional de SQLite (JSON1) para soportar transacciones ACID reales y atomicidad sin bloqueos manuales complejos.
* **Single Source of Truth**: La semántica funcional vive en el Core y en la implementación async. La capa sync se resuelve mediante un adaptador delgado y esa estrategia queda asumida como dirección actual del proyecto.
* **Zero External Dependencies**: La librería debe funcionar sin `pymongo` instalado. Todas las excepciones y clases de resultado (`InsertResult`, etc.) deben ser implementaciones propias que emulen la API de PyMongo para evitar el acoplamiento.
* **Modern Stack**: 
    * **Python 3.13+**: La base de código adopta sintaxis y tipado modernos de Python 3.13 (`PEP 695`, `@override`, aliases `type`, `TypeIs`, etc.) como decisión explícita de Fase 1.
    * **MongoDB 7.0 Parity**: El Core persigue la semántica de la v7.0 dentro del perímetro explícito de Fase 1.
    * **PyMongo Profile Compatibility**: La API pública ya modela perfiles reales `4.9`, `4.11` y `4.13`, manteniendo `4.9` como baseline y activando el primer delta público observable en `4.11` (`update_one(sort=...)`).
* **Baseline de Compatibilidad Explícito**:
    * no se acepta trabajo orientado a compatibilidad con MongoDB `< 7.0`
    * no se acepta trabajo orientado a compatibilidad con PyMongo `< 4.9`
    * toda ampliación futura de superficie, semántica o firmas se evalúa exclusivamente contra MongoDB `7.0+` y PyMongo `4.9+`

---

## 2. Capas del Sistema

### A. Core Logic (Pure Functions)
Ubicación: `src/mongoeco/core/`
* No tiene dependencias de I/O ni de red.
* Contiene la "sabiduría" de MongoDB: cómo funcionan los operadores (`$set`, `$push`), cómo se comparan tipos y cómo se filtran documentos.
* Fácil de testear con unit tests puros.

### B. Storage Engine (Interface/Protocol)
Ubicación: `src/mongoeco/engines/`
* Define un `AsyncStorageEngine` (Protocolo).
* **MemoryEngine**: Implementación rápida para tests volátiles con lifecycle acotado por conexiones y locks compatibles con hilos.
* **SQLiteEngine**: Baseline funcional de Fase 2 sobre `sqlite3` estándar. Comparte el mismo protocolo async-first y ya pasa los contratos comunes, aunque las optimizaciones SQL avanzadas quedan para iteraciones posteriores.
* El protocolo ya no es un almacén puramente ciego: permite pushdown de filtros y proyecciones, borrado y actualización atómicos sobre un documento coincidente, conteo directo y metadatos básicos de índices para que futuros motores no queden condenados a escaneos completos.

### C. API Drivers
Ubicación: `src/mongoeco/api/`
* **_async/**: La implementación maestra que el usuario importa como `AsyncMongoClient`.
* **_sync/**: Adaptador sincronico sobre la implementación async. La base del desarrollo sigue siendo la API asíncrona.
* La capa sync de Fase 1 está pensada como adaptador práctico para scripts y uso moderado. No se considera todavía una historia de concurrencia fuerte para cargas multihilo intensivas.

---

## 3. Hoja de Ruta Inicial (Fase 1)
1. **Protocolo Engine**: Definir la interfaz mínima de persistencia.
2. **Motor de Comparación**: Trasladar la lógica de comparación de tipos de la v1.0 al core.
3. **Primer Driver**: Implementar `insert_one` y `find_one` asíncronos sobre el motor de memoria.
4. **Identidad Nativa**: Implementar `ObjectId` propio sin dependencia de `bson`.
5. **Mutaciones Iniciales**: Validar `update_one` con `$set` y `$unset` en el Core.
6. **API Sync Funcional**: Exponer `MongoClient` como adaptador sincronico sobre la capa async.

### Perímetro de Cierre de Fase 1
Estado actual: **Completado**.

La Fase 1 se considera cerrada cuando se cumplen estos puntos y solo estos puntos:
* **API pública async estable**: `AsyncMongoClient`, acceso a bases y colecciones, `insert_one`, `find_one`, `update_one`, `delete_one`, `count_documents`, `create_index` y `list_indexes`.
* **API pública sync funcional**: `MongoClient` como adaptador sobre la capa async, con la misma superficie mínima de operaciones ya cerrada en Fase 1.
* **Core mínimo operativo**: filtrado con igualdad, operadores básicos (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`), `$and`, `$or`, dot notation, arrays básicos, proyecciones y updates con `$set` y `$unset`.
* **Engine de referencia**: `MemoryEngine` debe pasar contratos de engine, integración de API async y de API sync.
* **Modelo de tipos mínimo**: `ObjectId` propio y `DocumentCodec` suficiente para preservar `datetime`, `UUID` y `ObjectId` sin colisiones con documentos del usuario ni reinterpretaciones accidentales de claves reservadas.
* **Hueco de sesión reservado**: la API pública acepta un contexto de sesión mínimo para no bloquear una evolución posterior hacia transacciones.
* **Protocolo preparado para consultas**: `scan_collection` reserva `sort`, `skip` y `limit`, y el engine expone `count_matching_documents` y `delete_matching_document` para no forzar una refactorización tardía al entrar en SQLite.
* **Compatibilidad de lenguaje**: baseline fijado en Python 3.13+.
* **Verificación ejecutable**: compilación del paquete y suite en verde sobre contratos, integración y unit tests.

### Fuera de Alcance de Fase 1
Estos puntos quedan explícitamente fuera del cierre de Fase 1:
* **Optimización SQL real en SQLite** (`json_extract`, pushdown semántico profundo, uso de índices para lectura y planificación de consultas).
* **Generación automática de la capa sync**. La sync de Fase 1 es un adaptador funcional, no código generado.
* **Cursores públicos** tipo `find()` con iteración múltiple.
* **Sesiones, transacciones y semántica distribuida**.
* **Índices reales** con enforcement, planificación de consultas o garantías de rendimiento. En Fase 1 solo existen metadatos mínimos de índices.
* **Paridad completa con MongoDB/PyMongo** fuera del subconjunto mínimo ya implementado y testeado.
* **Semántica transaccional real** de sesiones. El contexto existe, pero todavía no coordina aislamiento ni commits.
* **Optimizador de consultas** o selección automática de índices. En Fase 1, `create_index` solo registra metadatos mínimos.

---

## 4. Por qué este diseño es mejor
* **Mantenibilidad**: Si MongoDB cambia una regla de filtrado, solo tocas `core/`. Si quieres un nuevo almacenamiento (ej. Redis), solo creas un nuevo `engine`.
* **Async Nativo**: Soporta `motor` y frameworks modernos (FastAPI, etc.) sin hacks de hilos.
* **Cero Acoplamiento**: La API no sabe cómo se guardan los datos, solo sabe hablar con el protocolo del motor.
* **Perímetro Explícito**: El documento distingue entre lo ya consolidado en Fase 1 y la deuda diferida a Fase 2+, evitando cerrar en falso sobre promesas no implementadas.

## 4.1 Riesgos Arquitectónicos y Mitigaciones
La arquitectura actual es útil y ya produce valor real, pero conviene dejar explícitos sus puntos débiles para no disfrazar límites locales como si fueran paridad completa con MongoDB/PyMongo.

* **SQLite mezcla pushdown útil con fallback Python costoso**
  * riesgo:
    * consultas complejas pueden materializar demasiados documentos
    * la traducción SQL para orden BSON sigue siendo estructuralmente compleja
  * mitigación actual:
    * el fallback está explícito y testeado
    * `EXPLAIN QUERY PLAN` ya forma parte del contrato

* **La asincronía de SQLite no equivale a paralelismo alto**
  * riesgo:
    * la conexión y el lock global siguen serializando gran parte del trabajo
    * una transacción activa monopoliza la conexión del motor
  * mitigación actual:
    * el límite queda asumido como diseño local, no como comportamiento de driver de red

* **La paridad por opción no es homogénea**
  * riesgo:
    * aceptar una opción sin efecto real puede inducir a error
  * mitigación actual:
    * el proyecto mantiene ya una matriz explícita de soporte por operación/opción
    * se distingue entre `effective` y `accepted-noop`

* **El protocolo de engine tiende a crecer**
  * riesgo:
    * CRUD, índices, administración, explain y sesión pueden quedar demasiado acoplados
  * mitigación actual:
    * el protocolo principal ya está factorizado en subcontratos menores (`Session`, `Lifecycle`, `CRUD`, `IndexAdmin`, `Explain`, `Admin`)

* **La metadata administrativa puede divergir entre engines**
  * riesgo:
    * `list_indexes()` e `index_information()` podían acabar devolviendo formas distintas
  * mitigación actual:
    * la metadata pública de índices ya sale de un tipo compartido (`IndexDefinition`)

* **La capa sync sigue siendo un adaptador**
  * riesgo:
    * no conviene leerla como historia fuerte para servidores concurrentes
  * mitigación actual:
    * el diseño la mantiene explícitamente como adaptador práctico sobre la implementación async


---

## 5. Detalles de Implementación Críticos

### A. El Benchmark de Verdad
* La suite de Fase 1 se organiza en `tests/contracts/`, `tests/integration/` y `tests/unit/` y actúa como red de seguridad ejecutable del estado actual.
* La reutilización de tests históricos de **mongoeco 1.0.0** queda como trabajo incremental cuando esos casos se migren y adapten al nuevo diseño async-first.

### B. Gestión de Errores y Paridad
* El archivo `errors.py` de la v1.0 será el primer módulo en ser portado (con las adaptaciones asíncronas necesarias) para garantizar que los bloques `try/except` de los usuarios sigan funcionando igual.

### C. Verificación de Cierre
* El cierre operativo de Fase 1 se valida con `python3 -m compileall src/mongoeco tests`.
* La suite ejecutable de referencia se valida con `python3 -m unittest discover -s tests -p 'test*.py'`.
* La suite de cobertura local se valida con `pytest --cov=src/mongoeco --cov-report=term-missing` usando directorios temporales dentro del repo cuando el entorno no expone un `TMPDIR` escribible.

### D. Estructura del Proyecto
* El código instalable vive bajo `src/mongoeco/`.
* El baseline de lenguaje (`Python 3.13+`) se declara en `pyproject.toml`, no solo en este documento.
* La estrategia de evolución por dialectos y perfiles de compatibilidad se documenta en `DIALECTS.md`.
* La infraestructura de compatibilidad ya existe en la API pública y en el core: clientes, bases de datos y colecciones aceptan y propagan `mongodb_dialect` y `pymongo_profile`, y el core ya delega en esos objetos truthiness, catálogos de operadores, semántica básica de proyección, orden de campos de updates y comparación/igualdad de valores.
* En el eje PyMongo ya hay un primer delta vivo y testeado: `update_one(sort=...)` solo está habilitado desde el perfil `4.11`.
* Ambos ejes de compatibilidad ya exponen metadata de resolución (`mongodb_dialect_resolution` y `pymongo_profile_resolution`) en cliente, base de datos y colección, de forma que una app o suite de tests puede inspeccionar si está usando baseline, alias explícito o fallback compatible.
* La ampliación de superficie PyMongo ya no se valida solo con tests locales: el repositorio incluye `scripts/run_pymongo_profile_matrix.py`, que contrasta opciones reales contra `PyMongo 4.9`, `4.11` y `4.13` antes de convertir una diferencia en hook de perfil, y conserva el último snapshot contractual en `tests/fixtures/pymongo_profile_matrix.json`.
* El cierre arquitectónico actual deja el catálogo oficial como fuente de verdad:
  * dialectos y perfiles oficiales son dataclasses inmutables con identidad declarativa por versión
  * `behavior_flags` y `capabilities` derivan de los objetos oficiales, no de matrices manuales paralelas
  * las majors soportadas se derivan del catálogo registrado
  * existe una capa específica de tests de contrato para asegurar que el core obedece hooks y catálogos del dialecto/perfil, no números de versión hardcodeados

---

## 6. Registro de Evolución y Decisiones

### Fase 1: Cimientos y Arquitectura Hexagonal Real (Marzo 2026)
*   **Decisión: Protocolo Delgado (Thin Engine)**: El motor de almacenamiento ya no contiene toda la lógica de MongoDB ni expone una API rica estilo driver. Su contrato se limita a persistencia básica, filtrado pushdown acotado, actualización atómica de un documento y metadatos mínimos de índices. La "sabiduría" semántica sigue residiendo en el Core.
*   **Decisión: Pushdown Selectivo**: Sin romper el papel del Core como fuente de verdad semántica, el protocolo del engine admite filtro y proyección en `scan_collection`, `count_matching_documents`, `delete_matching_document`, `update_matching_document` y metadatos mínimos de índices. Esto prepara SQLite para ejecutar consultas, borrados y updates con atomicidad razonable sin duplicar toda la semántica de MongoDB.
*   **Decisión: Plan Canónico de Consulta**: El core compila filtros a un plan de consulta intermedio (`core/query_plan.py`) con nodos tipados por operador. `QueryEngine` lo evalúa en memoria y el mismo AST queda preparado para futuras traducciones a backends como SQLite sin volver a parsear el filtro crudo.
*   **Deuda Aceptada para Fase 2**: Aunque el plan canónico de consulta ya existe, el protocolo del engine en Fase 1 sigue aceptando `Filter` como entrada pública. La migración interna hacia `QueryNode` como artefacto compartido entre driver y motor queda reservada para Fase 2, cuando exista un traductor real hacia SQLite.
*   **Decisión: Modelo Canónico (Codec)**: Se ha implementado un `DocumentCodec` que normaliza tipos (fechas, UUIDs, `ObjectId`) antes de que lleguen al motor y usa marcadores internos sin colisionar con documentos del usuario.
*   **Decisión: Identidad en Core**: La normalización canónica de `_id` se centraliza en `core/identity.py` para que la estrategia no quede escondida dentro de `MemoryEngine`.
*   **Decisión: Paths Compartidos**: La escritura y borrado por dot notation se centralizan en `core/paths.py`, evitando que `upserts` dependa de helpers privados de `UpdateEngine`.
*   **Decisión: Propiedad de Datos**: El motor siempre trabaja con copias profundas (vía Codec), evitando que mutaciones del usuario afecten al estado persistido de forma accidental.
*   **Decisión: Identidad Propia**: `mongoeco.types.ObjectId` pasa a ser el identificador por defecto generado por la biblioteca, manteniendo la politica de cero dependencias externas.
*   **Decisión: Core Operativo**: `QueryEngine` soporta igualdad, operadores basicos (`$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`), dot notation recursiva y coincidencia basica sobre arrays. `UpdateEngine` soporta `$set` y `$unset`.
*   **Decisión: API Pública Mínima**: El cierre de Fase 1 incluye `insert_one`, `find_one`, `update_one`, `delete_one`, `count_documents`, `create_index` y `list_indexes`, tanto en async como en sync.
*   **Decisión: Proyección Pushdown Preparada**: El protocolo del engine admite proyección opcional en lecturas y escaneos, y la capa de colección reaplica la proyección como red de seguridad para evitar fugas si un motor futuro la ignora por error.
*   **Decisión: Proyecciones en Core**: La logica de proyecciones se mueve a `mongoeco/core/projections.py` para evitar que el driver concentre semántica propia de MongoDB.
*   **Decisión: API Sync de Fase 1**: La capa `_sync/` se implementa como adaptador delgado sobre la capa `_async/`, de modo que la logica funcional siga viviendo en la implementacion async.
*   **Decisión: Concurrencia del Engine de Memoria**: `MemoryEngine` usa locks compatibles con hilos y adquisición no bloqueante para el loop, de modo que compartir una instancia entre clientes sync no dependa de `asyncio.Lock` ligado a un loop concreto ni bloquee el event loop por contención directa.
*   **Limitación Explícita del Adaptador Sync**: `MongoClient` sync utiliza un `asyncio.Runner()` por instancia. Esto es suficiente para Fase 1, pero no equivale todavía a una arquitectura optimizada para servidores multihilo de alta concurrencia.
*   **Decisión: Lifecycle Volátil con Refcount**: `MemoryEngine` limpia su estado cuando se cierra la última conexión activa. Así mantiene semántica volátil sin romper clientes sync/async que compartan la misma instancia.
*   **Decisión: Índices de Fase 1**: El engine expone solo metadatos mínimos de índices. No hay enforcement de unicidad por índice, ni optimizador, ni garantías de rendimiento todavía. La planificación de consultas e integración real con índices queda diferida a Fase 2.
*   **Decisión: Sesión Placeholder Estructural**: `ClientSession` existe ya como contexto mínimo, se propaga por la API y el protocolo, y conserva estado por engine y flags de transacción para no bloquear una evolución posterior hacia transacciones reales, aunque Fase 1 no implemente aislamiento ni commit.
*   **Deuda Aceptada para Fase 3**: La atomicidad de Fase 1 se limita a operaciones elementales que el engine implementa de forma indivisible (`update_matching_document`, `delete_matching_document`, etc.). El protocolo no modela todavía rollback, WAL ni transacciones multioperación.
*   **Decisión: Adaptador Sync Definitivo por Ahora**: La capa `_sync/` se mantiene como adaptador manual sobre la implementación async. No se introduce `unasync` en el cierre de Fase 1 ni se arrastra como pendiente inmediata.
*   **Decisión: Compatibilidad de Lenguaje**: La Fase 1 se cierra deliberadamente sobre Python 3.13+ y permite usar sintaxis moderna de tipado, `override` y validadores `TypeIs` sin arrastrar compatibilidad artificial con versiones anteriores.
*   **Decisión: Suite de Conformidad**: La suite se organiza por capas en `tests/contracts/`, `tests/integration/` y `tests/unit/`. Cualquier motor futuro debe pasar los contratos de engine y la integracion de API.
*   **Deuda Aceptada al Entrar en Fase 2**:
    * `find()` ya existe, pero al entrar en Fase 2 la capa sync seguía sin exponer todavía un cursor incremental estilo PyMongo.
    * `QueryPlan` ya es canónico y tipado, pero el contrato público del engine sigue aceptando `Filter`; la migración interna hacia `QueryNode` queda para el trabajo de SQLite.
    * `ClientSession` ya puede anclar estado por engine, pero sigue siendo un placeholder sin transacciones, rollback ni isolation real.
    * Los índices `unique=True` ya se aplican en `MemoryEngine`, pero todavía no existe planificación de consultas ni uso de índices para acelerar lecturas.
    * `paths.py` ya soporta segmentos numéricos simples sobre listas, pero no cubre todavía semántica Mongo más avanzada sobre arrays posicionales.
*   **Definition of Done**: El cierre de Fase 1 exige compilación del paquete, suite en verde, documentación alineada y cobertura de contratos para engine, API async, API sync y core mínimo.
*   **Estado**: **Fase 1 Completada** dentro del perímetro descrito arriba. SQLite, agregación, sesiones, transacciones, `find()` cursor público e índices reales quedan diferidos a fases posteriores.

### Fase 2: SQLite, Pushdown y Agregación Pública (Marzo 2026)
*   **Decisión: SQLite Baseline Primero**: Antes de optimizar consultas, se introduce un `SQLiteEngine` funcional que comparte el contrato del engine de memoria y permite validar persistencia real sin dependencias externas.
*   **Decisión: `sqlite3` Estándar**: El backend inicial se implementa sobre `sqlite3` de la stdlib con conexión async-first vía `asyncio.to_thread`, manteniendo la política de cero dependencias externas.
*   **Decisión: Paridad de Contrato Antes que Pushdown Completo**: El primer objetivo de Fase 2 es pasar contratos e integración compartidos con `MemoryEngine`; la traducción desde `QueryNode` y la optimización SQL ya forman parte del estado real del backend.
*   **Decisión: Pushdown SQL Selectivo Real**: `SQLiteEngine` ya empuja a SQL el subset traducible de `find`, `count`, `delete_one` y `update_one`, incluyendo `WHERE`, `ORDER BY`, `LIMIT/OFFSET`, borrado nativo y updates con `$set/$unset` por rutas simples y con dot notation.
*   **Decisión: Índices Físicos Normalizados**: Los índices SQLite se crean sobre expresiones normalizadas `type/value`, compartidas con el traductor SQL. Esto permite que igualdad y membresía usen el mismo índice físico tanto para escalares como para tipos codificados (`ObjectId`, `datetime`, `UUID`).
*   **Decisión: Validación del Plan Real**: La suite ya verifica con `EXPLAIN QUERY PLAN` que SQLite usa índices físicos en filtros traducibles representativos, evitando asumir ciegamente que el optimizador los aprovechará.
*   **Decisión: Paridad Diferencial**: Además de contratos e integración, la suite compara explícitamente resultados `memory vs sqlite` para filtros codec-aware, combinaciones de `$and/$or`, membresía, proyección, ordenación, paginación, updates anidados y deletes.
*   **Decisión: `QueryNode` como Artefacto Interno Compartido**: Aunque la API pública sigue aceptando `Filter`, la capa de colección ya compila una sola vez el filtro a `QueryNode` y lo propaga internamente al engine. `MemoryEngine` y `SQLiteEngine` consumen ese plan compartido para evitar recompilaciones y mantener una única representación canónica dentro del pipeline interno.
*   **Subset Optimizado Actual**:
    * igualdad y desigualdad sobre escalares
    * igualdad y membresía sobre `ObjectId`, `datetime` y `UUID`
    * comparaciones simples (`$gt`, `$gte`, `$lt`, `$lte`) sobre escalares
    * `AND` / `OR` / `EXISTS`
    * `ORDER BY`, `LIMIT`, `OFFSET` para sorts traducibles
    * `UPDATE` SQL para `$set` / `$unset`, incluyendo dot notation simple
    * `DELETE` SQL directo sobre el primer documento coincidente cuando el filtro es traducible
*   **Fallback Explícito**:
    * filtros no traducibles siguen evaluándose con `QueryEngine` en Python
    * sorts sobre arrays y sobre objetos JSON no tipados siguen ordenándose en Python para mantener semántica alineada con `MemoryEngine`
    * el fallback Python ya no materializa la colección completa para filtrar; cuando no hay `sort`, el recorrido sigue siendo incremental
    * si el fallback Python necesita ordenar, SQLite sigue materializando el subconjunto filtrado para poder aplicar `sort_documents(...)`; esto preserva correctitud, pero no equivale a memoria O(1) universal
    * sorts sobre escalares, dot notation escalar y valores codificados homogéneos (`ObjectId`, `datetime`, `UUID`) siguen yendo por la ruta SQL optimizada
    * updates no traducibles siguen cayendo a la ruta segura de reserialización del documento
*   **Limitación Explícita de Índices sobre Arrays en SQLite**:
    * los índices físicos actuales se crean sobre expresiones del campo completo
    * las búsquedas de membresía sobre arrays se resuelven con `json_each(...)`
    * esto no equivale a un multikey index real de MongoDB: en el diseño actual, esos índices no aceleran de forma fiable filtros de membresía sobre elementos individuales de arrays
*   **Decisión: Cursor Público Mínimo para `find()`**: `find()` ya no devuelve un iterable crudo en async ni una lista desnuda en sync. Ambas capas exponen un cursor pequeño y explícito con iteración, `to_list()`, `first()` y encadenado básico de `sort`, `skip` y `limit`. En la capa sync, la iteración ya consume el cursor async paso a paso y cierra el iterable subyacente de forma determinista.
*   **Decisión: Agregación Pública Materializada pero Ya Útil**: `aggregate()` ya existe en async y sync como superficie pública establecida, también con cursor mínimo público (`iteración`, `to_list()`, `first()` y `close()` en sync). El pipeline vive en `core/aggregation.py`, conserva pushdown conservador del prefijo seguro hacia `find()` y ya soporta un subconjunto funcional amplio:
    * stages: `$match`, `$project`, `$sort`, `$skip`, `$limit`, `$unwind`, `$group`, `$lookup`, `$replaceRoot`, `$replaceWith`, `$facet`, `$bucket`, `$bucketAuto`, `$setWindowFields`, `$count`, `$sortByCount`
    * acumuladores: `$sum`, `$min`, `$max`, `$avg`, `$push`, `$first`
    * expresiones: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$and`, `$or`, `$in`, `$cond`, `$ifNull`, `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$arrayElemAt`, `$size`, `$floor`, `$ceil`, `$let`, `$toString`, `$first`, `$map`, `$filter`, `$reduce`, `$concatArrays`, `$setUnion`, `$mergeObjects`, `$getField`, `$arrayToObject`, `$indexOfArray`, `$sortArray`, `$dateTrunc`
*   **Estado Actual de Stages Avanzados**:
    * `"$lookup"` soporta tanto la forma simple (`localField` / `foreignField`) como la forma con `let + pipeline`.
    * `"$bucketAuto"` ya existe con reparto automático básico por documentos ordenados y `output` funcional; `granularity` sigue fuera.
    * `"$setWindowFields"` ya soporta ventanas por `documents` y, para el caso numérico, ventanas por `range` con bounds numéricos, `"current"` y `"unbounded"`.
*   **Contrato Actual del Cursor**:
    * Async: iteración con `async for`, `to_list()`, `first()`, `sort()`, `skip()` y `limit()`.
    * Sync: iteración con `for` / `list(...)`, `to_list()`, `first()`, `sort()`, `skip()`, `limit()` y `close()`.
    * El cursor actual sigue siendo deliberadamente mínimo: no expone todavía la superficie completa de PyMongo.
*   **Compatibilidad Explícita del Cambio en `find()`**:
    * La API sync ya no devuelve una `list` directa en `find()`.
    * El consumidor debe iterar el cursor o materializarlo con `to_list()` / `list(...)`.
    * El cambio acerca la API al modelo de cursores de MongoDB/PyMongo y evita fijar demasiado pronto una interfaz basada en listas materializadas.
*   **Estado**: **Completada**. Fase 2 ya no está solo “iniciada”: existe un `SQLiteEngine` funcional con persistencia real, índices únicos físicos, pushdown SQL útil, cursor público mínimo para `find()`, agregación materializada amplia, streaming robusto en `find()` sync, arquitectura de dialectos/perfiles integrada y paridad observable reforzada frente a `MemoryEngine`.

### Perímetro de Cierre de Fase 2
La Fase 2 se considera cerrada cuando se acepta explícitamente este perímetro y no se sigue ensanchando el alcance dentro de la propia fase:
* **SQLiteEngine funcional y persistente**:
  * almacenamiento real sobre `sqlite3`
  * índices únicos físicos
  * lifecycle de conexión estable
  * contratos comunes de engine en verde
* **Pushdown SQL útil y verificado**:
  * `find`, `count_documents`, `delete_one` y parte de `update_one` ya usan SQL cuando el plan es traducible
  * `EXPLAIN QUERY PLAN` ya valida uso real de índices en casos representativos
  * fallbacks explícitos a Core/Python donde la traducción no es segura
* **Plan canónico compartido**:
  * la capa pública sigue aceptando `Filter`
  * internamente, driver y engines ya comparten `QueryNode` como representación canónica del filtro
* **Cursores públicos mínimos de lectura**:
  * async y sync exponen cursor para `find()`
  * soporte explícito de iteración, `to_list()`, `first()`, `sort()`, `skip()`, `limit()` y `close()` en la capa sync
  * la iteración sync de `find()` ya es incremental y cierra el iterable async subyacente en `break` temprano o `close()`
* **Agregación pública útil y materializada**:
  * `aggregate()` ya existe en async y sync como superficie pública estable
  * cursor mínimo público para `aggregate()` en async y sync, con iteración, `to_list()`, `first()` y `close()` en sync
  * la iteración sync de `aggregate()` ya es incremental sobre el cursor async, aunque la materialización del pipeline siga existiendo en el core de agregación
  * stages soportados: `$match`, `$project`, `$sort`, `$skip`, `$limit`, `$unwind`, `$group`, `$lookup`, `$replaceRoot`, `$replaceWith`, `$facet`, `$bucket`, `$bucketAuto`, `$setWindowFields`, `$count`, `$sortByCount`
  * acumuladores soportados: `$sum`, `$min`, `$max`, `$avg`, `$push`, `$first`
  * expresiones soportadas: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$and`, `$or`, `$in`, `$cond`, `$ifNull`, `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$arrayElemAt`, `$size`, `$floor`, `$ceil`, `$let`, `$toString`, `$first`, `$map`, `$filter`, `$reduce`, `$concatArrays`, `$setUnion`, `$mergeObjects`, `$getField`, `$arrayToObject`, `$indexOfArray`, `$sortArray`, `$dateTrunc`
* **Paridad observable reforzada**:
  * `MemoryEngine` y `SQLiteEngine` se comparan explícitamente en filtros, sorts, paginación, updates, deletes y pipelines de agregación representativos
* **Verificación ejecutable del cierre**:
  * compilación del paquete y tests
  * el snapshot concreto usado para cerrar la fase es histórico y no debe leerse como la métrica viva del repositorio actual

### Fuera de Alcance de Fase 2
Estos puntos quedan ya movidos explícitamente a fases posteriores y no deben seguir ensanchando el cierre de Fase 2:
* **Sesiones transaccionales reales**:
  * `BEGIN`
  * `COMMIT`
  * `ROLLBACK`
  * aislamiento y ownership transaccional de conexión
* **Ampliación ancha de la API de colección**:
  * `insert_many`
  * `update_many`
  * `delete_many`
  * `replace_one`
  * familia `find_one_and_*`
  * `bulk_write`
  * `distinct`
* **Cursor más cercano a PyMongo**:
  * más métodos, opciones y semántica incremental
  * ampliar la historia incremental de la capa sync más allá del baseline actual de `find()`
  * reducir la materialización que sigue siendo estructural en agregación y en caminos que requieren ordenación Python
* **Profundidad adicional de agregación**:
  * más semántica de `"$bucketAuto"`
  * más semántica de `"$setWindowFields"`
  * stages analíticos adicionales y variantes más profundas de `"$lookup"`
* **Compatibilidad PyMongo más ancha** fuera del subconjunto ya implementado y testeado
* **Planificación de consultas propia** más allá del uso que ya hace SQLite de sus índices y del pushdown selectivo actual
* **Reabrir la estrategia sync solo si aparece una necesidad real que el adaptador actual no cubra**

### Cierre Operativo de Fase 2
El cierre local de Fase 2 se considera ya satisfecho:
1. **El corte de alcance está fijado** y la fase no sigue ensanchándose con superficie ajena al perímetro descrito arriba.
2. **La documentación está alineada** con el estado real del código, la suite y la estrategia de compatibilidad.
3. **La verificación local está cerrada**:
   * la fase se cerró con suite y cobertura en verde en su momento
   * esos números concretos se consideran históricos
   * el estado vivo del repositorio se resume en la sección de reentrada consolidada

La validación diferencial contra MongoDB real queda como capa adicional recomendada, pero ya no como requisito bloqueante para declarar completada la implementación local de Fase 2.

### Decisión Consolidada sobre la Capa Sync
La capa sync se considera, por ahora, una adaptación manual y deliberada de la implementación async.

Esto implica:
* no introducir un pipeline `unasync` en Fase 2
* no mantener stubs o recordatorios de una generación automática que ya no forma parte del plan inmediato
* reevaluar esta decisión solo si fases posteriores exigen una historia sync con requisitos que el adaptador actual no pueda cubrir con claridad
* asumir explícitamente que `find()` sync ya itera de forma incremental, mientras que `to_list()` sigue materializando de forma deliberada
* asumir explícitamente que `aggregate()` sync ya expone iteración incremental, aunque la agregación subyacente siga materializándose en el core

### Cobertura Semántica Consolidada
El estado actual de la suite ya no se limita a “happy paths” por operador. La estrategia de verificación combina varias capas:
* **tests unitarios directos** para semántica fina de Core (`filtering`, `aggregation`, `paths`, `operators`, `projections`, `query_plan`, `sorting`)
* **tests de integración** para API async/sync, cursores, sesiones, agregación pública y paridad observable entre `MemoryEngine` y `SQLiteEngine`
* **tests de rechazo explícito** para operadores fuera de perímetro, de modo que el sistema falle con error claro en lugar de degradar silenciosamente a comportamientos inventados
* **tests agrupados con `subTest`** para inventarios grandes de operadores no soportados, reservando tests dedicados para los casos con semántica más delicada o con mayor valor diagnóstico

La cobertura semántica se organiza en tres categorías deliberadas:
* **soportado y verificado**: el operador o familia ya existe y tiene tests propios o de integración
* **no soportado pero rechazado explícitamente**: el operador queda fuera del perímetro actual, pero existe test que fija el fallo explícito
* **no aplicable al repo actual**: el caso pertenece a superficies que `mongoeco` no modela todavía como abstracción propia (por ejemplo, restricciones específicas de vistas)

### Testing Diferencial Contra MongoDB Real 7.0 y 8.0
Como siguiente capa de verificación, el repositorio ya incluye un arnés opcional de contraste contra MongoDB real compartido por versión:
* base compartida: `tests/differential/_real_parity_base.py`
* módulos por versión: `tests/differential/mongodb7_real_parity.py`, `tests/differential/mongodb8_real_parity.py`
* script genérico: `scripts/run_mongodb_real_differential.py`
* wrappers manuales: `scripts/run_mongodb7_differential.py`, `scripts/run_mongodb8_differential.py`
* dependencia opcional: extras `mongodb7`, `mongodb8` y `mongodb-real` en `pyproject.toml`

Objetivo del arnés diferencial:
* comparar `mongoeco` frente a MongoDB real en una selección reducida de casos de alto riesgo semántico
* priorizar zonas donde la documentación y la intuición del lenguaje anfitrión suelen divergir:
  * `"$expr"` comparando campos
  * truthiness de expresiones
  * sensibilidad al orden de campos en subdocumentos
  * `"$all"` con múltiples `"$elemMatch"`
  * `"$addToSet"` con documentos embebidos
  * traversal de arrays y `"$getField"` con nombres literales no triviales

Restricciones deliberadas del arnés diferencial:
* no forma parte del green bar obligatorio diario
* se activa solo si existe `MONGOECO_REAL_MONGODB_URI`
* requiere `pymongo` instalado mediante uno de los extras opcionales de Mongo real
* verifica explícitamente que el servidor remoto coincida con la versión objetivo (`7.0.x` o `8.0.x`)

Comando de ejecución previsto:
* `MONGOECO_REAL_MONGODB_URI=... .venv/bin/python scripts/run_mongodb7_differential.py`
* `MONGOECO_REAL_MONGODB_URI=... .venv/bin/python scripts/run_mongodb8_differential.py`
* `MONGOECO_REAL_MONGODB_URI=... .venv/bin/python scripts/run_mongodb_real_differential.py 7.0`
* `MONGOECO_REAL_MONGODB_URI=... .venv/bin/python scripts/run_mongodb_real_differential.py 8.0`

Esta capa no sustituye a la suite local pura; la complementa. La intención es usarla como detector de divergencias de semántica real, no como requisito permanente para cada ejecución local o CI mínima.

### Primer Delta Versionado Registrado
El catálogo de dialectos ya registra una primera divergencia oficial entre `7.0` y `8.0` como flag de comportamiento:
* `null_query_matches_undefined`

Actualmente:
* `MongoDialect70` la marca como `True`
* `MongoDialect80` la marca como `False`

Ese delta ya está fijado en tests, documentación y runtime. `mongoeco` modela
un valor `UNDEFINED` propio y aplica la diferencia `7.0/8.0` en:
* igualdad por `null` y `$eq`
* `$in` con `null`
* `$ne` / `$nin` sobre campo ausente frente a `null`
* `$lookup`

### Decisión Consolidada sobre Paths y Expansión de Arrays
`core/paths.py` ya no expande arrays sin límite ni sobrescribe silenciosamente padres escalares.

Esto implica:
* existe un límite configurable de expansión de índices de array (`get_max_array_index()` / `set_max_array_index()`)
* cruzar un padre escalar en una ruta anidada (`"a.b"` sobre `{"a": 1}`) ya se trata como error, alineando la semántica con MongoDB
* la protección actual es deliberadamente conservadora para evitar consumo abusivo de memoria y corrupción silenciosa de datos

### Fase 3: Ampliación Funcional Avanzada
La Fase 3 pasa a priorizar funcionalidad visible y útil que ya supera el baseline actual, sin mezclar todavía el cierre completo de la historia transaccional.

Estado actual: **Completada**.

Objetivos principales:
* **Agregación avanzada**:
  * ampliar `aggregate()` desde el subconjunto amplio actual hacia operadores más costosos o especializados
  * siguientes candidatos razonables: más semántica de `"$setWindowFields"`, más semántica de `"$bucketAuto"` y stages analíticos adicionales
* **Evolución de índices para arrays en SQLite**:
  * diseñar una estrategia tipo multikey sobre tablas auxiliares de entradas indexadas por elemento
  * mantener esas entradas sincronizadas en `insert` / `update` / `delete`
  * permitir que filtros de membresía sobre arrays puedan usar índices reales en vez de depender de `json_each(...)` sobre el documento completo
  * asumir explícitamente que esto exige un cambio de modelo físico y de planner, no solo retocar las expresiones SQL actuales
* **Ampliación de escrituras con alto valor práctico**:
  * `insert_many`
  * `update_many`
  * `delete_many`
  * `replace_one`
  * familia `find_one_and_*` si encaja bien con el estado del motor
* **Ampliación de operadores con alto retorno funcional**:
  * updates y queries ya no están en baseline mínimo; la prioridad pasa a semánticas más avanzadas, paridad fina y operadores de borde con valor real
* **Pushdown SQL adicional donde aporte valor directo**:
  * ampliar el subset traducible de updates
  * reforzar la consistencia entre `MemoryEngine` y `SQLiteEngine`
* **Cierre de la migración interna hacia `QueryNode`**:
  * usar el plan canónico como artefacto interno compartido de forma más explícita

Criterio de foco:
* la prioridad es aumentar el valor funcional visible sin perder coherencia arquitectónica
* aggregation mínima útil se considera más prioritaria que transacciones reales, porque abre más casos de uso inmediatos con menor coste estructural

### Perímetro de Cierre de Fase 3
La Fase 3 se considera cerrada con este alcance ya implementado y verificado:
* **Ampliación de escrituras y helpers de colección**:
  * `insert_many`
  * `update_many`
  * `delete_many`
  * `replace_one`
  * familia `find_one_and_*`
  * `bulk_write`
  * `distinct`
  * `estimated_document_count`
  * `drop`, `drop_collection` y `drop_database`
* **Agregación ensanchada con retorno práctico**:
  * stages adicionales: `"$unset"`, `"$sample"`, `"$unionWith"`
  * expresiones string adicionales: `"$concat"`, `"$toLower"`, `"$toUpper"`, `"$split"`, `"$strcasecmp"`, `"$substr"`
  * acumuladores adicionales: `"$last"`, `"$addToSet"`, `"$count"`, `"$mergeObjects"`
  * endurecimiento adicional de `"$lookup"`, `"$setWindowFields"` y validaciones asociadas
* **Índices multikey reales en SQLite para membresía sobre arrays**:
  * tabla auxiliar de entradas indexadas por elemento
  * mantenimiento automático en `insert`, `update`, `delete`, `overwrite` y `drop`
  * uso efectivo desde la ruta SQL para igualdad y `"$in"` sobre campos array indexados
  * verificación explícita mediante `EXPLAIN QUERY PLAN`
* **Consistencia reforzada entre engines**:
  * más paridad observable entre `MemoryEngine` y `SQLiteEngine` en filtros, escrituras y agregación
* **Verificación ejecutable del cierre**:
  * el snapshot concreto usado para cerrar la fase es histórico y no debe confundirse con el estado vivo actual del repositorio

### Fase 4: Transacciones, Ergonomía PyMongo y Administración Local
La Fase 4 se centra en cerrar primero la base que más condiciona el crecimiento posterior: sesiones reales, robustez de escritura y la primera gran ampliación de ergonomía PyMongo sobre la arquitectura local ya consolidada.

Estado actual: **Completada**.

Objetivos principales:
* **Sesiones transaccionales reales en SQLite**:
  * introducir `BEGIN`, `COMMIT` y `ROLLBACK`
  * anclar el estado transaccional a `ClientSession`
  * definir una historia pública mínima de transacción (`start_transaction`, `commit_transaction`, `abort_transaction`, `with_transaction`)
  * fijar ownership de conexión y lifecycle transaccional
* **Atomicidad y lifecycle de escritura más fuertes**:
  * cerrar bien la interacción entre sesiones, engine y cierre de recursos
  * reducir huecos entre rutas SQL nativas y fallbacks en Python
  * endurecer invariantes de `bulk_write`, resultados y errores de escritura
* **Ergonomía PyMongo de colección y cursor con alto retorno**:
  * aceptar más parámetros ya comunes en aplicaciones reales: `hint`, `comment`, `max_time_ms`, `let`, `batch_size`
  * extender el cursor hacia métodos y estado prácticos (`batch_size`, `hint`, `comment`, `max_time_ms`, `rewind`, `clone`, `alive`, `explain`)
  * ampliar `find`, `aggregate`, `update_*` y `find_one_and_*` con firmas más cercanas a PyMongo
* **Administración local de índices y colecciones**:
  * `IndexModel`
  * `create_indexes`
  * `drop_index`
  * `drop_indexes`
  * `index_information`
  * `rename`, `options`, `with_options`, `create_collection`, `list_collections`
* **Primer bloque de configuración estructural del driver local**:
  * `WriteConcern`, `ReadConcern`, `ReadPreference`, `CodecOptions`, `TransactionOptions`
  * soporte inicial como configuración local explícita, aunque no toda su semántica tenga efecto real inmediato en todos los engines

Perímetro real de cierre:
* `find(batch_size=...)` ya es `effective` con batching local observable del cursor.
* `let` en writes ya es `effective` cuando el filtro usa `$expr`, incluidas las rutas de selección previas a `update_*`, `replace_one`, `delete_*`, `find_one_and_*` y `bulk_write`.
* `aggregate(batch_size=...)` ya es `effective` en pipelines streamables; cuando aparecen stages globales (`$group`, `$sort`, `$facet`, ventanas, buckets, etc.), la ejecución sigue cayendo al camino materializado completo.

Criterio de foco:
* Fase 4 prioriza lo que habilita mejor el resto del roadmap: transacciones, opciones públicas estables y metadatos/administración con forma PyMongo.
* La regla es no abrir todavía grandes familias nuevas de semántica si antes no queda bien fijado el contrato de sesiones, cursores y opciones.
* Ningún bloque de Fase 4 se justifica por compatibilidad con MongoDB `< 7.0` o PyMongo `< 4.9`.

### Fase 5: Breadth Semántico de Query, Update y Aggregation
La Fase 5 ya se dedica a ensanchar de verdad la compatibilidad funcional del lenguaje MongoDB/PyMongo sobre la base transaccional y de ergonomía construida en Fase 4.

Objetivos principales:
* **Ampliar operadores de query**:
  * `regex`
  * `not`
  * `all`
  * `elemMatch`
  * `size`
  * `type`
  * operadores bitwise y semántica más fina sobre arrays
  * primeras familias de geospatial, text search y JSON Schema solo si su coste encaja con la arquitectura
* **Ampliar operadores de update**:
  * `$rename`
  * `$currentDate`
  * `$min`
  * `$max`
  * `$mul`
  * `$bit`
  * positional operators y `array_filters`
* **Ensanchar aggregation con foco en uso real**:
  * más stages
    * `$documents`
  * más acumuladores
  * más expresiones
  * más profundidad en `"$setWindowFields"`, `"$bucketAuto"`, `"$lookup"` y familias analíticas
* **Completar más opciones por método**:
  * `collation`
  * `allow_disk_use`
  * `bypass_document_validation`
  * variantes más finas en `bulk_write`, resultados y errores
* **Cursor y lectura avanzada**:
  * `find_raw_batches`
  * `aggregate_raw_batches`
  * más semántica incremental y menos materialización estructural donde sea razonable

Criterio de foco:
* Fase 5 ya no persigue solo infraestructura, sino cerrar el mayor hueco actual frente a PyMongo: la anchura semántica del lenguaje y de las combinaciones reales de uso.
* La prioridad se decide por frecuencia de uso y por cuánto reduce fricción al portar suites existentes.

### Fase 6: Superficie Administrativa, Observabilidad y Compatibilidad de Ecosistema
La Fase 6 agrupa la superficie PyMongo que aporta valor de compatibilidad, pero que no debería contaminar las fases anteriores ni bloquear el núcleo local.

Objetivos principales:
* **API administrativa y de metadata más ancha**:
  * `Database.command`
  * `server_info`
  * listados enriquecidos de bases y colecciones
  * más clases de resultados, errores y detalles de escritura
* **Search y APIs adyacentes de administración**:
  * `create_search_index`
  * `create_search_indexes`
  * `list_search_indexes`
  * `update_search_index`
  * `drop_search_index`
* **Change streams y superficies observables avanzadas**:
  * `watch` en cliente, base de datos y colección
  * solo si existe una historia interna coherente para eventos; en caso contrario, rechazo explícito documentado
* **Mejor alineación con el stack `bson`/PyMongo**:
  * más tipos
  * más utilidades de codec
  * mayor precisión en firmas, mensajes de error y clases públicas

Criterio de foco:
* Esta fase existe para recoger breadth útil del ecosistema PyMongo sin mezclarlo con el trabajo estructural de transacciones ni con el breadth semántico del lenguaje.
* Si una pieza es útil para portabilidad pero no cambia el núcleo de ejecución, suele pertenecer antes a Fase 6 que a Fase 4 o 5.

### Fase 7: Topología Real y Compatibilidad Driver-Adjacente
Esta fase es deliberadamente opcional y de baja prioridad relativa. Reúne aquello que PyMongo tiene como driver real de red, pero que no forma parte del corazón de `mongoeco` como mock local async-first.

Objetivos principales:
* **Conexión real a MongoDB**:
  * URI parsing completo
  * auth
  * TLS
  * SRV
  * replica sets
  * sharding
  * pooling
  * selección de servidor
* **Semántica completa de concerns y preferencias**:
  * `read_preference`
  * `write_concern`
  * `read_concern`
  * timeouts, retries y políticas de selección
* **Compatibilidad de monitoring y comportamiento de driver de red**

Criterio de foco:
* Nada de esta fase debe bloquear el crecimiento del mock local.
* Solo se aborda si el proyecto decide evolucionar explícitamente desde “mock compatible” hacia “driver local con ambición de paridad de cliente”.

### Fase 8: Cierre de Paridad Fina y Completitud
Esta fase agrupa todo lo que queda fuera del “núcleo ya correcto” pero sigue siendo relevante para hablar de cierre serio frente a `mongomock`, PyMongo y, en ciertos perímetros, MongoDB real. No es una fase de reescritura; es una fase de **cierre fino**, pensada para concentrar matices, bordes y profundidad restante sin volver a contaminar las fases ya consolidadas.

Objetivos principales:
* **Refinamientos estructurales para sostenibilidad de versiones y mantenimiento**:
  * mover los deltas de comportamiento del catálogo desde flags/hardcodes hacia capacidades semánticas explícitas;
  * cerrar la transición para que los engines consuman una IR única de consulta, en vez de convivir con filtros crudos y planes tipados en paralelo;
  * introducir una distinción formal entre stages streamables y stages materializantes en agregación, para ordenar mejor memoria, spill y optimizaciones futuras.
* **Semántica BSON total**:
  * cerrar overflow, conversiones y comparaciones raras que todavía no están modeladas al nivel más estricto;
  * ampliar la fidelidad numérica y escalar en caminos menos frecuentes de aggregation, sorting, filtering y writes;
  * reforzar el contrato observable cuando haya wrappers BSON internos y tipos públicos `bson` reales.
* **Pushdown, spill y planificación avanzada**:
  * seguir reduciendo los caminos físicos específicos del backend que todavía afectan al comportamiento observable;
  * mejorar el spill para agregación más allá del contrato actual de `allowDiskUse`;
  * avanzar en planificación y degradación segura sin materialización innecesaria cuando haya `sort`, stages globales o rutas mixtas SQL/Python.
* **Motor de indexación virtual de alta fidelidad**:
  * completar la historia de multikey avanzado;
  * seguir mejorando interacción con hints, explain y selección/planning;
  * cerrar matices de `sparse`, `partialFilterExpression` y tipos/arrays complejos.
* **Agregación y lenguaje MongoDB en su último tramo**:
  * cerrar complejidad residual del área de aggregation si nuevas superficies vuelven a tensionar el runtime;
  * mantener extensibilidad de stages y expresiones sin que reaparezcan monolitos;
  * abordar etapas o expresiones de borde que no compensaban en fases anteriores.
* **Paridad fina de API pendiente de fases previas**:
  * `find_raw_batches`
  * `aggregate_raw_batches`
  * `collation`
  * `bypass_document_validation`
  * `allow_disk_use` en todos los puntos donde la superficie PyMongo lo expone y no solo en el contrato interno actual
  * contraste exhaustivo de la suite de `mongomock`:
    * inventario completo de casos
    * clasificación por áreas
    * matriz `cubierto / equivalente / no cubierto / fuera de alcance`
    * importación selectiva o reescritura de los casos con más valor contractual
* **Proxy wire y superficie de servidor más profunda**:
  * ampliar la `WireSurface` cuando el crecimiento de comandos o semántica de servidor ya no quepa limpiamente en la capa actual;
  * seguir endureciendo compatibilidad con drivers externos, cursores, sesiones, errores y shape de respuestas.
* **Topología y comportamiento de driver real restantes**:
  * solo refinamientos de paridad fina de cliente de red que aparezcan después del cierre de Fase 7;
  * ampliaciones de superficie wire o de comportamiento de servidor que ya no exijan nueva infraestructura transversal;

Estado de avance dentro de Fase 8:
* ya están aplicados:
  * capabilities semánticas explícitas en compatibilidad oficial;
  * IR de lectura tipada hasta engines, con shims legacy solo como compatibilidad;
  * distinción formal entre stages streamables y materializantes en el core de agregación;
  * `find_raw_batches` y `aggregate_raw_batches`;
  * `collation` observable en query, sort, update, delete, aggregate y comandos administrativos compatibles, con fallback Python seguro cuando SQLite no puede preservar la semántica;
  * `bypass_document_validation` efectivo en escrituras de colección y en comandos administrativos de escritura;
  * type bracketing y comparación BSON aplicados también al razonamiento de índices virtuales;
  * degradación híbrida de SQLite cuando solo el `sort` requiere Python, manteniendo `scan` y `filter` en SQL;
  * centralización práctica de la capa sync en la superficie más amplia de colección, manteniendo la implementación maestra en async;
  * centralización adicional del wrapping sync en administración de base de datos, evitando más deriva manual en el borde sync;
  * inventario exhaustivo de la suite de `mongomock` y matriz base `cubierto / equivalente / no cubierto / fuera de alcance` como artefactos versionados;
  * reglas de triage inicial para marcar de forma explícita lo que queda fuera de alcance contractual en la propia suite de `mongomock`;
  * primera clasificación automática conservadora para casos de `collation` y `allow_disk_use` ya cubiertos por contratos locales.
* se consideran ya fuera del perímetro de cierre de esta fase y pasan a refino continuo:
  * refinamiento BSON restante en rutas menos frecuentes y comparación fina fuera de los caminos ya cerrados;
  * refinamiento de pool de conexiones y de concurrencia/locking de SQLite, si el proyecto decide perseguir comportamiento más cercano a producción y no solo paridad funcional fina;
  * profundidad adicional de cliente de red o del proxy wire que ya no exija nueva infraestructura transversal.
* refinamientos estructurales aún plausibles, a incorporar sin reabrir fases ya cerradas:
  * **origen Fase 7, prioridad media-alta**: convertir el pool de conexiones del driver en un subsistema con espera asíncrona real para `checkout`, `waitQueueTimeoutMS` o equivalente y degradación por latencia en vez de fallo inmediato cuando `max_pool_size` se satura;
  * **origen Fase 4, prioridad media**: reducir el locking global de `SQLiteEngine` y explorar un modelo más cercano a WAL/lecturas concurrentes si el proyecto decide perseguir ambición más cercana a producción y no solo corrección funcional local;
  * **origen transversal BSON/planning, prioridad alta**: unificar en un único helper compartido toda la sabiduría de orden de tipos y comparación BSON que hoy aparece repartida entre el core y claves auxiliares de engine, para evitar divergencias silenciosas entre memoria y SQLite;
  * **origen Fase 2/3, prioridad alta**: seguir endureciendo los caminos híbridos de fallback en SQLite y agregación para que un `sort` o stage Python no fuerce materialización completa prematura del subconjunto filtrado cuando todavía sería posible mantener cursores más incrementales o una estrategia de spill/límite de materialización más explícita;
  * **origen Fase 1, prioridad media**: endurecer el cierre de `_SyncRunner` y de recursos pendientes para minimizar fugas o estados zombie cuando existan sockets o tareas de transporte aún activas al cerrar el cliente sincronizado.

Criterio de foco:
* Esta fase existe para evitar que los “últimos 10-15%” de fidelidad y profundidad queden repartidos en notas marginales.
* Si una pieza no exige reabrir arquitectura base, pero sí mejora de forma clara la robustez contractual, la completitud o la paridad fina, pertenece antes a Fase 8 que a una nueva ola de refactor.
* Si una pieza es principalmente de producto o plataforma de red, pero ya depende de la arquitectura preparada en Fase 7 y no de nueva infraestructura transversal, también puede aterrizar aquí.
* Optimizaciones opcionales de implementación solo entran aquí si:
  * ayudan a sostener la fidelidad contractual sin duplicar semántica de forma peligrosa; y
  * no reabren otra vez el diseño central del core.

Nota de implementación opcional para Fase 8:
* **Capa sync generada o centralizada**:
  * sí es una mejora interesante de resultado final para evitar deriva entre async y sync;
  * no debe preceder al cierre de las fronteras estructurales del core;
  * en el cierre real de Fase 8 se acepta como suficiente una centralización fuerte del wrapping sync en los puntos de mayor superficie, sin imponer todavía generación automática.
* **Backend Rust opcional para BSON core**:
  * sí es una vía interesante para codec, comparación/orden BSON y conversiones numéricas si el profiling real lo justifica;
  * debe plantearse como backend intercambiable y no como segunda fuente de verdad semántica.
* **`QueryNode` en Rust**:
  * no es objetivo por defecto;
  * solo tendría sentido si la semántica del planner está completamente congelada y aparece un cuello de botella real que no compense seguir resolviendo en Python.

### Regla de Corte Entre Fases
Para evitar mezclar objetivos y perder foco:
* **Fase 3**: ampliación funcional visible, agregación útil, escrituras de alto valor y multikey real en SQLite
* **Fase 4**: transacciones reales, administración local, cursores más ergonómicos y primeras opciones PyMongo estructurales
* **Fase 5**: breadth semántico de query, update y aggregation
* **Fase 6**: API administrativa ancha, observabilidad y compatibilidad de ecosistema
* **Fase 7**: topología y semántica propia de driver de red real
* **Fase 8**: cierre de paridad fina, profundidad restante y completitud contractual

Regla práctica:
* si una funcionalidad habilita correctamente otras diez y reduce deuda estructural, debe ir antes aunque no sea la más vistosa
* si una funcionalidad ensancha mucho la compatibilidad semántica del lenguaje, debe evaluarse para Fase 5
* si una funcionalidad es principalmente administrativa, observable o de ecosistema, debe evaluarse para Fase 6
* si una funcionalidad exige comportarse como driver de red real, debe evaluarse para Fase 7 y no contaminar el roadmap principal del mock local
* si una funcionalidad ya no exige arquitectura nueva, pero sí cerrar matices, bordes o profundidad contractual, debe evaluarse para Fase 8

### Mapa de Superficie PyMongo Pendiente por Fase
Para evitar que la diferencia con PyMongo quede dispersa en notas sueltas, este es el backlog de alto nivel ya repartido por crecimiento esperado.

#### Fase 4
* transacciones reales (`start_transaction`, `commit_transaction`, `abort_transaction`, `with_transaction`)
* opciones estructurales en cliente, base de datos, colección y cursor:
  * `hint`
  * `comment`
  * `max_time_ms`
  * `let`
  * `batch_size`
* cursores con más ergonomía:
  * `rewind`
  * `clone`
  * `alive`
  * `explain`
* administración local:
  * `IndexModel`
  * `create_indexes`
  * `drop_index`
  * `drop_indexes`
  * `index_information`
  * `rename`
  * `options`
  * `with_options`
  * `create_collection`
  * `list_collections`
* configuración base del stack PyMongo:
  * `WriteConcern`
  * `ReadConcern`
  * `ReadPreference`
  * `CodecOptions`
  * `TransactionOptions`
* perímetro explícito:
  * `find(batch_size=...)` efectivo con batching local
  * `let` en writes efectivo vía filtros con `$expr`
  * `aggregate(batch_size=...)` efectivo en pipelines streamables; los stages globales siguen siendo materializados

#### Fase 5
* query operators adicionales:
  * ya implementados en esta fase:
    * `regex`
    * `not`
    * `all`
    * `elemMatch`
    * `size`
    * `type`
    * bitwise
  * siguiente bloque prioritario:
    * `where`
* update operators adicionales:
  * ya implementados en esta fase:
    * `$rename`
    * `$currentDate`
    * `$setOnInsert`
    * `$min`
    * `$max`
    * `$mul`
    * `$bit`
    * `$pullAll`
    * `$push` modifiers (`$each`, `$position`, `$slice`, `$sort`)
    * positional operators
    * `array_filters`
* aggregation adicional:
  * ya implementadas en esta fase:
    * `$bsonSize`
    * `$rand`
    * `$convert`
    * `$setField`
    * `$unsetField`
    * `$switch`
    * `$bitAnd`
    * `$bitNot`
    * `$bitOr`
    * `$bitXor`
    * `$abs`
    * `$exp`
    * `$ln`
    * `$log`
    * `$log10`
    * `$pow`
    * `$round`
    * `$sqrt`
    * `$trunc`
    * `$stdDevPop`
    * `$stdDevSamp`
    * `$firstN`
    * `$lastN`
    * `$maxN`
    * `$minN`
    * `$top`
    * `$bottom`
    * `$topN`
    * `$bottomN`
    * `$median`
    * `$percentile`
    * `$rank`
    * `$denseRank`
    * `$documentNumber`
    * `$objectToArray`
    * `$zip`
    * `$slice`
    * `$isArray`
    * `$cmp`
    * `$isNumber`
    * `$type`
    * `$toBool`
    * `$toDate`
    * `$toDecimal`
    * `$toObjectId`
    * `$toUUID`
    * `$toInt`
    * `$toDouble`
    * `$toLong`
    * `$dateAdd`
    * `$dateSubtract`
    * `$dateDiff`
    * `$dateFromString`
    * `$dateFromParts`
    * `$dateToParts`
    * `$dateToString`
    * `$year`
    * `$month`
    * `$dayOfMonth`
    * `$dayOfWeek`
    * `$dayOfYear`
    * `$hour`
    * `$minute`
    * `$second`
    * `$millisecond`
    * `$isoDayOfWeek`
    * `$week`
    * `$isoWeek`
    * `$isoWeekYear`
    * `$substrBytes`
    * `$substrCP`
    * `$strLenBytes`
    * `$strLenCP`
    * `$indexOfBytes`
    * `$indexOfCP`
    * `$binarySize`
    * `$regexMatch`
    * `$regexFind`
    * `$regexFindAll`
  * más stages
  * más acumuladores
  * más expresiones
  * más profundidad en ventanas, buckets y lookups
* raw batches:
  * `find_raw_batches`
  * `aggregate_raw_batches`
* opciones avanzadas por método:
  * `collation`
  * `allow_disk_use`
  * `bypass_document_validation`

#### Fase 6
* API administrativa y de metadata:
  * `Database.command`
    * ya soporta también `explain` para `find`, `aggregate`, `update` y `delete`
    * ya soporta también `listCommands` y `connectionStatus`
    * ya soporta también `renameCollection` dentro del mismo `db`
    * `listCollections` ya soporta `nameOnly`
    * ya soporta también `serverStatus` con metadata local honesta
    * ya soporta también `hostInfo`, `whatsmyuri` y `getCmdLineOpts`
    * `count` ya soporta `skip`, `limit`, `hint`, `comment` y `maxTimeMS`
    * `distinct` ya soporta `hint`, `comment` y `maxTimeMS`
    * `collStats` y `dbStats` ya soportan `scale`
    * comandos de índices ya propagan `comment` y `maxTimeMS` donde aplica
  * `server_info`
  * `validate_collection`
  * listados enriquecidos de bases de datos y colecciones
  * más resultados, errores y detalles de escritura
* API pública de colección:
  * `count_documents` ya soporta `hint`, `comment`, `max_time_ms`, `skip` y `limit`
  * `distinct` ya soporta `hint`, `comment` y `max_time_ms`
  * `estimated_document_count` ya soporta `comment` y `max_time_ms`
* search indexes:
  * ya soporta `create_search_index`
  * ya soporta `create_search_indexes`
  * ya soporta `list_search_indexes`
  * ya soporta `update_search_index`
  * ya soporta `drop_search_index`
* observabilidad:
  * ya soporta `watch` en cliente, base de datos y colección mediante un bus interno de eventos y scoping local explícito
  * ya soporta también reanudación local básica con `resume_after`, `start_after` y `start_at_operation_time`
* mejor alineación con `bson` y clases públicas asociadas
  * ya soporta también clases públicas BSON alineadas con el ecosistema:
    * `Binary`
    * `Regex`
    * `Timestamp`
    * `Decimal128`
    * `SON`
    * `DBRef`
  * codec, validación, queries y aggregation reconocen ya estas clases en el perímetro observable local

#### Fase 7
* topología real de red:
  * parsing completo de URI
    * ya soporta seeds, credenciales, `directConnection`, `loadBalanced`, `replicaSet`, compresores, auth y TLS tipados
    * ya soporta también derivación de `readPreference`, `readPreferenceTags`, `maxStalenessSeconds`, `readConcernLevel`, `w`, `journal` y `wtimeoutMS`
  * auth
    * ya existe `AuthPolicy` explícita derivada desde URI
    * ya valida mecanismos básicos como `SCRAM-*`, `PLAIN`, `GSSAPI` y `MONGODB-X509`
    * ya existe autenticación efectiva sobre conexiones wire del runtime
  * TLS
    * ya existe `TlsPolicy` explícita derivada desde URI
    * `mongodb+srv` activa TLS por defecto y se validan invariantes básicas de certificados y X509
  * SRV
    * ya existe resolución SRV tipada y materialización de seeds efectivos para el runtime
    * ya existe también resolución DNS SRV real, con degradación estable para entornos locales o de test
  * replica sets
  * sharding
  * pooling
    * ya existe pooling con recursos de conexión vivos y reutilización real sobre sockets
    * ya existe pruning por `maxIdleTimeMS`
  * selección de servidor
    * ya existe selección básica por topología y `readPreference`
    * ya existe degradación coherente para topologías no replicadas
    * ya soporta también filtrado básico por `readPreferenceTags`, `maxStalenessSeconds` y ordenación `nearest`
    * ya existe refresh de topología por `hello` real, con estado de miembros ocultos, árbitros y metadata básica de elección
* concerns y preferencias con semántica de driver real:
  * timeouts
  * retries
  * selección de lectura/escritura
  * ya se derivan desde URI hacia el runtime del cliente antes de ejecutar peticiones
  * ya existe pipeline explícito de ejecución con:
    * selección de servidor
    * checkout/checkin de conexión
    * envío por transporte
    * clasificación de errores
    * timeout de socket
    * retry básico para lecturas y escrituras
  * ya existe también transporte wire real sobre sockets mediante `OP_MSG`
* monitoring y comportamiento propio de un cliente de red completo
  * ya existe `DriverMonitor` tipado con eventos de:
    * selección de servidor
    * checkout/checkin de conexión
    * inicio, éxito y fallo de comando
  * ya existe transporte local explícito sobre `Database.command(...)`
  * el cliente ya puede ejecutar peticiones a través del runtime completo con `execute_driver_command(...)`
  * el cliente ya puede ejecutar también peticiones sobre transporte wire real con `execute_network_command(...)`
  * ya existe refresh explícito de topología contra `hello` real sobre conexiones del runtime
  * ya existe también monitoring periódico de topología con `heartbeatFrequencyMS`

Arquitectura ya preparada antes de abrir la funcionalidad de red real:
* `MongoUri` tipado y compilado, con semillas, credenciales y opciones normalizadas
* `TopologyDescription` y `ServerDescription` explícitos como frontera para selección de servidor
* políticas separadas para:
  * timeouts
  * retries
  * selección
  * concerns
* `DriverRuntime` como punto único para:
  * planificación de peticiones
  * preparación de ejecución
  * snapshot de topología
  * ciclo de vida de pools
* subsistema de conexiones/pooling separado del cliente:
  * `PoolKey`
  * `ConnectionPool`
  * `ConnectionRegistry`
  * leases y snapshots tipados

Esto deja la Fase 7 lista para crecer sin repartir lógica de URI, topología, políticas y pools entre `MongoClient`, el proxy wire y futuros conectores de red.

Estado actual de Fase 7:
* la URI deja de ser metadata pasiva y gobierna ya el runtime del cliente;
* concerns, preferencias, auth/TLS, SRV, topología, selección y pools ya entran por un `DriverRuntime` común;
* ya existe pipeline explícito de ejecución y monitoring de estilo driver;
* ya existen transporte local y transporte wire real sobre sockets;
* ya existe refresh de topología por `hello` y pooling con recursos vivos reutilizables;
* ya existe descarte de conexiones rotas y timeout honesto cuando no hay servidor elegible;
* con este cierre, la Fase 7 puede considerarse aplicada y cerrada en su perímetro.
* lo que quede a partir de aquí ya no es infraestructura base de driver, sino profundidad adicional de paridad y refinamiento de comportamiento.

---

## 7. Estado Arquitectónico Consolidado y Punto de Reentrada

Esta sección resume el estado real del refactor arquitectónico después de cerrar la base del proyecto, el bloque de compatibilidad, la capa wire y la evolución posterior de BSON, observabilidad, MVCC e indexación virtual. `DESIGN.md` es ya el documento principal de referencia y de reentrada.

### 7.1 Bloques Arquitectónicos Ya Consolidados

Estos bloques se consideran ya estructuralmente aplicados:

* **Compatibilidad declarativa**:
  * catálogo único para dialectos MongoDB, perfiles PyMongo, operadores, stages, acumuladores y opciones;
  * exports y tooling derivados desde el catálogo;
  * políticas de comportamiento oficiales ya derivadas desde `policy_spec`.
* **Administración y comandos**:
  * subsistema separado de CRUD;
  * parseo, ejecución y serialización desacoplados;
  * `Database.command(...)` ya apoyado en servicios y resultados tipados.
* **Tipado interno**:
  * snapshots, explain, resultados administrativos, writes complejos e índices internos ya no dependen de `dict` ad hoc como contrato principal.
* **Planes de operación**:
  * lectura, agregación, explain, rutas admin y writes propios viajan por operaciones compiladas;
  * los engines ejecutan sobre IR interna, no sobre argumentos crudos dispersos.
* **Update engine formal**:
  * paths compilados, contexto explícito, aplicaciones resueltas y planes reutilizables;
  * arrays y rutas posicionales ya siguen el mismo flujo base.
* **Semántica compartida entre engines**:
  * lectura y explain descansan ya sobre un núcleo semántico común;
  * `MemoryEngine` y `SQLiteEngine` se diferencian más por estrategia física que por reglas de lenguaje.
* **Estado transaccional y MVCC**:
  * estado explícito por sesión/engine;
  * snapshots y contexto MVCC formales para memoria y SQLite.
* **Observabilidad y paridad**:
  * `system.profile`;
  * lineage de ejecución;
  * paridad diferencial declarativa contra MongoDB real;
  * catálogo formal de errores con `code`, `codeName` y `errorLabels`.
* **Superficie de ecosistema de Fase 6**:
  * search indexes locales tipados y persistidos por engine;
  * `watch()` en cliente, base de datos y colección sobre un bus interno de change streams con scoping explícito;
  * clases públicas BSON (`Binary`, `Regex`, `Timestamp`, `Decimal128`, `SON`, `DBRef`) integradas en codec, validación y expresiones observables;
  * bridge BSON del proxy wire alineado con esas clases públicas en ambas direcciones;
  * compatibilidad observable suficiente para crecer esa superficie sin contaminar CRUD ni el core semántico.
* **Extensibilidad**:
  * SDK de extensión para operadores de expresión y stages de agregación.
* **Preparación explícita para Fase 7**:
  * `DriverRuntime` como orquestador de URI, topología, políticas y request planning;
  * subsistema separado de pools y conexiones listo para evolucionar a red real, TLS, retries y selección de servidor.
* **Proxy wire**:
  * `WireSurface` declarativa;
  * handshake separado;
  * contextos de conexión y petición;
  * store de cursores y sesiones;
  * executor desacoplado del adaptador TCP;
  * compatibilidad real con `pymongo.MongoClient` sobre `OP_MSG` y handshake legacy.

### 7.2 Bloques Aplicados con Matices

Estos bloques ya tienen la arquitectura correcta, pero todavía no deben venderse como fidelidad total frente a MongoDB real:

* **Separación core semántico vs engine**:
  * el corte principal ya existe;
  * siguen quedando rutas físicas específicas del backend que todavía aportan parte del comportamiento observable.
* **Políticas de comportamiento derivadas del catálogo**:
  * la fundación declarativa está cerrada;
  * la ganancia máxima llegará al versionar más deltas semánticos finos en el propio catálogo.
* **MVCC virtual**:
  * existe estado explícito y snapshots reales;
  * no pretende todavía igualar semántica distribuida completa de MongoDB real.
* **Spill-to-disk de agregación**:
  * `allowDiskUse` y la política de spill ya son parte del contrato;
  * la implementación actual no persigue todavía streaming fino ni minimización agresiva de memoria.
* **Fidelidad BSON escalar**:
  * wrappers internos, codec BSON-aware y helpers numéricos ya cubren lectura, updates y una parte importante de aggregation;
  * siguen faltando matices para hablar de fidelidad BSON total.
* **Motor de indexación virtual**:
  * ya cubre semántica `sparse` y `partialFilterExpression`, hints, unicidad, persistencia y `explain`;
  * el salto final exigiría una historia más ambiciosa para multikey avanzado y planificación automática.

### 7.3 Bloques Reabiertos o Deliberadamente No Cerrados del Todo

Estos son los puntos donde conviene volver si el objetivo vuelve a ser arquitectura, no solo superficie funcional:

* **Descomposición final de agregación**:
  * `mongoeco.core.aggregation` ya es un paquete y el runtime se ha troceado mucho;
  * aun así, el área de agregación sigue siendo la más compleja del core y sigue mereciendo vigilancia continua.
* **Pushdown/planificación avanzada y spill más fino**:
  * el diseño base ya existe;
  * el siguiente salto sería más de comportamiento y rendimiento que de refactor básico.
* **Fidelidad BSON total**:
  * la frontera arquitectónica ya es la correcta;
  * falta ampliar cobertura semántica y cerrar casos más raros de tipos/overflow/conversiones.

### 7.4 Cuándo Volver a Abrir Estos Puntos

La recomendación actual es **no reabrir ahora mismo la arquitectura base** salvo que aparezca una de estas señales:

* una nueva versión de MongoDB o PyMongo obliga a introducir semántica difícil de modelar con el catálogo/policies actuales;
* ampliar aggregation o índices empieza a producir fricción estructural real en vez de solo trabajo funcional;
* la fidelidad BSON pasa a ser requisito contractual fuerte frente a MongoDB real y ya no basta con el nivel actual;
* el crecimiento del proxy wire exige más superficie de servidor que ya no encaja limpiamente con la `WireSurface` y el executor actuales.

Mientras no aparezca una de esas señales, el criterio recomendado es:

1. mantener `DESIGN.md` como visión consolidada;
2. dedicar el siguiente esfuerzo principalmente a **superficie funcional, cobertura y paridad observable**, no a reabrir otra gran ola de refactor.

### 7.5 Estado Actual de Fase 8

Fase 8 puede considerarse **cerrada** dentro del perímetro que este documento le asigna.

Queda ya aplicada:

* la capa de compatibilidad con capabilities semánticas explícitas para deltas finos;
* la IR única de lectura hasta los engines;
* la distinción formal `streamable` / `materializing` en agregación;
* `find_raw_batches` y `aggregate_raw_batches`;
* `collation` y `bypass_document_validation` en la superficie observable local relevante;
* fallback híbrido de SQLite para evitar materialización innecesaria cuando solo el orden requiere Python;
* artefactos versionados para contraste exhaustivo con la suite de `mongomock`:
  * `tests/fixtures/mongomock_suite_inventory.json`
  * `tests/fixtures/mongomock_suite_matrix.json`
  * `tests/fixtures/mongomock_suite_rules.json`

Lo que quede a partir de aquí ya no forma parte del cierre de Fase 8, sino de refinamiento continuo:

* seguir importando o reescribiendo casos de alto valor desde la cola restante de `mongomock`, sobre todo en `collection` y `_mongomock`;
* seguir apurando fidelidad BSON rara;
* refinar comportamiento de driver o de SQLite con ambición más cercana a producción.

Refinamiento continuo ya aplicado después del cierre formal de Fase 8:

* helper compartido para orden BSON y clave de índice numérica, usado ya tanto por el core como por `SQLiteEngine`;
* soporte de rangos numéricos sobre índices multikey auxiliares en SQLite con orden textual estable;
* `multikey_entries` endurecida con `type_score` para que la comparación mixta no dependa de la semántica nativa de orden de SQLite;
* cache en memoria de metadata de índices SQLite con invalidación explícita en cambios estructurales;
* construcción de índice multikey en dos fases cuando hay reader dedicado disponible, evitando retener el lock exclusivo durante todo el escaneo de colección;
* fallback de SQLite ejecutado sobre un ejecutor propio del engine, sin depender del pool global de `asyncio.to_thread`;
* validación documental en `SQLiteEngine` adelantada fuera del lock en la ruta estable de `put_document`, con revalidación solo si el snapshot cambia al volver a entrar;
* spill de agregación mejorado con ordenación externa por chunks para `$sort` cuando la política de spill lo pide;
* `SyncRunner` más explícito al propagar `ExecutionTimeout` y `ServerSelectionTimeoutError` en la capa sync;
* pool de conexiones del driver con espera FIFO real para reducir starvation bajo contención.

Estado actual del contraste versionado con `mongomock`:

* `872` casos inventariados;
* `9` ya marcados como `covered`;
* `587` ya marcados como `equivalent`;
* `66` marcados como `outside-scope`;
* `210` quedan como `review-needed`.

Orden recomendado de refino continuo a partir de aquí:

1. seguir importando o reescribiendo casos de alto valor desde la cola restante de `mongomock`;
2. seguir apurando fidelidad BSON rara y semántica fina de comparación;
3. revisar si merece la pena otra vuelta sobre materialización/fallback en agregación y SQLite;
4. endurecer más pooling y concurrencia SQLite solo si el objetivo pasa de paridad fina a ambición más cercana a producción.

### 7.5 Estado Vivo Actual del Repositorio

Para evitar que los cierres históricos de cada fase se confundan con el estado corriente:

* la métrica viva de referencia debe tomarse siempre de la última ejecución real de la suite, no de snapshots antiguos incluidos en fases ya cerradas;
* en el estado actual del repositorio, la verificación de referencia es:
  * `python -m unittest discover -s tests -p 'test*.py'` -> `1452` tests, `OK (skipped=1)`
  * cobertura total sobre `src/mongoeco` -> `92%`
* cualquier mención anterior a `100%` o a snapshots de `726`/`831` tests debe leerse como contexto histórico del cierre de fase correspondiente, no como descripción del estado presente.

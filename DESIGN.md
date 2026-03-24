# Diseño de Arquitectura: mongoeco 2.0 (Async Reboot)

Este documento define la visión técnica para la reescritura de **mongoeco**, enfocándose en un diseño moderno, asíncrono nativo y desacoplado.

---

## 1. Objetivos de Diseño
* **Async-First**: Implementación nativa con `async/await`.
* **Arquitectura Hexagonal**: Separación total entre lógica de MongoDB, almacenamiento y API de cara al usuario.
* **Persistencia Profesional**: Uso opcional de SQLite (JSON1) para soportar transacciones ACID reales y atomicidad sin bloqueos manuales complejos.
* **Single Source of Truth**: La semántica funcional vive en el Core y en la implementación async. En Fase 1, la capa sync se resuelve con un adaptador delgado; la automatización completa vía `unasync` queda diferida.
* **Zero External Dependencies**: La librería debe funcionar sin `pymongo` instalado. Todas las excepciones y clases de resultado (`InsertResult`, etc.) deben ser implementaciones propias que emulen la API de PyMongo para evitar el acoplamiento.
* **Modern Stack**: 
    * **Python 3.13+**: La base de código adopta sintaxis y tipado modernos de Python 3.13 (`PEP 695`, `@override`, aliases `type`, `TypeIs`, etc.) como decisión explícita de Fase 1.
    * **MongoDB 7.0 Parity**: El Core persigue la semántica de la v7.0 dentro del perímetro explícito de Fase 1.
    * **PyMongo 4.9+ Compatibility**: La API pública busca compatibilidad práctica con PyMongo 4.9+ dentro del subconjunto ya implementado y testeado.

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
* **SQLiteEngine**: Objetivo explícito de Fase 2. Su diseño seguirá el mismo protocolo async-first cuando se implemente.
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
* **SQLiteEngine** real y optimizaciones SQL.
* **Pipeline automático `unasync`**. La sync de Fase 1 es un adaptador funcional, no código generado.
* **Agregación pública** (`aggregate`) y pipeline de `core/aggregation.py`.
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

### D. Estructura del Proyecto
* El código instalable vive bajo `src/mongoeco/`.
* El baseline de lenguaje (`Python 3.13+`) se declara en `pyproject.toml`, no solo en este documento.

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
*   **Decisión: unasync Diferido**: El pipeline automático de generación no forma parte del cierre de Fase 1. El script `scripts/unasync_api.py` queda explícitamente marcado como pendiente de Fase 2 para evitar generar código roto.
*   **Decisión: Compatibilidad de Lenguaje**: La Fase 1 se cierra deliberadamente sobre Python 3.13+ y permite usar sintaxis moderna de tipado, `override` y validadores `TypeIs` sin arrastrar compatibilidad artificial con versiones anteriores.
*   **Decisión: Suite de Conformidad**: La suite se organiza por capas en `tests/contracts/`, `tests/integration/` y `tests/unit/`. Cualquier motor futuro debe pasar los contratos de engine y la integracion de API.
*   **Deuda Aceptada al Entrar en Fase 2**:
    * `find()` ya existe, pero la capa sync devuelve una lista materializada, no un cursor estilo PyMongo.
    * `QueryPlan` ya es canónico y tipado, pero el contrato público del engine sigue aceptando `Filter`; la migración interna hacia `QueryNode` queda para el trabajo de SQLite.
    * `ClientSession` ya puede anclar estado por engine, pero sigue siendo un placeholder sin transacciones, rollback ni isolation real.
    * Los índices `unique=True` ya se aplican en `MemoryEngine`, pero todavía no existe planificación de consultas ni uso de índices para acelerar lecturas.
    * `paths.py` ya soporta segmentos numéricos simples sobre listas, pero no cubre todavía semántica Mongo más avanzada sobre arrays posicionales.
*   **Definition of Done**: El cierre de Fase 1 exige compilación del paquete, suite en verde, documentación alineada y cobertura de contratos para engine, API async, API sync y core mínimo.
*   **Estado**: Fase 1 Completada dentro del perímetro descrito arriba. SQLite, agregación, sesiones, transacciones, `find()` cursor público, índices reales y `unasync` real quedan diferidos a fases posteriores.

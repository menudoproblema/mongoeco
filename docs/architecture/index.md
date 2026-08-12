# Arquitectura de `mongoeco`

## Objetivo

Esta documentacion describe la arquitectura real de `mongoeco` a partir del
codigo actual. Su objetivo es explicar:

- como se estructura el sistema;
- que contratos publicos expone y donde acaba cada uno;
- que patrones de diseno se usan y por que;
- que decisiones de arquitectura estan vigentes hoy;
- que limites del producto son conscientes y no accidentes.

Este arbol es la referencia canonica para arquitectura. `README.md` y
`COMPATIBILITY.md` siguen siendo documentos de entrada y operacion, no la fuente
completa de verdad de la estructura interna.

## Mapa de subsistemas

`mongoeco` esta organizado en siete subsistemas principales:

| Subsistema | Responsabilidad principal |
| --- | --- |
| `api` | Superficie publica async/sync, normalizacion de argumentos y compilacion de operaciones |
| `core` | Runtime semantico: query planning, filtering, sorting, aggregation, updates, validation, collation |
| `engines` | Persistencia local y ejecucion fisica, hoy con `MemoryEngine` y `SQLiteEngine` |
| `driver` | Runtime de cliente local: URI, policies, seleccion de servidores, topologia y ejecucion de requests |
| `wire` | Surface del protocolo wire y proxy local para comandos estilo MongoDB |
| `compat` | Modelado explicito de dialectos MongoDB y perfiles PyMongo |
| `_types` | Tipos internos agrupados por dominio, reexportados publicamente desde `mongoeco.types` |
| `_change_streams` | Runtime interno de change streams locales, cursores, retencion y journal |

## Ruta de lectura recomendada

1. [system-overview.md](system-overview.md)
2. [api-surfaces.md](api-surfaces.md)
3. [core-runtime.md](core-runtime.md)
4. [storage-engines.md](storage-engines.md)
5. [engine-spi-v2.md](engine-spi-v2.md)
6. [driver-and-wire.md](driver-and-wire.md)
7. [change-streams-and-collation.md](change-streams-and-collation.md)
8. [testing-and-compatibility.md](testing-and-compatibility.md)
9. [decisions/index.md](decisions/index.md)

## Patrones principales

Los patrones de diseno que aparecen de forma sistematica en el codigo son:

- **Facade**: clientes, bases de datos, colecciones y cursores publicos.
- **Adapter**: superficie sync delegando sobre runtime async; wire/driver
  adaptando comandos al engine local.
- **Thin Protocol / Ports**: contrato de engine basado en protocolos delgados.
- **Strategy / Policy objects**: timeout, retry, seleccion, concerns, auth y
  tls.
- **Compile-then-execute**: operaciones publicas que primero se normalizan y
  compilan a semantica antes de ejecutarse.
- **Coordinator + subservices**: `SQLiteEngine`, `DriverRuntime` y
  `ChangeStreamHub`.
- **Thin composer**: `mongoeco.types` y `compat/catalog.py` como fachadas
  publicas que reexportan o componen datos desde modulos internos.
- **Declarative routing**: `database_admin`, `driver` y `wire` separan cada vez
  mas la fachada coordinadora del routing por familias y de la ejecucion por
  subsistemas.
- **Capability exposure**: APIs explicitas para hacer visibles contratos
  parciales, como `collation_capabilities_info()` o `sdam_capabilities_info()`.

## Decisiones relevantes

| ADR | Tema | Decision vigente |
| --- | --- | --- |
| [ADR-001](decisions/ADR-001-async-first-y-sync-adaptador.md) | Async-first | La semantica primaria vive en la superficie async y la sync adapta sobre ella |
| [ADR-002](decisions/ADR-002-dialecto-y-perfil-como-ejes-distintos.md) | Compatibilidad | `mongodb_dialect` y `pymongo_profile` se modelan como ejes separados |
| [ADR-003](decisions/ADR-003-planning-mode-y-degradacion-explicita.md) | Planning | `STRICT` y `RELAXED` hacen visible la degradacion en vez de ocultarla |
| [ADR-004](decisions/ADR-004-engines-con-protocolos-delgados.md) | Engines | El contrato de storage se define con protocolos delgados, no con una base monolitica |
| [ADR-005](decisions/ADR-005-sqlite-modular-y-memory-como-baseline.md) | Engines locales | `SQLiteEngine` se modulariza por subsistemas y `MemoryEngine` actua como baseline semantico local |
| [ADR-006](decisions/ADR-006-change-streams-locales-y-persistencia-opcional.md) | Change streams | Son locales, acotados y opcionalmente persistentes, no distribuidos |
| [ADR-007](decisions/ADR-007-sdam-parcial-y-explicito.md) | Driver | Se soporta un subconjunto honesto de SDAM, no una emulacion completa de cluster |
| [ADR-008](decisions/ADR-008-pyicu-opcional-y-pyuca-como-fallback.md) | Collation | `PyICU` es opcional y `pyuca` cubre el subset Unicode basico |
| [ADR-009](decisions/ADR-009-parity-tests-como-politica-de-aceptacion.md) | Testing | Toda feature publica compartida entra con parity tests async/sync y cross-engine |

## Limites conscientes del producto

Esta documentacion describe una arquitectura local y honesta, no una
implementacion completa de un cluster MongoDB de produccion. En particular:

- los change streams no son distribuidos;
- el SDAM soportado es parcial y explicitamente acotado;
- la collation avanzada depende de `PyICU`;
- `$search` y `$vectorSearch` tienen un perimetro local y no pretenden igualar
  Atlas Search, aunque `SQLiteEngine` ya use FTS5 y `usearch` como backends
  locales reales;
- el geoespacial soportado es amplio pero estrictamente planar/local, no
  geodesico;
- `currentOp` y `killOp` existen solo como introspeccion/cancelacion local,
  best-effort y sin semantica distribuida;
- `$merge`, `$densify` y `$fill` forman parte del runtime de agregacion, pero
  como subsets locales y documentados;
- el wire runtime existe para compatibilidad local y testing, no para escalar
  como un servidor MongoDB completo.

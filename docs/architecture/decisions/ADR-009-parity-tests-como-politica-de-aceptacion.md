# ADR-009 - Parity tests como politica de aceptacion

## Contexto

La arquitectura tiene varias superficies paralelas:

- async y sync;
- `MemoryEngine` y `SQLiteEngine`;
- runtime directo y degradacion observable.

Sin una politica explicita, la deriva entre rutas es facil.

## Decision

Tratar los parity tests como criterio de aceptacion obligatorio para nuevas
features publicas compartidas.

## Consecuencias

- Las features nuevas deben entrar con cobertura async/sync cuando ambas
  superficies existan.
- La semantica prometida igual entre engines debe fijarse con tests cruzados.
- Los errores publicos, `planning_issues` y la preservacion de opciones
  heredadas deben congelarse en tests cuando formen parte del contrato.
- Los agregadores y compositores publicos que reducen deuda estructural, como
  `mongoeco.types` o `compat/catalog.py`, pueden fijarse tambien con tests
  estructurales ligeros para evitar que reabsorban logica.
- Lo mismo aplica a coordinadores internos sensibles, como `DriverRuntime`,
  `WireCommandExecutor`, `database_admin.py` o `SQLiteEngine`, cuando ya se ha
  invertido en separar subsistemas auxiliares.

## Alternativas descartadas

- Confiar solo en tests unitarios locales.
- Permitir divergencia justificada caso a caso sin policy comun.
- Tratar parity como un objetivo deseable pero no obligatorio.

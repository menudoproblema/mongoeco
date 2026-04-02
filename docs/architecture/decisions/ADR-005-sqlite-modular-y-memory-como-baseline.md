# ADR-005 - `SQLiteEngine` modular y `MemoryEngine` como baseline

## Contexto

`SQLiteEngine` concentro durante mucho tiempo gran parte de la complejidad del
runtime fisico en un solo modulo. A la vez, `MemoryEngine` se consolidaba como
referencia semantica local y soporte natural para parity tests.

## Decision

Modularizar `SQLiteEngine` por subsistemas internos y mantener `MemoryEngine`
como baseline semantico local, no como backend de segunda categoria.

## Consecuencias

- `sqlite.py` queda orientado a coordinacion, lifecycle y wiring.
- La complejidad de planner, read execution, indices, search admin y runtime se
  reparte en modulos internos.
- La coordinacion de sesion/transaccion tambien se considera ya una frontera
  propia y vive fuera del engine principal.
- La administracion de namespaces y parte del lifecycle administrativo tambien
  vive ya en un runtime especifico, en vez de seguir mezclada con el engine
  principal.
- El profiling administrativo (`system.profile`, lectura puntual y ajuste de
  nivel) forma parte de esa misma frontera administrativa local.
- La politica de mantenimiento pasa a ser explicita: nuevos cambios de
  profiling, stats, invalidaciones o namespace admin deben entrar primero por
  la frontera administrativa extraida, no reabriendo `sqlite.py`.
- Los parity tests con `MemoryEngine` ganan valor como fijacion del contrato
  compartido.

## Alternativas descartadas

- Mantener `sqlite.py` como monolito.
- Reescribir ambos engines sobre una base comun mas agresiva.
- Tratar `MemoryEngine` como implementacion auxiliar sin peso arquitectonico.

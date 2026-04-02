# ADR-001 - Async-first y superficie sync adaptadora

## Contexto

`mongoeco` expone una superficie async y otra sync. Mantener dos
implementaciones semanticas completas habria multiplicado:

- el coste de cambio;
- la deriva de comportamiento;
- la necesidad de duplicar fixes y pruebas.

## Decision

La semantica primaria vive en la ruta async. La ruta sync se implementa como
fachada adaptadora sobre esa ruta, apoyada en un runner estable y wrappers sync
de clientes, bases de datos, colecciones y cursores.

## Consecuencias

- Los cambios funcionales deben entrar primero en async o compartidos.
- La superficie sync requiere parity tests, no reimplementacion divergente.
- La arquitectura gana consistencia a costa de una capa adaptadora adicional.

## Alternativas descartadas

- Mantener dos implementaciones completas en paralelo.
- Hacer la superficie sync la primaria y envolverla para async.
- Exponer solo API async.

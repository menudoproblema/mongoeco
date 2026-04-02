# ADR-008 - `PyICU` opcional y `pyuca` como fallback

## Contexto

La collation Unicode avanzada requiere capacidades que no estan siempre
disponibles en todos los entornos locales. Hacer `PyICU` obligatorio aumentaria
friccion de instalacion y packaging.

## Decision

Mantener `PyICU` como backend opcional y preferido, y `pyuca` como fallback para
el subset Unicode basico soportado. Las opciones avanzadas que requieren ICU
deben fallar explicitamente si no hay backend ICU.

## Consecuencias

- El baseline Unicode es util sin dependencia nativa fuerte.
- La collation avanzada solo existe cuando el entorno puede soportarla.
- El contrato queda visible via `collation_backend_info()` y
  `collation_capabilities_info()`.

## Alternativas descartadas

- Hacer `PyICU` obligatorio para toda instalacion.
- Fingir soporte avanzado con `pyuca`.
- Limitar todo a `simple` y no soportar collation Unicode.

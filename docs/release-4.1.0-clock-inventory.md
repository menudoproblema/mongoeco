# Inventario de lecturas de reloj — 4.1.0

El cerrojo `tests/unit/core/test_clock_architecture.py` recorre el AST de
`src/mongoeco` y exige que cada lectura directa de reloj esté clasificada en
`tests/fixtures/clock_call_allowlist.json`.

Las lecturas semánticas se resuelven mediante el contexto de ejecución del
cliente: `$$NOW`, `$currentDate` de tipo fecha y TTL en Memory/SQLite. Las
excepciones declaradas son deliberadamente no semánticas: telemetría,
handshakes, medición de latencia y timeouts, disponibilidad simulada de índices
y la generación de `ObjectId`, que permanece fuera del contrato público.

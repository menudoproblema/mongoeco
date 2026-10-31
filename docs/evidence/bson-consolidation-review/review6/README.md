# Revisión 6: autocrítica frente a entregas posteriores

Owner de obligaciones: [plan](../../../bson-semantics-consolidation-plan.md).
Owner de gates: [registro](../../../bson-consolidation-implementation.md).
Base de runtime: `ef604c7`. No se cambian runtime ni tests durante esta revisión.
El corpus write_effect y su registro ya estaban sin commitear: no se aceptan aquí.

- `representation-probe.py.txt` deriva de la sonda histórica refinement, sin su
  bloque TTL. Registra operandos/documentos reales, materialización y distinct
  en seis pares motor/dialecto. `representations.json` contiene 18 filas de
  frontera y controles eq/in para bytes/Binary 0/5.
- `probe.py.txt` usa bases UUID desechables en los replica sets propios ya
  disponibles 49973/49974/49975. Limpia cada base en finally; no inicia, detiene
  ni reconfigura servicios. `observations.json` registra versión/FCV, BSON
  posterior, error/contadores y campos presentes en el primer evento. Separa
  observaciones de productor y preparación watch (pipeline vacío).
- `sources-check.json` contiene nueva descarga y coincidencia de las catorce
  fuentes r8.0.0 históricas; añade parser y ejecutor pipeline por URL/hash.
  No modifica el manifiesto ni los receipts previos.
- `receipt.json` identifica inputs, commands, resultados, límites y hashes.

La primera ejecución de la sonda abortó: empleaba context manager en un cursor
local que no ofrece ese protocolo. La base nativa se limpió en finally; se
corrigió el observador a cierre explícito y se repitió todo. Ese intento no
se cuenta como aceptación ni como defecto nuevo dentro de este alcance.
La ampliación NaN abortó después durante delete de limpieza local, porque su
storage key no valida contra el NaN decodificado. Se aisló cada caso en una
colección y se repitió la sonda completa; ese error de producto queda dentro
de D01b, sin sustituirlo por un cambio de claves persistidas.

Los 345 checks de replay usan capturas nativas existentes y cubren superficies
directas/wire, sync/async. Los 69 checks de propiedades/fronteras son locales.
Las sondas nuevas son diagnóstico adicional, no golden ni aceptación D01b/D04:
no observan todos los campos del evento, no prueban ausencia/no-op por barrera,
rollback, reanudación, arrays del diff, ni todos los tamaños. No se ejecutan
suite global, wheel, matrices de entorno, watch con transformaciones ni TTL.
Los servidores temporales preexistentes permanecen disponibles para el trabajo
de implementación en curso; sus bases de esta revisión fueron eliminadas.

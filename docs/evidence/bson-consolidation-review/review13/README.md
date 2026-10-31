# Revisión 13: autocrítica repetida frente al runtime aceptado

Base `e8abf99`; el adjunto conserva el SHA-256 de revisiones 3–12. El plan
posee el orden activo y el registro de implementación posee estados/gates.
Esta revisión no cambia producción, tests ni el trabajo de CI preexistente.

- `focal.txt`: 172 pruebas, sin skips.
- `replay.txt`: 543 pruebas y 6.073 deseleccionadas; referencias nativas
  existentes, no recaptura física. Incluye DBRef, Binary, SQL y merge.
- `replay-sandbox.txt`: intento inicial, 279 pasan y 264 fallan al abrir sockets
  loopback con PermissionError. La ejecución completa posterior habilitada pasa;
  no se atribuyen esos fallos a semántica ni se borran del historial.
- `representations.json`: 18 trazas reales de matcher en seis pares locales;
  comparación/clave de subtipos 0/5 y resultados eq/in/distinct.
- `context-probe.json`: literal DBRef básico ordenado frente a invertido por
  ambos motores/tres dialectos; H55 sigue presente en preparación/runtime y
  `$literal` funciona como control. Diagnóstico local, no regresión permanente.
- `sources-check.json`: nueva descarga de las 16 URLs oficiales r8.0.0;
  todos los hashes coinciden con sus referencias históricas, que no se editan.
- `runtime-verification.json`: los 283 módulos coinciden con el manifiesto del
  corte aceptado namespace-images. `preexisting-workset.json` identifica los
  cambios de CI/documentación ya presentes, sin aceptarlos ni modificarlos.

Los scripts `.py.txt` permiten reproducir diagnósticos; no añaden un runner.
Las sondas locales usan engines temporales, sin servicios ni datos externos.
No hay nuevas sondas MongoDB, TTL/H53, recapturas, suite global, matrices,
benchmark o ejecución remota CI. Se reutilizan referencias y aceptaciones con
su alcance original. No se cierra D04, D05 ni B07.

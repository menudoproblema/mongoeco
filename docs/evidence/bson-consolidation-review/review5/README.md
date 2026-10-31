# Revisión 5: mismo informe, runtime posterior

Owner de decisión: [plan refinado](../../../bson-semantics-consolidation-plan.md).
Owner de progreso: [registro](../../../bson-consolidation-implementation.md).
El receipt distingue HEAD y árbol Binary en curso; no acepta esos cambios.

- `probe.py.txt`: reutiliza la sonda de revisión 4 en motores efímeros en memoria;
  `observations.json` conserva resultados DBRef, SQL ejecutado y llamadas al matcher.
- `native-probe.py.txt`: cinco agregaciones de solo lectura por versión en los
  servidores temporales ya existentes. Incluye control positivo y negativo DBRef.
  `native-and-raw.json` separa referencia nativa y diagnóstico del decoder local.
- `sources-check.json`: nueva descarga y coincidencia de las 14 fuentes fijadas;
  conserva URLs y hashes esperados/obtenidos, sin modificar el manifiesto histórico.
- `properties.txt`: tres propiedades existentes verdes; no acredita dominio nuevo.
- `runtime-sources.json`: hashes de runtime/tests presentes, incluidos cambios
  previos sin commitear. No es un manifiesto de entrega aceptada.

No se ejecutó wire/watch/TTL ni el replay completo. La primera sonda DBRef sin
`$literal` fallaba en `$documents`; se corrigió y se repitió con controles. No
se considera evidencia de gramática de consulta aquel error de expresión.
No se crearon, detuvieron ni reconfiguraron servidores. Los resultados son
caracterización del plan, no cierre de implementación.

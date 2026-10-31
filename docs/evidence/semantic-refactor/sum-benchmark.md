# Benchmark focal de acumulación numérica

Memory sync, 1.000 documentos, warmup 1 y tres repeticiones. El comparador mantenido falla porque los resultados cambian: los promedios anteriores no coinciden con MongoDB real. Las sondas `sum-benchmark-native-before.json` y `sum-benchmark-native-current.json` prueban esa diferencia en group y facet contra MongoDB 8. El harness y dataset conservan sus hashes.

Estos tiempos describen dos comportamientos distintos; no acreditan una optimización equivalente ni un gate verde de rendimiento. No se actualiza la referencia histórica para ocultar el cambio.

| Tarea | Antes (s) | Después (s) | Hash de resultado |
| --- | --- | --- | --- |
| materializing_aggregation_nested_group | 0.0292 | 0.0236 | distinto, corrección validada |
| materializing_aggregation_group_sort | 0.0220 | 0.0169 | distinto, corrección validada |

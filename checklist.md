# Performance Checklist

## Filtering

- [x] P3: `_evaluate_not_in` reutiliza lookup optimizado
- [x] P5: `$elemMatch` reutiliza plan precompilado
- [x] P2: mover aliases de `$type` a tiempo de compilación del plan
- [x] P4: evitar doble resolución de path en `_evaluate_all`
- [x] P6: evitar materializar `array_candidates` en `_evaluate_elem_match`
- [x] P7: precalcular máscara bitwise en el nodo compilado
- [x] P8: fast path de comparación para campo escalar
- [x] P9: fast path hashable en `$all` cuando sea semánticamente seguro

## Compiled Aggregation

- [x] P11: cachear compilación de `CompiledGroup`
- [x] P12: sustituir lambda `_truthy` por función/contexto reutilizable
- [x] P13: evitar `next(iter(...))` duplicado en `_compile_logic_only`
- [x] P14: microoptimizar validación dict-form de `$cond`
- [x] P15: ampliar cobertura compilada para expresiones comunes (`$add`, `$subtract`, `$multiply`, ...)

## Grouping Stages

- [x] P16: validar acumuladores una vez y separar inicialización
- [x] P17: `_apply_bucket` usa búsqueda binaria para boundaries
- [x] P18: evitar crear `cmp_to_key(...)` por elemento en `$bucketAuto`
- [x] P19: `$rank` / `$denseRank` pasan de O(N²) a O(N)
- [x] P20: reutilizar/resetear estado de ventana sin recrear bucket por `(doc, campo)`

## BSON Scalars

- [x] P21: fast path float para `bson_add` / `bson_subtract` / `bson_multiply`
- [x] P22: fast path nativo en `compare_bson_numeric`
- [x] P23: evitar `wrap_bson_numeric` redundante en `_wrap_numeric_result`
- [x] P24: colapsar `_wrap_from_templates` a una sola pasada
- [x] P25: microoptimizar `bson_numeric_alias` para ints pequeños
- [x] P26: limitar o iterativizar `validate_bson_value`

## Accumulators

- [x] P27: `_apply_accumulators` evita evaluaciones dobles innecesarias
- [x] P28: preextraer `(field, operator, expression)` para evitar `next(iter(...))` por documento
- [x] P29: early exit efectivo para `$firstN` cuando ya está lleno
- [x] P30: eliminar deepcopy redundante en finalización de `PickN` / `OrderedAccumulator`

## Fuera de Checklist

- No incluido `P1`: hoy rompe semántica observable fijada por tests de `extract_values`.
- No incluido `P10`: el fast path propuesto para `int` / `float` / `str` en `$in` no es seguro con collation y dialectos custom.

## Benchmark 500

Comparativa de la última tanda frente a la ejecución anterior. Valores más bajos son mejores.

```mermaid
xychart-beta
    title "Benchmark 500: Memory deltas (%)"
    x-axis ["cursor_mat_sync", "cursor_mat_async", "filter_low_sync", "filter_med_sync", "filter_high_sync", "filter_high_async"]
    y-axis "Delta %" -55 0
    bar [-39.1, -43.1, -49.3, -50.7, -39.0, -38.6]
```

- `cursor_consumption_materialized_200`: `memory-sync -39.1%`, `memory-async -43.1%`
- `filter_selectivity_low_100`: `memory-sync -49.3%`, `memory-async -49.2%`
- `filter_selectivity_medium_100`: `memory-sync -50.7%`, `memory-async -49.9%`
- `filter_selectivity_high_100`: `memory-sync -39.0%`, `memory-async -38.6%`

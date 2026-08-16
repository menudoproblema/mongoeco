# ADR-015 - Pushdown SQLite sujeto a prueba de equivalencia

## Contexto

SQLite puede reducir mucho el trabajo del runtime, pero BSON, arrays,
collation y orden heterogeneo no siempre tienen una traduccion SQL exacta. Un
fast path que aplica sort o ventana antes de un residual puede cambiar el
resultado aunque parezca correcto en casos simples.

## Decision

Toda optimizacion SQLite nace de un workload reproducible y se representa
mediante un plan tipado: SQL exacto, prefiltrado SQL con residual Python o
ejecucion Python. El plan declara fragmento, parametros, exactitud, residual,
ownership de sort y ventana, collectors, fallback y evidencia de explain.

Solo se implementan hasta tres shapes por ciclo, elegidos por frecuencia,
coste, selectividad, mejora potencial y capacidad de demostrar equivalencia.
No se aplica sort, skip o limit antes de un residual eliminatorio sin una
prueba especifica de seguridad.

Cada regla requiere benchmark antes/despues, mejora material, paridad con
Memory, paridad sync/async y comparacion con pushdown desactivado.

En 4.6 se midio como candidato el prefiltrado de una conjuncion formada por
igualdad escalar indexada y residual `$expr`. Sobre el workload reproducible de
100 documentos, tres repeticiones, el candidato hibrido obtuvo 0,8529 s de
media frente a 0,8464 s del baseline Python. Al no existir mejora material y
observarse una ligera regresion, la regla se retiro. El plan tipado y la
instrumentacion permanecen; este shape sigue declarando ownership Python. La
evidencia se conserva en
`benchmarks/reports/4.6.0-sqlite-pushdown-candidate-rejected.json` y
`benchmarks/reports/4.6.0-sqlite-pushdown-profile.json`.

El perfil final de 1.000 documentos anade predicados, sort/window, once shapes
Search y cuatro shapes de collectors. Registra ownership, selectividad,
candidatos, backend, estrategia top-k, fallback y memoria cuando el runtime la
puede medir. Los collectors exactos SQL quedan entre 0,0446 s y 0,0646 s,
frente a 0,9314 s del fallback semantico equivalente en esa maquina. Esta
evidencia confirma las reglas ya aceptadas, pero no justifica un shape nuevo.

## Consecuencias

- Una optimizacion sin evidencia medible se aplaza.
- Los fallbacks son resultados de planning observables, no excepciones
  accidentales del traductor.
- Arrays, collation, DBRef, Decimal y orden BSON complejo siguen en Python
  mientras no exista una traduccion exacta demostrada.

## Alternativas descartadas

- Ampliar el traductor por similitud sintactica.
- Considerar cualquier reduccion de candidatos semanticamente segura.
- Fijar los shapes de 4.6 antes de medir cargas representativas.

# Revisión 7: autocrítica, estado actual y efectos compartidos

Contraste del mismo adjunto de revisiones 3–6, sobre HEAD `375382a` y el
trabajo preexistente de eventos identificado en receipt/manifest. Solo se añade
documentación y diagnóstico; no se modifica producción ni se acepta ese trabajo.
El owner de obligaciones es el [plan](../../../bson-semantics-consolidation-plan.md);
el [registro](../../../bson-consolidation-implementation.md) conserva estado/gates.

## Disposición del informe

| Afirmación | Resultado actual |
| --- | --- |
| Retractación de críticas a causa Binary, `$in`, distinct y propiedades | Correcta; ya incorporada. No exige otro helper ni volver a ejecutar los arreglos. |
| Reconocimiento SDK/local/owned y traza de ambos lados del matcher | Cubierto en D03a; seis pares locales y 18 filas observadas de nuevo. Igualdad, in y distinct conservan payload/subtipo. |
| Parser literal DBRef y orden directo/wire | Implementados en `542bce9`. El replay vuelve a pasar; contextos/subpipelines restantes conservan D06a/b. |
| Eq/in SQL bytes frente a Binary 0 | Hay guarda explícita de elegibilidad y fallback. No se acepta pushdown por pasar un fallback. D03a/D03b mantienen sus testigos SQL. |
| TTL solo podría retrasarse un milisegundo irrelevante | No se adopta como garantía: mezcla de coordenadas antiguas/nuevas exige migración conservadora, ya entregada en D02. Sin nueva sonda TTL aquí. |
| Watch requiere fase, familia pública y código, además de contexto | Sigue pendiente D04; lectura actual confirma preparador restrictivo de replaceWith y apply_pipeline sin contexto explícito. No hay nueva ejecución watch transformada en este corte. |
| Filas D01/D03 ambiguas y 167 pruebas sobreinterpretadas | Los cortes estables y receipts ya distinguen entregas. Las 167 históricas no se presentan como aceptación de las pruebas nuevas. |
| Hashes r8.0.0 sin verificar por el autor | Descarga actual: coinciden las 14 fuentes iniciales y parser/pipeline executor adicionales. No implica verificar código de todas las versiones nativas mediante esa sola etiqueta. |

## Ejecuciones nuevas

- 69 checks de propiedades/fronteras pasan.
- 345 checks de replay Binary/DBRef/type pasan sobre capturas existentes. No
  se recapturan esos corpora ni se ejecuta una suite global.
- Las sondas de representación registran seis pares Memory/SQLite × 7/8/9,
  diferenciando documento y operando reales. Las clases observadas no son una
  nueva garantía pública.
- Cinco casos de efectos × tres referencias nativas y seis pares locales:
  45 observaciones, todas con barrera alcanzada. Las bases UUID se borran en
  finally. Los replica sets ya existentes de la tarea se conservan.

Las referencias son MongoDB 7.0.43/8.0.32/9.0.2, FCV 7.0/8.0/9.0,
puertos 49973/49974/49975 con directConnection. Se usan los observadores BSON
del SDK, sin importar identidad/diff/clasificación productivos para el esperado.
La sonda no compara tokens/tiempos de cluster ni pretende cubrir esos contratos.

## Efectos discriminantes

1. Unset de array null: nativo modifica/publica aunque el BSON guardado no
   cambie; local no. La igualdad física no puede decidir todos los efectos.
2. Rename a destino existente igual: el local mueve el destino al final;
   nativo conserva su posición. El contenido de evento puede coincidir y el
   BSON guardado seguir siendo incorrecto.
3. Merge int→double: ambas rutas locales usan igualdad semántica, omiten la
   escritura física y el evento. Comparar solo ambos motores daría un falso verde.
4. Pipeline que omite id, documento pequeño/grande: el trabajo local actual
   coincide en ambos casos; demuestra la necesidad de conservar imagen calculada
   e imagen almacenada, sin aceptar todo D01b ni fijar un umbral por padding.

La sonda es caracterización, no otro runner permanente. Los casos pendientes
deben incorporarse al registro/capturador/replay existentes al corregirlos.
No se acepta entrega transaccional, todos los modos merge, watch transformado,
wire watch (no soportado), reanudación ni costes con este diagnóstico.

La primera ejecución wire falló por PermissionError al abrir sockets en el
sandbox; se conserva como intento ambiental, no regresión funcional. La
repetición con sockets habilitados pasa. La primera descarga falló por DNS
del sandbox; la repetición habilitada verifica 16/16 hashes. No hubo rechazo
de revisión automática de permisos.

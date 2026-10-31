# Promoción, overflow e identidad de escrituras

Alcance del octavo corte de B03: H27 y consumidores numéricos compartidos de
H26/H05/H35. El [receipt](overflow-validation.json) posee fuentes, referencias
y resultados. B03 completo, B00, B05, B08 y B10 siguen abiertos.

## Reglas y owners

- `bson_scalars._bson_arithmetic` comparte la operación escalar. Las expresiones
  de agregación permiten promoción int64 → double; `$inc`/`$mul` mantienen el
  rechazo estricto. Promover convierte los operandos antes de operar: convertir
  el resultado entero exacto produce otro redondeo en `Long.MAX + 1025`.
- La suma/producto variádicos conservan su propia transición y ancho final. La
  división no decimal devuelve double aunque el cociente sea entero. Las
  funciones no decimales exp/sqrt/ln/log10 tampoco heredan el ancho int64 del
  operando cuando su resultado coincide con un entero.
- La conversión aritmética double → Decimal128 conserva quince cifras
  significativas, incluidos ceros finales; no usa la conversión exacta de
  comparación ni la conversión de acumulación. La implementación nativa está
  en el constructor de double de
  [Decimal128 en MongoDB 8.0.32](https://github.com/mongodb/mongo/blob/r8.0.32/src/mongo/platform/decimal128.cpp).
  Los testigos con referencias a campos verifican cohortes y orden de operandos,
  evitando depender únicamente del plegado de constantes nativo.
- El resto entero conserva su cálculo exacto. MongoDB 7 convierte un divisor
  double integral dentro de int64 cuando el dividendo es entero; 8/9 usan el
  recorrido double. El resto double usa `math.fmod`: conserva cero con signo,
  evita formar un cociente que puede desbordarse y acepta divisor infinito con
  dividendo finito. Dividendo infinito o NaN y divisor NaN producen NaN.

## Contextos de identidad

Asignación clásica y pipeline comparan identidad BSON en
`paths.same_bson_update_value`: tipo/ancho numérico, bits de double, cohorte
decimal, orden de claves y elementos. Ownership interno no cambia el tipo BSON.
Los replacements Memory/SQLite comparten `identity.replacement_document_changed`.
Debe conservarse el efecto nativo de añadir implícitamente `_id`: un replacement
sin `_id` puede contar como modificado aunque el documento materializado parezca
idéntico. El testigo incluye esa excepción, además de cambios de ancho/cohorte.

Los modificadores aritméticos tienen otra regla de no-op: igualdad aritmética
del mismo tipo BSON conserva el valor anterior, incluido cero con signo y
cohorte; NaN sí cuenta como escritura. Al crear un campo con `$mul` se calcula
`0 * factor`; reemplazarlo por un cero del mismo tipo perdería NaN/infinito y
la cohorte decimal. No se usa esta regla para asignación ni matching.

## Errores y efectos

El adaptador de escritura convierte únicamente `BsonScalarOverflowError` en
error de servidor código 2; no captura indiscriminadamente errores del evaluador.
La presentación del `_id` reutiliza el renderer de diagnóstico BSON extraído
de duplicate keys, sin otra copia. Los prefijos dependen de operación y versión:
findAndModify los añade en 7/8/9; update los añade en 9. Se conserva el orden de
campos index/code/errmsg del elemento de escritura sin cambiar la precedencia
de detalles frente a campos explícitos ni la copia defensiva.

Los testigos observan fallo y recuperación, retorno, contadores, payload y estado
posterior. El documento que falla conserva atomicidad incluso si un update de
array ya había calculado cambios en otro elemento. Update-many y bulk conservan
los prefijos válidos y la diferencia ordered/unordered. No se afirma atomicidad
del lote completo ni paridad de escrituras concurrentes nuevas.

## Observador independiente y límites

La comparación diferencial deja de usar `core.identity.canonical_document_id`.
`tests.support_bson_observation.bson_comparison_value` usa el encoder BSON de PyMongo
para escalares y recorre contenedores preservando orden. Distingue ancho,
cohortes, tipo público y cero con signo; las fechas conservan precisión y
awareness de salida, que recodificar a BSON borraría. No convierte objetos
desconocidos a repr. Conserva
la equivalencia list/tuple del transporte y la normalización previa de
payload/signo de double NaN; su tipo sigue siendo observable. Los casos de
escritura mantienen también su observación canónica y efectos explícitos.

Esta separación expuso el cero positivo incorrecto de `$mod` que el comparador
anterior aceptaba en lectura y pipeline-write. Las capturas anteriores ya
contenían el cero negativo; no se relajó ni sustituyó su expectativa para
resolverlo. Se añade un testigo con diecisiete bordes de resto double/int64.
Los formatos históricos de capturas permanecen; las nuevas observaciones de
escritura se decodifican una vez desde strings JSON dentro del envelope BSON,
evitando confundir tags Extended JSON literales con números del envelope.

B00 todavía debe completar H → caso → frontera, procedencia de helpers en el
soporte de captura y controles de las demás familias. La independencia focal
del comparador no acredita por sí sola cobertura completa de H34.

El helper de comparación se aloja en soporte independiente: las capturas antiguas
fijan el hash completo de `support_write_parity.py`. Ese módulo se conserva sin
cambios y el nuevo comparador no se introduce como dependencia de su observación
histórica. No se recapturan familias no modificadas para añadir una función que
no interviene en ellas.

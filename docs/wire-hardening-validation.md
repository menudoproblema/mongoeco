# Validación de sobres wire y diagnóstico

El alcance aprobado comprende tres bloques compatibles: validación del sobre y
codec compartido; contexto único para ejecución y diagnóstico; identidad estable
del observador. Conserva API pública, Engine SPI v2 y el formato SQLite.

## Implementación

- `_framing.py` valida flags requeridos, admite flags opcionales desconocidos y
  calcula CRC32C. Proxy y driver comparten codec y validación del tamaño.
- El proxy conoce la disposición de respuesta antes de decodificar BSON y valida
  la longitud antes de esperar el cuerpo. Conserva los códigos de parseo de
  MongoDB 7/8 y el cierre de conexión observado en MongoDB 9.
- `WireRequestState` mantiene fases y disposición por petición. El contexto
  normalizado alimenta ejecución y captura una sola vez, también en OP_QUERY.
  Se distinguen preparación, ejecución, codificación y fallos del envío.
- El diagnóstico reconoce `writeConcernError`, mantiene una copia acotada del
  primer error y conserva solo metadatos seguros. El informe pytest sigue
  leyendo snapshots después del teardown, sin abrir recursos.
- El observador fija fuentes antes de importar los helpers, verifica que no
  cambian e identifica el inventario de casos. Una regresión de subprocess
  reproduce deriva después de importar y exige rechazar el recibo.

## Corpus y evidencia

Los oráculos nativos independientes fijan MongoDB 7.0.43, 8.0.32 y 9.0.2 y sus
FCV correspondientes. Incluyen 16 lecturas ordinarias/espejo y 14 sobres con
flags opcionales/requeridos, checksum válido/incorrecto/ausente/truncado,
secciones inválidas, tamaños fuera de rango y namespace legacy no admitido. La comparación conserva códigos,
nombres de código, presencia de `errmsg`/`$err`, flag legacy de error, contadores,
efectos y ping posterior
o cierre. Los sobres se comprueban con clientes de socket sync/async,
Memory/SQLite y captura activada/desactivada.

Las regresiones PyMongo reales cubren errores 50 y 64, escritura rechazada o
aplicada según el error y recuperación posterior. El caso 64 local introduce una
respuesta de write concern controlada después de la escritura; no acredita una
réplica ni añade esa modalidad al failpoint local. La sonda nativa usa
`failCommand` con comandos de prueba habilitados y verifica una sola conexión
física en sync/async.

El [recibo del candidato](evidence/wire-hardening/validation.json) distingue los
checks focales, los intentos fallidos conservados y la prueba global integrada.
Las cifras finales corresponden al wheel instalado y sus fuentes congeladas. La evidencia de la incorporación inicial de diagnóstico se
conserva como antecedente; no sustituye la comprobación del candidato ampliado.

## Cierre del candidato

Wheel SHA-256:
`bf3d6b778b600a9b12913693eb98af4ff1f82b84c9e34f050a98533a2f2b2bab`.
Las dos construcciones son reproducibles y sus 293 archivos de runtime coinciden
con las fuentes y los entornos instalados. Los 31 inputs de este bloque y los
791 inputs integrados permanecen sin cambios al terminar.

| Comprobación | Resultado |
| --- | --- |
| Focales de wire, diagnóstico, pytest/xdist y contratos | 669 pruebas y 82 subtests correctos |
| Matriz nativa incluida en los focales | 432 comparaciones correctas |
| Gate con PyMongo 4.9.2 del wheel instalado | 1.033 pruebas correctas |
| Unittest completo | 3.628 pruebas, tres omitidas, sin fallos |
| Global Python 3.13, 3.14 y 3.15 | 45.203 pruebas y 14.463 subtests correctos por versión; 31 omisiones |
| Cobertura global Python 3.14 | 99,037312 %, mínimo 99 % |

También pasan el manifiesto de API pública, tipado, lint, canario, conformance
Memory/SQLite y smokes de wheel, sdist y extra pytest. Los baselines de errores y
los contratos protegidos conservan su identidad. API pública, SPI v2 y archivos
SQLite existentes quedan cubiertos por los gates integrados.

La primera matriz global completa encontró únicamente el traslado de emisores
sin código en el inventario estático. Se conservan esos tres resultados rojos y
su cobertura. La corrección restituyó los dos emisores de opcode a su owner
original y fijó el rechazo legacy con el resultado nativo; no amplió el baseline.
La matriz completa se repitió sobre el wheel posterior y terminó en verde.
La fixture de cierre se adaptó a la ejecución preparada manteniendo su respuesta
de 8 MiB y acotando la espera de su señal de entrada.

El [recibo integrado](evidence/engine-reads/validation.json) conserva comandos,
entornos, hashes de logs, gates, compatibilidad SQLite y los intentos anteriores.
El diff final se revisó y la documentación se alineó con el comportamiento probado.
La sonda nativa de errores 50/64 se ejecutó en procesos privados aislados; su fuente
se conserva como evidencia. Los cambios concurrentes se mantuvieron y formaron
parte del candidato global, sin mezclar sus objetivos con este bloque.

## Límites

La matriz acredita este corpus finito. No implica compatibilidad universal del
protocolo, replicación ni agotamiento real de un presupuesto. Un error 50
controlado comprueba manejo, efectos y recuperación; atribuir un timeout real
requiere observar esa ejecución y sus condiciones.

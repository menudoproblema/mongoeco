# Release 4.10.1

Fecha de preparación: **16 de octubre de 2026**. Estado: **candidato pendiente
de CI y publicación**. Este documento consolida el cierre de versión; el
[changelog](../CHANGELOG.md#4101---2026-10-16) resume los cambios.

## Versión y alcance

4.10.1 es una patch de Semantic Versioning. Corrige la composición de errores,
la validación y la conservación BSON en el subset ya soportado. Mantiene las
firmas públicas, Engine SPI v2, Python >=3.13, los perfiles de PyMongo, los
defaults MongoDB 7.0/PyMongo 4.9 y el formato de archivos SQLite.

- `findAndModify` devuelve errores completos: `ok`, `errmsg`, `code` y
  `codeName` cuando corresponde. Los duplicados conservan código 11000,
  patrón del índice y clave rechazada, para que el llamador pueda traducir el
  error y ejecutar su recuperación.
- Los errores de comando y por elemento conservan su familia, códigos y
  envoltorio. Los lotes retienen el primer error de comando, contadores y
  éxitos parciales permitidos por MongoDB, incluidos los lotes no ordenados.
- La preparación compartida valida proyecciones antes de mutar, gramática de
  expresiones y comandos BSON, flags, bindings y filtros posicionales. Las
  expresiones dinámicas se evalúan en la fase observada en el servidor,
  incluidos programas vacíos y operaciones sin coincidencias.
- Se corrigen los operadores aritméticos, null/missing, extremos numéricos,
  regex, proyección, unset, replacement, schema, índices y filtros lógicos
  ejercitados por el corpus. Los cursores síncronos conservan su limpieza
  también cuando falla la preparación o el consumo.
- Se conservan subtipos Binary, scopes JavaScript y metadatos BSON de duplicado.
  Las escrituras clásicas de SQLite evitan que JSON1 convierta NaN en null,
  también fuera del campo modificado; los archivos existentes se pueden reabrir
  sin migración.
- Se incluye el aviso ICU del recurso de diagnóstico de collation. La licencia
  de la distribución se declara `Apache-2.0 AND ICU`.

## Migración y límites

Actualiza a `mongoeco==4.10.1` o `mongoeco[pytest]==4.10.1`, según tu instalación.
No cambian los imports, las fixtures ni la configuración del cliente. No se
requiere migrar archivos SQLite.

Las pruebas que dependían de aceptar operaciones inválidas, de un código
incorrecto o de un diagnóstico incompleto deben ajustar sus expectativas al
comportamiento del servidor. La validación de forma puede rechazar un comando
antes de escribir; un fallo por documento puede conservar efectos parciales.

El corpus es finito. No acredita igualdad literal de todos los mensajes,
todos los keywords de JSON Schema, todos los operadores ni todas las
topologías. No añade replicación, sharding ni transacciones distribuidas.
Los límites vigentes pertenecen a [COMPATIBILITY.md](../COMPATIBILITY.md).

## Evidencia de implementación

Los informes conservan cada etapa, sus oráculos y los intentos rojos corregidos:

- [findAndModify](find-modify-errors.md).
- [Errores compartidos](shared-error-documents.md) y
  [rutas espejo](shared-error-mirrors.md).
- [Refactorización de preparación y transporte](error-contract-refactor.md).
- [Composición de expresiones y comandos BSON](error-composition-regressions.md),
  con el [registro local final](evidence/error-composition/validation.json).

| Gate local anterior al cambio de versión | Resultado |
| --- | --- |
| Suite completa del wheel, Python 3.13/3.14/3.15 | 43.982 passed, 31 skips previstos y 14.459 subtests por intérprete |
| Unittest por Python | 3.628 tests, 3 skips previstos |
| Cobertura instalada en Python 3.14 | 99,01 %, floor 99,00 % intacto |
| Corpus de fases/composición | 1.072 escenarios por dialecto y superficie; 12 celdas directas y 12 wire |
| Referencias nativas | MongoDB 7.0.43 / 8.0.32 / 9.0.2, FCV correspondiente; PyMongo 4.9.2 / 4.19.0, sync/async |
| Errores y efectos | Códigos, árboles estructurales, contadores, estado e índices posteriores y escritura de recuperación |
| Conexiones | Una conexión física reutilizada por pool en el corpus nativo y proxy |
| SQLite existente | Archivos, claves físicas y esquema conservados, incluidas 60 celdas de regresión no finita |
| Contratos y packaging | Manifiesto API, typing, SPI v2, imports mínimos, perfiles SDK, deep, snapshots, lint y smokes limpios aprobados |

La evidencia local usa el número provisional 4.10.0 y hashes distintos de la
4.10.0 publicada. Identifica el workset
`8a868e138af19784758f5d9735b43816687e824dd3a1a7616c80991a69c151a5`.
Se conserva como antecedente: la CI construirá, reproducirá y probará los
artefactos inmutables 4.10.1 del commit final. Los informes históricos no se
reescriben como ejecuciones de la versión nueva.

## Gates de publicación

Pendientes: CI del commit de main, referencia nativa 7/8/9, CI de la etiqueta,
Trusted Publishing y verificación de wheel/sdist descargados de PyPI.
Solo se creará `v4.10.1` si la CI del candidato termina en verde. El workflow de
la etiqueta vuelve a exigir la matriz Python, kit pytest y referencias nativas
antes de publicar las distribuciones probadas, sin reconstruirlas.

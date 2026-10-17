# Release 4.11.0

Fecha de cierre: **17 de octubre de 2026**. Estado: **preparada para CI**.
La etiqueta prevista es **v4.11.0**. Este documento no acredita por sí solo una
publicación; el [changelog](../CHANGELOG.md#4110---2026-10-17) resume el alcance.

## Versión y alcance

4.11.0 es una versión menor de Semantic Versioning: añade diagnóstico opcional
para pruebas, corrige el protocolo y simplifica la preparación y lectura
internas. Conserva los usos existentes de la API, Engine SPI v2 y los archivos
SQLite. Mantiene Python >=3.13, soporte de 3.13 hasta octubre de 2028, los
perfiles PyMongo y los defaults MongoDB 7.0/PyMongo 4.9.

- `--mongoeco-wire-diagnostics` captura metadatos acotados y muestra los fallos
  después del teardown, incluidos xdist y los adaptadores async. El proxy ofrece
  snapshots separados de su estado interno. La captura está desactivada por
  defecto; no conserva BSON, credenciales ni URIs ni abre recursos al informar.
- Proxy y driver comparten validación del tamaño, flags OP_MSG y CRC32C. El
  proxy conoce la política de respuesta antes de decodificar BSON y conserva
  la supresión de respuestas, códigos y cierres observados en MongoDB 7/8/9.
- Ejecución y diagnóstico usan el mismo contexto preparado. El registro
  distingue preparación, ejecución, codificación y envío, reconoce errores de
  write concern y conserva el primer fallo aunque el buffer rote.
- Los filtros wire `find` de tipo incorrecto, incluido null explícito, devuelven
  código 14. Las regex malformadas se rechazan con código 51091, también en
  colecciones vacías, consultas sin coincidencias y filtros de pertenencia.
- Las operaciones y lecturas internas comparten preparación y apertura de
  fuentes tipadas. Se eliminan wrappers SQLite privados sin uso y compilaciones
  repetidas, conservando fallback, snapshots, ventanas, deadlines y atomicidad.
- El canario independiente SPI v2 y sus comprobaciones de atomicidad verifican
  identidad y selector, imágenes, contadores, efectos y escrituras competidoras.
  Se mantiene v2; v3 queda aparcada, sin migración ni anuncio de una versión 5.
- Los observadores nativos rechazan recibos si sus fuentes cambian durante la
  captura. El corpus añade lecturas ordinarias/espejo y sobres wire con
  comparación de errores, efectos y reutilización de la conexión.

## Migración y uso

Al publicarse, actualiza a `mongoeco==4.11.0` o
`mongoeco[pytest]==4.11.0`. Los clientes, fixtures y adaptadores existentes
mantienen su configuración. No hace falta migrar archivos SQLite ni motores
SPI v2.

Para activar la nueva captura con el plugin ya cargado:

```sh
pytest --mongoeco-wire-diagnostics --mongoeco-backend=both
```

La [guía pytest](pytest.md) y la [guía de diagnóstico](wire-diagnostics.md)
describen sync/async, workers y los metadatos retenidos. Las pruebas que dependan
de filtros inválidos aceptados o códigos incorrectos deben ajustar sus
expectativas al comportamiento nativo corregido.

## Evidencia local y validación de entrega

Las ejecuciones locales completas corresponden al candidato previo al cambio
de versión, cuyo wheel de desarrollo conservaba el número **4.10.1**. Su hash es
`bf3d6b778b600a9b12913693eb98af4ff1f82b84c9e34f050a98533a2f2b2bab`.
No es la distribución 4.10.1 publicada ni acredita un artefacto 4.11.0.
Los recibos históricos permanecen intactos.

| Validación local de implementación | Resultado |
| --- | --- |
| Global del wheel, Python 3.13/3.14/3.15 | 45.203 pruebas y 14.463 subtests correctos por versión; 31 omisiones |
| Cobertura Python 3.14 | 99,037312 %, mínimo 99 % intacto |
| Unittest completo | 3.628 pruebas, tres omitidas, sin fallos |
| Comparaciones wire nativas | 432, con MongoDB 7.0.43 / 8.0.32 / 9.0.2 |
| Compatibilidad | Sync/async, Memory/SQLite, API, SPI v2 y archivos SQLite 4.5/4.10.1 |

- [Preparación de operaciones](engine-preparation-validation.md).
- [Límites tipados y atomicidad SPI v2](engine-boundary-validation.md).
- [Lecturas y regex](engine-read-validation.md).
- [Wire y diagnóstico](wire-hardening-validation.md).
- [Recibo integrado](evidence/engine-reads/validation.json) y
  [recibo wire](evidence/wire-hardening/validation.json).
- [Preparación de esta entrega](evidence/mongoeco-4.11.0/preparation.json).

La CI de la entrega construye y reproduce artefactos **4.11.0**, prueba el
wheel instalado en los tres Python y ejecuta la matriz pytest/PyMongo. Antes
de etiquetar deben pasar CI de main y el diferencial previo. La CI de etiqueta
exige de nuevo el corpus MongoDB 7/8/9 y publica mediante Trusted Publishing
los mismos artefactos que ha probado, sin reconstruirlos en el publisher.

## Límites

El corpus es finito: no acredita compatibilidad universal, igualdad literal de
todos los mensajes, replicación, sharding ni transacciones distribuidas. El
error 64 local es una respuesta controlada posterior a la escritura; no simula
una réplica. La receta de error 50 comprueba manejo y recuperación, sin
atribuir un agotamiento real del presupuesto. Los límites generales permanecen
en [COMPATIBILITY.md](../COMPATIBILITY.md).

# Regresiones de composición y comandos BSON

## Alcance

El encargo corrige las doce familias de la revisión posterior a la refactorización.
Reutiliza preparación, diagnóstico tipado, acumuladores, observador diferencial y
operaciones de motor existentes. Las firmas públicas, SPI v2 y el formato SQLite se
conservan. No incluye publicación ni cambios en otros repositorios.

| Familia | Corrección | Regresión principal |
| --- | --- | --- |
| R01 | Gramática recursiva contrastada con el registro del dialecto y extensiones; documentos de expresión con un único operador. Literales opacos y conservación de meta/operadores locales declarados. | Operadores desconocidos, varios operadores y cuerpos de map/filter vacíos, con y sin match. |
| R02 | Preparación de constantes anidadas, incluidas ramas condicionales, en los contextos nativos de optimización. | sqrt/divide anidados, concat y convert, consulta/agregación frente a update sin match. |
| R03 | Gramática de campos de comando y entrada antes de publicar efectos. | Campos desconocidos falsy y última entrada inválida tras una entrada válida. |
| R04 | Conversión BSON por campo separada de argumentos cliente Python. | Flags null, booleanos y números; controles de tipos rechazados y escalares BSON. |
| R05 | Presencia de arrayFilters:null conservada hasta comprobar el dialecto. | Código 10065 en MongoDB 7 y aceptación ejercitada en 8/9. |
| R06 | Validación de pipeline/arrayFilters en su contexto de operación. | Update acepta []; findAndModify lo rechaza con 9; filtros no vacíos conservan 9. |
| R07 | El acumulador retiene el primer fallo de comando y sigue registrando los errores por elemento y efectos permitidos. | Dos errores diferentes, en ambos órdenes, raw/bulk, ordered/unordered, prefijos y sufijos. |
| R08 | Diagnósticos completos de project/unset/replaceRoot/replaceWith. | Códigos de proyección según orden, formas de unset y error dinámico de replacement; lotes parciales. |
| R09 | size exige array también para null y campos ausentes. | Código 17124, ausencia de escritura inválida, consulta/agregación y actualizaciones. |
| R10 | round y trunc propagan null/missing en place y validan su tipo/rango antes de devolver Infinity/NaN. | Lecturas/escrituras, place nulo o ausente, extremos válidos y códigos 16004/51083. |
| R11 | Los parsers BSON producen errores de servidor, sin filtrar TypeError al consumidor. | let, bypassDocumentValidation y formas relacionadas; códigos 14 y reutilización wire. |
| R12 | Cada propiedad fallida conserva title/description del nodo de schema. | Metadatos individuales, combinados y anidados en errores 121. |

## Corpus y aceptación

`tests/differential/error_composition_cases.py` añade 583 escenarios a los 489
anteriores: **1.072 por dialecto y superficie**. Cada escenario conserva resultado
o error, código, árbol estructural, contadores, documentos e índices posteriores,
y una escritura de recuperación. Se contrastan MongoDB 7/8/9, clientes sync/async,
Memory/SQLite y acceso directo/wire. El proxy y los oráculos nativos deben
reutilizar exactamente una conexión física por pool. Las referencias nativas se
capturan en procesos que comprueban que no han importado Mongoeco.

Se conservan las normalizaciones explícitas del observador: redacción de errores,
módulo de excepción e índice único implícito. No se eliminan códigos, familias,
contadores, BSON de aplicación ni efectos para lograr igualdad. No se promete
igualdad literal de todos los mensajes ni compatibilidad universal. El árbol de
otros keywords JSON Schema, como minimum, sigue fuera de esta acreditación.
Los controles exploratorios cuyo seed incumplía el propio schema no se promueven
como evidencia de recuperación válida.

Además del corpus, 72 sondas nativas (tres versiones y clientes sync/async)
comprueban que `constants` no es un alias de `let`: conserva 40415, no modifica
documentos y permite una escritura posterior. Hay regresiones locales para ese
campo en cada comando. El inventario de productores legacy sin código se reduce
de 904 a 893; esta cifra mide construcciones pendientes, no bugs independientes.

Los controles válidos de NaN cubren también la escritura posterior de
recuperación. SQLite JSON1 puede convertir ese valor en null incluso fuera de
la ruta modificada: la traducción SQL deriva esos documentos y operandos a la
escritura Python existente. Se añaden 60 comprobaciones de archivos existentes y
reapertura, con valores raw/tagged, arrays, scopes de Code e identificadores DBRef,
en ambos clientes, tres dialectos y dos perfiles. Se conservan esquema y claves
físicas; no hay migración de formato.

Las notas del catálogo sitúan ahora let en el comando. Se conservan sus claves
y estados; la diferencia estructural del catálogo consta únicamente de esas dos
notas. Las pruebas de conservación verifican bindings válidos al nivel del
comando y rechazo 40415 de bindings por entrada, con documentos sin cambios.
Otras 24 sondas nativas confirman ese rechazo en update/delete, sync/async y 7/8/9.
Los inventarios de procedencia se enlazan a los hashes actuales sin cambiar su
conjunto de campos ni sus garantías.

## Validación

La última ejecución global del wheel instalado pasa en Python 3.13, 3.14 y
3.15: **43.982 pruebas aprobadas por versión**, con 31 skips previstos y
14.459 subtests aprobados. Unittest pasa 3.628 pruebas por versión, con tres
skips previstos. La cobertura instalada en 3.14 es **99.01 %**, con el umbral de
99,00 % intacto.

- Los 1.072 escenarios se contrastan en las doce celdas directas y doce wire;
  ambos clientes y motores conservan errores, contadores, efectos y recuperación.
- PyMongo 4.9.2 pasa 171 pruebas adicionales, incluidas las doce celdas wire con
  el corpus completo; sus únicas diferencias nativas respecto a 4.19 siguen
  siendo los dos payloads de multi-update con upsert:false.
- Manifiesto API, tipado público y canario independiente SPI v2 pasan en los
  tres Python. Las pruebas de archivos existentes conservan esquema y claves.
- Imports mínimos sin PyMongo, perfil profundo, instalaciones limpias,
  snapshots de compatibilidad revisados y ratchet de lint pasan.
- Wheel y sdist reproducibles, validados por twine; los 289 archivos de runtime
  coinciden con las fuentes y con las cuatro instalaciones comprobadas.
- Todas las referencias MongoDB privadas finalizaron y se eliminaron sus bases.
  Se revisaron el diff final, la documentación y la ausencia de identidades de
  consumidores; los cambios anteriores del workset se conservaron.

El [registro de validación](evidence/error-composition/validation.json) identifica
artefactos, comandos, resultados, hashes y los intentos rojos corregidos. Las
[referencias nativas](evidence/error-composition/native-references.json) fijan
versiones, FCV, SDK y corpus. Los workflows quedan preparados para el siguiente
push; esta evidencia corresponde a los gates locales.

# Migración a Mongoeco 5.0

Estado: candidato en preparación, no publicado. Comparación con 4.11.0.

## UUID: elegir representación explícita

CodecOptions usa UNSPECIFIED por defecto, como PyMongo. Un UUID Python sin
representación elegida se rechaza; el fallo de una lectura diferida puede aparecer
al consumir el cursor. Selecciona STANDARD para el comportamiento habitual:

```python
from mongoeco import MongoClient
from mongoeco.types import CodecOptions, UuidRepresentation

client = MongoClient(codec_options=CodecOptions(
    uuid_representation=UuidRepresentation.STANDARD,
))
```

Usa la representación legacy correspondiente si necesitas ese formato. Los
subtipos BSON almacenados no se reescriben al leer o reabrir SQLite.

## Consultas y expresiones

$exists, $elemMatch, $all y $nin conservan su función de consulta, pero dejan
de aceptarse como expresiones de agregación dentro de $expr. Separa el
predicado de consulta de la expresión o usa una expresión válida equivalente;
no hay una reescritura universal. $not y $last sí tienen soporte de expresión.
Las comparaciones MQL respetan type bracketing; la agregación conserva su orden
BSON. Ausencia y null ya no se confunden en expresiones. Ajusta expectativas
que dependiesen de estas divergencias del mock y comprueba los casos de aplicación
contra MongoDB real.

## Frontera BSON, resultados y errores

Las entradas pasan por admisión BSON y los resultados por los codecs efectivos.
No dependas de aceptar object(), Decimal Python crudo u otros valores no BSON;
convierte decimales a Decimal128. Se preservan anchuras Int64, subtipos Binary,
cohortes Decimal128 y efectos de decoders/document factories. Los contadores
de actualización distinguen identidad BSON de igualdad de consulta.

La validación puede fallar antes de leer filas o producir efectos. Las operaciones
por lote conservan los prefijos y errores parciales de su contrato. No reintentes
una escritura por un cambio de excepción sin comprobar qué efectos se confirmaron.
Los resultados de filtros, enum/uniqueItems, fechas, índices y joins pueden cambiar
porque se han corregido comparaciones y contextos compartidos.

## CXP: adoptar catálogo 2.0.0

CXP permanece en 5.0.0. La identidad org.mongoeco:mongodb pasa a 2.0.0 con hash
nuevo; adopta coherentemente catálogo, perfiles, tiers, snapshot y contexto.
No cambies solo el número ni conserves el hash anterior. Las referencias
históricas 1.2.0 no se reinterpretan. Véase [CXP exchange](cxp-exchange.md).

## Nuevas capacidades opcionales

El backend Python regex sigue por defecto. Para PCRE2 instala el extra pcre2
y fija MONGOECO_REGEX_BACKEND=pcre2 antes de importar mongoeco; cambia de proceso
para cambiar la selección. Una dependencia ausente o selección inválida no usa
fallback. Search-v1 y Regex.compile() siguen Python. Soporte PCRE2 acotado:
macOS arm64 comprobado; Python 3.15 requiere el build desde fuente verificado.
No se promete paridad PCRE universal ni disponibilidad binaria multiplataforma.

$bucketAuto admite trece series de granularity, con sus restricciones numéricas
y errores nativos; no equivale a redondear etiquetas después de repartir filas.

## Contratos conservados

Se conserva Engine SPI v2, Python >=3.13 (3.13 hasta octubre de 2028), las
fachadas sync/async, los perfiles PyMongo y la lectura de archivos SQLite previos.
No hay una migración obligatoria del formato de archivo ni transacciones
distribuidas nuevas. La major del paquete no cambia la versión del SPI.

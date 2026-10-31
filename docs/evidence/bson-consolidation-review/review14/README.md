# Relectura de la autocrítica sobre el candidato de respuestas

El [receipt](receipt.json) fija HEAD, hash del adjunto y archivos preexistentes.
El input coincide con las revisiones 3–13. Este corte modifica plan y evidencia;
no implementa correcciones ni acepta el candidato de respuestas en curso.

Se repiten 78 pruebas focales (Binary, gramática auxiliar, SQL, migración TTL y
contexto watch), todas verdes. No son las 167 pruebas históricas del informe ni
un cierre integrado. Las correcciones aceptadas mantienen su owner en el
[registro](../../../bson-consolidation-implementation.md).

La [sonda local](probe.py.txt), ejecutada en Memory y SQLite efímeros, observa:

- `_CursorPageSource.pull(1)` devuelve una fila pero consume cinco de su fuente.
  El iterator público rellena la tanda interna de 256. Reducir la página pedida
  por aggregate a uno no elimina esta prelectura compartida.
- Un pipeline con entrada de 1 KiB por fila, salida de 4 MiB y filtro que divide
  por cero en la quinta fila falla al abrir, sin entregar filas. El resultado
  esperado exacto por respuesta debe capturarse físicamente antes de corregirlo;
  esta sonda sola no acredita una nueva divergencia contra MongoDB.
- Para una String calculada de 16.777.216 bytes, `$concat` falla con 16493 y
  `$replaceAll` devuelve el valor, en el dialecto predeterminado. La cola null
  de `$concat` devuelve null. Es un testigo de la guarda aislada del candidato;
  no se ha repetido su matriz nativa en esta revisión.

Los [resultados](observations.json) solo registran tamaños, IDs y errores; no
persisten payloads grandes. La sonda usa instrumentación privada para explicar
el mecanismo; su forma no es un contrato público ni un runner nuevo de aceptación.
Los diferenciales permanentes deben incorporarse al registro existente.

Se consultan de nuevo dos fuentes oficiales r8.0.0:

- [Parser MQL](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp):
  el reconocimiento DBRef comprueba presencia de nombres y contexto; no replica
  las restricciones del constructor SDK.
- [Comparación y representación BSON](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/bson/bsonelement.cpp):
  comparación binaria y formato abreviado de diagnósticos. El renderer local ya
  tiene truncación String/Binary; no se ha demostrado que falte globalmente.

La verificación SHA-256 de las 16 fuentes pertenece al receipt anterior. Esta
consulta web no acredita otra descarga y verificación de todos esos hashes.
No se ejecuta MongoDB, no se recapturan referencias, no se ejecuta la suite
global ni se acepta D05. Los archivos históricos y observadores protegidos se
conservan. La revisión del runtime se limita a los mecanismos descritos, no al
diff completo del candidato.

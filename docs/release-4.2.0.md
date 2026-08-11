# Release 4.2.0

Status: release `4.2.0` cerrada; publicacion pendiente de los gates indicados
al final.

## Contrato PyMongo sin adaptadores de consumidor

`mongoeco 4.2.0` incorpora al runtime los comportamientos que antes obligaban
a los consumidores de test a mantener shims propios:

- `$min`, `$max`, `$sum` y `$avg` funcionan como expresiones escalares con la
  semantica MongoDB de arrays y listas de operandos;
- `bulk_write` acepta los seis modelos oficiales de PyMongo y conserva sus
  opciones soportadas;
- documentos y resultados exponen tipos BSON oficiales de PyMongo;
- los `datetime` se normalizan en la frontera BSON a UTC naive y precision de
  milisegundos;
- `aggregate()` fija `$$NOW` al crear el cursor;
- los cursores async soportan consumo incremental y cierre nativo.

La correccion mantiene separadas las representaciones publicas e internas. En
particular, una escritura selecciona y revalida `_id` sin convertirlo antes a
la clase publica de PyMongo, preservando la clave estable de Memory y SQLite.

## Compatibilidad

No hay pasos de migracion incompatibles. El cambio observable intencionado es
que los valores BSON publicos, incluidos IDs generados y upserted IDs, usan las
clases oficiales de `bson` en lugar de las clases internas de `mongoeco`.

Los consumidores que mantenian lowering de `$max`, emulacion de bulk, wrappers
de cursor o restauracion BSON deben eliminarlos y depender de `mongoeco>=4.2.0`.

## Validacion ejecutada

- `pytest`: 2997 tests, 2132 subtests y 15 skips;
- `unittest`: 3126 tests y 2 skips;
- PyMongo: matriz 4.9.2, 4.11.3, 4.13.2 y 4.17.0 sin delta de snapshot;
- consumidores: suites de `mochuelo-testkit` y runtimes Mongo/reloj de
  `cosecha-mochuelo`;
- catalogo de compatibilidad, compilacion, `diff --check`, wheel, sdist y
  smokes de instalacion limpia.

## Gates de publicacion pendientes

- el checklist historico exige cobertura global `>=99%`; la medicion real de
  esta candidata es `82%`, aunque toda la suite pasa;
- no se ejecutaron las suites diferenciales contra servidores MongoDB 7/8
  porque `MONGOECO_REAL_MONGODB_URI` no esta configurada;
- la dependencia y el lock del testkit solo podran resolverse contra
  `mongoeco>=4.2.0` cuando la distribucion exista en el indice configurado.

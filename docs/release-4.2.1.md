# Release 4.2.1

Status: release cerrada; publicacion pendiente.

## Correcciones de persistencia

`mongoeco 4.2.1` cierra dos diferencias observables de `4.2.0`:

- los `array_filters` de todas las operaciones de update atraviesan la misma
  frontera BSON que los documentos persistidos, incluidos los modelos de
  `bulk_write` y las superficies sync/async;
- las mutaciones con preseleccion vuelven a comprobar dentro del lock del
  engine la identidad elegida y el filtro original completo.

La normalizacion de `array_filters` es recursiva, convierte fechas aware a UTC
naive, trunca a precision BSON de milisegundos, se ejecuta una sola vez y no
muta el argumento recibido.

La revalidacion atomica evita que dos competidores que observaron la misma
preimagen superen simultaneamente un filtro CAS. El perdedor devuelve no-match,
no modifica el documento y no genera change event. La auditoria incluye
`update_one` con `sort` o `hint`, `update_many`, reemplazos y borrados en Memory
y SQLite.

## Compatibilidad y migracion

No hay cambios incompatibles ni pasos de migracion. Los consumidores deben
elevar su minimo a `mongoeco>=4.2.1` y eliminar cualquier normalizador local de
`array_filters` o mecanismo de serializacion que oculte carreras CAS.

## Validacion

- `pytest`: 3006 tests, 2152 subtests y 15 skips;
- `unittest`: 3135 tests y 2 skips;
- cobertura global: 99%;
- PyMongo: matriz 4.9.2, 4.11.3, 4.13.2 y 4.17.0 sin delta de
  snapshot;
- paridad Memory/SQLite y sync/async para las superficies corregidas;
- carrera determinista que obliga a dos operaciones a observar la misma
  preimagen y comprueba un unico ganador y un unico change event;
- reproducciones consumidoras de `PERSIST-008` y `PERSIST-009`;
- build de wheel/sdist, `twine check` y smoke tests de instalacion limpia.

Los resultados finales de matrices consumidoras y los hashes de artefactos se
registran tras publicar los bytes definitivos en PyPI.

Las suites diferenciales contra MongoDB real 7/8 no se ejecutaron porque
`MONGOECO_REAL_MONGODB_URI` no esta configurada. No bloquean este hotfix: no se
ha modificado la integracion de red ni la semantica delegada a un servidor real.

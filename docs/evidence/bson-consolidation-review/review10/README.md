# Revisión 10 de la autocrítica

Base inspeccionada: `6ff34d4`. El input es idéntico al de revisiones 3–9;
su SHA-256 se conserva en [receipt.json](receipt.json). No se copia el adjunto
privado al repositorio. Esta revisión cambia documentación y diagnósticos,
sin modificar runtime, tests, goldens ni aceptación previa.

- [Representaciones](representations.json): seis pares motor/dialecto,
  18 filas con documento y operando separados. Script reproducible:
  [representation-probe.py.txt](representation-probe.py.txt).
- [Fuentes verificadas](sources-check.json): 16 descargas desde las URLs
  oficiales fijadas; hashes coincidentes, incluidos parser y fuentes adicionales.
  [check-sources.py.txt](check-sources.py.txt) no modifica manifests anteriores.
- [Cursor](cursor.json): diagnóstico local del decoder e invalidate filtrado,
  producido por [cursor-probe.py.txt](cursor-probe.py.txt). No caracteriza bytes
  de tokens nativos ni fija códigos de error del servidor.
- [88 comprobaciones focales](focal.txt) y [408 de replay](replay.txt), con
  capturas existentes. No hubo ejecución física nueva en esta revisión.
- [Runtime identificado](runtime-sources.json): hashes del código inspeccionado;
  no confundirlos con hashes de las fuentes externas.

El plan conserva los owners existentes y elimina del orden activo la extracción
de entrega/DDL ya aceptada. El siguiente corte D04 reúne lectura, ciclo de vida
y checkpoint coherentes, con oráculos nativos y pruebas locales de intercalados.
Las retractaciones históricas no reabren Binary, fechas o SQL ya aceptados.

La lectura actual encuentra las autoridades en `core/bson_values.py`,
`engines/sqlite_query.py`, `core/query_operators.py`, `core/bson_raw.py`,
`api/_change_delivery.py` y `_change_streams/{cursor,hub,pipeline}.py`.
La selección de opciones del driver se contrastó además con PyMongo 4.19.0
instalado, `pymongo/change_stream_shared.py`, sin copiar su implementación.

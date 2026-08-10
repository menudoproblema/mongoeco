# Release 4.1.1

Status: preparada para publicacion desde la version etiquetada `4.1.1`.

## Indices locales fiables

`mongoeco 4.1.1` corrige la semantica de indices locales de Memory y SQLite
sin cambiar la API publica.

* Una igualdad sobre una ruta anidada sigue encontrando el documento correcto
  en `MemoryEngine` cuando existe un indice sobre esa ruta.
* Los indices `unique` respetan entradas multikey, rutas que atraviesan arrays,
  collations y patrones compuestos. Las entradas repetidas dentro del mismo
  documento son validas; las colisiones con otro documento fallan.
* Los patrones compuestos rechazan arrays paralelos desde mas de una ruta
  indexada, como hace MongoDB.
* SQLite mantiene los indices fisicos como aceleradores y valida la unicidad
  logica por base y coleccion, evitando restricciones entre namespaces
  distintos.

## Migracion

No hay pasos de migracion ni cambios incompatibles. Las aplicaciones que
dependian de los casos corregidos obtendran ahora el mismo resultado que con el
contrato MongoDB esperado.

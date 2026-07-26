# Release 4.0.0

Status: preparada para publicación desde la versión etiquetada `4.0.0`.

## Motivo del cambio de major

La versión 4.0 incorpora `$$NOW` como variable de sistema efectiva y corrige
varias diferencias observables respecto de MongoDB. El cambio incompatible
principal es que una variable de agregación no definida deja de resolverse en
silencio como `None`. SemVer exige un cambio de major porque aplicaciones que
dependieran de esa laxitud ahora recibirán un error.

## Breaking changes

### Las variables no definidas ahora fallan

Antes de 4.0, una referencia como `"$$cutoff"` sin un binding podía convertirse
en `None`. Desde 4.0 lanza `OperationFailure` con código `17276` (`Use of
undefined variable: cutoff`).

Antes:

```python
collection.find_one({"$expr": {"$lt": ["$due", "$$cutoff"]}})
# Podía evaluar $$cutoff como None.
```

Después, declare el valor en `let` cuando la operación lo soporte:

```python
collection.find_one(
    {"$expr": {"$lt": ["$due", "$$cutoff"]}},
    let={"cutoff": datetime(2026, 7, 1)},
)
```

`$$NOW` no necesita `let`: el runtime lo captura una vez por comando real y lo
reutiliza en filtros `$expr`, actualizaciones por pipeline, agregaciones y
subpipelines. `distinct` también captura `$$NOW`, pero no acepta `let` porque
el comando MongoDB `distinct` no lo admite.

### `$currentDate` usa precisión BSON

Las fechas de `$currentDate` ahora se truncan a milisegundos. Si la aplicación
comparaba microsegundos o serializaba la fecha antes de persistirla, actualice
esas aserciones para comparar el valor truncado a milisegundos. El tipo
`timestamp` conserva su contador por documento.

### `$$REMOVE.path` representa ausencia durante transformaciones de campo

En `$project`, `$addFields` y `$set`, `$$REMOVE.path` omite el campo calculado,
igual que una ruta ausente. Fuera de esas transformaciones se comporta como
`null`, de modo que no se filtra ningún centinela interno a resultados o a
acumuladores.

```python
collection.aggregate([
    {"$set": {"legacy": "$$REMOVE.old"}},
])
# 4.0: no añade "legacy" cuando la ruta es ausente.
```

## Compatibilidad y límites explícitos

La especificación técnica completa está en [COMPATIBILITY.md](../COMPATIBILITY.md),
incluida la independencia de `$$NOW` respecto al perfil PyMongo. `bulk_write`
comparte un `$$NOW` por lote lógico clásico (`insert`/`update`/`delete`, hasta
100.000 modelos); no pretende emular el comando `bulkWrite` moderno ni los
límites de tamaño BSON/mensaje.

## Lista de comprobación para actualizar

1. Busque referencias `$$` de variables de usuario y proporcione `let` o
   reemplace la expresión por el valor deseado.
2. Actualice expectativas que comparen microsegundos de `$currentDate`.
3. Revise transformaciones que dependieran de que `$$REMOVE.path` produjese
   `null` en vez de omitir un campo.
4. Ejecute la suite de integración contra los dialectos MongoDB 7.0 y 8.0 y
   los perfiles PyMongo soportados por su aplicación.

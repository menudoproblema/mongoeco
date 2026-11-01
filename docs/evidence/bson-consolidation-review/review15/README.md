# Contraste del informe y refinamiento vigente

1 de noviembre de 2026, HEAD `eb85ea3`. El adjunto es idéntico al contrastado en
revisiones 3–14 (SHA-256 en [receipt.json](receipt.json)). Esta revisión modifica
documentación/evidencia; conserva los 12 archivos preexistentes de runtime/tests
sin modificarlos ni aceptarlos. [workset.json](workset.json) identifica esos archivos.
El [plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
posee la única secuencia de ejecución vigente; este receipt no crea otra.

## Disposición de las críticas

| Cuestión | Contraste y disposición |
| --- | --- |
| H48, interno/local/SDK y distintos subtipos | La autocrítica corrige su acusación anterior. Vistas `binary_parts`/`binary_order_key`, clasificación y propiedades ya cubren los mecanismos distintos. Conservar resultado BSON y trazas de ambos lados del matcher; no añadir otra tabla ni reabrir D03a. |
| Parser DBRef y SQL bytes eq/in | Ya corregidos en cortes aceptados. El parser distingue primer campo/contexto y presencia de ref/id; no exige los tipos del decoder. SQL declina valores binarios y conserva fallback. Mantener controles de SQL ejecutado y del orden raw, no inferirlos de un helper. |
| TTL, un milisegundo «irrelevante» | Retirar la cota no demostrada y el juicio contractual. SQLite actual usa coordenadas enteras en auxiliares/cutoff y migración; el residual no recupera candidatos omitidos. D02 conserva su aceptación. |
| Distinct, eq/in, propiedad desde D00, IDs y 167 pruebas | Ya incorporados. Las 167 eran regresión histórica, nunca aceptación nueva. No crear otra propiedad/runner o bloques duplicados. |
| Watch y H53 | Clase/código/fase/contexto tienen aceptación D04 acotada. H53 sigue en C03/B08, sin prometer logging nuevo. |
| Fuentes r8.0.0 | Se vuelven a descargar las 16 URLs fijadas: coinciden 16 hashes. [sources-check.json](sources-check.json) conserva esperado/obtenido. Esto verifica identidad, no paridad 7/8/9. |

## Evidencia actual y causa compartida

Las 78 regresiones focales de Binary, gramática, SQL, migración TTL y contexto
watch pasan (1,98 s). El replay del corpus preexistente `bson_dispatch` da
**111 correctas y 36 fallos** (5,59 s): scalar/single-pass/DBRef-order, 12 fallos
wire por familia. Las 72 celdas directas y 36 wire de las otras tres familias
pasan, además de tres controles de metadata. No significa aceptación completa:
las seis acciones agrupan muchos testigos y no cubren todas las fronteras.
El log completo está en [dispatch.txt.gz](dispatch.txt.gz).

El codec reconoce tipo exacto en el candidato y su materialización SDK directa
reproduce los testigos nuevos. Wire aún construye cursor con id/ns antes del
batch, frente al batch/id/ns observado en las referencias: cambia el orden de
callbacks del cliente. La misma representación puede tener igualdad de valor y
efectos distintos. Compartir la construcción de la respuesta evita dos órdenes
implementados a mano; conservar un control diferencial de esos efectos.

La sonda [probe.py.txt](probe.py.txt) ejecuta bases locales efímeras:

- [Con SDK, Python 3.14](sdk-observations.json), se observan tres callbacks dict
  (dos documentos anidados y la fila) y dos callbacks list (array y batch).
- [Sin SDK, Python 3.13](sdk-free-observations.json), solo uno de cada clase.
  El [control con SDK en el mismo Python 3.13](sdk-python313-observations.json)
  recupera tres/dos. Este control añade las fuentes Python instaladas del SDK
  al path después de las dependencias 3.13; no es una celda oficial de distribución.
- El helper UUID convierte el valor canónico almacenado como Binary 4 a Binary 3
  en los tres modos legacy. La decodificación BSON SDK independiente conserva 4.
  El camino SDK nuevo puede ocultar que el helper portable continúa equivocado.
- Ambos motores aceptan UUID crudo con el default UNSPECIFIED del candidato;
  `BSON.encode` predeterminado lo rechaza con ValueError. Es frontera cliente de
  admisión, anterior al servidor. No requiere confundirlo con InvalidDocument.

Las unitarias de codec/materialización/configuración dan 101 correctas, 21
subtests y un fallo por el cambio del default UUID ([codecs.txt.gz](codecs.txt.gz)).
La unitaria legacy UUID actual pasa aun exigiendo subtipo 3: demuestra por qué
una expectativa local verde necesita un oráculo externo.

## Refinamiento adoptado

La causa transversal sigue siendo decidir después de perder representación,
contexto u orden. Se reutilizan las vistas BSON y los owners de respuesta,
materialización y planes; cada uno conserva su contrato. D06a/b precisa ahora:

1. Envoltura compartida directa/wire, orden/tipos y efectos de callbacks por
   respuesta completa; root/row/batch/Code.scope tienen contextos distintos.
2. Dispatch exacto y de pase único en rutas SDK y portable, con errores/cierre.
3. Corte UUID coherente entre lectura, escritura y default público, coordinado
   con B07; el default puede esperar mientras se entrega dispatch.
4. Opciones equivalentes de hint/collation bajo el owner existente, con controles
   de rutas bloqueantes, residual y fase. No otro planificador ni caché.
5. Corpus final congelado y baseline/candidato, replay/wheel/matrices alcanzadas
   y coste también bajo opciones explícitas; conservar D06c/H53/sNaN/D07/D05/B07.

Las referencias 7.0.43/8.0.32/9.0.2 y sus checks temporales ya existían antes de
esta revisión; sus seis acciones coinciden por versión
([preexisting-native-checks.json](preexisting-native-checks.json)). Aquí no se
ejecuta MongoDB ni se recapturan referencias. No se acepta el candidato, el
corpus nuevo, wheel, coste o D05. El [receipt](receipt.json) identifica fuentes,
resultados y límites; se conservan intactos observadores y receipts históricos.

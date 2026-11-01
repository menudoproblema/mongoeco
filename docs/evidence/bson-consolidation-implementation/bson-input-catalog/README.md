# B07: frames cliente de creación de colecciones e índices

Implementación `7524e39`, base `c1255eb`. Cierre acotado de create_collection,
create_index y create_indexes; B07/D06/D05 y el objetivo completo siguen abiertos.

## Causa, cambio y revisión

Los wrappers validaban o copiaban antes de admitir BSON. Un modelo SDK del
segundo índice podía contener un Decimal crudo y dejar ejecutarse el primero.
Ahora los modelos se adaptan como vistas, sin deepcopy previo. El nombre solo se genera ante None para conservar
un Decimal(0) que evalúa falso hasta llegar al encoder. Tres controles usan el BSON encoder
real del SDK para ese caso. Se recopila el
lote completo y el encoder existente admite una sola solicitud. Solo después
se usa el normalizador servidor de modelos, compartido con createIndexes.
Se retira el normalizador de lista redundante y el adaptador individual delega
en ese owner común. El constructor del modelo propio conserva su contrato.

create_index conserva su ejecución existente; create_indexes separa preparación
pública y ejecución preparada. Los comandos raw invocan esa ejecución sin otra
codificación cliente. La capa wire conserva el diagnóstico servidor del lote
vacío, código 2, en lugar de envolverlo en un error sin código. Los aliases
partialFilterExpression, expireAfterSeconds, wildcardProjection,
defaultLanguage, languageOverride y maxTimeMS se declaran con el catálogo de
argumentos existente. Cuatro firmas añaden `**kwargs` para esos aliases;
ninguna otra declaración del manifiesto cambia ni se añade un flag de bypass.

Las opciones pertenecen a cada solicitud: los índices usan UNSPECIFIED aunque
la colección tenga STANDARD/legacy; create hereda las opciones de la colección
que devuelve, incluidas las explícitas, también en sync. El precheck de
existencia precede a BSON, size se convierte a float donde lo hace el SDK y la
validación servidor ocurre después de la admisión. El preparador interpreta
los null opcionales como ausencia. El código de size no positivo conserva el
dialecto del llamador: 51024 en 7.0, 2 en 8.0/9.0.

Con check_exists=False, el create servidor es idempotente si coinciden sus
opciones. Se reutilizan igualdad BSON y normalización de collation para esa
política concreta: orden de opciones raíz irrelevante, orden dentro de
validator significativo y ausencia de validationLevel distinta de strict
explícito. Los controles del comando no se persisten como opciones. La lectura
[del servidor r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/catalog/collection_options.cpp)
explica matchesStorageOptions; las capturas por versión son el oráculo.

El create decodifica su respuesta con el materializador común aun cuando
retorne una colección. Si la fábrica falla, la creación ya confirmada permanece.
Los recuentos internos de índices usan el SPI y no ejecutan codecs del usuario.
listCollections materializa con sus opciones propias UNSPECIFIED: el perfil
sin SDK detectó UUID como UUID en vez de Binary 4, y se corrige esa respuesta
sin modificar el encoder portable ni las claves persistidas.

## Evidencia

- [Receipt](receipt.json), [runtime](runtime-sources.json), [cambio](changed-sources.json)
  y guards: wheel de 285 módulos, SHA `cfc565878c102210b8a16254efd9f815941a042893890988bfb8ba56ce740281`.
- Nueve acciones / 233 observaciones por MongoDB 7.0.43/8.0.32/9.0.2 standalone,
  SDK 4.19.0; capturas y checks finales coinciden byte a byte con el hash final
  del corpus. Las siete rondas conservan manifiestos, controles y feedback.
  El puente async añade únicamente la delegación real create_indexes; se
  repiten baseline y candidato con el mismo puente. Los 72 falsos fallos
  anteriores del harness no se atribuyen al producto ni se normalizan.
- [Base](baseline.txt.gz): 156 fallos/63 correctos. Candidato: 219 checks nuevos
  correctos, directa/wire sync/async × Memory/SQLite × 7/8/9.
  [Replay](replay.txt.gz): 9660 correctos y 72 fallos
  restantes, subconjunto exacto del conjunto previo y sin nodos nuevos.
- [Cohorte ampliada](unit.txt.gz): 2149 correctas,
  4690 subtests y dos fallos de batching ya reproducidos en la
  base anterior. [Frontera](boundary.txt.gz): 18 correctas. Comprobar ausencia
  de llamadas de creación antes de admitir el lote, estado/índices intactos,
  barrera de eventos, reapertura SQLite, ausencia de deepcopy previo,
  admisión única, preservación de opciones devueltas y efectos tras fallo de
  fábrica. Los gates interrumpidos por contraejemplos se conservan como
  feedback; no acreditan aceptación.
- [Sin SDK](sdk-free.json): 1.362 observaciones de catálogo en Python 3.13.15,
  usando modelos propios; Code exclusivo del SDK queda fuera. Decimal128 usa
  su texto público e Int64 la clase int portable. Las secuencias no tipadas
  usan exactamente el helper del capturador existente; las etiquetas de tipo
  observadas se conservan. Regresiones: 1.362 comandos, 2.880 bulk,
  3.918 mutaciones, 942 inserciones y 748 lecturas, sobre el mismo wheel.
- [API](api-check.txt.gz): 18 pruebas/13 subtests; [delta](public-api-delta.json)
  limitado a las cuatro firmas previstas. [Typing](typing.txt): dos positivos
  y 20/25 negativos. [Lint](lint.txt) correcto. [Protección](protected.json):
  284 fixtures/observadores históricos intactos.
- [Recursos](resources.json): 21 contenedores y 42 volúmenes propios verificados
  ausentes con Docker operativo. Las rutas se leen de los controladores reales.
- [Coste](cost.json), ABBA×2/100 solicitudes, CPU con replay concurrente:
  MemoryEngine/create: 1.95, MemoryEngine/index: 1.39, MemoryEngine/indexes: 1.21, SQLiteEngine/create: 1.99, SQLiteEngine/index: 1.09, SQLiteEngine/indexes: 1.07.
  Incluye precheck y respuesta de create; no se acepta presupuesto D05.

## Pendientes

El [censo](frontier-census.json) identifica 17 entrypoints, con delegaciones,
que requieren disposición explícita antes de cerrar B07: listados, información
y retirada de índices, namespace/validación y métodos de índices Search.
Es un censo, no 17 defectos demostrados ni una ampliación a Atlas. Reutilizar
las fronteras existentes cuando ya estén cubiertos; para los restantes,
caracterizar admisión/fase/owner y contrastar la parte nativa aplicable.
Después coordinar UUID global y completar positivos, batching y otros contextos
D06/B08. Siguen H53, decisión D07 y D05 (matrices, cobertura, persistencia,
costes y gates integrados). STANDARD global permanece. No se acredita nueva
implementación de capped, transacciones, cifrado ni Atlas con este corte.

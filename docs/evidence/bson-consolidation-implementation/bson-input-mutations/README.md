# B07: admisión de mutaciones y respuestas builtin

Implementación `c83c95c`, base de runtime `79d5a62`. Aceptación acotada de
update_one/update_many/replace_one/delete_one/delete_many y los tres
findAndModify públicos. B07/D06/D05 y el objetivo completo siguen abiertos.

## Cambio y revisión

Las ocho rutas admiten el frame BSON cliente una vez antes de normalizar,
compilar o escribir. Pipelines de update usan la misma frontera. El encoder
común conserva valores originales y opciones UUID. Los validadores de forma
no transforman valores: solo el primer campo decide la forma update/replacement,
como en el SDK. Filtro None en escritura ya no significa una consulta vacía.
Los IDs de upsert y errores usan el materializador existente con opciones builtin
UUID/fecha y dict, sin fábricas o decoders del usuario.

El orden de validaciones y sesión conserva diferencias nativas entre escrituras
simples, múltiples y findAndModify. Dos errores simultáneos distinguen forma,
capacidad, sesión, codificación y semántica servidor. Las cabeceras de comandos
clásicos se codifican antes de q/u; findAndModify conserva su propio orden.
No se usa la validación de lecturas para imponer reglas anticipadas de sort o
maxTime a los writes. No se endurece DocumentCodec.to_internal ni se añade un
flag público para omitir la frontera.

Bulk reutiliza los ejecutores internos preparados: conserva contexto/let y no
recodifica cada elemento como una llamada cliente independiente. Este cambio
no acepta la admisión de la familia bulk completa: sigue pendiente preservar
originales y codificar el grupo antes de su primera escritura. Los dos tests de
delegación pasan a comprobar el dispatcher interno real, conservando metadata
y contadores. El adaptador async añade find_one_and_delete por delegación real.

## Evidencia y límites

- [Receipt](receipt.json), [fuentes runtime](runtime-sources.json) y
  [fuentes cambiadas](changed-sources.json): wheel de 285 módulos y guardas de
  imports en master/workers, sin mezcla con src. Fuente del corpus congelada.
- Siete acciones / 701 observaciones por MongoDB 7.0.43/8.0.32/9.0.2 standalone;
  imágenes fijadas, buildInfo/FCV y SDK 4.19 en los manifests native-7. Cada
  check final es idéntico al golden correspondiente. Los checks se archivan
  comprimidos; el hash del BSON/JSON original queda en el receipt.
- [Baseline](baseline.txt.gz): 72 fallos y 99 checks correctos. Los 171 checks
  nuevos pasan en el [replay instalado](replay.txt.gz), con directa/wire,
  sync/async, Memory/SQLite y 7/8/9. El gate amplio tiene 9027
  correctos y 96 fallos, todos bson_admission preexistentes,
  visibles y obligatorios. No se declara un gate integrado verde.
- [Contratos/unitarias/propiedades](unit.txt.gz): 1491 pruebas y
  4569 subtests correctos, sin los warnings tratados como error.
  Los 49 controles nuevos incluyen una codificación por solicitud, objetos con
  deepcopy prohibido, estado e índice único, eventos mediante barrera posterior,
  reapertura SQLite y respuestas upsert sin callbacks/fábricas del usuario.
- El corpus de errores tipados reutiliza public_error/BSONObservation protegidos:
  conserva familia compatible, fase/código y contenido/orden BSON de errInfo.
  Usa el transporte canonical-json existente para no convertir el modelo de
  observación EJSON en DBRef/fecha antes de compararlo; no cambia el observador.
- [Sesión y sort modernos](native-modern-sort-witness.json): testigo nativo
  SDK4.19 de ronda 5 y control local con perfil 4.19. El perfil predeterminado
  4.9 rechaza ese keyword por capacidad antes de sesión; no se exige una
  capacidad moderna al perfil antiguo ni se normalizan sus excepciones.
- Python 3.13.15 realmente sin SDK: [3.918 observaciones](sdk-free.json) de seis
  acciones de mutación, excluyendo constructor Code exclusivo del SDK y contrato
  completo de errores de validación tipados. UUID tiene cinco modos explícitos;
  Decimal128 portable se observa por texto decimal público. También pasan
  [942 observaciones de inserción](sdk-free-inserts.json) y
  [748 controles de lectura](sdk-free-reads.json). El perfil SDK conserva BID
  y errores tipados completos; no se atribuye esa cobertura al portable.
- [Typing](typing.txt): dos positivos y 20/25 negativos; [API](public-api.json)
  idéntica al baseline; [lint](lint.txt) correcto. [Históricos](protected.json):
  275 fixtures/observadores sin cambios. [Recursos](resources.json):
  21 contenedores y 42 volúmenes propios comprobados ausentes con Docker operativo.
- [Coste diagnóstico](cost.json), ABBA×2/200 solicitudes y replay concurrente:
  MemoryEngine update_one: 1.16; MemoryEngine update_many: 1.07; MemoryEngine find_one_and_update: 1.16; SQLiteEngine update_one: 1.06; SQLiteEngine update_many: 1.06; SQLiteEngine find_one_and_update: 1.09. D05 conserva estos incrementos; no es aceptación de presupuesto.

## Pendiente

Admisión por familia bulk antes de sus writes, comandos/índices/validators,
UUID coordinado y cambio de default (STANDARD permanece), proyecciones calculadas,
sobres raw/update y resultado público bulk D06/B08. El resto de contextos D06,
H53, decisión opcional D07, matrices/cobertura/coste e integración D05 conservan
sus owners. Las rondas intermedias son feedback; no se alteran referencias u
observadores históricos para aceptar el candidato.

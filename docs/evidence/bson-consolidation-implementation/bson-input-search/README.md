# B07 — Admisión BSON de solicitudes Search

Base `aceaa5e8`; código `2723fb28`. Se aceptan las cinco entradas públicas del
censo: create_search_index/create_search_indexes/list_search_indexes/
update_search_index/drop_search_index, sync/async y sus controles wire.
No acredita positivos Atlas, nuevo soporte de comandos Search en el proxy,
UUID global, respuestas completas/batching ni el cierre integrado D05.

## Causa y refactorización

Las fachadas llamaban al motor sin codificar solicitudes. Los normalizadores
copiaban antes de rechazar BSON y no reconocían el modelo SDK; validación local
podía ocultar el error cliente original. Los cinco métodos usan las opciones
propias UNSPECIFIED del driver, sin codecs de la colección. Crear uno y varios
comparte el frame admitido y ejecución local existente; no se crea otro codec,
planificador o almacén. El listado admite antes de devolver su cursor local.

El módulo privado _collection_search_indexes concentra esta frontera y separa
la administración Search de índices ordinarios. Reutiliza encode_bson_request,
guarda de sesión, modelos locales, cursor y métodos del motor. Se retiran los
dos normalizadores de AsyncCollection que copiaban antes de codificar. Se
conservan la definición directa local y los modelos locales; se añaden el modelo
SDK y su sobre mapping sin perder los originales. El constructor del modelo
ordena name/definition/type como el SDK. maxTimeMS precede a comment en el
frame: dos valores inválidos distintos deben producir el mismo primer error.
El fallo de los cuatro testigos previos queda en feedback-order-before.

La creación de un único índice evita la lectura de catálogo que solo requiere
el rollback de varios. La creación múltiple conserva el snapshot, deadline,
rollback de nuevos nombres y protección de índices preexistentes. Los tests
privados retirados pasan a validación pública; los controles positivos de los
cuatro modelos y el rollback se ejercitan por las APIs reales.

## Verificación

- Cinco acciones, 303 observaciones por MongoDB 7.0.43/8.0.32/9.0.2, PyMongo
  4.19.0. Capture y --check finales son idénticos. El dominio nativo es rechazo
  cliente: encabezados, operandos, maxTimeMS, cinco opciones UUID, sesión cerrada,
  competidores y construcción mapping. No se normaliza un rechazo de capacidad
  standalone como éxito ni se deduce semántica positiva Atlas.
- Base archivada: 60 checks fallan y 63 pasan con corpus/harness finales.
  Candidato instalado: 123 checks nuevos pasan; replay amplio 10.131 correctos
  y exactamente los mismos 72 nodos previos de bson_admission.
- 24 controles nuevos: rechazo antes de cualquier llamada al motor, objeto con
  deepcopy engañoso, catálogo/datos intactos y reapertura SQLite, codificación
  una vez, codecs/fábricas ignorados, modelos y rollback. La cohorte instalada
  completa da 2.274 correctos/4.686 subtests y los dos fallos previos de batching.
- SDK-free real Python 3.13.15: 1.716 observaciones, usando sobres públicos mapping
  en lugar del constructor SDK, salvo Code que no tiene constructor portable.
  Las nueve familias BSON previas vuelven a pasar con la wheel final.
- Wheel y 287 módulos verificados por hash, importación instalada en master y
  cuatro workers; manifiestos públicos idénticos. API: 18 tests/13 subtests.
  Typing instalado: dos positivos y 20/25 negativos; lint ratchet pasa.
- 299 fixtures/observadores históricos intactos. Nueve contenedores y 18 volúmenes
  propios confirmados ausentes con Docker operativo.
- Costes ABBA×2 sobre los cinco métodos y ambos motores son diagnóstico bajo
  gates concurrentes, no aceptación del presupuesto D05. Se conservan muestras.
- Los gates iniciales se interrumpieron de forma deliberada al detectar el orden
  incorrecto de opciones. Se conserva ese feedback; solo los gates finales del
  candidato reconstruido acreditan el corte.

El receipt y los manifests fijan fuentes/resultados. El censo histórico de 17
entradas queda dispuesto, con cero pendientes de admisión en ese censo; no es
un censo universal de toda la API. Sigue el default UUID coordinado y se
conservan las obligaciones de positivos/respuestas, D06, H53, D07 y D05.

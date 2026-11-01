# D06c: contexto de candidatos, ranking y reapertura Search

Corte sobre `47336ce`. El [receipt](receipt.json) identifica fuentes, wheel,
oráculos, base aislada y límites. El [registro](../../../bson-consolidation-implementation.md)
posee el estado; el [plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
posee las dependencias y garantías restantes. No se declara cierre D06/D05.

## Causas y simplificación

1. SQLite recibía una tanda ANN, pero retornaba al encontrar limit matches antes
   de examinar el resto. Ordenaba ese subconjunto, omitiendo mejores scores o
   desempates ya recibidos. Ahora evalúa la tanda completa antes de truncar. Se
   elimina el retorno duplicado; se puntúa toda la tanda y solo se adjunta metadata
   a los ganadores. Un único orden de hits sirve para ANN, fallback exacto y
   resultados ya decorados. Los
   contadores físicos pueden aumentar: describen el trabajo realmente efectuado.
2. El adaptador del comando explain devolvía verbosity sin transmitírselo al
   cursor agregado. queryPlanner ejecutaba estadísticas y fallaba con un índice
   pendiente. Se transmite la opción explícita y se conserva el default local;
   no se implementa allPlansExecution ni se afirma paridad del plan local con el
   servidor. La [documentación oficial del comando](https://www.mongodb.com/docs/manual/reference/command/explain/)
   distingue sus modos y defaults del método de shell; no se infiere uno del otro.
3. Los prefiltros escalares representaban políticas nativas, pero se aplicaban
   también a dialectos que redefinían igualdad. Find reconocía KEEP y keep;
   vectorSearch descartaba KEEP en ambos motores. La guarda común
   supports_mql_candidate_prefilter permite los tres singletons nativos sin
   collation y el camino legacy 7 sin contexto; los demás contextos conservan
   residual. No se amplían claves ni dominios. Ejecución y explain, texto y
   vectores reutilizan esa guarda. Se retira un cálculo de candidatos duplicado
   y sin consumidor en el planificador Memory.
4. Varias pruebas sync activaban la referencia después de aggregate, cuando ya
   se había ejecutado la primera respuesta. Podían comparar dos ejecuciones
   optimizadas. El helper test-only configura el hook existente antes de abrir y
   lo retiene en la instancia para async/continuación; no crea otro ejecutor.
   Sus tests observan el modo y estrategia recibidos por el ejecutor real y la
   restauración del hook tras fallo. Los errores de apertura se capturan dentro
   del ámbito correcto. Un índice pendiente se inspecciona con el comando de
   planificación, sin abrir antes un cursor de datos.

El contrato search-v1 conserva su semántica propia. No se comparan scores Atlas
con el standalone ni se extrapolan estas pruebas a vecinos que ANN no devolvió.
No cambia ningún formato de disco: los índices vectoriales se reconstruyen por
versión de colección. Claves persistidas, storage keys y desempates no cambian.

## Oráculos y comprobaciones

El corpus nuevo tiene dos dominios MQL: documental y vectorial. MongoDB
7.0.43/8.0.32/9.0.2 standalone, imágenes fijadas y FCV correspondiente, captura y
comprueba selección tras seed, cambios de tipo/inserción, restauración y borrado.
Incluye Binary 0/5, Code/String/scope, DBRef/documentos/orden/null-db, rutas,
números exactos grandes, decimal/double, fechas, arrays y missing/null.
[native-checks.json](native-checks.json) conserva hashes de captura/comprobación;
los goldens finales están en tests/fixtures. Los fixtures registran topología y
ownership. Ninguna referencia histórica se recaptura.

La acción carga su propio seed BSON y el registro declara una entrada inicial
neutra. La primera versión de los metadatos EJSON canonicalizaba DBRef y perdía
orden/null-db; el validador byte a byte lo detectó. Se corrigió la descripción del
fixture, sin relajar el validador ni convertir el constructor SDK en gramática.
El fichero completo del corpus permanece fijado por hash en cada referencia.

Las APIs sync/async y wire sync/async ejecutan vector, vector-downstream, text y
compound sobre las mismas selecciones. Los controles distinguen filtro anterior
a top-k de match posterior, ventana, calentamiento y cambios de datos. SQLite se
cierra y reabre con motores nuevos en cada fase, sin recrear índices; se exige
el backend concreto. Un control separado prueba la ruta Python y registra que
realmente fue seleccionada. Dos pruebas invierten deliberadamente el orden ANN,
con empates y scores diferentes, y exigen examinar toda la tanda.

Sesenta controles locales de overrides 7/8/9 añaden ejecución sync/async,
planificación y reapertura, con spies que fallan si se intenta una poda no
acreditada. La expectativa literal de dos IDs evita otro oráculo espejo. No son
una afirmación de que MongoDB ejecute extensiones Python.

La base Git aislada falla 32 y pasa 141 en el corpus público final, falla cuatro
y pasa trece en los controles de explain/oráculo, y falla 48/pasa doce en los
controles de overrides. Memory ya preservaba el residual en las rutas de texto.
Los dos controles deterministas de ranking fallan en la base. Todas las
comprobaciones verifican procedencia de imports del paquete archivado.

El wheel definitivo pasa **8.454 checks** de replay en 211,57 s y **1.144
pruebas** unitarias/contratos/Search con **181 subtests** en 70,32 s. No hay
avisos, con RuntimeWarning y avisos de GC/hilos elevados a error. Los cuatro
workers verifican sus 262 módulos cargados y los 284 archivos del wheel.
La prueba final de imports unitarios tiene nombre propio para que el maestro
del replay no la sobrescriba; se conservan ambas guardas de artefacto.
Typing conserva dos positivos y 20/25 negativos esperados; el manifiesto público
no cambia y el lint ratchet no añade deuda.

El diagnóstico CPU ABBA dos veces conserva BSON idéntico, con doce muestras por
runtime/caso. Puntuar todo y decorar todos los candidatos daba ratios 1,66/1,75
en los dos casos limit=2. La selección antes de decorar los reduce a 1,11/1,47;
los controles limit=64 quedan en 0,99. Son cuatro casos sobre 64 documentos y
presupuesto 128, no aceptación del presupuesto D05. El residual Binary con límite
pequeño conserva revisión de coste explícita, junto al find(limit=1) anterior.
La materialización limitada se verifica en ambos motores: todos los candidatos
recibidos se puntúan, pero solo los seleccionados reciben metadata de salida.
Los dos replay de 8.394 anteriores a la guarda final son feedback: aunque verdes,
no se presentan como aceptación del último cambio. La primera suite amplia
identificó cinco fallos de expectativas/oráculo/verbosity y no se aceptó.

Python 3.13.15 realmente sin SDK conserva 960 comparaciones contra selecciones
nativas, cuatro rutas y cuatro fases sobre SQLite persistido/reabierto. Usa el
subconjunto declarado: excluye IDs y filtros Code, no finge un módulo bson. Los
IDs esperados se proyectan del oráculo nativo, sin matcher local. Se observan
usearch, fts5 y fts5-prefilter; el probe de overrides comprueba ambos motores.
No se presenta como replay íntegro del corpus sin SDK.

La auditoría Docker independiente confirma ausencia de nueve contenedores y
18 volúmenes propios de las tres rondas de captura. Las dos primeras son
feedback, no oráculos finales intercambiables. Observadores y catálogos
protegidos conservan sus hashes.

## Límites y disposición

Search conserva candidatos según la política que representan sus claves; los
contextos no acreditados usan residual. Se conserva la cobertura anterior de
collation y no se implementa otro comparador universal. El ranking solo asegura
la tanda efectivamente recibida dentro del presupuesto ANN.

El nodo histórico D05 de test_generated_search_errors_match_reference_engines_and_surfaces
se comprueba ahora con un oráculo bien configurado. Esto no vuelve verdes por
asociación los otros nodos del listado de 97, ni sustituye la suite global.
Permanecen matrices Python/SDK/pytest, cobertura, CI remoto y coste D05, los
contextos restantes D06, H53/C03/B08, sNaN, la decisión opcional D07 y B07.
UUID predeterminado sigue STANDARD hasta su corte coordinado.

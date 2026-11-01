# Relectura de la autocrítica sobre el estado actual

1 de noviembre de 2026, HEAD `5348004`. El SHA-256 del adjunto coincide con
revisión 15 y las anteriores: no hay un nuevo informe ni siete defectos nuevos.
El [receipt](receipt.json) identifica input, runtime, pruebas y límites. El
[workset preexistente](preexisting-workset.json) conserva los tres archivos en
curso de fixture/corpus: esta revisión no los modifica ni acepta.
El [plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
posee el orden vigente; este documento conserva evidencia, no otro backlog.

## Disposición de las afirmaciones

| Afirmación | Contraste y decisión |
| --- | --- |
| H48: interno/local, bytes y SDK son mecanismos distintos | La retractación es correcta. `binary_parts` reconoce bytes y wrappers; clasificación/comparación delegan en las vistas comunes. `binary_order_key` incluye longitud y subtipo 2. Conservar propiedades por representación, sin otra tabla de clases. |
| Distinct requiere contenido/subtipo, no solo cardinalidad | Ya cubierto por D03a. La sonda actual conserva bytes/Binary 5 con el mismo payload; eq/in distinguen 0/5 en ambos motores y los tres dialectos. |
| SQL eq/in puede omitir Binary 0 por tags | Defecto histórico corregido: igualdad y membership declinan operandos binarios hacia fallback. Conservar controles de SQL efectivamente ejecutado; no reintroducir pushdown por simplificación. |
| Parser DBRef literal y wire | El parser común reconoce primer campo/contexto y presencia de ref/id. No exige el mismo criterio de tipos que el decoder SDK. La corrección básica ya está aceptada; los demás contextos D06 siguen pendientes por sus testigos propios. |
| Observar ambos lados del matcher | Se repite la sonda: seis combinaciones motor/dialecto, 18 trazas separadas. No convertir nombres de clases privadas en API contractual. |
| TTL solo puede retrasarse un milisegundo irrelevante | No está demostrado ni es una garantía aceptada. El código actual usa coordenadas enteras y residual exacto; migración/reapertura tienen controles D02. Separar inclusión de candidatos, decisión exacta y cadencia. |
| Watch exige clase/código/fase; H53 no fue reproducido localmente | Ya integrado: D04 tiene aceptación acotada propia; H53 conserva disposición C03/B08. La ausencia de warning no prueba rechazo incorrecto de escrituras. No declarar una reproducción nueva por inspección. |
| $in/distinct, propiedad desde D00, filas D03/D01 y 167 pruebas | Las retractaciones se mantienen; cortes con IDs únicos y propiedades desde D00 ya están definidos. Las 167 pruebas conservan alcance histórico, sin sumarse como aceptación de esta revisión. |
| Identidad de fuentes r8.0.0 | Revisión 15 verificó 16 hashes descargados. Aquí no se descargan de nuevo y no se atribuye aquella verificación a este receipt. Fuentes explican mecanismos; no reemplazan diferenciales por versión. |

## Comprobaciones nuevas y simplificación

Pasan **85 pruebas focales** ([log](focal.txt)): propiedades Binary, gramática de
comandos, traductor SQL, migración TTL y contexto watch. La
[sonda de representaciones](representation-probe.py.txt) produce
[seis combinaciones y 18 trazas](representations.json). Son regresiones y
diagnóstico local, no nueva aceptación física ni cierre integrado.

El [control de fábricas](probe.py.txt) usa únicamente la API pública BSON del SDK
como composición independiente y el materializador local actual. Sobre la misma
fila anidada, decodificar `cursor/ok` construye cuatro documentos y materializar
solo el subdocumento cursor construye tres ([resultado](factory-control.json)).
Las filas coinciden; los efectos no. Este control no ejecuta MongoDB y no prueba
el comportamiento completo del driver ni su decodificación selectiva.

El plan precisa la aceptación pendiente de `document_class` con/sin registro:
constructores, setters, errores por nivel y consumo/estado público. Una topología
RS puede añadir metadatos con efectos; se exige un oráculo comparable standalone
para esa superficie, conservando RS para watch. No se normalizan los efectos ni
se simulan relojes distribuidos. Se reutilizan fixture, registro/capturador/replay
y materializador existentes; APIs públicas del SDK y contrato portable tienen
pruebas propias. No se añade otro motor semántico ni un comparador universal.

El siguiente corte no necesita reabrir D01/D02/D03/D04. D06c, H53, sNaN, decisión
D07, D05 y admisión B07 conservan sus owners. Los 97 nodos previos identificados
en la suite amplia no equivalen a un gate global verde. La política UUID default
continúa coordinada con B07, sin cambiarla de forma aislada.

No hay nuevos servidores, capturas, wheel, replay completo, cobertura ni medidas
de coste en esta revisión documental. Los receipts y goldens históricos se
conservan intactos; tampoco se acepta el fixture/corpus sin commitear.

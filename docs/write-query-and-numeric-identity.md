# Filtros obligatorios y unicidad numérica de identidad

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

## Contrato aprobado y alcance

El usuario autoriza corregir la validación de `q` en comandos update/delete y
la unicidad y selección de `_id` entre representaciones numéricas BSON. Se exige
contraste real en sync/async, Memory/SQLite, con códigos, contadores y efectos
parciales, conservando compatibilidad con archivos SQLite existentes. El encargo
no incluye publicar una versión ni modificar el consumidor externo.

La revisión previa fijó el workset
`02453e5b488884e137191099ff09542910f7d29a6404340f094c93ab69302679`
y reprodujo ambos defectos con MongoDB 7.0.43, 8.0.32 y 9.0.2.

## Censo y mecanismo suficiente

- Componer la validación de `q` en el preflight existente de comandos clásicos:
  comprobar todos los elementos antes de ejecutar el primero. `{}` sigue siendo
  un filtro válido; ausente/null debe producir 40414, y tipos escalares/array 14.
- Reutilizar la igualdad BSON para identidad lógica, separada de las claves
  físicas. No cambiar `_storage_key`, esquema, serialización ni filas existentes.
- Comprobar claves lógicas de lote dentro de la frontera de escritura. En la
  selección SQLite, usar el fallback semántico cuando SQL o una búsqueda física
  no pueden acreditar igualdad numérica entre representaciones.
- Ampliar el corpus y el capturador nativo existentes para helpers y raw/wire;
  incluir negativos, controles booleanos/no equivalentes y recuperación.
- Abrir copias de archivos creados por el candidato anterior y de la fixture
  4.5; verificar lecturas, escrituras, índices, eventos y conservación de claves.

## Plan acotado

1. Materializar los testigos y controles del corpus; capturar referencias reales
   independientes del candidato en los tres servidores y ambas fachadas PyMongo.
2. Corregir validación y comparación sin migraciones ni cambios del SPI.
3. Verificar los caminos individuales, lotes, upserts, selección e invariantes
   SQLite heredadas, incluida ausencia de efectos de comandos malformados.
4. Construir candidato reproducible, ejecutar gates finales instalados en
   Python 3.13/3.14/3.15 y canario, revisar diff y registrar evidencia.

## Correcciones y regresiones

El preflight comprueba `q` en todos los elementos antes de ejecutar el primero.
Un filtro ausente/null produce **40414 / IDLFailedToParse**; un escalar, booleano,
string o array produce **14 / TypeMismatch**. El fallo es del comando completo,
incluso con `ordered=false`: no conserva un prefijo escrito. `{}` sigue siendo
válido. Se comprueban estado final, contadores y ausencia de publicación, además
de una escritura posterior de recuperación.

La unicidad primaria compara `bson_equality_key`, como los índices secundarios,
en lugar de deducir identidad lógica de la clave física. Los lotes mantienen un
conjunto de identidades dentro de la frontera de escritura para detectar también
colisiones entre elementos de un mismo lote. En SQLite se consulta el estado
vigente bajo la transacción de escritura, incluidas escrituras desde otra
conexión. Cuando una actualización conserva el ID físico, no se añade un
recorrido completo a la selección indexada; se mantienen los presupuestos y
los tests de acceso a candidatos existentes. Los errores **11000** conservan los índices originales, `op`, contadores,
prefijos y éxitos posteriores permitidos por ordered/unordered.

SQLite conserva las funciones de clave física y serialización existentes. Una
búsqueda física fallida con ID numérico usa igualdad BSON; las comparaciones SQL
que no representan esa igualdad recurren al evaluador semántico. La selección,
upsert y eliminación trabajan sobre el documento existente. El contraste detectó
también que la ordenación SQL trataba los números etiquetados como objetos de
almacenamiento: esas ordenaciones usan ahora el comparador BSON.

El corpus de identidad pasa de **143 a 246 escenarios**: **103 nuevos**, con siete
pares numéricos equivalentes, IDs documento, controles booleanos, strings y
números grandes no equivalentes. Las vistas raw pasan de **26 a 110**. Un catálogo
adicional contiene **28 comandos** update/delete con `q` malformado o vacío y
ambos valores de ordered. El observador distingue ahora los valores escalares de
`count_documents`; una prueba negativa detecta conteos y tipos incorrectos.

Las referencias se capturan exclusivamente con **PyMongo 4.19.0** en MongoDB
**7.0.43, 8.0.32 y 9.0.2**, FCV exacta, y coinciden entre sync/async. Incluyen
códigos, familias, codeName, contadores, BSON almacenado y efectos parciales.
La matriz directa usa perfiles 4.9/4.19, dos fachadas y dos motores: **9.216
observaciones** de los 246 helpers, 110 vistas raw y 28 comandos. Wire reproduce
**4.608 observaciones** con los clientes oficiales sync/async en doce celdas.

El wheel anterior `fe4ea943e6b1d7f72fbe8f5fc68630840b68b3d8ed9ce075aae8822e0034b932`
presenta **288 divergencias en 524 observaciones** de los 103 nuevos helpers y
28 comandos con MongoDB 8. Los controles que ya coincidían se conservan.

La fixture `numeric-identities-before-fix.sqlite` se generó con ese wheel
anterior instalado, sin importar el source corregido. Se comprueban su hash y
procedencia, lecturas mediante representaciones equivalentes, rechazo de
inserciones duplicadas, upserts sin inserción, índices secundarios y reapertura.
El esquema y las claves físicas son idénticos antes y después. También pasa la
fixture histórica 4.5, incluidos índices, Search y replay de outbox. No se
migran ni se fusionan filas antiguas que ya contuvieran identidades duplicadas.

## Validación del candidato final

El wheel tiene SHA-256
`ecb65044a3d47ef9d2792cd10131907192179170acd30d5c738618251bf1a294`;
el sdist,
`4068311af8ac0144c9a9838aeac670b30ef2b379c75c03d6b4285bef08a1749c`.
Ambos se reproducen byte a byte en dos builds secuenciales y pasan twine.
Los 275 archivos de runtime coinciden con el checkout y las tres instalaciones.
Se conserva provisionalmente el número 4.9.1 para pruebas del workset; este
artefacto no está publicado y es distinto de la 4.9.1 distribuida.

Las suites completas del wheel instalado en Python **3.13.15, 3.14.5 y 3.15.0**
pasan **18.856 pruebas y 14.445 subtests** en cada versión, con **31 skips
opcionales esperados**. La cobertura 3.14 es **99,04 %**, por encima del mínimo
99 %. Typing, SPI externo, manifiesto público, snapshots de compatibilidad y
ratchet Ruff pasan en las tres versiones.

El candidato intermedio `6fd9957d265f71fb4ffb341b99a11284f42bf490a10531024be75e02b4c94993`
se descartó por añadir un recorrido completo a updates indexados de Memory.
El guard final evita revalidar la unicidad de un ID que conserva su clave física;
la regresión original de acceso indexado pasa sin cambiar presupuestos. Se
actualizaron además dos tests antiguos que exigían aceptar el par `1`/`1.0`.
Un timeout de 25 ms en una ejecución bajo carga se conserva en los logs del
candidato descartado; el test y su presupuesto no se modificaron y la suite
final completa pasa.

Unittest pasa sus **3.613 pruebas**, con tres skips esperados, en los tres
intérpretes. Pasan los seis smokes limpios wheel/sdist y las seis ejecuciones de
conformance de Memory/SQLite. Pasan las nueve propiedades deep y los dos smokes
de benchmark Search/vector. Se validan los workflows y la continuación del
comando pytest que incluye las nuevas regresiones. Los 324 probes de admisión
SDK se conservan por independencia causal: el último cambio afecta solo a los
guards privados de escaneo de los engines; sus contratos y entradas no cambian.

El canario final ejecuta **16 flujos y 48 replays nativos**, sin correctores
semánticos ni cambios permanentes del consumidor. Se comprueba la ausencia de
bases de pruebas y se detienen los tres servidores; sus puertos están cerrados
y sus directorios temporales, eliminados.

Después de la suite completa se corrigió únicamente el cierre de la conexión
de lectura del helper de snapshots de las pruebas nuevas. No cambian aserciones,
especímenes, IDs de test ni runtime. El módulo completo se repite en los tres
intérpretes: **13 pruebas** por versión, con ResourceWarning tratado como error.
La evidencia separa esta comprobación focal del gate completo del mismo wheel.

Se revisó el diff final y la documentación, preservando el trabajo previo.
La evidencia está en [validation.json](evidence/query-numeric-identity/validation.json)
y liga workset, runtime, distribuciones, fixtures y logs por SHA-256. Los gates
son locales macOS arm64; no acreditan una ejecución alojada de GitHub. No se hace
commit, tag, publicación ni elección de número de versión. Python 3.13 mantiene
la fecha acordada de octubre de 2027.

Estado: **implementación cerrada para los dos defectos solicitados**.


## Límite y pendiente separado

La conversión explícita descrita abajo se resuelve en el bloque posterior de
[conversión BSON de identidad](id-bson-conversion.md). Se conserva aquí el
diagnóstico del candidato anterior y su evidencia histórica.

El corpus acredita los estímulos enumerados. No implica paridad universal de
identidad ni repara duplicados ya almacenados por versiones anteriores.

La exploración detectó una divergencia adicional al **cambiar explícitamente
el tipo BSON** del `_id` mediante replacement/pipeline, manteniendo su valor
numérico. MongoDB permite esas dos rutas; el candidato rechaza algunas con 66.
Un `$set` clásico sí es rechazado por MongoDB con 66. Este disparador es distinto
de buscar mediante otro tipo y conservar el ID almacenado, cubierto aquí.
La solución requiere revisar el invariante que liga ID físico y payload en
mutaciones; se registra como seguimiento, sin migrar o reescribir claves en este
bloque. El testigo se conserva en `evidence/query-numeric-identity/typechange-followup.json`.

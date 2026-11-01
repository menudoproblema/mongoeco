# D06c: agrupación, conjuntos y decisión de ejecución compartida

Candidato sobre `7629ea6`. El [receipt](receipt.json) identifica fuente final,
wheel, oráculos, base aislada, comprobaciones y límites. Este corte corrige los
consumidores descritos aquí; no cierra todos los contextos D06 ni D05/B07.
El [registro](../../../bson-consolidation-implementation.md) conserva el backlog
por owner y el [plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
posee aceptación y dependencias.

## Causas y cambio

- La clave de grupo recorría dict, pero enviaba el DBRef decodificado a una
  guarda de hashabilidad. La representación SDK seguía otro camino sin aplicar
  collation a sus campos. `document_mapping` es ahora la vista común del grupo,
  con orden/extras y recursión propios. Se retira `_aggregation_key` y sus
  exports privados; se conserva su guarda escalar efectiva. No se añade un
  validador de entrada ni se declara soporte para objetos arbitrarios B07.
- El índice temporal de conjuntos 7 utilizaba igualdad Python sobre bytes y
  Binary, y aceptaba Code por herencia de str. Ahora usa átomos de igualdad BSON
  para el mismo dominio; Code conserva residual. No se amplían dialectos, tipos
  numéricos ni collation en el índice. Subtipos 0/5 permanecen distintos.
- El diferencial descubrió otra regla compartida mal situada: el identificador
  Code sin scope se convertía a String por versión, también en facet/lookup
  clásicos. Se reutiliza la decisión de preparación que ya gobernaba average,
  con nombres de ejecución de grupo en `group_execution.py` y
  `group_uses_slot_semantics`. Un único dato se conserva en vistas, plan/bucket y
  spill; controla finalización numérica y representación del ID. Se elimina el
  parámetro de dialecto redundante de finalización. No se replica un motor SBE,
  se amplía la gramática ni se añade un selector público del motor nativo.

La igualdad del grupo sigue separada de su representante físico. Identidad de
updates, claves de almacenamiento, desempates y formatos persistidos no cambian.
Las referencias anteriores de average conservan su identidad y aceptación.

## Pruebas y revisión

Cinco acciones nuevas se capturan y comprueban con MongoDB
7.0.43/8.0.32/9.0.2 standalone, imágenes fijadas y FCV correspondiente. Las
capturas finales están en tests/fixtures y cada subdirectorio 7.0/8.0/9.0
conserva fixture y salida de comprobación. Las capturas previas de cuatro
acciones solo fueron feedback: se amplió y congeló el corpus nuevo antes del
wheel final, sin recapturar goldens históricos.

- Group: miembros, cuenta y tipo; top y facet con/sin collation.
- Representantes: BSON raw exacto, evitando normalización DBRef del SDK; top,
  prefijo unwind clásico, prefijo set, facet, lookup y union, cada uno con/sin
  collation. El resultado se ordena antes de proyectar solamente el ID.
- Sets: tamaños y matriz de pertenencia a valores testigo; addToSet separado,
  Code/String/scope, Binary 0/5, DBRef/orden/extras/null-db, números y booleanos.
- Lookup: pertenencia y orden por ID. Los tests de planes registran el plan real
  con presupuesto ausente/agotado/suficiente y fallback de collation/dialecto9.
  El presupuesto del índice de candidatos se aísla del límite independiente de
  materialización; no se usa un fallo de capacidad como test de fallback.
- Spill: se fuerzan particiones y se comprueba su apertura, con/sin spill, por
  ambos motores y APIs sync/async, frente a miembros y BSON raw nativos.
- Propiedades: alias documentales producen la misma clave y bytes serializados
  de partición; equivalencia binaria se establece mediante BSON público SDK,
  con negativos de subtipo/orden/extras. Search conserva el universo completo
  ante dominios BSON no representados en índices materializados Memory/SQLite.

La base aislada de Git falla **88** y pasa **101** comprobaciones del corpus final
y planes. Las fuentes e imports archivados impiden mezclar checkout/candidato.
Antes de ampliar el corpus, el primer arreglo dejaba 16 fallos de tipos en
facet 8/9; ese feedback condujo al owner de ejecución compartido. Las pruebas de
planes iniciales contenían ocho errores de diseño (capacidad y orden de lectura
SQLite), conservados como feedback, corregidos antes del gate final.

El wheel final se verifica por hash en sus 284 archivos. Unitarias/propiedades y
contratos instalados pasan **675**, con **159 subtests**; typing conserva dos
positivos y 20/25 negativos esperados, y el manifiesto público no cambia. El replay completo final pasa **8.221** comprobaciones en 200,26 s sin avisos,
con RuntimeWarning/avisos de GC e hilos elevados a error. Los cuatro workers
verifican sus 262 módulos cargados y los 284 archivos del wheel. El diagnóstico
CPU ABBA dos veces conserva BSON idéntico y ratios 0,99–1,01 en seis casos de
grupo numérico/documental y conjuntos, con 12 muestras por runtime/caso. No es
aceptación del presupuesto integrado D05.

Python 3.13.15 realmente sin SDK pasa doce acciones del subconjunto DBRef/Binary
de los mismos oráculos nativos. Se eliminan exclusivamente los IDs Code no
admitidos en ese entorno; la expectativa se proyecta desde los miembros nativos,
sin matcher local, y conserva tipos/orden. No es aceptación de Code sin SDK ni
replay íntegro del corpus. Permanecen los 186 + 72 controles portables anteriores
y las 30 acciones/1.278 trazas de consumo del cursor.

La auditoría Docker independiente confirma ausencia de seis contenedores y doce
volúmenes propios. Observadores, manifiestos históricos y catálogos protegidos
no cambian. El lint ratchet no introduce deuda.

## Límites que permanecen

Los tests de Search aquí comprueban conservación en índices materializados, no
Atlas Search real ni toda la persistencia/reapertura pública de su contrato.
Los prefiltros y dominios de elegibilidad conservan sus owners actuales; no se
amplían por la nueva clave de agrupación. El selector de grupo conserva el alcance
nativo por defecto y las expresiones caracterizadas; no emula cualquier
configuración interna del servidor.

Este corte no acredita CI remoto, matrices completas Python/SDK/pytest,
cobertura global o el presupuesto D05. Permanecen los demás contextos D06a/b/c,
H53, conversión sNaN, decisión opcional D07 y B07, incluidos sus gates y clases de
rechazo. UUID predeterminado sigue STANDARD hasta su corte coordinado.

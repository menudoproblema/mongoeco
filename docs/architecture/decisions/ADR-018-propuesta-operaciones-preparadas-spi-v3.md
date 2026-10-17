# ADR-018 - Propuesta: preparación interna conservando SPI v2

## Estado

**Propuesta revisada, pendiente de aprobación.** Autoría inicial: 2026-10-16;
revisión 2: 2026-10-17. Base: Mongoeco 4.10.1,
`738fd0eb5ece0adabf071e312ee0ff7b82b8589d`.

Conserva ID y estado. No hubo aceptación de la revisión 1 ni se publica un SPI
sucesor. El encargo posterior del usuario de 2026-10-17 autoriza implementar
T1–T6 conservando API, SPI v2 y archivos SQLite. El
[registro de implementación](../../engine-preparation-validation.md) posee el
avance y sus gates; no se simula una transición formal de este registro ni se
cambia ADR-017.

## Decisión posterior a la propuesta

Tras completar T1–T6 y S1/S2, el usuario acuerda el 2026-10-17 **mantener SPI v2
y aparcar SPI v3**. La [decisión vigente](../engine-spi-v3-proposal.md) posee
esa disposición y los requisitos para reconsiderarla. Este ADR conserva su ID,
el estado documental anterior y el alcance revisado como antecedente; no hay
una aceptación retrospectiva de la revisión 1 ni una migración a v3 pendiente.
El apartado siguiente de decisión propuesta describe la revisión 2 histórica.

## Contexto

La propuesta original comparaba un núcleo de requests A traducido a v2 con un
B que añadía una frontera v3. Exigía a B reducir el total de puntos de coherencia
mientras conservaba A para motores v2: el criterio mezclaba el coste temporal de
convivencia con el estado final de una retirada todavía no decidida.

Además, A ya introducía las requests de B: no era el control mínimo. La revisión
confirma mejoras concretas posibles con el SPI actual: propagación de contexto y
dialecto, construcción controlada de planes, diferencias de vida entre contexto
y deadline, rutas directas de planning y semántica SQLite preparada dos veces.
No hay evidencia suficiente para atribuir los errores recientes a la versión SPI
ni para justificar una ruptura para autores de motores.

La [propuesta técnica vigente](../engine-preparation-proposal.md) posee el diseño
T1–T6 y la aceptación. La [síntesis de los tres informes](../spi-proposal-review.md)
conserva hallazgos, contraste y disposiciones; la
[revisión 1 archivada](../../evidence/spi-v3-proposal/revision-1.json) preserva
texto y hashes para no perder la historia del cambio de alcance.

## Decisión propuesta

Elegir **A mínima**: refactorizar productores y preparación existentes conservando
API, tipos y firmas SPI v2, canario, protocolos, capacidades y ficheros SQLite.

1. Consolidar contexto y conversión de dialecto en productores internos.
2. Conservar coherencia filtro/plan y ownership sin una jerarquía nueva de requests.
3. Caracterizar y conservar plazos por ruta, incluidos diferimiento y rewind.
4. Concentrar llamadas de planning/explain en el adapter interno sin renombrar
   primitivas ni exigir planning a todos los motores.
5. Reutilizar en el helper SQLite la semántica de mutación ya preparada.
6. Ejecutar un piloto representativo, corpus diferencial y cierre global del
   candidato, sin presentar proyecciones como mejoras medidas.

Cerrar B para este alcance. No se crean nuevos exports, protocolo v3, selector
público de versión, evaluador universal ni perfiles duales. Un sucesor requiere
nueva evidencia, demanda/garantía para autores externos y decisión independiente
con migración/retirada explícita. V2 continúa estable; no se declara definitivo.

Aprobar este texto autorizaría el plan interno y sus correcciones concretas;
no una entrega 5.0, retirada contractual, commit, push o publicación.

## Trazabilidad documental

ADR-017 es la autoridad vigente del SPI exclusivo. ADR-016 queda como antecedente
histórico; su ciclo de convivencia no se importa como autorización actual.

La primera revisión retiró de engine-spi-v2.md la frase prospectiva «En 5.0 esos
duplicados deben desaparecer de los comandos ejecutables». Esa edición se
clasifica aquí expresamente como corrección de una promesa de planificación
histórica, no como cambio de firmas o del contrato ejecutable v2. No se declara
cumplida ni anulada una migración aceptada a v3: esa migración no existe.

La mención de timeout en ADR-017 describe un invariante de ejecución, pero el
objeto OperationContext actual no contiene ese campo. Su aclaración editorial
conserva el cuerpo y estado de la decisión; T3 no mueve límites al contexto.

## Consecuencias

- Menos preparación redundante sin imponer una migración a motores externos.
- Los DTO expuestos conservan duplicados y guardas cuando los exige compatibilidad;
  el beneficio se mide en productores y rutas implementadas, no en renombres.
- Rewind puede conservar contexto y renovar deadline: no se fusionan sus vidas.
- La evaluación del selector por candidato y la atomicidad continúan. El objetivo
  SQLite es evitar la segunda preparación del mismo intento, no retirar el CAS.
- Errores, outcomes, snapshots, cambios y formatos persistentes mantienen garantías.
- En la revisión 2 estaban pendientes rendimiento, ahorro total y matrices.
  Los registros posteriores de [T1–T6](../../engine-preparation-validation.md)
  y [S1/S2](../../engine-boundary-validation.md) poseen los resultados y límites;
  no acreditan una mejora general de latencia ni la necesidad de v3.

## Alternativas descartadas

- A con las requests completas de B: añade traducción sin garantía diferencial.
- SPI v3 paralelo ahora: utilidad no acreditada y coste de convivencia omitido.
- Ratificar v2 como contrato inmutable para siempre: conclusión no sustentada.
- Capturar todos los plazos al despacho: cambia fases actuales y potencialmente rewind.
- Congelar todo BSON, añadir tokens o un reloj público: mecanismos sin necesidad probada.
- Retirar tipos no reexportados sin revisar sus usos: FindOperation es entrada SPI
  de planning y conserva obligaciones transitivas.
- Reescribir Search, motores o planner para justificar una major: fuera de alcance.

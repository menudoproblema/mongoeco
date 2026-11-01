# Relectura sobre 3c4032b: diagnóstico pendiente por consumidor

1 de noviembre de 2026. Revisión documental del mismo adjunto de la autocrítica,
SHA-256 `a53e71d1c6b1b1c18ff87809edb637a003dd2449ea861c64a9506cdd503b6bca`.
El [receipt](receipt.json) identifica alcance y comprobaciones. El
[plan](../../../bson-semantics-consolidation-plan.md#contraste-actual-y-orden-mínimo-de-ejecución)
posee las decisiones y el orden; este documento solo conserva evidencia.

## Disposición del informe

- Las retractaciones sobre causa de H48, $in/distinct y propiedad desde D00 se
  mantienen. Los cortes D01a/b, D02, D03a/b y D04 ya tienen aceptación funcional
  acotada; no describir los fallos históricos como defectos actuales.
- SQL eq/in declina binarios mediante binary_parts; se conserva fallback, sin
  reintroducir pushdown ni ampliar su dominio. El parser común diferencia la
  gramática del literal DBRef de la regla del decoder SDK.
- El [parser oficial r8.0.0](https://raw.githubusercontent.com/mongodb/mongo/r8.0.0/src/mongo/db/matcher/expression_parser.cpp)
  consultado en esta revisión confirma primer campo, contexto y nombres para
  reconocer DBRef. No impone los tipos que requiere su materialización SDK.
  La identidad de los 16 archivos fijados conserva la verificación histórica de
  [revisión 15](../review15/sources-check.json); no se vuelven a descargar aquí.
- TTL actual usa coordenadas enteras y residual exacto. No aceptar la cota de un
  milisegundo ni llamarla irrelevante; inclusión, expiración y cadencia son
  garantías diferentes. D02 ya acredita migración/reapertura con su evidencia.
- Watch conserva clase/código/fase/contexto y su oráculo RS. H53 permanece en
  C03/B08: ausencia de warning no demuestra rechazo incorrecto de escrituras.
- Las 167 pruebas y las anteriores caracterizaciones siguen siendo historia.
  Fábricas y consumo/propiedad de cursores tienen aceptación propia posterior a
  revisión 16; no se vuelven a enumerar como arreglos pendientes.

## Comprobaciones de esta revisión

Pasan **97 pruebas focales** ([log](focal.txt)): propiedades Binary/documentos,
gramática, traductor SQL, migración TTL, contexto watch, lookup físico y
conservación de candidatos vectoriales. Es regresión local, no gate integrado.
Comando reproducible desde el checkout:

```sh
.venv/bin/python -m pytest -q tests/property/test_bson_binary_coherence.py tests/property/test_bson_document_keys.py tests/unit/api/test_command_bson_grammar.py tests/unit/core/test_sqlite_query.py tests/integration/api/test_ttl_exact_millisecond_migration.py tests/integration/api/test_watch_context_contract.py tests/unit/core/test_lookup_physical_plan.py tests/property/test_search_candidate_conservation.py
```

La [sonda de representación](representation-probe.py.txt) verifica IDs eq/in y
contenido/subtipo de distinct en seis combinaciones motor/dialecto, con 18
trazas de documento/operando ([resultados](representations.json)). Sus aserciones
no convierten las clases privadas en contrato público.

La [sonda de consumidores](consumer-probe.py.txt) reproduce mecanismos locales
([resultados](consumer-probe.json)):

1. La clave de grupo rechaza el DBRef que produce DocumentCodec.decode; la clave
   BSON común acepta la misma vista documental. Con collation, DBRef SDK y dict
   tampoco recorren la misma política de String. El problema está en el
   consumidor, aunque el helper de igualdad común y sus propiedades pasen.
2. El índice de conjuntos 7 considera ausente bytes frente a Binary 0 aun cuando
   la igualdad del dialecto devuelve true. Subtipos 0/5 siguen distintos: no hay
   evidencia aquí de colapso entre esos dos subtipos.
3. Code entra por isinstance(str) en el mismo atajo y falla al construir el set.
   Los controles 8/9 usan residual y no reproducen estos dos fallos.

Son testigos de helpers actuales, no reproducciones públicas diferenciales.
El plan exige estas últimas antes de aceptar las correcciones. El corpus y sus
capturas preexistentes no son evidencia de esta revisión ni quedan aceptados por
leerlos; sus hashes están en [preexisting-workset.json](preexisting-workset.json).

La refactorización mínima propuesta reutiliza document_mapping y los átomos de
clave para los dominios ya elegibles; conserva las guardas y los fallbacks.
Spill necesita además coherencia de serialización; lookup/Search necesitan
inclusión de candidatos y prueba del plan alcanzado. No se crea otro comparador,
runner, normalizador, autoridad de admisión o formato de clave persistida.

No se modificó producción ni el trabajo de tests preexistente. No se ejecutaron
nuevos servidores, capturas nativas, wheel, matrices completas, cobertura o
benchmark. Los cierres anteriores conservan sus receipts y límites propios.

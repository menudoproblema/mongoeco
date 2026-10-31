# D04: preparación y contexto del stream

Corte de preparación/contexto implementado; **D04 sigue abierto** para imágenes,
DDL/cierre por ámbito y fixture replica set de CI. El
[plan](../../../bson-semantics-consolidation-plan.md) posee las obligaciones y el
[registro](../../../bson-consolidation-implementation.md) conserva su estado.

## Mecanismo y review

Reutilización/composición de PreparedPipeline, compilación y contexto existentes.
No otro AST, contexto, runner ni cambio SPI v2. Watch prepara antes de registrar
un watcher, conserva su allowlist, captura dialecto y reloj una vez al abrir y
materializa con codec_options. La ejecución retiene el token original; errores
de evaluación y alteración del token cierran el cursor con código nativo.

La validación estructural de project/group/replaceRoot y colisiones de paths se
comparte con aggregate/update. Se retiran validaciones duplicadas de group/root
y el validador propio de watch. El catálogo de stages reconocidos es distinto
de las capacidades físicas del dialecto: conserva Search y extensiones
registradas. No se modifican las políticas particulares de esas superficies.
La revisión encontró pérdida de now_factory al reconstruir collection.database;
el handle derivado ahora lo conserva, también para aggregate ordinario.

Workset fijado en workset.json sobre base de código bc8fb1c (HEAD documental
63e3879 al aceptar). Los 281 hashes Python productivos son idénticos a los
usados en el gate integrado final; producción no cambió durante los gates.
No se añaden allowlists de reloj ni de emisores de errores para acomodar cambios.

## Aceptación discriminante

Cinco acciones permanentes del registro/capturador/replay existentes tienen
capturas nuevas y --check para replica sets 7.0.43/8.0.32/9.0.2, FCV respectivo:

- preparación sin eventos: entradas frontend, forma/operador/stage, errores de
  query/expresión, variables/constantes, colisiones y replaceWith válido;
- evaluación: resultado BSON, raíz inválida, token eliminado/alterado y error
  dependiente del evento, con fase/familia/código y alive;
- matching tipado de null/undefined/missing, respetando diferencias nativas 7/8/9;
- reloj por colección/DB/cliente, entre etapas, dos eventos/getMore vacío y
  reanudación con reloj nuevo;
- validación compartida con aggregate/update vacíos y poblados; no se fuerza un
  error dependiente del documento en una actualización que no encuentra nada.

Tests deterministas sync/async × ambos motores × tres ámbitos verifican reloj
inyectado capturado una vez, ownership del pipeline, codec_options y handle
derivado; overrides de dialecto y ausencia de watcher tras error de apertura.

La base archivada bc8fb1c falla 56 checks y pasa siete controles/metadata sobre
el corpus final. El candidato pasa 6.019 checks integrados de semántica (incluye
63 del corpus nuevo) y 2.676 de core/propiedades/contexto/entrega con 936 subtests
y un skip local. Pasan 146 checks adicionales de componentes, typing público y
boundary, lint sin deuda nueva; 82 watch/contexto con orjson. SDK-free real 3.13
pasa dos smokes de imports y un recorrido de watch válido/inválido en seis pares
motor/dialecto. Los observadores protegidos y fixtures históricos no cambian.

Los intentos intermedios y sus fallos se conservan. El global de feedback se
interrumpió tras modificar fuentes; no se cuenta como aceptación. Solo los
logs final-* y los checks identificados en receipt son aceptación del candidato.

## Límites que conservan su owner

Las sondas full-document-probe son diagnóstico, también repetido con DDL en
revisión 8 del plan: documentan errores abiertos, no casos verdes. Este corte
no acepta fullDocument modes, invalidate/rename, resumeAfter/startAfter de token
invalidate, persistencia de imágenes/DDL, jobs nativos RS ni watch wire.
No se atribuye cierre de D04/D05/B07 ni paridad distribuida.

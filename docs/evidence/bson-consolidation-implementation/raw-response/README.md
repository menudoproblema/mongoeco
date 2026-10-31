# D06: lecturas raw, apertura y cantidades

Base `7f8110a`; corrección `30c8ae2`. El [receipt](receipt.json) identifica el
candidato y sus gates; el [registro de implementación](../../../bson-consolidation-implementation.md)
posee el estado del programa. Este corte no acepta todo D06.

Los seis testigos permanentes observan bytes BSON concatenados y framing con
un parser mínimo independiente, conservando SHA-256 y tamaños de cada tanda.
Cubren DBRef ordenado/invertido/extras/null, Code scope y Binary 0/5; un decoder
de enteros mutante tiene control de lectura decodificada activo. No se usa un
roundtrip productivo como oráculo ni se generalizan las llamadas de este decoder
a todos los posibles tipos/metadatos del SDK.

- Find raw conserva el iterador y omite codecs de contenido; por defecto abre
  con hasta 101 documentos y los getMore sin cantidad fijada no repiten ese límite.
- Aggregate raw abre sin datos; getMore con tamaño solicitado 1 se promueve a 2,
  como PyMongo. El `$merge` soportado ejecuta al abrir, incluso con batchSize 0.
- El transporte codifica `cursor.id` como int64, incluido cero. La entrada de
  getMore/killCursors normaliza esos controles sin borrar los anchos de enteros
  de filtros o documentos. Se conserva el cierre y el error original ante fallos
  de cleanup/cancelación; un fallo de close explícito permite reintento.

Las seis acciones se capturan y comprueban en los tres majors. La baseline
realmente aislada falla 84 checks y pasa 63 controles; el candidato y el wheel
instalado pasan 147. El replay integrado pasa 7.201 checks. Core/wire/API pasa
3.202 con un skip y 1.842 subtests; opciones/fronteras pasa 466 y 108 subtests.
Python 3.13 sin SDK ejecuta 96 escenarios previos y 24 guardas raw de dependencia.
Typing, SPI y lint pasan, y el manifiesto público efectivo coincide con la base.

`native-final.tar.gz`, manifest del corpus y hashes de módulos/workset fijan las
entradas. Los logs de feedback separan errores de setup/expectativas antiguas
de defectos del producto. Los observadores protegidos y goldens históricos no
se modifican. `native-cleanup.json` verifica ausencia de los 15 contenedores y
30 volúmenes propios, incluidos los cohorts del diagnóstico siguiente.

Las [sondas de bytes y callbacks](../response-frontier-diagnostic/README.md)
encuentran fallos pendientes y una diferencia 7 frente a 8/9. Su resultado no se
oculta con estos gates verdes. Falta incorporar esos testigos al corpus y corregir
la formación de respuesta antes de aplicar codecs, además del resto de contextos,
consumidores D06c y gates integrados D05.

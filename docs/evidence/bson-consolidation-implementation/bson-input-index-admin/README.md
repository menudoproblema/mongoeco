# B07 — Administración de índices: admisión y dueño de codecs

Base `bab8758`; código `c715ae7`. Se contrastan cuatro entradas públicas (`list_indexes`,
`index_information`, `drop_index`, `drop_indexes`) en Memory/SQLite,
sync/async y wire. Este corte acredita sus frames cliente, los dueños de
codificación/decodificación observados y los efectos de rechazo/commit;
no cierra B07, el default UUID ni toda la semántica positiva de los listados.

La causa era la ausencia de admisión cliente en estos wrappers, junto con la
reutilización de codecs de colección en listados que el SDK ejecuta con opciones
propias. Añadir el codificador directamente habría hecho que los comandos internos
volvieran a codificar y a materializar. Por eso se comparten la lectura preparada
de catálogo y el ejecutor preparado de retirada, sin flags públicos para omitir
validación ni otra autoridad BSON.

## Contrato observado

- Listados e información codifican con UUID UNSPECIFIED y decodifican documentos
  SON, ignorando fábricas/TypeDecoders/UUID de la colección. `index_information`
  deriva su resultado del listado: raíz dict y key como lista de pares.
- La retirada usa las opciones de la colección. Las formas cliente str/list se
  resuelven antes de codificar; las sesiones terminadas fallan antes del frame.
  Un comentario no codificable no elimina índices ni alcanza el motor.
- La fábrica de la respuesta de retirada se ejecuta después de la operación:
  su fallo conserva el borrado. No se ejecutan TypeDecoders personalizados.
  El DTO compartido entrega nIndexesWas, msg opcional y ok en orden nativo;
  msg es `non-_id indexes dropped for collection`. No se simula la respuesta
  descartada con un documento que solo contenga ok.
- Las entradas de comando preparadas leen/retiran sin reingresar en wrappers
  públicos. Los comentarios BSON válidos no se restringen a cadenas ni en el
  parser ni en el proxy. El manifiesto de API permanece idéntico.

## Evidencia y límites

El receipt conserva resultados terminales, runtime del servidor, wheel/hash,
fuentes, observadores protegidos, nodos fallidos pendientes y recursos.

- Cuatro acciones, 127 observaciones por versión en MongoDB 7.0.43/8.0.32/9.0.2
  standalone, PyMongo 4.19.0. Captura y segunda ejecución `--check` coinciden.
  El testigo de fábricas observa asignaciones y valores, además de invocaciones.
- Base del corpus final: 84 fallos/15 aciertos. El adaptador async delega realmente
  los tres métodos que le faltaban; no inventa respuestas ni modifica el observador.
- 16 pruebas de frontera: rechazo previo al SPI, índice único intacto, barrera de
  eventos, reapertura SQLite, una codificación pública y ninguna en el ejecutor
  preparado. Las expectativas unitarias previas de comentarios string se sustituyen
  por aceptación positiva acreditada nativamente, conservando errores de forma.
- Perfil real sin SDK en Python 3.13.15: 738 observaciones. Omite el constructor
  Code del SDK; adapta la representación de Decimal128 BID a texto decimal y el
  tipo Int64 al int portable. El observador de producción no se modifica.
  Las seis familias BSON anteriores se vuelven a ejecutar en este perfil.
- Los gates amplios rojos conservan los nodos pendientes del bloque anterior;
  no equivalen a aceptación global D05. El intento de unitarias sin permisos TCP
  se interrumpió y se conserva como feedback; no acredita el candidato.
- Las primeras ruedas y capturas son feedback previo al testigo de campos de
  respuesta. Solo la última wheel y `native-response` acreditan este corte.
  El coste ABBA×2 es diagnóstico bajo carga concurrente, no aceptación de presupuesto.
- Los 287 fixtures/observadores anteriores permanecen intactos. Se confirma con
  Docker operativo la ausencia de los 12 contenedores y 24 volúmenes de cuatro
  rondas; los errores de observación iniciales no se presentan como fugas.

Quedan 13 entradas del censo administrativo histórico sin disposición (incluye
las tres delegaciones de base de datos). Estas cuatro admisiones no aceptan por
asociación sus respuestas completas, errores servidor positivos, namespace,
validate ni Search. El default global continúa en STANDARD hasta integrar todas
las fronteras. Positivos/batching, otros contextos D06, H53, decisión D07 y
matrices/cobertura/costes integrados D05 conservan sus owners.

Resultado instalado final: 72 failed, 9759 passed in 272.00s (0:04:32); cohorte:
2 failed, 2165 passed, 4688 subtests passed in 105.94s (0:01:45). API: 18 pruebas/13 subtests; typing: dos positivos y
20/25 negativos; lint sin infracciones nuevas en 13 archivos Python. Los 99
checks del corpus nuevo pasan dentro del replay; no se sustituyen los fallos
obligatorios por xfail ni se declara un gate global verde.

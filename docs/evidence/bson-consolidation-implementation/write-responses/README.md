# B07: DTOs positivos, orden del sobre y diagnóstico de escritura

Corte aceptado en `05b3f655`, sobre base `63e625da`. El
[receipt](receipt.json), las [fuentes](runtime-sources.json) y la
[wheel](wheel.json) identifican la verificación. La baseline instalada conserva
el runtime de `dbe8257f`, no una aproximación de su código.

## Causa y corrección compartida

Los DTOs públicos de escritura y `BulkWriteResult.bulk_api_result` ya conservaban
los resultados de los controles añadidos. No se reimplementan. Los comandos
compartían otro fallo: `WriteCommandResult.to_document` colocaba `nModified`
antes de los resultados de upsert y errores. Los tres servidores devuelven:

- Upsert: `n`, `upserted`, `nModified`, `ok`.
- Error: `n`, `writeErrors`, `nModified`, `ok`.
- Ambos: `n`, `writeErrors`, `upserted`, `nModified`, `ok`.

El orden se corrige en el DTO existente, utilizado por las rutas directas y wire.
No se ordenan las claves del observador ni se ocultan metadatos.

Los controles de error mostraron dos huecos relacionados:

1. El guard clásico de `_id` emitía un WriteError sin diagnóstico semántico. Ahora
   conserva esa familia en el núcleo y añade la razón interna `immutable_id`.
   `operation_error` reutiliza la política existente: prefijo de findAndModify
   en 7/8/9 y de update en 9. No se infiere el contexto analizando el texto ni
   se duplican prefijos en command/bulk/API.
2. FindAndModify materializaba su error como una respuesta write convencional,
   descartando la fábrica documental. Reutiliza `materialize_command_response`:
   mantiene los efectos de la fábrica y descarta los TypeDecoders como el SDK.
   Los renderers de diagnóstico/fallback conservan `ok`, `errmsg`, `code`,
   `codeName` antes del contexto, con precedencia de los metadatos propios.

El rechazo conserva el documento. Los controles distinguen valor final, orden,
clase/código/mensaje, efectos de fábrica y llamadas a decoders. El código público
no incorpora otro comparador, codec o clase de resultado.

## Contraste nativo y casos

MongoDB standalone 7.0.43, 8.0.32 y 9.0.2: cuatro acciones y 95 observaciones por
versión, capturadas y comprobadas con el fixture/capturador existente.

| Acción | Observaciones por versión | Contrato observado |
| --- | --- | --- |
| write_dto_options | 35 | Insert/insert_many/update/replace/delete y upsert Date/null; propiedades públicas y estado |
| bulk_dto_options | 10 | Familias mixtas, ordered/unordered, índices de upsert, contadores y bulk_api_result completo |
| command_write_options | 30 | Insert/update/upsert/delete, error y mezcla error/upsert; orden y contenido del sobre |
| write_immutable_error_options | 20 | Update/update_many/findAndModify/bulk; familia/código/mensaje y efectos de la fábrica |

Cinco perfiles: predeterminado, TypeDecoders activos, fábrica activa, ambos y
un decoder que falla si se invoca. Los writes convencionales no ejecutan esos
callbacks; el error findAndModify construye su sobre documental y conserva el
orden de asignación. Builtins como datetime aware siguen aplicándose donde
corresponde. El observador conserva estado y contenido tipado, no solo tamaños.

Las copias de comprobación de [native](native/) coinciden exactamente en
`source`, `targetDialect`, `corpus`, `case_manifests` y `cases`, además de
BuildInfo. `hello` conserva hora/conexión variables: los archivos completos no
son idénticos byte a byte. Las cuatro rondas conservan manifests y claims
propios; solo la ronda frozen de cuatro acciones acredita el corpus final.
Los scripts archivados son provenance histórica; para verificar de nuevo una
referencia congelada se usa `--check` con salida distinta.

## Verificación sobre la fuente final

- [Baseline instalada](baseline-frozen.txt.gz): 48 fallos y 51 correctas, con
  los cuatro testigos finales y sus tres controles de procedencia. Sus
  [287 fuentes](baseline-runtime-sources.json) coinciden con `dbe8257f`; se
  conservan [wheel](baseline-wheel.json) y [pruebas de importación](baseline/).
- [Focal checkout](feedback2.txt.gz): 99 correctas tras la corrección.
- [Replay instalado](replay.txt.gz): 10.759 correctas, incluidos los 99 checks
  nuevos. [Cohorte](unit.txt.gz): 2.457 correctas y 4.709 subtests, cero fallos.
- [Identidad integrada](identity-integration.txt.gz): 12.068 correctas.
  [Identidad del núcleo](identity-unit.txt): 53 correctas, incluida la familia
  WriteError del guard antes de adaptarla a su operación pública.
- Wheel SHA-256 `71349b8777261fc8dc744d7a6bd66762b4589886bc4dfbde3eccc28e3b228374`:
  287 módulos verificados por hash y [origen de importaciones](installed/).
- [SDK-free](sdk-free-all.txt): 570 observaciones en Python 3.13.13 con `-I -S`,
  wheel instalado y ausencia comprobada de bson. Los cuerpos de las funciones
  no cambian. La clave de decoder Int64 se vincula al int público portable;
  el corpus no contiene valores Int64 ni Code, comprobado antes de comparar.
  Eso no acredita un constructor ni un hook Int64/Code inexistentes sin SDK.
- [API](api.txt): 18 correctas/13 subtests; [typing](typing-installed.txt) y
  [ratchet](lint.txt) correctos. Los [415 archivos protegidos](protected.json)
  siguen intactos. No se modifican referencias históricas para aceptar el fix.
- [Recursos](resources.json): los 36 recursos Docker de 12 manifests retenidos
  están ausentes por ID; los 12 claims coinciden con su owner. No se reutiliza
  ni sobrescribe ningún directorio de evidencia.

No es cierre integrado D05, ejecución remota CI ni aceptación de watch sobre
standalone. Siguen los fallos de constructores/setters en respuestas de
escritura/comandos, los contextos restantes D06 y las disposiciones H53/D07.
Los efectos de una fábrica correcta no aceptan su fallo ni su precedencia
frente a un error servidor; el siguiente corte debe observar también el estado
confirmado cuando la decodificación falla después de escribir.

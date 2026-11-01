# B07: codificación de lecturas y solicitudes find diferidas

Candidato sobre `ecfac84`. Corte funcional de lectura y componente común de
entrada; no completa la admisión de escrituras/comandos ni cambia el default
UUID STANDARD. El receipt final conserva el estado rojo restante de B07.

## Cambios y causas

- `api/bson_input.py` separa codificación de solicitudes del codec reversible
  interno. Usa el encoder SDK cuando existe y el lector BSON raw para conservar
  orden, Binary cortos y forma documental; no aplica codecs de usuario a la
  entrada. Adapta los tipos BSON públicos locales; no admite wrappers numéricos
  privados como entrada pública. El perfil portable valida sin instalar SDK.
- Las lecturas find/find_one/count/distinct/aggregate codifican una vez por
  solicitud. La forma de argumentos conserva su owner; proyección por nombres
  se convierte antes de codificar y sort/hint mantienen su representación API.
  Los opcionales nulos se omiten del cuerpo codificado; el valor original None
  y los null anidados conservan su semántica. Distinct usa el constructor de
  operaciones preparadas, evitando entrar otra vez por el frontend find.
- Find conserva las referencias vivas hasta ejecutar. Cerrar no codifica;
  clone toma su snapshot, rewind prepara otra ejecución y explain usa un clon
  independiente. Reutiliza AsyncCursor._as_operation y su caché/contexto, sin
  otra clase de cursor o marca pública para saltarse la admisión.
- Compilación y binding usan el contexto final, una sola vez. Un fallo de
  contexto no publica una operación parcialmente preparada. Los cursores
  internos ya preparados conservan su ruta y el fallback de agregación retiene
  el artefacto/contexto, sin serializar NOW como declaración let de cliente.
- Find_one acepta el ID escalar como el driver, incluidas sus formas BSON y
  rechazos de codificación. El flag regex BSON u del re.Pattern de Python se
  acepta en el compilador de regex compartido de consulta.
- Search consume parámetros numéricos normalizados mediante unwrap_bson_numeric
  en sus consumidores. Conserva enteros precisos, dominio local y filtros MQL
  sin aplanarlos; Decimal/bool no entran en el dominio vectorial. No acredita
  semántica Atlas ni cambia prefiltros, formatos o desempates persistidos.

## Oráculos

El corpus nuevo bson_request congela tres acciones y 39 observaciones por
versión: referencias de filtro/proyección/let/comment, clone/rewind/explain,
regex/binaries cortos y find_one por ID. Referencias y checks 7.0.43/8.0.32/9.0.2
son idénticos. Se mantienen intactas las referencias BSON históricas y el corpus
bson_admission rojo previo. Las primeras rondas del corpus nuevo son feedback;
la cuarta fija la fuente final. Se verifican ausentes 12 contenedores y 24
volúmenes propios con Docker operativo.

El baseline ecfac84 falla en 48 replays del corpus nuevo y pasa 27 checks. El
subset portable real Python 3.13.15, sin módulo bson, contrasta rechazos/fases,
resultados y representación UUID con referencias nativas/bytes SDK. Usa
UNSPECIFIED explícito: no disimula el default pendiente.

## Verificación y alcance pendiente

El receipt conserva fuente/wheel exactos, pruebas instaladas y fallos restantes.
Los gates de desarrollo que detectaron regex u, Int64 Search y snapshots de
explain no aceptan el candidato posterior por asociación. Solo el último
artefacto congelado acredita este corte.

B07 mantiene escrituras por grupo bulk/insert_many, comandos/índices/validators,
corte UUID coherente y consumidores positivos de proyección/sobres/resultados.
El corpus previo sigue siendo obligatorio y rojo: no hay gate integrado verde
ni aceptación global D05. D06 restante, H53 y decisión D07 conservan sus owners.
Las mediciones de coste son diagnósticas; los incrementos en lecturas pequeñas
Memory quedan en D05, junto a los presupuestos anteriores todavía pendientes.

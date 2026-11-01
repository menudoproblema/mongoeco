# B07: frontera de codificación, fase y lotes

Caracterización sobre `2ae4b56`, sin cambios productivos. B07 sigue abierto.
El [receipt](receipt.json) identifica fuentes, capturas, baseline y límites.

Seis acciones y **676 observaciones por versión**, contra MongoDB standalone
7.0.43/8.0.32/9.0.2, con imágenes/FCV registrados y captura/check finales
idénticos. Hay controles de lecturas, escrituras, opciones/índices/validators,
comandos raw, modos UUID y lifecycle del cursor. El observador conserva tipos,
orden de campos, BID, estado y fase. Solo sustituye el namespace efímero del
sobre del cursor después de verificar su valor original. Los objetos imposibles
se describen sin repr/direcciones; las Strings usan bytes UTF-8 observados para
poder registrar surrogates rechazados sin romper la evidencia.

## Invariantes que fijan el diseño

- Un filtro inválido de find falla al consumir; find_one y aggregate fallan al
  invocar. Cerrar sin consumir no codifica. Clone/rewind/explain tienen testigos.
  Cambiar raw Decimal a 1 antes de iterar permite la consulta; el cambio inverso
  la rechaza. Validar/capturar un error al crear el cursor sería incorrecto.
- Insert_many y los grupos bulk de una misma familia se codifican antes de sus
  escrituras: un segundo valor inválido impide también la primera escritura
  válida del grupo. En bulk mixto, un grupo insert previo puede quedar confirmado
  antes del error de codificación del grupo update. No validar todo el bulk al
  principio ni convertir el error cliente en un error parcial del servidor.
- InvalidDocument, OverflowError, UnicodeEncodeError y ValueError UUID son
  categorías distintas. El codec interno no puede endurecerse globalmente: lo
  usan también valores producidos internamente y entradas wire ya codificadas.
- Los controles positivos muestran problemas adicionales de sus consumidores:
  proyección calculada de find, tipos/orden del sobre raw find, orden n/
  nModified/ok de update y resultado bulk público. No se borran/sortan esos
  campos del observador para hacer pasar el caso.

## Resultados y límites

El baseline Git aislado ejecuta las cuatro superficies × dos motores × tres
versiones: **96 fallan y 51 pasan** en 40,57 s. Son 72 fallos directos, 24 wire
(opciones/comandos), 48 controles wire correctos y tres controles de metadata.
Las APIs wire ya rechazan en el SDK los objetos imposibles: esos rechazos no
son defectos de su servidor. Las sondas locales y wire separan los mecanismos
([local](local-differences.json.gz), [wire](wire-differences.json.gz)).

Se conservan cinco rondas: las dos primeras detectaron namespaces aleatorios
sin normalizar en el observador; las tres siguientes verificaron el corpus
progresivamente ampliado. El [audit](resources.json) comprueba con Docker
operativo que los 11 contenedores y 22 volúmenes propios están ausentes.
No se modifican capturas históricas, normalizadores ni observadores protegidos.
El adaptador async añade explain y delega al método real; lifecycle wire pasa.

Esta es evidencia previa a implementación: el corpus obligatorio nuevo queda
rojo, sin xfail ni exclusión de superficies. No se acepta B07/D06, no se presenta
un candidato corregido ni un gate global verde, y no se ejecutan matrices,
cobertura o benchmarks. El orden y los owners de implementación están en el
[plan](../../../bson-semantics-consolidation-plan.md#b07--admisión-en-la-fase-y-el-lote-correctos).

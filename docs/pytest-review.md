# Revisión del kit pytest y del proxy

Este informe conserva un candidato histórico anterior al cierre. El estado de la
entrega y el candidato final se registran en [release-4.10.0.md](release-4.10.0.md).

Revisión solicitada el 15 de octubre de 2026 sobre el candidato del
[cierre inicial](pytest-integration-validation.md), antes de decidir la siguiente
publicación. Se preserva el contrato K01–K20 y el trabajo anterior autorizado.

## Defectos observados y corregidos

| Caso | Causa e impacto | Corrección y regresión |
| --- | --- | --- |
| R1: scope efectivo del test async | Un marcador sin `loop_scope` se trataba como función aunque el default global fuese módulo. También se ignoraba `scope`, aún reconocido por el runner. Se aceptaban tests cuyo loop no coincidía con el de la fixture. | Resolver marcador, opción heredada y default con la precedencia del runner. Rechazar scopes incompatibles antes de crear el servidor. Cuatro variantes negativas, Memory/SQLite, y un positivo con override explícito a función y CRUD real. |
| R2: cancelación de cierre esperando al arranque | La protección comenzaba después de adquirir el bloqueo de lifecycle. Cancelar durante la espera abandonaba el cierre y permitía que el arranque concurrente dejase el servidor abierto. | Proteger también la adquisición del bloqueo. La regresión pausa el connect real, cancela dos veces el cierre y exige completar la desconexión antes de propagar la cancelación. |
| R3: cancelación después del bind | `asyncio.start_server` habilitaba accepts y cedía el control antes de devolver el servidor. Una cancelación en ese punto dejaba el socket sin referencia en el proxy, fuera del rollback. | Crear con `start_serving=False`, guardar la referencia y después activar accepts. La regresión cancela en el primer yield tras el bind de un socket real y comprueba que el listener desaparece. |

Son defectos locales del contrato existente. No requieren nuevos conceptos
públicos, dependencias, cambios en motores, SPI ni formato SQLite. Las seis
variantes negativas añadidas fallan antes de sus arreglos; el positivo evita
rechazar una configuración válida. Los logs de reproducción conservan esos
fallos como evidencia esperada, sin contarlos como gates verdes del candidato.

## Validación del candidato corregido

El wheel corregido tiene SHA-256
`414e6d2978fc8bf0dbbb33846fc6a9651c6d0df310e4c179359b6442703b1249`.
Conserva provisionalmente la versión de paquete 4.9.1 y no se ha publicado:
se distingue por ese hash del candidato inicial y del release existente.
Wheel y sdist son reproducibles byte a byte y sus fuentes coinciden con el
checkout y las tres instalaciones usadas por los gates.

| Gate | Resultado |
| --- | --- |
| Corpus del kit y lifecycle | 45 passed contra el wheel instalado |
| Python 3.13/3.14/3.15 × pytest 8.4.2/9.1.1 × PyMongo 4.9.2/4.19.0 | 12 celdas verdes, 540 ejecuciones del corpus |
| Canario anónimo, ambos motores, tres Python, 0/2/4 workers | 684 ejecuciones, sin fallos ni skips |
| Suite global, por intérprete | 22.404 passed, 14.450 subtests passed, 31 skips previstos |
| Unittest, por intérprete | 3.627 tests, OK, 3 skips previstos |
| Cobertura en Python 3.14 | 99,05 % |
| Typing estricto, manifiesto, snapshots y consumidor SPI | Correctos en los tres intérpretes |
| Instalaciones limpias | 9 con extras y una base sin pytest, PyMongo ni runners async |
| Ruff ratchet, twine, whitespace y configuración CI | Correctos |

El canario conserva sus 31 casos de negocio async y siete de construcción sync;
el CRUD sync lo comprueba el corpus público del kit por separado. Memory/SQLite,
ambos adaptadores async y paralelismo se vuelven a ejecutar con el nuevo wheel.
Los checks exclusivos de motores y los probes de firmas SDK conservan las
mismas entradas; el registro explica su reutilización causal. Los nuevos gates
no cambian los skips ni rebajan el umbral de cobertura.

## Alcance y límites de la revisión

Se revisaron proxy, helper de cleanup, runtime del hilo, plugin común, ambos
adaptadores, sus callers y regresiones, además de packaging, activación explícita
y canario. La base es el wheel identificado en el cierre inicial; el registro
de esta revisión conserva su hash y el manifiesto de fuentes final.
El workset se puede fijar aunque no tenga commit: los cambios previos permanecen
separados por identidad de runtime y no se atribuyen a esta corrección.

La revisión no ha identificado otros defectos relevantes pendientes dentro de
ese alcance. Esto no acredita ausencia absoluta de defectos ni ejecución de CI
alojada. Las ejecuciones locales usan macOS arm64. Este bloque no recaptura
MongoDB real ni cambia semántica de escrituras; las referencias nativas existentes
se reproducen en la suite global.

Los resultados finales, comandos, artefactos y reutilización causal de gates se
registran en [la evidencia de revisión](evidence/pytest-review/validation.json).
La identidad y las fuentes del consumidor siguen siendo privadas; la evidencia
committable usa únicamente un identificador anónimo, hashes y contadores.

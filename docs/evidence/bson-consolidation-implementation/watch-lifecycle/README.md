# D04: caracterización de continuidad y DDL sin efecto

Base productiva `f16ffb2`. No modifica producción ni acepta D04. El
[plan](../../../bson-semantics-consolidation-plan.md) posee el contrato y el
[registro](../../../bson-consolidation-implementation.md) mantiene el pendiente.

`generation-probe.py.txt` ejecuta seis combinaciones de modo/configuración
sobre update→drop→recreate con el mismo `_id` y un drop de colección inexistente.
Son 21 observaciones nativas y 42 locales en 7/8/9 × Memory/SQLite. Bases UUID
limpiadas en finally en los replica sets temporales propios 49973/49974/49975;
ningún cambio de configuración ni de la instancia habitual.

Los tres servidores leen el documento nuevo (v999) mediante updateLookup, incluso
si el evento pertenece a la colección borrada. El lookup local ya coincide:
esta sonda no justifica introducir una identidad de encarnación en el evento.
WhenAvailable/required conservan v1 si la imagen se capturó al escribir; sin
captura dan null/47 respectivamente. Es control de la implementación previa,
no aceptación completa de imágenes con DDL ni de las secuencias posteriores.

Drop de colección inexistente devuelve un batch vacío y mantiene el cursor
nativo vivo. Local publica invalidate y lo cierra: el productor debe determinar
el efecto real dentro de la mutación, no deducirlo de la llamada API.

La sonda anterior de
[protocolo](../watch-images/lifecycle-probe.json) conserva las diferencias de
iteración/context manager/resume_token, argumentos, cierre e invalidate filtrado,
resumeAfter 260 y startAfter sin entrega posterior local. El token anidado ya
coincide (280/cierre). Los siguientes testigos permanentes deben unir estas
observaciones con registro atómico y entrega compartida, ambos motores y
sync/async, sin crear otro contador ni cambiar SPI v2.

Diagnóstico sync, no otro runner ni regresión permanente. No se comparan valores
opacos de tokens/clusterTime, no se acepta rename/distribución ni se atribuye al
timeout aislado la prueba de la causa: el bypass de secuencias se deduce además
del código y debe tener traza/controles discriminantes al corregirlo.

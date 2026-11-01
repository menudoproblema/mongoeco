# Conversión Decimal: gramática, tipos y preservación BSON

Candidato sobre `a2c3cbb`. Este corte implementa la conversión escalar del plan;
no cierra D06, H53, D07, D05 ni B07. Los manifiestos identifican la fuente exacta.

## Causa y solución

- Decimal de Python acepta formas que el parser MongoDB no admite. La entrada
  String usa gramática ASCII y contexto Decimal128 propio: rechaza sNaN,
  payloads, separadores, espacios, Unicode lookalikes, overflow y underflow;
  permite redondeo de precisión como el servidor. Code no es String BSON.
- Double comparte la conversión de 15 cifras ya existente, conservando NaN
  negativo en esta frontera. Decimal almacenado conserva signo/señal/cohorte en los controles probados;
  no se prohíben NaN/Infinity como datos ni se cambia la comparación numérica.
  Date reutiliza el cálculo entero de milisegundos, incluidos negativos.
- `$convert` admite decimal también en 7/8: el gating exclusivo de 9 era
  accidental. `$toDecimal` participa en la preparación de constantes existente.
  `onError`, `onNull`, vacíos y ramas tienen testigos nativos propios; no se
  presume que una rama no seleccionada impida optimizar sus constantes.
- El entorno de pipeline conserva tipos de conversión BSON. La API directa ya
  no devuelve Decimal Python dependiendo de que wire codifique el resultado.
  Se reutilizan EvaluationEnvironment y su propagación lexical/subpipeline;
  los helpers de expresión independientes conservan su conveniencia Python.
- DocumentCodec serializaba el Decimal128 SDK mediante su presentación String,
  que convierte todos los NaN a NaN positivo quiet. Ahora serializa su Decimal
  subyacente: el tag/formato de almacenamiento no cambia. Los datos antiguos
  que ya perdieron bits no se reconstruyen por esta corrección.

## Oráculos y alcance

Seis acciones nativas: parsing de Strings, valores almacenados, Code, ramas,
updates pipeline repetidos y contextos let/facet/lookup. Directa sync/async y
wire sync/async × Memory/SQLite × dialectos 7/8/9. El observador comprueba tipos
y BID Decimal128; no convierte Decimal Python a Decimal128 para esconder fugas.
La unitaria que exigía rechazar Infinity se cambia al control positivo nativo.

Referencias 7.0.43/8.0.32/9.0.2 en standalone, imágenes fijadas y FCV registrados.
El corpus final se congela antes de captura/check; [native-checks.json](native-checks.json)
acredita coincidencia exacta. Las siete rondas conservan diagnóstico de desarrollo;
no recapturan ninguna referencia histórica commiteada. El [audit de recursos](resources.json)
verifica ausentes 21 contenedores y 42 volúmenes propios con Docker operativo.
Los 263 archivos protegidos preexistentes permanecen idénticos.

El baseline Git sobre `a2c3cbb` falla en los 144 replays públicos, con tres
comprobaciones de metadata correctas. La celda SDK-free real Python 3.13.15
pasa 228 controles basados en los resultados nativos y reapertura SQLite;
no simula instalar/eliminar módulos del SDK.

## Verificación final

Pasan **8.601 checks** del replay amplio instalado y **1.144 pruebas** de
unitarias/SPI/propiedades, con 181 subtests. Typing mantiene 2 controles positivos
y 20/25 errores negativos esperados; el manifiesto público no cambia. El lint
ratchet pasa sin actualizar baseline. Los proofs de imports verifican hashes y
sitio instalado en los cuatro workers, master y la sesión de unitarias.
El [receipt](receipt.json) y el registro de implementación fijan alcance y pendientes.
El benchmark escalar es diagnóstico, no aceptación de coste global D05.

La ejecución amplia de desarrollo conservada en development-wide-feedback.txt.gz
usa el wheel anterior y coincidió con cambios del corpus durante el feedback
(24 fallos / 8.577 correctas). No se usa para aceptar ni diagnosticar el candidato
final. Solo el gate posterior con runtime y corpus congelados puede aceptarlo.

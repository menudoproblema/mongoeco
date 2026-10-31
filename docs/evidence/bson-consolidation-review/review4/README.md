# Revisión actual del mismo informe

El hash del adjunto coincide con el input de revisión 3. Esta revisión distingue
su caracterización histórica del runtime ahora corregido parcialmente.

`probe.py.txt` deriva de la sonda de revisión 3, limitada a API directa y trazas
SQLite. Inserta datos únicamente en motores locales efímeros. No abre proxies
ni conexiones nativas. `observations.json` conserva resultados y SQL ejecutado:
48 observaciones de formas DBRef y nueve filtros binarios con contador de matcher.
No son capturas de aceptación ni nuevos goldens. `properties.txt` registra la
ejecución de las tres propiedades existentes, sin cambiar tests ni producción.

HEAD es 93b9b34; había cambios de fechas sin commitear. El receipt fija los
hashes del source efectivo y del diff; las conclusiones se limitan a las rutas
medidas. No se acepta ni se altera aquí ese trabajo de implementación.

Se relee expression_parser.cpp de r8.0.0: isExpressionDocument y sus callers en
campo, in y elemMatch. La comprobación de los 14 hashes se conserva como
resultado de revisión 3, no se declara repetida. No se repiten nativos, wire,
TTL, watch, suite global ni matrices de paquete.

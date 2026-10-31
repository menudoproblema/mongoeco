# Procedencia del kernel Decimal128

El módulo `src/mongoeco/core/decimal_math.py` reproduce pasos de redondeo de
Intel BID mediante `Decimal` y `Fraction` de la biblioteca estándar. No añade
backend, extensión nativa, IR de expresiones ni dependencia numérica opcional.
La licencia de los algoritmos adaptados se conserva en
`src/mongoeco/core/resources/Intel-Decimal-Math-LICENSE.txt` y en wheel/sdist.

Fuentes primarias consultadas: wrapper
[Decimal128 de MongoDB r8.0.32](https://github.com/mongodb/mongo/blob/r8.0.32/src/mongo/platform/decimal128.cpp)
y funciones
[BID vendorizadas](https://github.com/mongodb/mongo/tree/r8.0.32/src/third_party/IntelRDFPMathLib20U1/LIBRARY/src).
Se consultaron exp/log/log2/log10/exp2/exp10/pow y la conversión de dos partes
`bid128_to_binary128_2part`. Los hashes siguientes corresponden a los archivos
raw consultados, incluidos sus finales de línea originales.

| Fuente | SHA-256 |
| --- | --- |
| `decimal128.cpp` | `fa4296b23ca26165541b4fb52fd6feed3d842f8cf0079ce4be20b8c9aa58ca71` |
| `bid128-log.c` | `bf33dd219b1276c6fb4d4c4f541e9af80600ff2cb41c211cb6a4ecc668100fb6` |
| `bid128-log2.c` | `9f9419884190c0374d1a60377f44a553623c0a75903adce1b25e7e19b37fb4b4` |
| `bid128-log10.c` | `21c8c57c5cbb8f01f27a82b0c99c0b92fe77832e4dec24ec379ece2fa151634a` |
| `bid128-exp.c` | `f33e670fad991814578f7f1509b8d50bea6f8310b0b0bf8ba1759e765d945ed0` |
| `bid128-exp2.c` | `892cd6ea565f72c01a6398d68713edcb5be7defde1c1d7ea0b95d1961476a948` |
| `bid128-exp10.c` | `6fc37ea381f61c0862c81b11d70829e4464f2647874d4bc80ce67c2ab426dfd0` |
| `bid128-pow.c` | `23259dfe3936a9b3f56ea58c5ebc3e06cb5c67e339d8d87760c838e031becc1a` |
| `bid-binarydecimal.c` | `368955b2649f43aef431fbb3603822c3bd02937a4816de0e84040c2fd059bf36` |

Las fracciones conservan los pasos finitos binary128 (113 bits) y su redondeo.
La conversión de dos partes de exp trunca cada parte; no equivale a redondear
la primera al más cercano. La potencia genérica conserva las partes de
logaritmo a exponentes -28/-47, el exponencial dominante, la corrección residual
y el orden del retorno por overflow frente al ajuste de signo. La aproximación
extendida se calcula con Decimal de 100 cifras; no se copian tablas de respuestas
ni se relajan las expectativas mediante tolerancias.

Las capturas conservan comportamientos contraintuitivos del servidor, como la
rama de exponente cero de potencia Decimal128 de base diez y el retorno de
infinito antes de ajustar el signo en ciertos overflows de potencia. También
conservan double NaN para logaritmos de NaN y Decimal128 NaN para determinados
floor/ceil no cuantizables. No se sustituyen por identidades matemáticas.

El delta de `$log` base 2/10 está demostrado por capturas, no extrapolado del
wrapper 8.0: 7/8 seleccionan funciones especializadas; 9 coincide con el cociente
de logaritmos naturales. Se aplica en el adaptador del dialecto, con el mismo
kernel y las mismas observaciones de valor/tipo.

El corpus cubre diez entradas finitas seleccionadas, 32 generadas mediante una
recurrencia fija, cuatro controles int/int64/double, diez entradas límite,
catorce pares de potencias y cinco errores de dominio. Comprueba proyección,
escritura por pipeline y recuperación. Los errores de este corte observan
familia/código y estado; los envelopes y fases completos siguen en B04/B08.
Las capturas 7/8/9 y el replay de cuatro superficies son el gate mantenido;
esto no demuestra equivalencia universal para toda entrada Decimal128.

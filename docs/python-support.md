# Soporte de Python

## Política desde Mongoeco 4.10.0

Mongoeco admite oficialmente CPython **3.13, 3.14 y 3.15**, con la compilación
estándar con GIL. El mínimo de instalación sigue siendo `>=3.13`.
Las versiones futuras de Python requieren su propia validación antes de
anunciar soporte; el rango de instalación no acredita esa compatibilidad.
No se añade una promesa de soporte para builds free-threaded, JIT o PyPy.

El soporte de Python **3.13 finaliza en octubre de 2028**. Se mantiene durante
todo ese mes; una entrega posterior podrá subir el mínimo a Python 3.14 y
retirar su celda de CI. El cambio de mínimo se anunciará en las notas de esa
entrega y en sus metadatos. Las distribuciones ya publicadas conservarán sus
requisitos originales. No se programan avisos de runtime ni una caducidad por
fecha de las instalaciones existentes.

La fecha vigente de octubre de 2028 sustituye la decisión anterior de octubre
de 2027 por petición explícita del usuario. Las futuras ampliaciones requieren
una nueva decisión; no se aplican automáticamente.

Esta fecha corresponde a la política de Mongoeco: CPython 3.13 tiene previsto
recibir actualizaciones de seguridad hasta octubre de 2029, según
[PEP 719](https://peps.python.org/pep-0719/). Python 3.15.0 estable se publicó
el 9 de octubre de 2026, según la
[publicación oficial](https://www.python.org/downloads/release/python-3150/).

## Censo y aceptación

El encargo del usuario aprueba incorporar Python 3.15 y, tras valorar el coste
de mantener 3.13, fija su retirada en octubre de 2028. Es una ampliación de
compatibilidad sin cambios en los contratos de persistencia, SPI ni dialectos.
Se preservan las implementaciones y pruebas anteriores del checkout.

| Superficie | Mecanismo reutilizado | Resultado exigido |
| --- | --- | --- |
| Packaging | `pyproject.toml` y build de wheel/sdist | Clasificador 3.15; mínimo 3.13 conservado |
| CI y publicación | Matriz obligatoria `test`, dependiente del build inmutable | 3.13/3.14/3.15; pytest completo también en 3.15, sin `continue-on-error` |
| Contratos | Typing, manifest, SPI y compat snapshots existentes | Ejecutados contra el wheel instalado en 3.15 |
| Semántica | Suites completas existentes | Fachadas sync/async y Memory/SQLite, incluido replay del corpus nativo |
| Distribución | Smokes limpios existentes | Wheel y sdist instalables en 3.15 sin constraints de CI |
| Dependencias | Constraints controladas existentes | Resolución compatible con 3.15 sin relajar versiones silenciosamente |
| Comunicación | Esta política, README, compatibilidad, changelog y checklist | Una fecha de retirada coherente y verificaciones exigidas explícitas |

## Plan y estado

1. Añadir la versión a metadatos y al gate obligatorio, conservando cobertura
   y tooling principal en Python 3.14.
2. Obtener Python 3.15.0 estable y resolver las dependencias controladas.
3. Construir e instalar el candidato y ejecutar suites, contratos y smokes.
4. Corregir incompatibilidades observadas, revisar el diff y registrar resultados.

La primera instalación controlada detectó que `usearch==2.26.0` carece de una
distribución instalable en 3.15. Se eleva su mínimo a `2.26.4` y se fija ese
patch en CI para las tres versiones de Python. La distribución fuente oficial
2.26.4 se compila correctamente en 3.15 y supera una inserción/búsqueda nativa.
En plataformas sin wheel de esa
dependencia, la instalación requiere un compilador C++17 y los requisitos de
build publicados por USearch. No se incorpora un binario privado ni un fallback
que omita Search.

El pin Shapely 2.1.2 tampoco dispone de wheel 3.15; su build local falla al
resolver los headers nativos. Se utiliza Shapely >=2.2 y el pin 2.2.0 para toda
la matriz, que publica wheels compatibles. El usuario pide unificar los pins
si pasan los gates en los tres intérpretes: su aceptación exige instalaciones
y suites nuevas en 3.13/3.14/3.15 con estas mismas dependencias. Solo una
incompatibilidad concreta justificaría separar versiones. Los smokes limpios
verifican además la resolución pública sin esas constraints de CI.

El gate del manifiesto público detectó cambios de representación de
`TypeAliasType` en Python 3.15: los alias dentro de uniones y genéricos añaden
su módulo, y ahora existe `__qualname__` en el alias. Se normaliza únicamente
su representación en el export para conservar el contrato `mongoeco-public-api/v1`
y la misma fixture en todos los intérpretes. Las nuevas regresiones cubren
uniones, genéricos, argumentos de Callable, alias exportados y nombres de
clases parecidos que deben conservar su módulo. No se reescribe la referencia
para aceptar diferencias propias del intérprete.

## Validación final

Implementación completada el 15 de octubre de 2026. Se construyeron dos veces
el wheel y el sdist; ambos artefactos coinciden byte a byte tras normalizar
el sdist y superan `twine check`. El wheel de esa validación tiene SHA-256
`25157c8b94aa7c2dceb5f7b1575faf7fb3b82e82bb653b64a0e9a104f7bd10f3`.
Sus 275 archivos de runtime coinciden con el checkout y con las tres instalaciones.
Conserva provisionalmente la etiqueta 4.9.1: es un build de trabajo no
publicado, distinto de la 4.9.1 ya distribuida.

| Gate | Python 3.13.15 | Python 3.14.5 | Python 3.15.0 estable |
| --- | --- | --- | --- |
| Instalación con constraints comunes | Conforme | Conforme | Conforme |
| Pytest completo, wheel instalado | 11.024 passed | 11.024 passed | 11.024 passed |
| Subtests / skips esperados | 14.445 / 31 | 14.445 / 31 | 14.445 / 31 |
| Unittest | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK | 3.613 pruebas, 3 skips, OK |
| Typing / SPI / manifiesto / compat | Conformes | Conformes | Conformes |
| Instalaciones limpias wheel y sdist | Conformes | Conformes | Conformes |
| Conformance Memory y SQLite | Conforme | Conforme | Conforme |

Las seis regresiones nuevas del manifiesto se incluyen en esas suites; la
selección focal de manifiesto pasa sus doce pruebas en los tres intérpretes.
La cobertura en 3.14 es **99,04176168428812 %**, superior al mínimo 99 %.
También pasan las nueve propiedades del perfil deep, los smokes de benchmark
Search/vector Memory/SQLite, el ratchet Ruff sobre 76 archivos modificados y el
formato de los dos archivos Python de este bloque. Se han validado los YAML,
33 bloques shell y tres bloques Python de los workflows.

La suite completa incluye ambas fachadas, ambos motores y los replays del
corpus nativo previo, con clientes PyMongo sync/async sobre wire. En este
bloque no se recapturan las referencias de MongoDB real. La validación es
local en macOS arm64; los jobs obligatorios alojados quedan configurados,
sin atribuirles una ejecución que no se ha producido aquí.

El diff final se revisó y la documentación está alineada. Se conserva todo el
trabajo autorizado anterior. La identidad del candidato, los entornos, comandos
y logs están en [la evidencia](evidence/python-support/validation.json).
La [primera corrección de la fecha](evidence/python-support/policy-amendment.json)
conserva la decisión histórica de octubre de 2027. La
[ampliación vigente](evidence/python-support/policy-extension-2028.json) la
sustituye por octubre de 2028. Estos registros documentan cambios de política,
sin alterar el runtime ni atribuir nuevas ejecuciones a los candidatos anteriores.
Los artefactos de cada validación conservan el README de su momento; se debe
reconstruir el candidato de entrega para incluir la política actual.
No quedan pendientes técnicos de este bloque. Esta ampliación no publica una
versión ni cambia el número de la siguiente entrega.

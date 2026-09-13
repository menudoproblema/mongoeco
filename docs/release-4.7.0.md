# Release 4.7.0

Status: candidata local preparada; no publicada ni etiquetada.

## Resumen

MongoEco 4.7.0 elimina deliberadamente el engine SPI v1 y deja SPI v2 como
unico contrato publico estable para engines. Es una ruptura aprobada dentro de
una version minor: no hay shims, aliases, adapters, fallbacks ni deteccion por
shape. La [guia de migracion](migrating-to-4.7.md) contiene las sustituciones
concretas.

Esta version no publica SPI v3 ni cambia la semantica publica de SPI v2.
`search-v1`, los schemas de informes y el formato de catalogo mantienen su
version propia y no forman parte de la retirada.

## Superficie retirada

- resolucion de capabilities v1 e inferencia por shape;
- adapter y warning de compatibilidad;
- primitivas CRUD booleanas y batch antiguas de Memory/SQLite;
- metodos Search antiguos y traduccion de metadata por sidecar en la frontera
  de engine;
- flags de captura, callbacks y delivery mode exclusivos de la compatibilidad;
- exports, entradas del catalogo, fixtures y pruebas cuyo unico contrato era
  conservar v1.

Todos los consumidores internos, el runner de conformidad y los engines
integrados usan la frontera SPI v2 y outcomes tipados.

## Garantias conservadas

- identidad y contexto unico por operacion;
- ownership y cierre idempotente de snapshots;
- timeout, cancelacion, primer error y cleanup parcial supervisado;
- entrega de cambios inmediata, secuenciada o por outbox segun capability;
- ausencia de callbacks o cleanup externo bajo locks no reentrantes;
- lifecycle independiente y ownership de engines externos.

## Evidencia de cierre

Evidencia local capturada el 13 de septiembre de 2026 sobre Python 3.14.5,
macOS 15.6 arm64:

- `pytest --cov=mongoeco --cov-report=term -q`: 4135 passed, 26 skipped,
  2468 subtests passed; cobertura 99.00% (suelo 99.00%);
- `python -m unittest discover -s tests -p 'test*.py'`: 3540 tests, 2
  skipped, OK;
- property tests: perfil `ci` con seed 4700 y perfil `deep` con seeds 4600,
  4601 y 4602; cada ejecucion: 6 passed, 4 subtests passed;
- typing publico: positivo valido y 14 errores negativos esperados;
- ratchet Ruff: 116 ficheros Python cambiados, cero infracciones nuevas;
- manifest publico y snapshots de compatibilidad: actuales, sin diff;
- matriz PyMongo 4.9.2, 4.11.3, 4.13.2 y 4.17.0: resumen identico al
  fixture acreditado;
- conformidad completa Memory y SQLite: 10 passed por engine; canary externo:
  5 passed, 5 not-applicable, cero failed/error;
- wheel y sdist construidos dos veces: identicos byte a byte y validos segun
  `twine check`; ambos pasan el smoke desde un entorno limpio.

Benchmark general acreditado contra el baseline local anterior
`a95311c1cfc12c25172fe4db21444da8bc4df4b1`, con 1000 documentos, un warmup y
cinco repeticiones: los cuatro engines propios mejoran 2.57% de media y 2.90%
de mediana. El peor delta aislado es +4.87% y queda dentro del umbral del 10%.
El benchmark de spill con 100000 documentos empeora 3.17% de media y 4.74% de
mediana; su peor delta es +6.34%, tambien dentro del umbral. Los smokes Search
y vectorSearch de Memory/SQLite terminan correctamente.

Los informes locales viven en `/tmp/mongoeco-470-spi-v2-only-*.{json,md}` y
no forman parte del artefacto. Los servicios MongoDB 7.0/8.0 no estaban
disponibles localmente: el diferencial real requerido para publicar un tag
debe ejecutarse en CI. Los gates remotos y la publicacion requieren
autorizacion posterior.

## Publicacion

No se ha creado tag, release remota ni artefacto publicado. La publicacion solo
puede realizarse tras revisar los commits y recibir autorizacion explicita.

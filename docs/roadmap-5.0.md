# Roadmap verificable hacia MongoEco 5.0

Los responsables indicados son roles, no personas. Un gate bloqueado no se
convierte en verde por documentacion o por paridad local indirecta.

## 4.6.0 - bridge estable

| Gate | Evidencia | Responsable | Bloqueo |
| --- | --- | --- | --- |
| Diferenciales MongoDB 7/8 | JSON, JUnit, logs, buildInfo y 25 casos por version | release operator | cualquier caso pendiente o infraestructura no ejecutada |
| Catalogo de deprecaciones | schema, recurso y tests de ownership | maintainer de API | IDs duplicados, retirada sin sustituto o estado prematuro |
| Manifest publico | fixture, `--check` y diff revisado | maintainer de API | cambio no clasificado |
| SQLite 4.5 | wheel oficial, hashes, fixture y tests | maintainer SQLite | procedencia o replay no demostrados |
| CLI de conformidad | Memory, SQLite, canario y wheel | maintainer SPI | fallo, error o ejecucion vacia |
| Release local | suite, 99 %, lint, typing y builds reproducibles | release operator | cualquier gate local rojo |

Publicar requiere autorizacion humana posterior. Mientras MongoDB real no se
ejecute, 4.6 no esta recomendada para publicacion.

## Minor 4.x de transicion

| Gate | Evidencia | Responsable | Bloqueo |
| --- | --- | --- | --- |
| SPI v3 opt-in | protocolos, typing y canario externo | maintainer SPI | mutacion silenciosa de SPI v2 |
| `search-v2` opt-in | schema, fixtures y dual conformance | maintainer Search | mezcla de shapes v1/v2 |
| Adapters versionados | ratchets de imports y validators comunes | maintainer core | ramas versionadas distribuidas |
| Telemetria de deprecacion | warnings y catalogo actualizado | maintainer de API | uso legacy no observable |
| Datos 4.x | migraciones ensayadas sobre fixtures | maintainer SQLite | perdida o migracion no atomica |

La minor mantiene SPI v2 y `search-v1` estables. No cambia defaults.

## 5.0.0rc1

| Gate | Evidencia | Responsable | Bloqueo |
| --- | --- | --- | --- |
| Retirada SPI v1 | adapter eliminado y diff API esperado | maintainer SPI | dependencia no allowlisted o consumidor sin ruta v2 |
| Search v2 default | matrices duales y guia ejecutada | maintainer Search | aliases mezclados o provenance incompleta |
| SQLite | fixture 4.5 y ultima 4.x migradas | maintainer SQLite | schema ilegible o rollback incompleto |
| API 4.x/5.0 | manifest semantico aprobado | maintainer de API | ruptura no inventariada |
| Matrices consumidoras | wheel rc en canarios externos | release operator | imports privados o typing divergente |

SPI v2 permanece soportado salvo ADR posterior. Search v1 y sus aliases solo se
retiran si tuvieron una minor de convivencia completa.

## 5.0.0

Requiere todos los gates de rc1, diferenciales reales, cero defectos
blocker/critical, artefactos reproducibles y autorizacion explicita de version,
commit, tag y publicacion.

## Fuera de alcance

- Atlas Search remoto;
- nuevos backends;
- Rust;
- planner universal;
- cambios de producto no necesarios para la frontera contractual.

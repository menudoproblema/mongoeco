# Mongoeco 4.9.0: censo previo

Contrato: objective attachment 4896c5fd-4c6a-4dde-bb8e-f048d2d5c82b,
14 bloques. Base efectiva aa626c51fea8a055510f89a2acf2f8ed1ce33761;
última release v4.8.1 (ad90d846). Workset inicial fijado en
initial-workset.json; manifest SHA b99e89a7219c2f54abc2ab8f6f053e62b9078e18d789a5418ff9cca0681dab93.
No AGENTS local/adicional. ADR-001/002/004/009/015/017 gobiernan fronteras,
paridad, defaults y SPI. No existe transición RFC exigida para esta entrega;
la autorización y contrato constan en el encargo.

Owners de implementación: core aggregation (densify, ventanas, scopes,
preparación y fragmentos), API index boundary (opciones/metadata/lotes),
compat/CXP authored catalog y derivados, driver/SRV/wire, engines como SPI
estable, packaging/version y harness. Fronteras a probar: sync/async,
Memory/SQLite, ejecución directa/preparada/compilada/materializada/spill,
commands/wire y pipelines anidados. Fallbacks existentes conservados:
planner Python/residual, operaciones directas, perfil 4.9 por defecto,
PyMongo opcional y wrappers legacy anunciados. Capacidades aplazadas no se amplían.

Consumidores owner: cosecha main y gdynamics-testing main, ver
consumer-census.json. Sus checkouts primarios no coinciden con main;
los snapshots de main se extraerán a directorios efímeros, sin modificar owner.
Se conservarán también datos de sus revisiones actuales. El testkit
histórico cutover estaba en 4.6; no representa el owner main actual.
Consumo: provider sync y cliente proxy, testkit AsyncMongoClient/reloj/failpoints,
agregador gdynamics-testing vía mochuelo-testkit. Cambios de pin/adaptación solo
en copias efímeras; resolución completa y pip check obligatorios.

Gates: Python3.13/3.14 pytest/unittest; property ci/deep nuevas familias;
cov real99 sin nuevas exclusiones; lint lastrelease/baseline intacta;
manifest/exports/typing source y wheel; conformance3 engines/canario;
consumidores aislados; MongoDB7/8/9 strict+capturas nuevas FCV exacto;
SQLite4.5 copias; imports mínimos; wheel/sdist limpios; dos builds reproducibles;
benchmark contra paquete4.8.1 con mismo harness/datasets/deps, warmup y >=5rep,
spill realmente ejercitado, comparador sin relajar; CI/doc/version4.9 alineados.

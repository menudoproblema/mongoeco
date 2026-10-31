# Revisión 11: disposición histórica y aceptación restante

Base: `f0a14fb`, con candidato watch sin commitear. El adjunto tiene el mismo
SHA-256 que revisiones 3–10; no se copia el texto privado al repositorio.
El [receipt](receipt.json) identifica fuentes, workset y límites. Esta revisión
solo cambia plan y evidencia; no modifica ni acepta producción/tests.

- [Representaciones](representations.json): 18 trazas en seis pares locales,
  documento y operando separados, eq/in y distinct por subtipo/contenido.
  [Script reproducible](representation-probe.py.txt).
- [Fuentes oficiales](sources-check.json): 16 hashes coincidentes, incluidos
  parser y comparación BSON de r8.0.0. [Script](check-sources.py.txt).
  El primer intento no tuvo acceso de red; el intento autorizado terminó
  correctamente. No se sobrescribe ningún manifiesto histórico.
- [Focal](focal.txt): 120 pruebas verdes. [Protocolo async](protocol.txt):
  resultado del helper corregido para recibir el cliente real.
- [Replay previo](replay.txt): 408 pruebas verdes usando capturas existentes.
  [Replay de primera tanda](batch-replay.txt): 75 verdes sobre las referencias
  nuevas sin commitear del candidato; sus hashes constan en el receipt.
  Ninguno es una recaptura nativa ni aceptación global del candidato.
- [Lint del candidato](candidate-lint.txt): una nueva E501 en hub.py.
  Se registra como gate pendiente; esta revisión documental no la corrige.

La precisión nueva afecta a fases de watch, buffer/materialización y validez
del ámbito de pruebas async. La invalidación filtrada en la primera tanda
vacía no tiene el mismo resultado que en getMore: las referencias del candidato
registran código 260 y alive verdadero con PyMongo 4.19.0 en los tres majors.
Este contraste reutiliza esas referencias; no ejecuta nuevos servidores.

No se reabren Binary/fechas/SQL ni se crean bloques, runners, normalizadores
o políticas nuevas. El owner de aceptación sigue siendo el registro de
implementación. El orden restante está en la revisión 11 del plan; los receipts
anteriores mantienen su alcance y no se modifican.

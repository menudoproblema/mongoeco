# Diagnóstico nativo de tandas, sin aceptación local

MongoDB 7.0.43, 8.0.32 y 9.0.2, standalone temporal. Find ordenado y aggregate
con `$sort`, tres documentos, primera tanda y getMore de tamaño uno. En los
tres servidores ambos cursores conservan ID no cero tras la tercera fila;
el siguiente getMore devuelve tanda vacía e ID cero. Se registran IDs de
documentos y presencia de cursor, sin comparar los IDs de cursor asignados.

Esto caracteriza esas formas exactas. No acredita pipelines vacíos, limit,
bytes, singleBatch, raw ni la adaptación local de Database.command que
materializa una respuesta completa. El siguiente corte B07 debe incorporar
los testigos adecuados al registro diferencial existente antes de corregir
comportamiento o expectativas, y observar la demanda real en vez de espiar
un método legado que ya no participa en la ruta de respuesta.

Los dos fallos de la cohorte de proyecciones permanecen abiertos. Esta sonda
no los convierte por sí sola en tests obsoletos ni acepta la ruta aggregate
local por el hecho de que su test antiguo pase. Los nueve recursos temporales
se comprobaron ausentes con Docker inspect; el ledger está en resources.json.

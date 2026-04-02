# ADR-007 - SDAM parcial y explicito

## Contexto

El runtime del driver necesita conocer topologia, seleccionar servidores y
reaccionar a `hello`, pero el objetivo del proyecto no es implementar SDAM
completo como un driver de produccion.

## Decision

Soportar un subconjunto util y explicito de SDAM, con introspeccion publica de
capacidades y limites claros.

## Consecuencias

- Hay discovery por `hello`, awareness de `topologyVersion` y seguimiento de
  salud por servidor.
- No hay full SDAM, long-polling `hello` ni monitorizacion distribuida.
- La documentacion y la API deben dejar el limite claro.

## Alternativas descartadas

- Pretender compatibilidad SDAM completa sin implementarla.
- No modelar topologia en absoluto.
- Reabrir el alcance del driver hasta convertirlo en un producto distinto.

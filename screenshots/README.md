# Evidencia de la corrida real

Capturas reales de Arize Phoenix (proyecto `pre-entrega-7`) tomadas el 18/10/2026 después de
ejecutar cinco tareas concurrentes con `AGENT_MODE=openai` (`gpt-4.1-mini`).

1. `01-trace-completa.png`: traza expandida de una ejecución, con Supervisor, especialistas y
   llamadas `ChatOpenAI`.
2. `02-costos-por-ejecucion.png`: las cinco ejecuciones con latencia, tokens y costo.
3. `02b-costo-total-metrics.png`: costo total de la corrida separado en prompt y completion.
4. `03-latencia-p95.png`: percentiles de latencia; p95 = 9,57 s.

Phoenix muestra los importes menores a un centavo como `<$0.01`. Los valores exactos (USD 0,0092
en total) figuran en la sección **Evidencia de la corrida real** del README principal.

Ninguna captura contiene API keys ni datos personales.

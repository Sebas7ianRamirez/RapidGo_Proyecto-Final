# ADR-002: Validación y calibración de umbrales de alerta

## Contexto / Problema
En la Semana 1 definimos los umbrales iniciales en `docs/kpis_alertas.md` basados en SLOs genéricos (error rate > 5% y p95 > 2000 ms) y se prometió recalibrarlos con datos reales. Al ejecutar el pipeline de Spark Structured Streaming (Issue #13 / PR #45), necesitábamos verificar si estos umbrales disparaban falsas alarmas en un entorno normal o si dejaban pasar degradaciones reales.

## Opciones Consideradas
1. **Bajar los umbrales:** Reducir el p95 a 1000 ms y el error rate al 2%. 
2. **Subir los umbrales:** Aumentarlos porque el entorno local es lento.
3. **Mantener los umbrales actuales:** Conservar error rate > 5% y p95 > 2000 ms, pero justificar su validez con los datos de ejecución reales.

## Decisión Tomada
Se decidió **mantener los umbrales actuales** (error rate > 0.05 y p95 > 2000 ms). No se requiere recalibración numérica porque los datos reales del pipeline validaron su eficacia.

## Evidencia Utilizada
Basado en la ejecución de los escenarios controlados en Spark:
-   **Escenario normal:** Las latencias (p95) se mantuvieron estables y lejos del umbral crítico. El error rate fue de **0.0**.
-   **Escenario de degradación y latencia alta:** La alerta de error rate se observó activada en el escenario de degradación, con un error rate de 13.3% para `pagos-api`. Asimismo, en la ejecución de demostración con el escenario `latencia_alta`, **se observó con éxito la activación de `alerta_latencia: true` para `pagos-api`** durante la anomalía, confirmando la correcta operación de la regla ante degradaciones de rendimiento reales.
-   **Latencia bajo carga:** Los picos de latencia observados validan que el sistema responde de manera predecible a las anomalías inyectadas desde el producer, sin generar falsos positivos fuera de las ventanas de anomalía.

## Consecuencias / Trade-offs
-   **Beneficio:** Evitamos que picos de latencia menores saturen a los operadores con falsas alarmas, manteniendo una observabilidad precisa respaldada por pruebas de integración end-to-end (Kafka -> Spark -> Data Lake).
-   **Trade-off:** Se requerirá monitoreo continuo en producción por si la latencia base de los servicios evoluciona con el tiempo.
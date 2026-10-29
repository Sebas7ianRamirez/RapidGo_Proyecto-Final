# Checkpoint Semana 2 - Observabilidad Tecnológica (Variante 9)

## 1. Qué quedó funcionando
*   **Kafka + Spark (PR #11, #12 y #13):** El pipeline consume eventos del topic `rapidgo.observabilidad.eventos`, valida el JSON y divide los flujos entre válidos e inválidos sin caerse.
*   **Data Lake:** Los eventos limpios se guardan en `datalake/processed/eventos` y los KPIs en `datalake/curated/kpis_alertas` en formato Parquet.
*   **Alertas:** Se implementaron las reglas en `spark/reglas.py`.
*   **CI (PR #15):** El workflow de GitHub Actions está configurado y validando la sintaxis de Python, ejecutando las pruebas (`58 passed`) y validando Docker Compose.

## 2. Valor o evidencia real observada en la ejecución
Al ejecutar el pipeline con Kafka y Spark (PR #12), inyectamos un escenario de degradación. El servicio `pagos-api` registró un `error_rate` de **0.133 (13.3%)** y su disponibilidad bajó a **0.935**. El servicio `pedidos-api` subió su p95 de latencia de **617.1 ms a 791.8 ms**. En el escenario normal, el error rate se mantuvo en **0.0**.

## 3. Decisión técnica tomada y por qué
**Decisión:** Mantener los umbrales de alerta en 5% para error rate y 2000 ms para latencia.
**Por qué:** Los datos reales demostraron que la latencia normal está en ~700 ms. El umbral de 2000 ms absorbe bien los picos y solo alertará ante fallos graves. El error rate superó el 5% durante la degradación y disparó la alerta (`true`) de forma correcta. El detalle está en el `ADR-002-validacion-umbrales.md`.

## 4. Problema real encontrado y cómo se resolvió
**Problema:** Durante las pruebas del PR #15 (CI), el flujo de trabajo fallaba porque no existían los archivos de pruebas en la rama `main`. 
**Solución:** Se esperó a que el PR #14 (que agregaba los tests con `pytest`) se fusionara a `main`, se actualizó la rama y el CI pasó en verde con 58 pruebas. Enlace: PR #15.

## 5. Enlaces a Issues / PR / Action
*   Issues: #11, #12, #13, #15.
*   PRs: #13 (Spark y alertas), #14 (Pruebas), #15 (CI).
*   Actions: Ejecución del workflow de CI en el PR #15.
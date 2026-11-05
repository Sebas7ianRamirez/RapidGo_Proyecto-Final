# ADR-003: Estrategia de orquestación y recuperación ante fallos

## Contexto / Problema
En la Semana 3 se construyó el DAG `rapidgo_pipeline_observabilidad` en Airflow 3.3.1 con cinco tareas lineales (`check_fuentes` → `validar_raw` → `agregar` → `validar_curated` → `reporte`), reintentos configurados (Issue #20) y paso de métricas por XCom. Faltaba definir qué debe pasar cuando una tarea falla y comprobarlo con un fallo real, no con una suposición.

En el Issue #23 se provocó un fallo controlado: se detuvo Spark y se renombró `datalake/processed/eventos` a `eventos_respaldo`. El Issue sugería "Kafka detenido", pero el DAG no se conecta a Kafka (solo lee Parquet del Data Lake), así que detener Kafka no habría hecho fallar ninguna tarea. Por eso el escenario fue la ausencia de la capa `processed`.

## Fallo real observado
Ejecución `manual__2026-11-05T14:42:37.229580+00:00`, estado final **Failed**, duración **4 min 9 s**.

- **Tarea que falló:** `check_fuentes`, con 3 intentos (Try Number 1, 2 y 3). El último terminó a las 09:46:43 (hora local).
- **Mensaje real del log:** `FileNotFoundError: El directorio de fuentes no existe: /opt/airflow/datalake/processed/eventos`, lanzado en la línea 20 de `rapidgo_pipeline_dag.py`, función `check_fuentes`.
- **Tareas afectadas:** `validar_raw`, `agregar`, `validar_curated` y `reporte` quedaron en `upstream_failed` con 0 intentos. Ninguna llegó a ejecutarse.
- **Recuperación:** se devolvió el nombre `eventos` a la carpeta y se disparó una ejecución nueva, `manual__2026-11-05T14:52:52.882538+00:00`, con las 5 tareas en `success` en 15,7 s. La ejecución de línea base, antes del fallo, fue `manual__2026-11-05T14:38:08.772650+00:00` (success, 44 s).

## Opciones Consideradas
1. **Sin reintentos (fallar de inmediato):** el fallo se ve en segundos, pero cualquier error pasajero (un archivo aún en escritura, un montaje lento) obligaría a intervenir a mano.
2. **Reintentos por tarea + validaciones estrictas que lanzan excepción (la configuración actual):** `retries=2` con `retry_delay` de 2 minutos en cada tarea, y validaciones que lanzan `FileNotFoundError` cuando falta la capa o no hay `.parquet`.
3. **Continuar con datos parciales (por ejemplo `trigger_rule="all_done"`):** el reporte se generaría siempre, pero podría salir con métricas de una capa que no se validó, es decir, un "falso verde".

## Decisión Tomada
Se mantiene la **opción 2**:

- Cada tarea tiene `retries=2` y `retry_delay` de 2 minutos. El log confirma el efecto: 3 intentos y una duración total cercana a 4 minutos.
- Las validaciones fallan en voz alta (`FileNotFoundError`) en lugar de continuar. Al fallar `check_fuentes`, Airflow marca las tareas siguientes como `upstream_failed` y no se genera ningún reporte.
- La recuperación es manual y consta de dos pasos: corregir la causa raíz y lanzar una ejecución nueva, o usar **Clear** sobre la tarea fallida para reanudar desde ahí. No se añaden mecanismos automáticos de reparación.

## Evidencia Utilizada
- Estados de las tareas en la ejecución fallida (`check_fuentes` en Failed con Try Number 3; las otras cuatro en Upstream Failed).
- Traceback real de `check_fuentes` en el log de Airflow.
- Historial de ejecuciones: success (14:38) → failed (14:42) → success (14:52).
- Capturas y procedimiento completo en `docs/fallo_controlado.md` y `docs/evidencias/`.

## Consecuencias / Trade-offs
- **Beneficio:** el fallo queda aislado en la primera tarea, sin reportes engañosos y con trazabilidad completa en la UI (intentos, log y estados).
- **Trade-off:** los reintentos no corrigen una causa permanente. Aquí la carpeta no iba a aparecer sola, así que los 2 reintentos gastaron unos 4 minutos antes de declarar el fallo definitivo. Es aceptable para un pipeline de demostración. Si fuera un problema, se podría reducir `retry_delay` en las tareas de validación, aunque este ADR no lo implementa.
- **Limitaciones conocidas del DAG actual (no se cambian en este ADR):**
  - `agregar` usa valores simulados (`total_registros=100`) si no logra leer ningún Parquet.
  - `validar_curated` solo imprime los archivos encontrados y no lanza error si la carpeta existe pero está vacía.
  - `generar_reporte` acepta métricas vacías de XCom y escribe "Desconocido".
  - `retries` de `default_args` vale 1, pero cada tarea lo sobrescribe con 2. El efecto real es 2, como muestran los 3 intentos.

  Estas tres primeras brechas podrían producir un "éxito" sin datos y conviene revisarlas en una iteración posterior.
- **Alertas:** no hay notificación automática ante un fallo (`email_on_failure=False`). Un `on_failure_callback` sería el siguiente paso natural.
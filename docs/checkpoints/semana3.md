# Checkpoint Semana 3 - Observabilidad Tecnológica (Variante 9)

## 1. Qué quedó funcionando
*   **Airflow en Docker (Issue #18 / PR #52):** Airflow 3.3.1 en modo `standalone` con SQLite, perfil `airflow` de `docker-compose.yml`, interfaz en `http://localhost:8088` (solo desde la máquina local) y DAGs montados desde `airflow/dags`.
*   **DAG de observabilidad (Issue #19 / PR #53):** `rapidgo_pipeline_observabilidad` con 5 tareas y dependencias lineales: `check_fuentes` → `validar_raw` → `agregar` → `validar_curated` → `reporte`.
*   **Retries y XCom (Issue #20 / PR #54):** `retries=2` con espera de 2 minutos por tarea. `agregar` lee los Parquet reales y pasa sus métricas a `reporte` por XCom.
*   **Pruebas y CI (Issue #21 / PR #55 e Issue #22 / PR #56):** se agregaron las pruebas del DAG (`test_dag_calidad.py`) y de disponibilidad (78 pruebas en local). El CI instala Airflow y valida el DAG con `DagBag`, de modo que los tests del DAG se ejecutan en GitHub Actions.
*   **Fallo controlado y recuperación (Issue #23 / PR #57):** fallo provocado, documentado y recuperado en `docs/fallo_controlado.md`.

## 2. Valor o evidencia real observada en la ejecución
En la primera ejecución completa, `agregar` leyó los Parquet reales de `processed/eventos` y calculó **2400 registros** y una latencia promedio de aproximadamente **280,9 ms**, valores que `reporte` recibió por XCom. En la prueba de recuperación, el DAG pasó de fallar tras 4 min 9 s (3 intentos de `check_fuentes`) a completar las 5 tareas en 15,7 s una vez restaurada la carpeta.

## 3. Decisión técnica tomada y por qué
**Decisión:** mantener reintentos por tarea (`retries=2`, espera de 2 minutos) y validaciones estrictas que lanzan excepción, con recuperación manual (corregir la causa y lanzar una ejecución nueva o usar Clear).
**Por qué:** el fallo real mostró que el error se queda aislado en la primera tarea, las cuatro siguientes quedan en `upstream_failed` y no se genera un reporte con datos incompletos. El detalle y las alternativas descartadas están en `ADR-003-recuperacion-ante-fallos.md`.

## 4. Problema real encontrado y cómo se resolvió
**Tarea que falló:** `check_fuentes`, en la ejecución `manual__2026-11-05T14:42:37.229580+00:00` (estado final Failed, 3 intentos, 4 min 9 s).

**Mensaje real del log:**
```text
FileNotFoundError: El directorio de fuentes no existe: /opt/airflow/datalake/processed/eventos
(rapidgo_pipeline_dag.py, línea 20, en check_fuentes)
```

**Causa:** para provocar el fallo se detuvo Spark y se renombró `datalake/processed/eventos` a `eventos_respaldo`, por lo que la carpeta que lee el DAG dejó de existir.

**Tareas afectadas:**

| Tarea | Estado final | Intentos | Causa |
| :--- | :--- | :--- | :--- |
| `check_fuentes` | failed | 3 | FileNotFoundError: no existe `processed/eventos` |
| `validar_raw` | upstream_failed | 0 | Depende de `check_fuentes` |
| `agregar` | upstream_failed | 0 | Depende de `validar_raw` |
| `validar_curated` | upstream_failed | 0 | Depende de `agregar` |
| `reporte` | upstream_failed | 0 | Depende de `validar_curated` |

**Cambio que resolvió el problema:** no fue un cambio de código, sino restaurar la carpeta (`Rename-Item datalake\processed\eventos_respaldo eventos`) y disparar una ejecución nueva. La ejecución `manual__2026-11-05T14:52:52.882538+00:00` terminó con las 5 tareas en success.

**Qué se aprendió:** los reintentos no arreglan una causa permanente (gastaron unos 4 minutos), y las validaciones estrictas evitaron que el pipeline terminara en verde sin datos. Quedan tres brechas documentadas en el ADR-003: la agregación con valores simulados de respaldo, la validación de `curated` que no falla con carpeta vacía y el reporte que tolera métricas vacías.

## 5. Enlaces a Issues / PR / Action
- Issues: #18, #19, #20, #21, #22, #23 y #24.
- Pull Requests: #52, #53, #54, #55, #56 y #57 (Issue #23).
- Documentos: `docs/fallo_controlado.md`, `docs/evidencias/` y `docs/decisions/ADR-003-recuperacion-ante-fallos.md`.
- CI: enlace al run en `main` tras el merge (pendiente de pegar).

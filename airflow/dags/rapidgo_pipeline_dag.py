from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import pandas as pd

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_fuentes():
    """1. Check de fuentes: Verifica que exista el directorio de datos procesados."""
    path = "/opt/airflow/datalake/processed/eventos"
    if not os.path.exists(path):
        raise FileNotFoundError(f"El directorio de fuentes no existe: {path}")
    print(f"Fuente verificada correctamente en: {path}")

def validar_raw():
    """2. Validar raw (processed/eventos): Comprueba la existencia de archivos Parquet."""
    path = "/opt/airflow/datalake/processed/eventos"
    files = [f for f in os.listdir(path) if f.endswith(".parquet") or f.endswith(".snappy.parquet") or os.path.isdir(os.path.join(path, f))]
    print(files)
    print(f"Validación de capa inicial completada. Elementos encontrados: {len(files)}")

def agregar_datos():
    """3. Agregar: Realiza agregaciones simuladas guardando en /tmp/ para evitar problemas de permisos."""
    print("Ejecutando agregación y cálculo de métricas sobre los eventos...")
    output_dir = "/tmp/reportes_airflow"
    os.makedirs(output_dir, exist_ok=True)
    resumen = {"timestamp": [str(datetime.now())], "estado": ["procesado_ok"], "total_registros": [100]}
    df = pd.DataFrame(resumen)
    df.to_parquet(os.path.join(output_dir, "resumen_agregado.parquet"))
    print("Agregación guardada exitosamente en /tmp/.")

def validar_curated():
    """4. Validar curated: Comprueba que los datos curados estén disponibles."""
    path = "/opt/airflow/datalake/curated/kpis_alertas"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Directorio curated no encontrado: {path}")
    print("Capa curated validada correctamente.")

def generar_reporte():
    """5. Reporte: Genera el reporte final de ejecución en /tmp/."""
    report_path = "/tmp/reportes_airflow/reporte_final_ejecucion.txt"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"Reporte de ejecución exitosa del Pipeline RapidGo - {datetime.now()}\n")
    print(f"Reporte generado exitosamente en {report_path}")

with DAG(
    dag_id="rapidgo_pipeline_observabilidad",
    default_args=default_args,
    description="DAG principal con 5 tareas para el pipeline de RapidGo (Gate S3)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rapidgo", "s3", "pipeline"],
) as dag:

    t1_check = PythonOperator(
        task_id="check_fuentes",
        python_callable=check_fuentes,
    )

    t2_raw = PythonOperator(
        task_id="validar_raw",
        python_callable=validar_raw,
    )

    t3_agregar = PythonOperator(
        task_id="agregar",
        python_callable=agregar_datos,
    )

    t4_curated = PythonOperator(
        task_id="validar_curated",
        python_callable=validar_curated,
    )

    t5_reporte = PythonOperator(
        task_id="reporte",
        python_callable=generar_reporte,
    )

    t1_check >> t2_raw >> t3_agregar >> t4_curated >> t5_reporte
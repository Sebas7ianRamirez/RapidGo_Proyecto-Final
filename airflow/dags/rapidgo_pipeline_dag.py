from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os
import pandas as pd

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def check_fuentes():
    """1. Check de fuentes: Verifica que exista el directorio de datos procesados."""
    path = "/opt/airflow/datalake/processed/eventos"
    if not os.path.exists(path):
        raise FileNotFoundError(f"El directorio de fuentes no existe: {path}")
    print(f"Fuente verificada correctamente en: {path}")

def validar_raw():
    """2. Validar raw (processed/eventos): Valida existencia exclusiva de archivos .parquet reales."""
    path = "/opt/airflow/datalake/processed/eventos"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Directorio processed/eventos no encontrado: {path}")
    
    # Buscar archivos .parquet reales (excluyendo _spark_metadata y archivos .crc)
    parquet_files = [
        f for f in os.listdir(path) 
        if f.endswith(".parquet") and not f.startswith(".")
    ]
    
    print(f"Archivos Parquet válidos encontrados en raw: {parquet_files}")
    if not parquet_files:
        raise FileNotFoundError("La capa processed/eventos no contiene archivos Parquet válidos para procesar.")

def agregar_datos(**context):
    """3. Agregar: Lee los archivos Parquet de processed/eventos, calcula métricas y usa XCom."""
    print("Iniciando agregación y cálculo de métricas reales...")
    path = "/opt/airflow/datalake/processed/eventos"

    if not os.path.exists(path):
        raise FileNotFoundError(f"Directorio processed/eventos no encontrado: {path}")

    parquet_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet") and not file.startswith("."):
                parquet_files.append(os.path.join(root, file))

    if not parquet_files:
        raise FileNotFoundError(
            "La capa processed/eventos no contiene archivos Parquet válidos para procesar."
        )

    df_list = []
    errores_lectura = []

    for full_path in parquet_files:
        try:
            df_list.append(pd.read_parquet(full_path))
        except Exception as exc:
            errores_lectura.append(f"{full_path}: {exc}")

    if errores_lectura:
        raise RuntimeError(
            "No fue posible leer todos los archivos Parquet de processed/eventos: "
            + "; ".join(errores_lectura)
        )

    df = pd.concat(df_list, ignore_index=True)

    if df.empty:
        raise ValueError("Los archivos Parquet de processed/eventos no contienen registros.")

    if "latencia_ms" not in df.columns:
        raise ValueError("La columna 'latencia_ms' no está presente en los eventos procesados.")

    total_registros = int(len(df))
    latencia_promedio = float(df["latencia_ms"].mean())

    output_dir = "/tmp/reportes_airflow"
    os.makedirs(output_dir, exist_ok=True)

    resumen = {
        "timestamp": [str(datetime.now())],
        "estado": ["procesado_ok"],
        "total_registros": [total_registros],
        "latencia_promedio_ms": [latencia_promedio],
    }

    pd.DataFrame(resumen).to_parquet(
        os.path.join(output_dir, "resumen_agregado.parquet")
    )

    metricas = {
        "total_registros": total_registros,
        "latencia_promedio_ms": latencia_promedio,
        "timestamp_agregacion": str(datetime.now()),
    }

    print(f"Agregación completada. Métricas calculadas: {metricas}")
    return metricas

def validar_curated():
    """4. Validar curated: Comprueba que existan archivos Parquet de KPIs."""
    path = "/opt/airflow/datalake/curated/kpis_alertas"

    if not os.path.exists(path):
        raise FileNotFoundError(f"Directorio curated no encontrado: {path}")

    parquet_files = [
        file
        for file in os.listdir(path)
        if file.endswith(".parquet") and not file.startswith(".")
    ]

    if not parquet_files:
        raise FileNotFoundError(
            "La capa curated/kpis_alertas no contiene archivos Parquet válidos."
        )

    print(f"Archivos Parquet curados encontrados: {parquet_files}")
    print("Capa curated validada correctamente.")

def generar_reporte(**context):
    """5. Reporte: Consume las métricas mediante XCom y genera el reporte final."""
    ti = context['ti']
    # Recuperar datos de la tarea 'agregar' usando XCom (Requisito Issue #20)
    metricas_agregadas = ti.xcom_pull(task_ids='agregar')
    
    if not metricas_agregadas:
        metricas_agregadas = {"total_registros": "Desconocido", "latencia_promedio_ms": "N/A"}

    report_path = "/tmp/reportes_airflow/reporte_final_ejecucion.txt"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== REPORTE DE EJECUCIÓN PIPELINE RAPIDGO ===\n")
        f.write(f"Fecha de generación: {datetime.now()}\n")
        f.write(f"Total de registros procesados: {metricas_agregadas.get('total_registros')}\n")
        f.write(f"Latencia promedio (ms): {metricas_agregadas.get('latencia_promedio_ms')}\n")
        f.write("Estado: ÉXITO - Todas las validaciones de calidad superadas.\n")
        
    print(f"Reporte final generado exitosamente con datos de XCom en {report_path}")

with DAG(
    dag_id="rapidgo_pipeline_observabilidad",
    default_args=default_args,
    description="DAG principal con 5 tareas, validaciones estrictas y XCom para RapidGo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rapidgo", "s3", "pipeline"],
) as dag:

    t1_check = PythonOperator(
        task_id="check_fuentes",
        python_callable=check_fuentes,
        retries=2,
    )

    t2_raw = PythonOperator(
        task_id="validar_raw",
        python_callable=validar_raw,
        retries=2,
    )

    t3_agregar = PythonOperator(
        task_id="agregar",
        python_callable=agregar_datos,
        retries=2,
    )

    t4_curated = PythonOperator(
        task_id="validar_curated",
        python_callable=validar_curated,
        retries=2,
    )

    t5_reporte = PythonOperator(
        task_id="reporte",
        python_callable=generar_reporte,
        retries=2,
    )

    # Definición de dependencias estrictas
    t1_check >> t2_raw >> t3_agregar >> t4_curated >> t5_reporte
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from pyspark.sql.types import BooleanType
from esquema import evento_schema
from agregaciones import agregar_columnas_calidad, ventana_1min_error_y_p95
from reglas import evaluar_alerta_error, evaluar_alerta_latencia
from parseo import condicion_valido

# Variables de entorno (con valores por defecto para local)
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("RAPIDGO_TOPIC", "rapidgo.observabilidad.eventos")

# Ruta base para el Data Lake y Checkpoints (por defecto sube un nivel desde spark/)
BASE_DIR = os.environ.get("RAPIDGO_BASE_DIR", "..")
DATALAKE_PATH = os.path.join(BASE_DIR, "datalake")
CHECKPOINTS_PATH = os.path.join(BASE_DIR, "spark-checkpoints")

# --- CORREGIDO: Se agrega el paquete de Kafka para evitar el error AnalysisException ---
spark = (
    SparkSession.builder
    .appName("RapidGo-Alertas")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

alerta_error_udf = udf(evaluar_alerta_error, BooleanType())
alerta_latencia_udf = udf(evaluar_alerta_latencia, BooleanType())

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load())

parsed = raw.select(from_json(col("value").cast("string"), evento_schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))
parsed = agregar_columnas_calidad(parsed)

# --- APLICAMOS LA VALIDACIÓN DEL ISSUE #11 ---
valid_df = parsed.filter(condicion_valido())

# --- processed: solo eventos válidos a Parquet ---
processed_query = (valid_df.writeStream
    .format("parquet")
    .option("path", os.path.join(DATALAKE_PATH, "processed/eventos"))
    .option("checkpointLocation", os.path.join(CHECKPOINTS_PATH, "processed_eventos"))
    .outputMode("append")
    .start())

# --- curated: KPIs + columnas de alerta (también usando solo válidos) ---
agg_1min = ventana_1min_error_y_p95(valid_df)
agg_con_alertas = (agg_1min
    .withColumn("alerta_error", alerta_error_udf(col("error_rate")))
    .withColumn("alerta_latencia", alerta_latencia_udf(col("p95_latencia_ms"))))

curated_query = (agg_con_alertas.writeStream
    .format("parquet")
    .option("path", os.path.join(DATALAKE_PATH, "curated/kpis_alertas"))
    .option("checkpointLocation", os.path.join(CHECKPOINTS_PATH, "curated_kpis"))
    .outputMode("append")
    .start())

# --- consola para ver alertas en vivo ---
console_query = (agg_con_alertas.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("update")
    .start())

spark.streams.awaitAnyTermination()

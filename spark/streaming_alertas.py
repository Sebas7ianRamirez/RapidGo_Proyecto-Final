from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, udf
from pyspark.sql.types import BooleanType
from esquema import evento_schema
from agregaciones import agregar_columnas_calidad, ventana_1min_error_y_p95
from reglas import evaluar_alerta_error, evaluar_alerta_latencia

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rapidgo.observabilidad.eventos"

spark = SparkSession.builder.appName("RapidGo-Alertas").getOrCreate()
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

# --- processed: eventos parseados a Parquet ---
processed_query = (parsed.writeStream
    .format("parquet")
    .option("path", "../datalake/processed/eventos")
    .option("checkpointLocation", "../spark-checkpoints/processed_eventos")
    .outputMode("append")
    .start())

# --- curated: KPIs + columnas de alerta ---
agg_1min = ventana_1min_error_y_p95(parsed)
agg_con_alertas = (agg_1min
    .withColumn("alerta_error", alerta_error_udf(col("error_rate")))
    .withColumn("alerta_latencia", alerta_latencia_udf(col("p95_latencia_ms"))))

curated_query = (agg_con_alertas.writeStream
    .format("parquet")
    .option("path", "../datalake/curated/kpis_alertas")
    .option("checkpointLocation", "../spark-checkpoints/curated_kpis")
    .outputMode("append")
    .start())

# --- consola para ver alertas disparadas en vivo durante la demo ---
console_query = (agg_con_alertas.writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("update")
    .start())

spark.streams.awaitAnyTermination()

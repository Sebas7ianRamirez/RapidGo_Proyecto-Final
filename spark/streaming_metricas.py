from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from esquema import evento_schema
from agregaciones import agregar_columnas_calidad, ventana_1min_error_y_p95, ventana_5min_disponibilidad

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rapidgo.observabilidad.eventos"

spark = SparkSession.builder.appName("RapidGo-Metricas").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load())

parsed = raw.select(from_json(col("value").cast("string"), evento_schema).alias("data")).select("data.*")
parsed = parsed.withColumn("event_time", to_timestamp(col("timestamp")))
parsed = agregar_columnas_calidad(parsed)

agg_1min = ventana_1min_error_y_p95(parsed)
agg_5min = ventana_5min_disponibilidad(parsed)

q1 = (agg_1min.writeStream.format("console").outputMode("update")
      .option("truncate", False).queryName("agg_1min").start())
q2 = (agg_5min.writeStream.format("console").outputMode("update")
      .option("truncate", False).queryName("agg_5min").start())

spark.streams.awaitAnyTermination()

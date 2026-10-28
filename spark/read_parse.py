from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import evento_schema

# Configuración de conexión al Kafka local levantado por Docker
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rapidgo.observabilidad.eventos"

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("RapidGo-ReadParse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Leer el flujo de datos desde Kafka usando Structured Streaming
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load())

# Parsear el valor binario a JSON aplicando el esquema estructurado
parsed = raw.select(
    from_json(col("value").cast("string"), evento_schema).alias("data")
).select("data.*")

# Escribir la salida en la consola de manera incremental
query = (parsed.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start())

query.awaitTermination()

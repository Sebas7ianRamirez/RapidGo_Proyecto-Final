from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

evento_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("servicio", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("metodo_http", StringType(), True),
    StructField("codigo_http", IntegerType(), True),
    StructField("latencia_ms", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("instancia", StringType(), True),
])

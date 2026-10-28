import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from esquema import evento_schema

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("RAPIDGO_TOPIC", "rapidgo.observabilidad.eventos")
SERVICIOS_VALIDOS = ["pedidos-api", "pagos-api", "tracking-api", "usuarios-api"]
CAMPOS_OBLIGATORIOS = [
    "event_id", "event_time", "servicio", "endpoint",
    "metodo_http", "codigo_http", "latencia_ms", "region", "instancia",
]


def main():
    spark = (
        SparkSession.builder
        .appName("RapidGo-SparkStreaming-ReadParse")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = (
        json_df.select(
            from_json(col("json_str"), evento_schema).alias("data"),
            col("json_str").alias("raw_json"),
        )
        .select("data.*", "raw_json")
    )

    final_df = parsed_df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
    )

    cond = col("event_id").isNotNull()
    for c in CAMPOS_OBLIGATORIOS:
        cond = cond & col(c).isNotNull()
    cond = cond & col("servicio").isin(SERVICIOS_VALIDOS)
    cond = cond & col("codigo_http").between(100, 599)

    valid_events = final_df.filter(cond)
    invalid_events = final_df.filter(~cond)

    print("--- SCHEMA DEL DATAFRAME PARSEADO ---")
    final_df.printSchema()

    valid_events.writeStream.format("console") \
        .outputMode("append").option("truncate", False).option("numRows", 10).start()
    invalid_events.select("raw_json").writeStream.format("console") \
        .outputMode("append").option("truncate", False).option("numRows", 10).start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

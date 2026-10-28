from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from esquema import evento_schema

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "rapidgo.observabilidad.eventos"

def main():
    spark = SparkSession.builder \
        .appName("RapidGo-SparkStreaming-ReadParse") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df = json_df.select(
        from_json(col("json_str"), evento_schema).alias("data"),
        col("json_str").alias("raw_json")
    ).select("data.*", "raw_json")

    final_df = parsed_df \
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))

    valid_events = final_df.filter(col("event_id").isNotNull() & col("event_time").isNotNull())

    print("--- SCHEMA DEL DATAFRAME PARSEADO ---")
    final_df.printSchema()

    query_valid = valid_events.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .start()

    query_valid.awaitTermination()

if __name__ == "__main__":
    main()

from pyspark.sql.functions import col, window, count, sum as spark_sum, percentile_approx

def agregar_columnas_calidad(df):
    df = df.filter((col("codigo_http") >= 100) & (col("codigo_http") <= 599))
    df = df.withColumn("es_error", (col("codigo_http") >= 500).cast("int"))
    return df

def ventana_1min_error_y_p95(df):
    return (
        df.withWatermark("event_time", "2 minutes")
          .groupBy(window(col("event_time"), "1 minute"), col("servicio"))
          .agg(
              count("*").alias("total_eventos"),
              spark_sum("es_error").alias("eventos_error"),
              percentile_approx("latencia_ms", 0.95).alias("p95_latencia_ms"),
          )
          .withColumn("error_rate", col("eventos_error") / col("total_eventos"))
    )

def ventana_5min_disponibilidad(df):
    return (
        df.withWatermark("event_time", "6 minutes")
          .groupBy(window(col("event_time"), "5 minutes"), col("servicio"))
          .agg(
              count("*").alias("total_eventos"),
              spark_sum("es_error").alias("eventos_error"),
          )
          .withColumn("error_rate_5min", col("eventos_error") / col("total_eventos"))
          .withColumn("disponibilidad", 1 - col("error_rate_5min"))
    )

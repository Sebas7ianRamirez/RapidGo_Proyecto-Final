from pyspark.sql.functions import col

SERVICIOS_VALIDOS = ["pedidos-api", "pagos-api", "tracking-api", "usuarios-api"]
CAMPOS_OBLIGATORIOS = [
    "event_id", "event_time", "servicio", "endpoint",
    "metodo_http", "codigo_http", "latencia_ms", "region", "instancia"
]

def condicion_valido():
    """
    Condición reutilizable para validar los campos obligatorios y rangos
    del contrato de datos (Issue #11 y #13).
    """
    cond = col("event_id").isNotNull()
    for c in CAMPOS_OBLIGATORIOS:
        cond = cond & col(c).isNotNull()
    cond = cond & col("servicio").isin(SERVICIOS_VALIDOS)
    cond = cond & col("codigo_http").between(100, 599)
    return cond

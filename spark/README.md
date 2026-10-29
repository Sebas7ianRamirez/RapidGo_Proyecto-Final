# Módulo de Procesamiento con Spark Structured Streaming (Gate S2)

## Requisitos
- Python 3.10+
- Dependencias de `requirements-spark.txt`.

## Componentes
- `esquema.py`: `StructType` con los 9 campos del contrato.
- `streaming.py`: job que lee de Kafka, parsea, convierte `timestamp`→`event_time`, valida campos/rangos y separa malformados con `raw_json`.

## Configuración por Entorno
- `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`)
- `RAPIDGO_TOPIC` (default `rapidgo.observabilidad.eventos`)

## Ejecución
```bash
pip install -r requirements-spark.txt
python streaming.py
```
## Manejor de Errores
- Se filtran nulos en los 9 campos, servicios fuera del contrato y codigo_http fuera de 100-599.

- Los inválidos se muestran en una segunda consulta vía raw_json sin cortar el stream.

## Alertas y Data Lake (Issue #13)
- La lógica de validación estricta está centralizada en `spark/parseo.py` (`condicion_valido()`).
- El job `streaming_alertas.py` escribe los eventos válidos en `datalake/processed/eventos` en formato Parquet.
- Los KPIs con las alertas calculadas se escriben en `datalake/curated/kpis_alertas`.
- Las alertas se definen en `spark/reglas.py` (`evaluar_alerta_error` > 0.05 y `evaluar_alerta_latencia` > 2000 ms).

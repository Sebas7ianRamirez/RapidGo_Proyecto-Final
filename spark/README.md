# Módulo de Procesamiento con Spark Structured Streaming

## Requisitos

- Docker Desktop o Docker Engine con Docker Compose v2.
- Python 3.12 o superior para ejecutar el productor desde el host.
- Puertos libres: `9092` para Kafka.

## Componentes

- `esquema.py`: define el `StructType` con los nueve campos del contrato.
- `streaming.py`: job base de lectura y validación desde Kafka.
- `streaming_alertas.py`: job utilizado por Docker Compose; escribe eventos válidos y KPIs en Parquet.
- `streaming_metricas.py`: job alternativo para calcular métricas desde Kafka.
- `parseo.py`: centraliza la validación de eventos.
- `reglas.py`: define las reglas de alerta.

## Ejecución recomendada con Docker

1. Inicia Kafka y crea el topic:

```powershell
docker compose up -d
docker compose ps -a
```

2. Inicia Spark antes del productor:

```powershell
docker compose --profile spark up -d spark-streaming
docker compose logs -f spark-streaming
```

3. Espera hasta ver `Batch: 0`.

4. En otra terminal, genera eventos:

```powershell
python -m producer.main --tasa 5 --escenario latencia_alta --anomalia-inicio 60 --anomalia-duracion 180 --max-eventos 1500
```

Spark lee únicamente eventos posteriores a su arranque (`startingOffsets=latest`). Por eso debe iniciarse antes del productor.

Los archivos Parquet se escriben en `datalake/` y los checkpoints en `spark-checkpoints/`. Ambas carpetas están en `.gitignore`.

Las ventanas de la capa `curated` se escriben cuando el watermark de dos minutos las cierra. Por ello, el productor debe seguir enviando eventos durante varios minutos.

Para detener Spark sin borrar checkpoints:

```powershell
docker compose --profile spark stop spark-streaming
```

## Manejo de errores

- Se filtran eventos con campos nulos.
- Se filtran servicios fuera del contrato.
- Se filtran códigos HTTP fuera del rango 100–599.
- Los eventos inválidos se exponen mediante `raw_json` sin detener el stream.

## Alertas y Data Lake

- `spark/parseo.py` contiene `condicion_valido()`.
- `spark/streaming_alertas.py` escribe eventos válidos en `datalake/processed/eventos`.
- `spark/streaming_alertas.py` escribe KPIs y alertas en `datalake/curated/kpis_alertas`.
- `spark/reglas.py` define alertas cuando el error rate supera 0.05 o la latencia p95 supera 2000 ms.
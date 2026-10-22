# Checkpoint Semana 1

## Qué quedó funcionando

- Repositorio creado con los tres integrantes como colaboradores.
- Kanban con Issues distribuidos en las cuatro semanas, responsables y estados.
- Arquitectura de la variante 9 documentada con diagrama.
- Contrato JSON del evento y ejemplos en `samples/`.
- KPIs y reglas de alerta iniciales definidos.
- Decisión de topic, key y particiones de Kafka documentada en ADR-001.
- `docker-compose.yml` preliminar creado.

## Decisión más importante

Se definió `servicio` como key de Kafka y 3 particiones para el topic `rapidgo.observabilidad.eventos`.

Esta decisión permite agrupar los eventos de un mismo servicio en la misma partición, facilitando el cálculo de KPIs por servicio y manteniendo un orden relativo dentro de cada partición. La decisión está documentada en [ADR-001](../decisions/ADR-001-kafka-topic-key-particiones.md).

## Riesgo principal

Un servicio puede generar muchos más eventos que los demás y desbalancear las particiones. Esto podría reducir el paralelismo efectivo y afectar el procesamiento en Spark.

## Plan de la Semana 2

- Implementar el producer de eventos conforme al contrato JSON.
- Implementar Kafka con el topic, la key y las particiones definidas.
- Implementar Spark Structured Streaming para consumir, parsear y transformar eventos.
- Calcular los KPIs y generar alertas.
- Persistir resultados en Parquet usando checkpoint.
- Agregar pruebas y activar `ci.yml`.

## Evidencias

- Issue #3: contrato JSON del evento y ejemplos.
- Issue #4: KPIs y reglas de alerta.
- Issue #5: arquitectura de la variante 9.
- Issue #6: topic, key y particiones de Kafka.
- Issue #7: `docker-compose.yml` preliminar.
- Issue #8: README inicial, checkpoint y release.
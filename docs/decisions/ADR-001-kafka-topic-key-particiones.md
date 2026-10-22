# ADR-001: Topic, key y particiones de Kafka

## Contexto

RapidGo necesita transportar eventos de observabilidad generados por sus APIs hacia Spark Structured Streaming. Los eventos incluyen servicio, endpoint, código HTTP, latencia y timestamp.

## Opciones consideradas

### Opción 1: key = null
- Ventaja: distribución uniforme entre particiones.
- Desventaja: no garantiza orden por servicio.

### Opción 2: key = servicio
- Ventaja: los eventos de un mismo servicio se agrupan en la misma partición.
- Desventaja: puede generar particiones desbalanceadas si un servicio produce muchos más eventos.

### Opción 3: key = endpoint
- Ventaja: mayor granularidad.
- Desventaja: genera muchas claves distintas y complica el análisis por servicio.

## Decisión

- Topic: `rapidgo.observabilidad.eventos`
- Key: `servicio`
- Particiones: 3

## Evidencia

La decisión se basa en el contrato del evento definido en `samples/contrato_evento.schema.json`, donde `servicio` es un campo obligatorio y los KPIs se calculan por servicio.

## Consecuencias

- Los eventos de un mismo servicio mantienen orden relativo dentro de su partición.
- Spark puede procesar eventos en paralelo mediante las tres particiones.
- Si un servicio genera muchísimo más tráfico que los demás, será necesario revisar el balanceo de particiones en la Semana 2.
\# Arquitectura de la variante 9 (Observabilidad tecnológica)



Pipeline de observabilidad para RapidGo: los servicios generan eventos que se capturan, procesan y agregan para calcular KPIs y disparar alertas.



!\[Arquitectura](arquitectura.png)



\## Componentes



\*\*Generador de eventos (Python).\*\* Simula el tráfico de los servicios de RapidGo: `pedidos-api`, `pagos-api`, `tracking-api` y `usuarios-api`. Produce eventos JSON con el contrato del equipo: `event\_id`, `timestamp`, `servicio`, `endpoint`, `metodo\_http`, `codigo\_http`, `latencia\_ms`, `region` e `instancia`.



\*\*Kafka (topic `observability-events`).\*\* Buffer distribuido que desacopla la producción del procesamiento. La key es `servicio`, así todos los eventos de un mismo servicio caen en la misma partición y conservan su orden. Se usan 3 particiones con replicación 1 (un solo broker en el entorno de desarrollo).



\*\*Spark Structured Streaming.\*\* Consume el topic, valida cada evento contra el contrato y lo parsea con un esquema explícito. Aplica ventanas temporales para calcular los KPIs por servicio: tasa de error (`codigo\_http >= 500`), p95 de `latencia\_ms` y disponibilidad. También evalúa las reglas de alerta.



\*\*Data Lake (Raw → Processed → Curated).\*\*

\- \*Raw\*: eventos tal como llegan, sin transformar.

\- \*Processed\*: eventos limpios, tipados y validados contra el contrato.

\- \*Curated\*: KPIs agregados por servicio y ventana temporal.



\*\*Airflow.\*\* Orquesta las tareas de calidad sobre los datos procesados, la agregación final y la generación del reporte.



\*\*KPIs y alertas.\*\* Salida final con tres KPIs por servicio: tasa de error, p95 de latencia y disponibilidad. Dos reglas de alerta mínimas: \*\*tasa de error alta\*\* y \*\*latencia crítica\*\*, que se disparan cuando los valores superan los umbrales definidos por el equipo.



\*\*Transversales.\*\* Docker para el entorno reproducible, Git/GitHub para control de versiones y PRs, Kanban para la gestión de issues, Actions para CI y Releases para las versiones (v0.1.0, v0.2.0, etc.).


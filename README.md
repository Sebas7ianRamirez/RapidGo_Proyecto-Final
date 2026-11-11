# RapidGo — Observabilidad Tecnológica

Proyecto Final de Big Data: pipeline end-to-end para observar la degradación de las APIs y servicios de RapidGo.

## Equipo

| Integrante | Usuario de GitHub | Responsabilidades principales |
|---|---|---|
| Juan Sebastián Organista Ramirez | [@Sebas7ianRamirez](https://github.com/Sebas7ianRamirez) | Kafka (topic y ADR-001), producer, Spark en Docker, Airflow, README y releases |
| Joseph Santiago Morales Carrillo | [@JsantiagoM21](https://github.com/JsantiagoM21) | Contrato de datos, KPIs y alertas, Spark Structured Streaming |
| Richard Jesus Chachalo Madriñan | [@RichardChachalo](https://github.com/RichardChachalo) | Arquitectura, Docker Compose base, pruebas, CI, ADR-002 y checkpoint de la Semana 2 |

## Problema de negocio

RapidGo requiere detectar oportunamente la degradación de sus APIs y servicios. Actualmente no se tiene una visión unificada de errores, latencia y disponibilidad por servicio, lo que dificulta identificar incidentes y actuar a tiempo.

## Variante asignada

**Variante 9 — Observabilidad tecnológica.**

El sistema captura eventos operacionales de APIs, los transporta mediante Kafka, los procesa con Spark Structured Streaming y genera indicadores y alertas sobre la salud de los servicios.

## Arquitectura

![Arquitectura RapidGo](docs/arquitectura.png)

El flujo general es:

```text
Generador de eventos -> Kafka -> Spark Structured Streaming -> Data Lake -> Airflow -> Indicadores y alertas
```

- **Generador de eventos:** simula eventos de `pedidos-api`, `pagos-api`, `tracking-api` y `usuarios-api`.
- **Kafka:** transporta los eventos mediante el topic `rapidgo.observabilidad.eventos`.
- **Spark Structured Streaming:** consume, valida y procesa los eventos; calcula KPIs y escribe resultados en el Data Lake.
- **Data Lake:** guarda eventos válidos en `datalake/processed/eventos` y KPIs con alertas en `datalake/curated/kpis_alertas`.
- **Airflow:** valida las fuentes, agrega métricas, valida la capa `curated` y genera un reporte final.
- **Indicadores y alertas:** error rate, latencia p95, disponibilidad y reglas de alerta.

## Contrato del evento

Los eventos siguen el esquema definido en [`samples/contrato_evento.schema.json`](samples/contrato_evento.schema.json).

Campos principales:

- `event_id`
- `timestamp`
- `servicio`
- `endpoint`
- `metodo_http`
- `codigo_http`
- `latencia_ms`
- `region`
- `instancia`

## KPIs y alertas

Los KPIs iniciales son:

- **Error rate:** porcentaje de eventos con código HTTP mayor o igual a 500.
- **Latencia p95:** percentil 95 de `latencia_ms` por servicio.
- **Disponibilidad:** aproximación basada en `1 - error_rate`.

Las reglas de alerta iniciales son:

- **Error alto:** error rate superior al 5 % en una ventana de 1 minuto.
- **Latencia crítica:** p95 de latencia superior a 2000 ms en una ventana de 1 minuto.

Los detalles están en [`docs/kpis_alertas.md`](docs/kpis_alertas.md).

## Decisiones técnicas

- [ADR-001 — Topic, key y particiones de Kafka](docs/decisions/ADR-001-kafka-topic-key-particiones.md)
- [ADR-002 — Validación y calibración de umbrales de alerta](docs/decisions/ADR-002-validacion-umbrales.md)
- [ADR-003 — Recuperación ante fallos](docs/decisions/ADR-003-recuperacion-ante-fallos.md)

## Checkpoints semanales

- [Semana 1](docs/checkpoints/semana1.md)
- [Semana 2](docs/checkpoints/semana2.md)
- [Semana 3](docs/checkpoints/semana3.md)
  
## Estado actual

La Semana 3 quedó cerrada con el release [`v0.3.0-orchestration`](https://github.com/Sebas7ianRamirez/RapidGo_Proyecto-Final/releases/tag/v0.3.0-orchestration).

El pipeline implementa:

- Generación y publicación de eventos en Kafka con key `servicio` y escenarios de anomalía.
- Topic `rapidgo.observabilidad.eventos` con 3 particiones.
- Procesamiento con Spark Structured Streaming ejecutado en Docker.
- Persistencia de eventos válidos en `datalake/processed/eventos`.
- Persistencia de KPIs y alertas en `datalake/curated/kpis_alertas`.
- Alertas de error rate alto y latencia crítica.
- Airflow 3.3.1 en Docker con el DAG `rapidgo_pipeline_observabilidad`.
- DAG con cinco tareas, dependencias lineales, reintentos y transferencia de métricas mediante XCom.
- Validación de eventos Parquet en `datalake/processed/eventos` y de KPIs Parquet en `datalake/curated/kpis_alertas`.
- Fallo controlado y recuperación documentados en el Issue #23.
- CI que valida sintaxis, pruebas, importación del DAG y `docker-compose.yml`.

## Cómo ejecutarlo desde cero

### Prerrequisitos

- Git.
- Docker Desktop o Docker Engine con Docker Compose v2.
- Python 3.12 o superior.
- Conexión a internet en el primer arranque, para descargar imágenes y dependencias de Spark.
- Puertos libres: `9092` para Kafka y `8088` para la interfaz de Airflow.
- Memoria suficiente para ejecutar Kafka, Spark y Airflow simultáneamente.

### 1. Clonar y preparar Python

```powershell
git clone [https://github.com/Sebas7ianRamirez/RapidGo_Proyecto-Final.git](https://github.com/Sebas7ianRamirez/RapidGo_Proyecto-Final.git)
cd RapidGo_Proyecto-Final
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

En Linux o macOS usa:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Iniciar Kafka y crear el topic

```powershell
docker compose up -d
docker compose ps -a
```

Verifica que:

- `kafka` esté `Up` y healthy.
- `kafka-init` termine con `Exited (0)`.
- `spark-streaming` y `airflow` permanezcan detenidos en este paso.

### 3. Iniciar Spark antes del producer

```powershell
docker compose --profile spark up -d spark-streaming
docker compose logs -f spark-streaming
```

Espera hasta ver `Batch: 0`. Spark está configurado para leer solo eventos publicados después de su arranque.

Para salir del log sin detener Spark, presiona `Ctrl+C`.

### 4. Generar eventos

Abre otra terminal, activa el mismo entorno virtual y ejecuta:

```powershell
python -m producer.main --tasa 5 --max-eventos 1500
```

Deja el producer ejecutándose al menos cinco minutos. Las ventanas de la capa `curated` se cierran con un watermark de dos minutos.

### 5. Verificar el Data Lake

```powershell
(Get-ChildItem datalake\processed -Recurse -Filter *.parquet).Count
(Get-ChildItem datalake\curated -Recurse -Filter *.parquet).Count
```

Deben existir archivos Parquet en ambas rutas. Si `curated` todavía muestra cero archivos, continúa generando eventos y vuelve a verificar después de unos minutos.

### 6. Iniciar Airflow

```powershell
docker compose --profile airflow up -d airflow
docker compose logs airflow | Select-String password
```

Abre <http://localhost:8088>, inicia sesión con el usuario `admin` y la contraseña mostrada en los logs. Luego busca y ejecuta el DAG `rapidgo_pipeline_observabilidad`.

### 7. Ejecutar pruebas

```powershell
python -m pytest -v
```

Las pruebas del DAG pueden aparecer como omitidas si Apache Airflow no está instalado localmente. El workflow de GitHub Actions sí las ejecuta.

### Detener sin perder datos

```powershell
docker compose --profile spark --profile airflow stop
```

Para volver a iniciar los servicios detenidos:

```powershell
docker compose --profile spark --profile airflow start
```

### Comandos que borran datos

`docker compose down` elimina los contenedores. Esto puede borrar los mensajes almacenados en Kafka y el historial de ejecuciones de Airflow, porque la base SQLite vive dentro del contenedor.

No borres manualmente `datalake/` ni `spark-checkpoints/` si deseas conservar los resultados y checkpoints previos.

## Airflow

```powershell
docker compose --profile airflow up -d airflow
docker compose logs airflow | Select-String "password"
```

- Interfaz web: <http://localhost:8088> (solo accesible desde la máquina local).
- Los DAGs se leen de `airflow/dags`, montada en el contenedor (con el datalake integrado en modo de solo lectura :ro).
- La contraseña del usuario administrador se genera al primer arranque y se muestra en los logs.
- **Versión:** Airflow 3.3.1, la versión estable que muestra la documentación oficial; la imagen `apache/airflow:3.3.1` está disponible. Se usa el modo `standalone` con SQLite en un solo contenedor, porque el compose oficial con Celery y Redis exige más memoria (la documentación pide al menos 4 GB para Docker) y aquí comparte recursos con Kafka y Spark. Es suficiente para un DAG lineal; no soporta tareas en paralelo.
- El historial de ejecuciones vive dentro del contenedor: usar `docker compose stop` y `start`, no `down`.

## Contribuciones

| Integrante | Pull Requests principales (Issue) |
|---|---|
| Juan Sebastián Organista Ramirez | #35 ADR-001 (#6), #38 README y plantilla de PR (#8), #39 topic de Kafka (#10), #40 producer (#9), #49 documentación de la Semana 2, #50 conector Kafka de Spark, #51 Spark en Docker (#17), #52 Airflow en Docker Compose (#18), #53 DAG de observabilidad (#19), #54 retries y XCom (#20) |
| Joseph Santiago Morales Carrillo | #33 contrato JSON (#3), #34 KPIs y alertas (#4), #41 lectura y parseo con Spark (#11), #43 transformaciones y ventanas (#12), #45 alertas, Parquet y checkpoint (#13), #55 pruebas de reglas de negocio ampliadas (#21), #56 CI: validación del DAG (#22) |
| Richard Jesus Chachalo Madriñan | #36 arquitectura (#5), #37 compose base (#7), #46 pruebas (#14), #47 CI (#15), #48 ADR-002 y checkpoint de la Semana 2 (#16), #57 fallo controlado y recuperación (#23), #58 ADR-003 y checkpoint de la Semana 3 (#24) |

## Plan de las siguientes semanas

- **Semana 4:** integración final, quality gate, release `v1.0.0` y video final.

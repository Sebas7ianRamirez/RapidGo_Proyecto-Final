# KPIs y Reglas de Alerta — Variante 9 (Observabilidad tecnológica)
El presente documento define formalmente los KPIs y las reglas de alerta automáticas para la infraestructura de microservicios. Todos los cálculos se basan estrictamente en los campos `servicio`, `codigo_http` y `latencia_ms` definidos en el contrato de datos formal (`samples/contrato_evento.schema.json`).
## KPIs

### 1. Error rate
- **Fórmula matemática:** error_rate= count(eventos con codigo_http >= 500) / count(total de eventos válidos)
- **Ventana:** 1 minuto (tumbling o sliding)
- **Umbral de referencia:** normal si ≤ 5%

### 2. Latencia p95
- **Fórmula e implementación en Spark:** Cálculo del percentil 95 de latencia_ms utilizando la función de agregación de Spark Structured Streaming: percentile_approx(latencia_ms, 0.95) o approx_percentile(latencia_ms, 0.95).
- **Contexto y granularidad:** Calculado de forma independiente por servicio y por una ventana de 1 minuto.
- **Ventana:** 1 minuto.
- **Umbral de referencia inicial:** Normal si <= 2000 ms (2 segundos).

### 3. Disponibilidad
- **Fórmula:** disponibilidad = 1 - error_rate = count(eventos con codigo_http < 500) / count(total de eventos válidos en la misma ventana y servicio)
- **Ventana:** 5 minutos
- **Umbral de referencia:** normal si ≥ 98%
- **Aclaración técnica:** Se explicita que disponibilidad = 1 - error_rate es una aproximación basada exclusivamente en errores del lado del servidor (HTTP 5xx). Las respuestas con códigos HTTP 4xx (errores del cliente como 400 o 404) no se consideran indisponibilidad del servicio, salvo que el equipo decida una regla distinta justificada.

## Reglas de alerta

### Alerta 1: Error alto
- **Condición:** error_rate > 5% en ventana de 1 minuto
- **Severidad:** Alta
- **Ventana:** 1 minuto
- **Acción esperada:** revisar logs del servicio afectado
- **Servicio Afectado:** Evaluado por servicio (pedidos-api, pagos-api, tracking-api, usuarios-api)
- **Criterio de Cierre / resolcuión:** error_rate <= 0.05 de manera continua durante al menos 2 ventanas consecutivas (2 minutos).

### Alerta 2: Latencia crítica
- **Condición:** p95_latencia_ms > 2000 ms en ventana de 1 minuto
- **Ventana:** 1 minuto
- **Severidad:** Alta
- **Acción esperada:** revisar carga/infraestructura del servicio afectado
- **Servicio Afectado:** Evaluado por Servicio
- **Acción esperada:** Revisar carga/infraestructura o cuellos de botella del servicio afectado.
- **Criterio de Cierre / resolcuión:** p95_latencia_ms <= 2000 de manera continua durante al menos 2 ventanas consecutivas (2 minutos).



## Justificación de umbrales iniciales
Se parte de valores de referencia típicos de SLOs de APIs REST (error rate < 5%, p95 < 2s, disponibilidad > 98%). Serán recalibrados en la Semana 2 con datos reales del pipeline y documentados formalmente en el ADR-002.
# KPIs y Reglas de Alerta — Variante 9 (Observabilidad tecnológica)

## KPIs

### 1. Error rate
- **Fórmula:** count(codigo_http >= 500) / count(total eventos), agrupado por `servicio`
- **Ventana:** 1 minuto (tumbling o sliding)
- **Umbral de referencia:** normal si ≤ 5%

### 2. Latencia p95
- **Fórmula:** percentil 95 de `latencia_ms`, agrupado por `servicio`
- **Ventana:** 1 minuto
- **Umbral de referencia:** normal si ≤ 2000 ms

### 3. Disponibilidad
- **Fórmula:** 1 - error_rate (equivalente a % de respuestas con código < 500)
- **Ventana:** 5 minutos
- **Umbral de referencia:** normal si ≥ 98%

## Reglas de alerta

### Alerta 1: Error alto
- **Condición:** error_rate > 5% en ventana de 1 minuto
- **Severidad:** Alta
- **Acción esperada:** revisar logs del servicio afectado

### Alerta 2: Latencia crítica
- **Condición:** p95_latencia_ms > 2000 ms en ventana de 1 minuto
- **Severidad:** Alta
- **Acción esperada:** revisar carga/infraestructura del servicio afectado

## Justificación de umbrales iniciales
Se parte de valores de referencia típicos de SLOs de APIs REST (error rate < 5%, p95 < 2s, disponibilidad > 98%). Serán recalibrados en Semana 2 con datos reales del pipeline y documentados en ADR-002.

def evaluar_alerta_error(error_rate: float) -> bool:
    """Alerta de tasa de error alta. Umbral: docs/kpis_alertas.md (> 0.05)."""
    return error_rate > 0.05

def evaluar_alerta_latencia(p95_ms: float) -> bool:
    """Alerta de latencia crítica. Umbral: docs/kpis_alertas.md (> 2000 ms)."""
    return p95_ms > 2000

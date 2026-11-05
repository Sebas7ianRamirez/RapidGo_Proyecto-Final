def evaluar_alerta_error(error_rate: float) -> bool:
    """Alerta de tasa de error alta. Umbral: docs/kpis_alertas.md (> 0.05)."""
    return error_rate > 0.05

def evaluar_alerta_latencia(p95_ms: float) -> bool:
    """Alerta de latencia crítica. Umbral: docs/kpis_alertas.md (> 2000 ms)."""
    return p95_ms > 2000

def calcular_disponibilidad(error_rate: float) -> float:
    """Disponibilidad aproximada basada en error_rate. Docs: docs/kpis_alertas.md.

    disponibilidad = 1 - error_rate (basado solo en codigo_http >= 500).
    """
    if error_rate < 0 or error_rate > 1:
        raise ValueError("error_rate debe estar entre 0 y 1")
    return 1 - error_rate


def evaluar_disponibilidad_critica(disponibilidad: float) -> bool:
    """True si la disponibilidad cae por debajo del umbral normal (< 98%)."""
    return disponibilidad < 0.98

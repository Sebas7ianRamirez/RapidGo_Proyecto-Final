"""Generación de eventos de observabilidad según samples/contrato_evento.schema.json.

Módulo puro: no importa Kafka ni hace entrada/salida, así que se puede probar sin broker.
Los umbrales de las alertas están en docs/kpis_alertas.md; aquí solo se modelan los datos.
"""

import math
import random
import uuid
from datetime import datetime, timezone

SERVICIOS = ("pedidos-api", "pagos-api", "tracking-api", "usuarios-api")
ESCENARIOS = ("normal", "error_alto", "latencia_alta", "degradacion")

# Métodos permitidos por el contrato: GET, POST, PUT, DELETE, PATCH
ENDPOINTS = {
    "pedidos-api": (("GET", "/api/v1/pedidos/{id}"), ("POST", "/api/v1/pedidos"), ("PUT", "/api/v1/pedidos/{id}")),
    "pagos-api": (("POST", "/api/v1/pagos"), ("GET", "/api/v1/pagos/{id}")),
    "tracking-api": (("GET", "/api/v1/tracking/{id}"), ("POST", "/api/v1/tracking/ubicacion")),
    "usuarios-api": (
        ("GET", "/api/v1/usuarios/{id}"),
        ("POST", "/api/v1/usuarios"),
        ("PATCH", "/api/v1/usuarios/{id}"),
    ),
}
REGIONES = ("us-central1", "us-east1")
INSTANCIAS_POR_SERVICIO = 3

CODIGO_EXITO = {"GET": 200, "POST": 201, "PUT": 200, "PATCH": 200, "DELETE": 204}
CODIGOS_4XX = (400, 401, 404, 429)
CODIGOS_5XX = (500, 502, 503, 503, 504)

# Parámetros del modelo (decisiones de modelado, ajustables).
P_ERROR_5XX_NORMAL = 0.01  # ruido de fondo: por debajo del umbral de 5 %
P_ERROR_4XX = 0.03  # errores de cliente: no cuentan como indisponibilidad (kpis_alertas.md)
P_ERROR_5XX_ANOMALIA = 0.15  # con 15 % de 5xx el error rate supera con margen el umbral de 5 %
P_LATENCIA_ALTA_ANOMALIA = 0.15  # con más del 5 % de eventos sobre 2000 ms, el p95 supera 2000 ms
LATENCIA_MEDIANA_MS = 180.0
LATENCIA_SIGMA = 0.6  # distribución lognormal: cola larga, el p95 difiere del promedio
LATENCIA_ALTA_MIN_MS = 2500.0
LATENCIA_ALTA_MAX_MS = 6000.0


def formatear_timestamp(momento: datetime) -> str:
    """ISO 8601 en UTC con milisegundos y sufijo Z, como en samples/evento_ok.json."""
    momento = momento.astimezone(timezone.utc)
    return momento.strftime("%Y-%m-%dT%H:%M:%S.") + f"{momento.microsecond // 1000:03d}Z"


def anomalia_activa(escenario: str, transcurrido_s: float, inicio_s: float = 0.0, duracion_s: float = 0.0) -> bool:
    """Indica si la anomalía está activa en el segundo `transcurrido_s` de la ejecución.

    duracion_s = 0 significa que la anomalía dura hasta el final.
    """
    if escenario == "normal" or transcurrido_s < inicio_s:
        return False
    return duracion_s <= 0 or transcurrido_s < inicio_s + duracion_s


class GeneradorEventos:
    def __init__(self, servicios=SERVICIOS, escenario="normal", servicio_afectado="pagos-api", semilla=None):
        if escenario not in ESCENARIOS:
            raise ValueError(f"Escenario desconocido: {escenario}")
        servicios = tuple(servicios)
        if not servicios or any(s not in SERVICIOS for s in servicios):
            raise ValueError(f"Los servicios deben estar en {SERVICIOS}")
        if escenario != "normal" and servicio_afectado not in servicios:
            raise ValueError("El servicio afectado debe estar entre los servicios generados")
        self.servicios = servicios
        self.escenario = escenario
        self.servicio_afectado = servicio_afectado
        self.rng = random.Random(semilla)  # misma semilla -> misma secuencia de eventos
        self.instancias = {s: [self._nueva_instancia(s) for _ in range(INSTANCIAS_POR_SERVICIO)] for s in servicios}

    def _nueva_instancia(self, servicio: str) -> str:
        sufijo = "".join(self.rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=5))
        return f"{servicio}-{self.rng.getrandbits(24):06x}-{sufijo}"

    def _codigo(self, metodo: str, afectado: bool) -> int:
        p_5xx = P_ERROR_5XX_NORMAL
        if afectado and self.escenario in ("error_alto", "degradacion"):
            p_5xx = P_ERROR_5XX_ANOMALIA
        r = self.rng.random()
        if r < p_5xx:
            return self.rng.choice(CODIGOS_5XX)
        if r < p_5xx + P_ERROR_4XX:
            return self.rng.choice(CODIGOS_4XX)
        return CODIGO_EXITO[metodo]

    def _latencia(self, afectado: bool) -> float:
        if afectado and self.escenario in ("latencia_alta", "degradacion") and self.rng.random() < P_LATENCIA_ALTA_ANOMALIA:
            valor = self.rng.uniform(LATENCIA_ALTA_MIN_MS, LATENCIA_ALTA_MAX_MS)
        else:
            valor = self.rng.lognormvariate(math.log(LATENCIA_MEDIANA_MS), LATENCIA_SIGMA)
        return round(valor, 1)

    def siguiente(self, ahora: datetime = None, anomalia: bool = False) -> dict:
        """Genera un evento. `anomalia=True` aplica el escenario solo al servicio afectado."""
        ahora = ahora or datetime.now(timezone.utc)
        servicio = self.rng.choice(self.servicios)
        metodo, endpoint = self.rng.choice(ENDPOINTS[servicio])
        afectado = anomalia and servicio == self.servicio_afectado
        return {
            "event_id": "evt-" + str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "timestamp": formatear_timestamp(ahora),
            "servicio": servicio,
            "endpoint": endpoint,
            "metodo_http": metodo,
            "codigo_http": self._codigo(metodo, afectado),
            "latencia_ms": self._latencia(afectado),
            "region": self.rng.choice(REGIONES),
            "instancia": self.rng.choice(self.instancias[servicio]),
        }
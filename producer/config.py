"""Parámetros configurables del producer: argumentos de línea de comandos con valores por defecto."""

import argparse
import os
from dataclasses import dataclass

from .eventos import ESCENARIOS, SERVICIOS


@dataclass(frozen=True)
class Config:
    bootstrap: str
    topic: str
    tasa: float
    servicios: tuple
    escenario: str
    servicio_afectado: str
    anomalia_inicio: float
    anomalia_duracion: float
    semilla: int
    max_eventos: int
    log_cada: int
    origen: str
    dry_run: bool


def parsear_argumentos(argv=None) -> Config:
    p = argparse.ArgumentParser(
        prog="python -m producer.main",
        description="Generador de eventos de observabilidad de RapidGo (variante 9).",
    )
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), help="Broker de Kafka (por defecto localhost:9092, listener del host)")
    p.add_argument("--topic", default=os.environ.get("RAPIDGO_TOPIC", "rapidgo.observabilidad.eventos"), help="Topic de destino (ADR-001)")
    p.add_argument("--tasa", type=float, default=5.0, help="Eventos por segundo en total (por defecto 5)")
    p.add_argument("--servicios", nargs="+", choices=SERVICIOS, default=list(SERVICIOS), help="Servicios que generan eventos (por defecto los 4 del contrato)")
    p.add_argument("--escenario", choices=ESCENARIOS, default="normal", help="normal | error_alto | latencia_alta | degradacion")
    p.add_argument("--servicio-afectado", choices=SERVICIOS, default="pagos-api", help="Servicio que sufre la anomalía (por defecto pagos-api)")
    p.add_argument("--anomalia-inicio", type=float, default=0.0, help="Segundos de funcionamiento normal antes de la anomalía (por defecto 0)")
    p.add_argument("--anomalia-duracion", type=float, default=0.0, help="Segundos que dura la anomalía; 0 = hasta el final")
    p.add_argument("--semilla", type=int, default=None, help="Semilla para reproducir la misma secuencia")
    p.add_argument("--max-eventos", type=int, default=0, help="Detenerse tras N eventos; 0 = sin límite")
    p.add_argument("--log-cada", type=int, default=50, help="Mostrar partición y offset cada N entregas")
    p.add_argument("--origen", default=os.environ.get("RAPIDGO_ORIGEN", "equipo1-variante9"), help="Valor del header Kafka 'origen' que identifica al equipo")
    p.add_argument("--dry-run", action="store_true", help="No usa Kafka: genera eventos, los valida contra el contrato y resume por servicio")
    a = p.parse_args(argv)

    if a.tasa <= 0:
        p.error("--tasa debe ser mayor que 0")
    if a.max_eventos < 0 or a.log_cada < 1:
        p.error("--max-eventos no puede ser negativo y --log-cada debe ser al menos 1")
    servicios = tuple(dict.fromkeys(a.servicios))
    if a.escenario != "normal" and a.servicio_afectado not in servicios:
        p.error("--servicio-afectado debe estar incluido en --servicios")

    return Config(
        bootstrap=a.bootstrap,
        topic=a.topic,
        tasa=a.tasa,
        servicios=servicios,
        escenario=a.escenario,
        servicio_afectado=a.servicio_afectado,
        anomalia_inicio=a.anomalia_inicio,
        anomalia_duracion=a.anomalia_duracion,
        semilla=a.semilla,
        max_eventos=a.max_eventos,
        log_cada=a.log_cada,
        origen=a.origen,
        dry_run=a.dry_run,
    )
"""Producer de eventos de observabilidad de RapidGo (variante 9).

Uso (desde la raíz del repositorio):
    python -m producer.main --dry-run --escenario degradacion      # sin Kafka
    python -m producer.main --tasa 5 --max-eventos 100             # publica en Kafka
"""

import json
import math
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config, parsear_argumentos
from .eventos import GeneradorEventos, anomalia_activa

RUTA_ESQUEMA = Path(__file__).resolve().parent.parent / "samples" / "contrato_evento.schema.json"


def percentil(valores, q):
    """Percentil por rango más cercano. Spark usa percentile_approx, que puede diferir levemente."""
    ordenados = sorted(valores)
    return ordenados[max(0, math.ceil(q * len(ordenados)) - 1)]


def resumen_por_servicio(eventos):
    por_servicio = defaultdict(list)
    for e in eventos:
        por_servicio[e["servicio"]].append(e)
    filas = []
    for servicio in sorted(por_servicio):
        evs = por_servicio[servicio]
        errores_5xx = sum(1 for e in evs if e["codigo_http"] >= 500)
        filas.append((servicio, len(evs), errores_5xx / len(evs), percentil([e["latencia_ms"] for e in evs], 0.95)))
    return filas


def validar_contra_contrato(eventos):
    """Valida con jsonschema y comprueba el formato de timestamp (jsonschema no lo exige por defecto)."""
    try:
        import jsonschema
    except ImportError:
        print("jsonschema no esta instalado: se omite la validacion contra el contrato")
        return
    esquema = json.loads(RUTA_ESQUEMA.read_text(encoding="utf-8"))
    validador = jsonschema.Draft7Validator(esquema)
    invalidos = 0
    for e in eventos:
        try:
            datetime.fromisoformat(e["timestamp"])
            valido = validador.is_valid(e)
        except ValueError:
            valido = False
        if not valido:
            invalidos += 1
            print("EVENTO INVALIDO:", json.dumps(e))
    print(f"Validacion contra el contrato: {len(eventos) - invalidos}/{len(eventos)} eventos validos")


def ejecutar_dry_run(cfg: Config, gen: GeneradorEventos) -> int:
    n = cfg.max_eventos or 200
    base = datetime.now(timezone.utc)
    eventos = []
    for i in range(n):
        t = i / cfg.tasa  # reloj simulado: no se espera tiempo real
        anomalia = anomalia_activa(cfg.escenario, t, cfg.anomalia_inicio, cfg.anomalia_duracion)
        eventos.append(gen.siguiente(base + timedelta(seconds=t), anomalia))
    for e in eventos[:3]:
        print(json.dumps(e, ensure_ascii=False))
    validar_contra_contrato(eventos)
    print(f"\nEscenario={cfg.escenario} afectado={cfg.servicio_afectado} eventos={n}")
    print(f"{'servicio':<14}{'eventos':>8}{'error_rate':>12}{'p95_ms':>10}")
    for servicio, total, tasa_error, p95 in resumen_por_servicio(eventos):
        print(f"{servicio:<14}{total:>8}{tasa_error:>12.1%}{p95:>10.1f}")
    return 0


def publicar(producer, cfg: Config, evento: dict, al_entregar):
    valor = json.dumps(evento, ensure_ascii=False).encode("utf-8")
    clave = evento["servicio"].encode("utf-8")  # key = servicio (ADR-001)
    while True:
        try:
            producer.produce(cfg.topic, key=clave, value=valor, headers={"origen": cfg.origen}, on_delivery=al_entregar)
            return
        except BufferError:  # cola local llena: se atienden entregas y se reintenta
            producer.poll(0.5)


def ejecutar_envio(cfg: Config, gen: GeneradorEventos) -> int:
    from confluent_kafka import Producer  # importación tardía: el dry-run no necesita Kafka

    entregas = Counter()  # (servicio, partición) -> cantidad
    fallos = []
    ok = [0]

    def al_entregar(err, msg):
        if err is not None:
            fallos.append(str(err))
            print("ERROR de entrega:", err)
            return
        ok[0] += 1
        servicio = msg.key().decode("utf-8")
        entregas[(servicio, msg.partition())] += 1
        if ok[0] == 1 or ok[0] % cfg.log_cada == 0:
            print(f"entregado #{ok[0]} servicio={servicio} particion={msg.partition()} offset={msg.offset()}")

    producer = Producer({"bootstrap.servers": cfg.bootstrap, "client.id": "rapidgo-producer", "acks": "all"})
    inicio = time.monotonic()
    proximo = inicio
    enviados = 0
    print(f"Publicando en {cfg.bootstrap} topic={cfg.topic} tasa={cfg.tasa}/s escenario={cfg.escenario}. Ctrl+C para detener.")
    try:
        while cfg.max_eventos == 0 or enviados < cfg.max_eventos:
            t = time.monotonic() - inicio
            anomalia = anomalia_activa(cfg.escenario, t, cfg.anomalia_inicio, cfg.anomalia_duracion)
            publicar(producer, cfg, gen.siguiente(anomalia=anomalia), al_entregar)
            enviados += 1
            producer.poll(0)  # atiende los callbacks de entrega
            proximo += 1.0 / cfg.tasa
            espera = proximo - time.monotonic()
            if espera > 0:
                time.sleep(espera)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        pendientes = producer.flush(15)

    print(f"\nEnviados={enviados} entregados={ok[0]} fallos={len(fallos)} sin confirmar={pendientes}")
    print("Reparto servicio -> particion (cantidad de eventos):")
    for (servicio, particion), cantidad in sorted(entregas.items()):
        print(f"  {servicio:<14} particion {particion}: {cantidad}")
    return 0 if not fallos and pendientes == 0 else 1


def main(argv=None) -> int:
    cfg = parsear_argumentos(argv)
    gen = GeneradorEventos(cfg.servicios, cfg.escenario, cfg.servicio_afectado, cfg.semilla)
    return ejecutar_dry_run(cfg, gen) if cfg.dry_run else ejecutar_envio(cfg, gen)


if __name__ == "__main__":
    sys.exit(main())
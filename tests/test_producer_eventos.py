"""Pruebas del generador de eventos (producer/eventos.py). Es un módulo puro: no necesita Kafka."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

from producer.eventos import ESCENARIOS, SERVICIOS, GeneradorEventos, anomalia_activa

ESQUEMA = json.loads(
    (Path(__file__).resolve().parent.parent / "samples" / "contrato_evento.schema.json").read_text(encoding="utf-8")
)
VALIDADOR = Draft7Validator(ESQUEMA)
AHORA = datetime(2026, 10, 28, 12, 0, 0, tzinfo=timezone.utc)


def tasa_5xx(eventos, servicio):
    del_servicio = [e for e in eventos if e["servicio"] == servicio]
    return sum(1 for e in del_servicio if e["codigo_http"] >= 500) / len(del_servicio)


@pytest.mark.parametrize("escenario", ESCENARIOS)
def test_los_eventos_generados_cumplen_el_contrato(escenario):
    gen = GeneradorEventos(escenario=escenario, semilla=1)
    for _ in range(300):
        evento = gen.siguiente(AHORA, anomalia=True)
        assert VALIDADOR.is_valid(evento), evento
        datetime.fromisoformat(evento["timestamp"].replace("Z", "+00:00"))


def test_la_misma_semilla_produce_la_misma_secuencia():
    a = GeneradorEventos(semilla=5)
    b = GeneradorEventos(semilla=5)
    assert [a.siguiente(AHORA) for _ in range(20)] == [b.siguiente(AHORA) for _ in range(20)]


def test_escenario_normal_no_supera_el_umbral_de_error():
    gen = GeneradorEventos(escenario="normal", semilla=3)
    eventos = [gen.siguiente(AHORA, anomalia=True) for _ in range(2000)]
    assert all(tasa_5xx(eventos, s) <= 0.05 for s in SERVICIOS)


def test_error_alto_supera_el_umbral_solo_en_el_servicio_afectado():
    gen = GeneradorEventos(escenario="error_alto", servicio_afectado="pagos-api", semilla=3)
    eventos = [gen.siguiente(AHORA, anomalia=True) for _ in range(2000)]
    assert tasa_5xx(eventos, "pagos-api") > 0.05
    assert tasa_5xx(eventos, "pedidos-api") <= 0.05


def test_latencia_alta_genera_latencias_sobre_2000_ms_solo_en_el_afectado():
    gen = GeneradorEventos(escenario="latencia_alta", servicio_afectado="pagos-api", semilla=3)
    eventos = [gen.siguiente(AHORA, anomalia=True) for _ in range(2000)]
    lentos = {e["servicio"] for e in eventos if e["latencia_ms"] > 2000}
    assert lentos == {"pagos-api"}


def test_sin_anomalia_el_escenario_no_se_aplica():
    gen = GeneradorEventos(escenario="error_alto", semilla=3)
    eventos = [gen.siguiente(AHORA, anomalia=False) for _ in range(2000)]
    assert tasa_5xx(eventos, "pagos-api") <= 0.05


@pytest.mark.parametrize(
    "escenario, t, inicio, duracion, esperado",
    [
        ("normal", 10, 0, 0, False),  # el escenario normal nunca tiene anomalía
        ("degradacion", 5, 10, 0, False),  # antes del inicio
        ("degradacion", 10, 10, 0, True),  # justo en el inicio (límite)
        ("degradacion", 500, 10, 0, True),  # duración 0 = hasta el final
        ("degradacion", 19.9, 10, 10, True),  # dentro de la duración
        ("degradacion", 20, 10, 10, False),  # justo al terminar (límite)
    ],
)
def test_anomalia_activa(escenario, t, inicio, duracion, esperado):
    assert anomalia_activa(escenario, t, inicio, duracion) is esperado


def test_escenario_desconocido_se_rechaza():
    with pytest.raises(ValueError):
        GeneradorEventos(escenario="inexistente")


def test_servicio_fuera_del_contrato_se_rechaza():
    with pytest.raises(ValueError):
        GeneradorEventos(servicios=["otro-api"])


def test_servicio_afectado_debe_estar_entre_los_generados():
    with pytest.raises(ValueError):
        GeneradorEventos(servicios=["pedidos-api"], escenario="error_alto", servicio_afectado="pagos-api")
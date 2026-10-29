"""Pruebas del contrato del evento (samples/contrato_evento.schema.json). No usan Kafka ni Spark."""

import copy
import json
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft7Validator

SAMPLES = Path(__file__).resolve().parent.parent / "samples"
ESQUEMA = json.loads((SAMPLES / "contrato_evento.schema.json").read_text(encoding="utf-8"))
VALIDADOR = Draft7Validator(ESQUEMA)


def cargar(nombre):
    return json.loads((SAMPLES / nombre).read_text(encoding="utf-8"))


def con_cambio(campo, valor):
    evento = copy.deepcopy(cargar("evento_ok.json"))
    evento[campo] = valor
    return evento


def test_el_esquema_es_un_json_schema_valido():
    Draft7Validator.check_schema(ESQUEMA)


@pytest.mark.parametrize("nombre", ["evento_ok.json", "evento_error.json"])
def test_samples_cumplen_el_contrato(nombre):
    assert VALIDADOR.is_valid(cargar(nombre))


@pytest.mark.parametrize("nombre", ["evento_ok.json", "evento_error.json"])
def test_timestamp_de_los_samples_es_iso8601(nombre):
    # jsonschema no exige "format": "date-time" por defecto, por eso se comprueba aparte.
    datetime.fromisoformat(cargar(nombre)["timestamp"].replace("Z", "+00:00"))


@pytest.mark.parametrize("campo", ESQUEMA["required"])
def test_falta_un_campo_obligatorio(campo):
    evento = cargar("evento_ok.json")
    del evento[campo]
    assert not VALIDADOR.is_valid(evento)


@pytest.mark.parametrize("campo", ["region", "instancia"])
def test_los_campos_opcionales_pueden_faltar(campo):
    evento = cargar("evento_ok.json")
    del evento[campo]
    assert VALIDADOR.is_valid(evento)


@pytest.mark.parametrize(
    "campo, valor",
    [
        ("codigo_http", 99),
        ("codigo_http", 600),
        ("codigo_http", 700),
        ("codigo_http", "200"),
        ("servicio", "otro-api"),
        ("metodo_http", "FETCH"),
        ("latencia_ms", -1),
        ("latencia_ms", "rapido"),
    ],
)
def test_valores_invalidos_se_rechazan(campo, valor):
    assert not VALIDADOR.is_valid(con_cambio(campo, valor))


@pytest.mark.parametrize(
    "campo, valor",
    [("codigo_http", 100), ("codigo_http", 599), ("latencia_ms", 0), ("latencia_ms", 4210.5)],
)
def test_valores_limite_validos(campo, valor):
    assert VALIDADOR.is_valid(con_cambio(campo, valor))
"""Pruebas de las reglas de alerta (spark/reglas.py, Issue #13). Sin Spark ni Kafka.

Umbrales de docs/kpis_alertas.md: error_rate > 5 % y p95 > 2000 ms.
La condición es "mayor que": exactamente en el umbral NO alerta.
"""

import pytest

from spark.reglas import evaluar_alerta_error, evaluar_alerta_latencia


@pytest.mark.parametrize(
    "error_rate, alerta",
    [
        (0.0, False),  # normal
        (0.01, False),  # ruido de fondo del escenario normal
        (0.0476, False),  # 1 error en 21 eventos: caso real observado, bajo el umbral
        (0.05, False),  # límite exacto
        (0.0501, True),  # apenas por encima
        (0.111, True),  # degradación observada en el Issue #13
        (1.0, True),  # todos los eventos fallan
    ],
)
def test_alerta_error_alto(error_rate, alerta):
    assert evaluar_alerta_error(error_rate) is alerta


@pytest.mark.parametrize(
    "p95_ms, alerta",
    [
        (0.0, False),
        (485.1, False),  # p95 normal observado en el Issue #12
        (1999.9, False),
        (2000.0, False),  # límite exacto
        (2000.1, True),  # apenas por encima
        (5977.1, True),  # degradación observada en el Issue #13
    ],
)
def test_alerta_latencia_critica(p95_ms, alerta):
    assert evaluar_alerta_latencia(p95_ms) is alerta


def test_las_dos_alertas_son_independientes():
    # Mucho error con latencia normal: solo alerta de error.
    assert evaluar_alerta_error(0.2) is True and evaluar_alerta_latencia(300.0) is False
    # Latencia crítica sin errores: solo alerta de latencia.
    assert evaluar_alerta_error(0.0) is False and evaluar_alerta_latencia(3000.0) is True
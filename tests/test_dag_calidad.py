"""Pruebas de calidad del DAG de Airflow (Issue #21).

Valida que rapidgo_pipeline_dag.py se pueda importar sin errores, que tenga
las 5 tareas esperadas, que las dependencias sean la cadena lineal esperada
y que los retries estén configurados como exige el Issue #20.
"""

import sys
from pathlib import Path

import pytest

DAG_FOLDER = Path(__file__).resolve().parent.parent / "airflow" / "dags"

airflow_models = pytest.importorskip(
    "airflow.models", reason="apache-airflow no está instalado en este entorno"
)
DagBag = airflow_models.DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAG_FOLDER))


def test_el_dag_se_importa_sin_errores(dagbag):
    assert dagbag.import_errors == {}, dagbag.import_errors


def test_el_dag_existe(dagbag):
    assert "rapidgo_pipeline_observabilidad" in dagbag.dags


def test_el_dag_tiene_al_menos_cinco_tareas(dagbag):
    dag = dagbag.dags["rapidgo_pipeline_observabilidad"]
    assert len(dag.tasks) >= 5


def test_las_tareas_esperadas_existen(dagbag):
    dag = dagbag.dags["rapidgo_pipeline_observabilidad"]
    ids_esperados = {
        "check_fuentes",
        "validar_raw",
        "agregar",
        "validar_curated",
        "reporte",
    }
    ids_reales = {task.task_id for task in dag.tasks}
    assert ids_esperados.issubset(ids_reales)


def test_la_cadena_de_dependencias_es_lineal(dagbag):
    dag = dagbag.dags["rapidgo_pipeline_observabilidad"]
    orden_esperado = [
        "check_fuentes",
        "validar_raw",
        "agregar",
        "validar_curated",
        "reporte",
    ]

    for actual, siguiente in zip(orden_esperado, orden_esperado[1:]):
        tarea_actual = dag.get_task(actual)
        ids_downstream = {t.task_id for t in tarea_actual.downstream_list}
        assert siguiente in ids_downstream, (
            f"Se esperaba que '{actual}' tuviera como downstream a '{siguiente}'"
        )


def test_todas_las_tareas_tienen_retries_configurados(dagbag):
    dag = dagbag.dags["rapidgo_pipeline_observabilidad"]
    for task in dag.tasks:
        assert task.retries is not None and task.retries >= 1, (
            f"La tarea '{task.task_id}' no tiene retries configurados (Issue #20)"
        )


def test_el_dag_no_tiene_ciclos(dagbag):
    dag = dagbag.dags["rapidgo_pipeline_observabilidad"]
    # test_cycle lanza AirflowDagCycleException si hay un ciclo; no lanzar nada = sin ciclos
    dag.check_cycle()

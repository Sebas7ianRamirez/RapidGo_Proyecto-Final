"""Verificador ligero del pipeline RapidGo.

Comprueba, sin modificar datos, que existan archivos Parquet en las capas
processed y curated, y que el servicio de Airflow responda en el puerto 8088.

Uso:
    python scripts/verificar_pipeline.py
"""

from pathlib import Path
from urllib.request import urlopen


def contar_parquet(ruta: Path) -> int:
    if not ruta.exists():
        return 0

    return len(
        [
            archivo
            for archivo in ruta.rglob("*.parquet")
            if not archivo.name.startswith(".")
        ]
    )


def main() -> None:
    base = Path(__file__).resolve().parent.parent
    processed = base / "datalake" / "processed" / "eventos"
    curated = base / "datalake" / "curated" / "kpis_alertas"

    parquet_processed = contar_parquet(processed)
    parquet_curated = contar_parquet(curated)

    print("=== Verificación del pipeline RapidGo ===")
    print(f"Parquet en processed/eventos: {parquet_processed}")
    print(f"Parquet en curated/kpis_alertas: {parquet_curated}")

    if parquet_processed == 0:
        print("ADVERTENCIA: no hay Parquet en processed/eventos.")

    if parquet_curated == 0:
        print("ADVERTENCIA: no hay Parquet en curated/kpis_alertas.")

    try:
        with urlopen("http://localhost:8088/health", timeout=5) as respuesta:
            print(f"Airflow respondió con código HTTP {respuesta.status}")
    except Exception as exc:
        print(f"ADVERTENCIA: Airflow no respondió en el puerto 8088: {exc}")

    print("Verificación finalizada.")


if __name__ == "__main__":
    main()
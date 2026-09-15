#!/usr/bin/env python3
"""Flow observable: Prefect orquesta y Dask ejecuta una tarea por partición."""

from __future__ import annotations

import glob
import os
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect_dask import DaskTaskRunner, get_dask_client

from src.cleaning import ANOMALY_TOKEN, clean_partition
from src.observability import execution_breakdown

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/shared-data"))
DASK_SCHEDULER = os.environ.get("DASK_SCHEDULER", "tcp://dask-scheduler:8786")
EXPECTED_ROWS = int(os.environ.get("NUM_ROWS", "300000"))
EXPECTED_WORKERS = 3
RAW_PATTERN = str(DATA_ROOT / "raw" / "transactions_dirty_part_*.csv")
PROCESSED_DIR = DATA_ROOT / "processed"


def execution_location() -> tuple[str, int]:
    """Identifica el worker y el hilo que materializan una partición."""
    try:
        from distributed import get_worker

        return str(get_worker().name), threading.get_ident()
    except (ImportError, ValueError):
        return "local", threading.get_ident()


@task(name="1. Validar infraestructura Dask", retries=3, retry_delay_seconds=5)
def validate_cluster() -> dict[str, int]:
    """Comprueba que el flow no empiece hasta tener los tres workers."""
    logger = get_run_logger()
    with get_dask_client() as client:
        workers = client.scheduler_info()["workers"]
        threads = sum(item["nthreads"] for item in workers.values())
    logger.info("Clúster conectado: %s workers y %s hilos", len(workers), threads)
    if len(workers) < EXPECTED_WORKERS:
        raise RuntimeError(f"Se requieren {EXPECTED_WORKERS} workers; disponibles: {len(workers)}")
    return {"workers": len(workers), "threads": threads}


@task(name="2. Verificar CSV crudos particionados")
def find_raw_partitions() -> list[str]:
    paths = sorted(glob.glob(RAW_PATTERN))
    if len(paths) != 6:
        raise FileNotFoundError(
            f"Se esperaban seis CSV en {RAW_PATTERN}; ejecute primero python -m src.generate_dirty_data"
        )
    if any(Path(path).stat().st_size == 0 for path in paths):
        raise ValueError("Hay una partición CSV vacía")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    get_run_logger().info("Se detectaron %s particiones crudas", len(paths))
    return paths


@task(name="3. Limpiar partición en worker Dask", retries=2, retry_delay_seconds=5)
def clean_one_partition(csv_path: str) -> dict[str, Any]:
    """Lee, limpia y escribe solamente el CSV asignado a este worker."""
    logger = get_run_logger()
    worker, thread = execution_location()
    partition = Path(csv_path).stem
    started = time.perf_counter()

    raw = pd.read_csv(csv_path, dtype=str)
    cleaned = clean_partition(raw)
    output_path = PROCESSED_DIR / f"{partition}.parquet"
    cleaned.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

    seconds = time.perf_counter() - started
    anomalies = int((cleaned["customer_code"] == ANOMALY_TOKEN).sum())
    repaired = int(raw["city_notes_corrupted"].astype("string").str.contains("Ã|Â", regex=True, na=False).sum())
    report = {
        "partition": partition,
        "worker": worker,
        "thread": thread,
        "rows": len(cleaned),
        "canonical_codes": len(cleaned) - anomalies,
        "anomalies": anomalies,
        "repaired_mojibake": repaired,
        "valid_phones": int(cleaned["phone"].notna().sum()),
        "seconds": seconds,
        "output": str(output_path),
    }
    logger.info("[%s] %s: %s filas en %.2f s", worker, partition, len(cleaned), seconds)
    return report


@task(name="4. Publicar observabilidad y quality gates")
def publish_observability_and_validate(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """Archiva la distribución del trabajo y valida el Parquet independiente."""
    logger = get_run_logger()
    matrix, details = execution_breakdown(reports)
    create_table_artifact(
        key="dask-cluster-execution-breakdown",
        table=matrix,
        description="Carga agregada por worker del clúster Dask.",
    )
    create_table_artifact(
        key="dask-partitions-by-worker",
        table=details,
        description="Partición, worker, hilo, métricas y tiempo de ejecución.",
    )

    output_paths = [report["output"] for report in reports]
    final = pd.concat([pd.read_parquet(path, engine="pyarrow") for path in output_paths], ignore_index=True)
    rows = len(final)
    anomalies = int((final["customer_code"] == ANOMALY_TOKEN).sum())
    canonical = int(final["customer_code"].str.fullmatch(r"CUST-\d{5}", na=False).sum())
    mojibake = int(final["city_notes"].astype("string").str.contains("Ã|Â", regex=True, na=False).sum())
    null_codes = int(final["customer_code"].isna().sum())

    assert rows == EXPECTED_ROWS, f"Se esperaban {EXPECTED_ROWS:,} filas y se escribieron {rows:,}"
    assert canonical + anomalies == rows, "Hay códigos fuera del formato canónico o de anomalía"
    assert null_codes == 0, "Quedaron códigos nulos"
    assert mojibake == 0, "Persisten secuencias mojibake"

    for row in matrix:
        logger.info("%s procesó %s filas en %s particiones", row["worker"], row["filas procesadas"], row["particiones"])
    return {
        "rows": rows,
        "canonical_codes": canonical,
        "code_anomalies": anomalies,
        "remaining_mojibake": mojibake,
        "partitions": len(reports),
        "workers_used": len(matrix),
    }


@flow(
    name="dask-prefect-distributed-cleaning",
    task_runner=DaskTaskRunner(address=DASK_SCHEDULER),
    log_prints=True,
)
def distributed_cleaning_flow() -> dict[str, Any]:
    """Expone en Prefect las seis ramas paralelas del patrón Master-Worker."""
    infrastructure = validate_cluster.submit()
    partitions = find_raw_partitions.submit(wait_for=[infrastructure]).result()
    cleaning_futures = [clean_one_partition.submit(path) for path in partitions]
    result = publish_observability_and_validate.submit(cleaning_futures).result()
    print(f"[OK] Pipeline completado: {result}")
    return result


if __name__ == "__main__":
    distributed_cleaning_flow()

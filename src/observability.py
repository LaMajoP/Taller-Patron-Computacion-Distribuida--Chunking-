"""Agregación de evidencia sobre el reparto real de trabajo en el clúster."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def execution_breakdown(reports: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Construye tablas por worker y por partición a partir de los reportes."""
    labels: dict[str, dict[int, str]] = defaultdict(dict)
    for report in reports:
        worker_threads = labels[report["worker"]]
        if report["thread"] not in worker_threads:
            worker_threads[report["thread"]] = f"hilo-{len(worker_threads)}"

    details = []
    totals: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"partitions": 0, "rows": 0, "seconds": 0.0, "threads": set()}
    )
    for report in sorted(reports, key=lambda item: item["partition"]):
        worker = report["worker"]
        thread_label = labels[worker][report["thread"]]
        details.append(
            {
                "partición": report["partition"],
                "worker": worker,
                "hilo": thread_label,
                "filas": report["rows"],
                "códigos canónicos": report["canonical_codes"],
                "anomalías": report["anomalies"],
                "mojibake reparado": report["repaired_mojibake"],
                "teléfonos válidos": report["valid_phones"],
                "segundos": round(report["seconds"], 3),
            }
        )
        total = totals[worker]
        total["partitions"] += 1
        total["rows"] += report["rows"]
        total["seconds"] += report["seconds"]
        total["threads"].add(thread_label)

    elapsed_total = sum(total["seconds"] for total in totals.values()) or 1.0
    matrix = [
        {
            "worker": worker,
            "hilos usados": len(total["threads"]),
            "particiones": total["partitions"],
            "filas procesadas": total["rows"],
            "CPU (s)": round(total["seconds"], 2),
            "% de la carga": round(total["seconds"] * 100 / elapsed_total, 1),
        }
        for worker, total in sorted(totals.items())
    ]
    return matrix, details

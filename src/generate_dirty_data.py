#!/usr/bin/env python3
"""Genera seis CSV reproducibles con 300.000 transacciones deliberadamente sucias."""

from __future__ import annotations

import argparse
import os
import random
from pathlib import Path

import numpy as np
import pandas as pd

NUM_ROWS = int(os.environ.get("NUM_ROWS", "300000"))
NUM_CHUNKS = 6
SEED = 42
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "shared-data"))

CODE_TEMPLATES: list[str | None] = [
    "  CLI-{id}-A  ", "cli_{id}_norm", "RAW#{id}-[V2]", "CUST-{id}-EXP",
    "  {id}  ", "ANOMALOUS_STR", "INVALID", "   ", None,
]
CODE_WEIGHTS = [0.45, 0.20, 0.15, 0.10, 0.04, 0.02, 0.02, 0.01, 0.01]
CLEAN_PHRASES = [
    "Transacción exitosa en Bogotá D.C.",
    "Cliente atendido en Medellín por garantía",
    "Verificación de crédito rechazada en Popayán",
    "Envío exprés hacia Cartagena de Indias",
    "Actualización de dirección en Cali (Valle)",
    "Operación pendiente de conciliación bancaria",
    "Sin observaciones registradas",
    "ñandú importado - paquete especial",
]
PHRASE_WEIGHTS = [0.25, 0.20, 0.15, 0.15, 0.10, 0.08, 0.03, 0.02, 0.01, 0.01]
PHONE_TEMPLATES = [
    "+57 (310) {p1}-{p2}", "310.{p1}.{p2}", "TEL: 310{p1}{p2} Ext 402",
    "0057 310 {p1} {p2}", "310{p1}{p2}", "DESCONOCIDO", "N/A", "--", "",
]
PHONE_WEIGHTS = [0.35, 0.25, 0.15, 0.10, 0.08, 0.03, 0.02, 0.01, 0.01]
CATEGORIES = ["FINANCE", "LOGISTICS", "RETAIL", "HEALTH", "TECH"]


def corrupt_encoding(phrase: str) -> str:
    """Simula UTF-8 interpretado como Latin-1: Bogotá -> BogotÃ¡."""
    return phrase.encode("utf-8").decode("latin-1")


def generate_dirty_dataset(num_rows: int = NUM_ROWS, output_dir: str | Path | None = None) -> None:
    """Escribe exactamente seis particiones que, juntas, contienen ``num_rows``."""
    if num_rows <= 0:
        raise ValueError("num_rows debe ser positivo")
    destination = Path(output_dir) if output_dir else DATA_ROOT / "raw"
    destination.mkdir(parents=True, exist_ok=True)
    print(f"[*] Generando {num_rows:,} filas con ruido sintético...")

    np.random.seed(SEED)
    random.seed(SEED)
    customer_ids = np.random.randint(10000, 99999, size=num_rows)
    raw_codes = []
    for customer_id in customer_ids:
        template = random.choices(CODE_TEMPLATES, weights=CODE_WEIGHTS, k=1)[0]
        raw_codes.append(np.nan if template is None else template.format(id=customer_id))

    corrupted_notes = random.choices(
        [corrupt_encoding(phrase) for phrase in CLEAN_PHRASES] + ["   ", None],
        weights=PHRASE_WEIGHTS,
        k=num_rows,
    )
    raw_phones = []
    for _ in range(num_rows):
        template = random.choices(PHONE_TEMPLATES, weights=PHONE_WEIGHTS, k=1)[0]
        raw_phones.append(template.format(p1=random.randint(100, 999), p2=random.randint(1000, 9999)))

    amounts = np.random.exponential(scale=150.0, size=num_rows)
    amounts[np.random.rand(num_rows) < 0.03] = np.nan
    dataframe = pd.DataFrame(
        {
            "transaction_id": [f"TX-{index:07d}" for index in range(1, num_rows + 1)],
            "raw_customer_code": raw_codes,
            "city_notes_corrupted": corrupted_notes,
            "phone_raw": raw_phones,
            "amount_usd": np.round(amounts, 2),
            "business_category": random.choices(CATEGORIES, k=num_rows),
        }
    )

    base_size, remainder = divmod(num_rows, NUM_CHUNKS)
    start = 0
    for index in range(NUM_CHUNKS):
        size = base_size + (1 if index < remainder else 0)
        chunk = dataframe.iloc[start : start + size]
        start += size
        path = destination / f"transactions_dirty_part_{index + 1}.csv"
        chunk.to_csv(path, index=False, encoding="utf-8")
        print(f" -> Partición {index + 1}/{NUM_CHUNKS}: {path} ({len(chunk):,} filas)")
    print(f"[OK] Dataset generado en {destination}/")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=NUM_ROWS)
    parser.add_argument("--output-dir", default=None)
    arguments = parser.parse_args()
    generate_dirty_dataset(arguments.rows, arguments.output_dir)


if __name__ == "__main__":
    main()

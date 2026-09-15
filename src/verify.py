#!/usr/bin/env python3
"""Verificación independiente del resultado Parquet, sin Dask ni Prefect."""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path

import pandas as pd

from src.cleaning import ANOMALY_TOKEN

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/shared-data"))
EXPECTED_ROWS = int(os.environ.get("NUM_ROWS", "300000"))
OUTPUT_PATTERN = str(DATA_ROOT / "processed" / "transactions_dirty_part_*.parquet")


def main() -> int:
    paths = sorted(glob.glob(OUTPUT_PATTERN))
    if len(paths) != 6:
        print(f"[X] Se esperaban 6 Parquet en {OUTPUT_PATTERN}; encontrados: {len(paths)}")
        return 1
    dataframe = pd.concat([pd.read_parquet(path, engine="pyarrow") for path in paths], ignore_index=True)
    codes = dataframe["customer_code"]
    canonical = int(codes.str.fullmatch(r"CUST-\d{5}", na=False).sum())
    anomalies = int((codes == ANOMALY_TOKEN).sum())
    null_codes = int(codes.isna().sum())
    mojibake = int(dataframe["city_notes"].astype("string").str.contains("Ã|Â", regex=True, na=False).sum())
    valid_phones = int(dataframe["phone"].notna().sum())
    checks = [
        (f"{EXPECTED_ROWS:,} filas exactas", len(dataframe) == EXPECTED_ROWS),
        ("seis Parquet de salida", len(paths) == 6),
        ("todo código canónico o anomalía", canonical + anomalies == len(dataframe)),
        ("ningún código nulo", null_codes == 0),
        ("cero restos de mojibake", mojibake == 0),
    ]
    print(f"Filas: {len(dataframe):,}; canónicos: {canonical:,}; anomalías: {anomalies:,}")
    print(f"Mojibake restante: {mojibake:,}; teléfonos válidos: {valid_phones:,}")
    for label, ok in checks:
        print(f"[{'OK' if ok else 'X '}] {label}")
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    sys.exit(main())

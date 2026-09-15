"""Transformaciones puras de calidad de datos para una partición Pandas."""

from __future__ import annotations

import re

import pandas as pd

ANOMALY_TOKEN = "CUST-00000-ANOMALY"
CUSTOMER_CODE_RE = re.compile(r"(?:CLI-|cli_|RAW#|CUST-)?(\d{5})")
MOJIBAKE_MARKERS = ("Ã", "Â")
PHONE_SENTINELS = {"DESCONOCIDO", "N/A", "--", "", "NAN", "NONE"}
PHONE_EXTENSION_RE = re.compile(r"\s*(?:ext|extension)\.?\s*\d+", re.IGNORECASE)
NON_DIGIT_RE = re.compile(r"\D")

OUTPUT_COLUMNS = [
    "transaction_id",
    "customer_code",
    "city_notes",
    "phone",
    "amount_usd",
    "business_category",
]


def canonical_customer_code(raw: object) -> str:
    """Extrae un id de cinco dígitos o conserva la fila como anomalía."""
    if raw is None or pd.isna(raw):
        return ANOMALY_TOKEN
    match = CUSTOMER_CODE_RE.search(str(raw))
    return f"CUST-{match.group(1)}" if match else ANOMALY_TOKEN


def repair_mojibake(text: object) -> str | None:
    """Revierte UTF-8 leído erróneamente como Latin-1 sin tocar texto sano."""
    if text is None or pd.isna(text):
        return None
    value = str(text)
    if not any(marker in value for marker in MOJIBAKE_MARKERS):
        return value
    try:
        return value.encode("latin-1").decode("utf-8")
    except (UnicodeDecodeError, UnicodeEncodeError):
        # Conservar el dato permite auditar una corrupción no contemplada.
        return value


def normalize_phone(raw: object) -> str | None:
    """Devuelve un celular colombiano de 10 dígitos, sin prefijo de país."""
    if raw is None or pd.isna(raw):
        return None
    value = str(raw).strip()
    if value.upper() in PHONE_SENTINELS:
        return None

    without_extension = PHONE_EXTENSION_RE.sub("", value)
    digits = NON_DIGIT_RE.sub("", without_extension)
    if digits.startswith("0057"):
        digits = digits[4:]
    elif len(digits) == 12 and digits.startswith("57"):
        digits = digits[2:]

    return digits if len(digits) == 10 and digits.startswith("3") else None


def clean_partition(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Limpia una sola partición, sin cargar ninguna otra en memoria."""
    return pd.DataFrame(
        {
            "transaction_id": dataframe["transaction_id"],
            "customer_code": dataframe["raw_customer_code"].map(canonical_customer_code),
            "city_notes": dataframe["city_notes_corrupted"].map(repair_mojibake),
            "phone": dataframe["phone_raw"].map(normalize_phone),
            "amount_usd": pd.to_numeric(dataframe["amount_usd"], errors="coerce"),
            "business_category": dataframe["business_category"].astype("string").str.strip().str.upper(),
        }
    )

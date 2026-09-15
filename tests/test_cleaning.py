"""Pruebas rápidas de las funciones puras de limpieza."""

import numpy as np
import pandas as pd
import pytest

from src.cleaning import ANOMALY_TOKEN, clean_partition, canonical_customer_code, normalize_phone, repair_mojibake
from src.generate_dirty_data import CLEAN_PHRASES, corrupt_encoding


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  CLI-98234-A  ", "CUST-98234"),
        ("cli_98234_norm", "CUST-98234"),
        ("RAW#98234-[V2]", "CUST-98234"),
        ("CUST-98234-EXP", "CUST-98234"),
        ("  98234  ", "CUST-98234"),
    ],
)
def test_customer_code_variants(raw, expected):
    assert canonical_customer_code(raw) == expected


@pytest.mark.parametrize("raw", ["ANOMALOUS_STR", "INVALID", "", "   ", None, np.nan])
def test_invalid_customer_code_is_preserved_as_anomaly(raw):
    assert canonical_customer_code(raw) == ANOMALY_TOKEN


def test_customer_code_never_returns_null():
    assert canonical_customer_code(None) is not None


@pytest.mark.parametrize("phrase", CLEAN_PHRASES)
def test_mojibake_round_trip(phrase):
    assert repair_mojibake(corrupt_encoding(phrase)) == phrase


def test_healthy_text_is_unchanged_and_repair_is_idempotent():
    healthy = "Transacción exitosa en Bogotá D.C."
    assert repair_mojibake(healthy) == healthy
    assert repair_mojibake(repair_mojibake(corrupt_encoding("Medellín"))) == "Medellín"


@pytest.mark.parametrize("raw", [None, np.nan])
def test_null_notes_remain_null(raw):
    assert repair_mojibake(raw) is None


@pytest.mark.parametrize(
    "raw",
    [
        "+57 (310) 123-4567",
        "310.123.4567",
        "TEL: 3101234567 Ext 402",
        "0057 310 123 4567",
        "3101234567",
    ],
)
def test_phone_variants_normalize_to_same_local_number(raw):
    assert normalize_phone(raw) == "3101234567"


@pytest.mark.parametrize("raw", ["DESCONOCIDO", "N/A", "--", "", "   ", None, np.nan])
def test_phone_sentinels_are_null(raw):
    assert normalize_phone(raw) is None


@pytest.mark.parametrize("raw", ["310123", "6011234567", "315123456789", "7712345678"])
def test_invalid_phone_numbers_are_null(raw):
    assert normalize_phone(raw) is None


def test_clean_partition_returns_only_the_contract_schema():
    source = pd.DataFrame(
        {
            "transaction_id": ["TX-0000001", "TX-0000002"],
            "raw_customer_code": ["CLI-98234-A", "INVALID"],
            "city_notes_corrupted": [corrupt_encoding("Bogotá"), None],
            "phone_raw": ["+57 (310) 123-4567", "DESCONOCIDO"],
            "amount_usd": ["150.0", "not-a-number"],
            "business_category": [" finance ", "TECH"],
        }
    )
    result = clean_partition(source)
    assert list(result.columns) == ["transaction_id", "customer_code", "city_notes", "phone", "amount_usd", "business_category"]
    assert result["customer_code"].tolist() == ["CUST-98234", ANOMALY_TOKEN]
    assert result["city_notes"].iloc[0] == "Bogotá"
    assert pd.isna(result["city_notes"].iloc[1])
    assert result["phone"].iloc[0] == "3101234567"
    assert pd.isna(result["phone"].iloc[1])
    assert result["amount_usd"].isna().iloc[1]
    assert result["business_category"].tolist() == ["FINANCE", "TECH"]

"""Unit tests for the cleaning rules (pandas implementation) and the DuckDB SQL (on a tiny CSV)."""
import duckdb
import pandas as pd
import pytest

from config import RAW_COLUMNS
from src.clean import (
    clean_frame,
    clean_sql_for,
    normalise_postcode,
    postcode_area,
    postcode_district,
    price_band,
)


@pytest.mark.parametrize("raw, expected", [
    ("L1 8JQ", "L1 8JQ"), ("l18jq", "L1 8JQ"), ("  SW1A 1AA ", "SW1A 1AA"), ("M1 1AE", "M1 1AE"),
    ("EC1A1BB", "EC1A 1BB"), ("", None), ("NOTAPOSTCODE", None), (None, None), (float("nan"), None),
])
def test_normalise_postcode(raw, expected):
    assert normalise_postcode(raw) == expected


def test_postcode_parts():
    assert postcode_district("L1 8JQ") == "L1"
    assert postcode_area("L1 8JQ") == "L"
    assert postcode_area("SW1A 1AA") == "SW"
    assert postcode_district(None) is None
    assert postcode_area(None) is None


@pytest.mark.parametrize("price, band", [
    (50_000, "<100k"), (99_999, "<100k"), (100_000, "100k-250k"), (300_000, "250k-500k"),
    (750_000, "500k-1m"), (2_000_000, "1m+"),
])
def test_price_band(price, band):
    assert price_band(price) == band


def _rows():
    return [
        ["{A}", 250000, "2021-06-15 00:00", "L1 8JQ", "F", "N", "L", "10", "FLAT 3", "DUKE ST", "", "LIVERPOOL", "LIVERPOOL", "MERSEYSIDE", "A", "A"],
        ["{B}", 450000, "2021-03-01 00:00", "m1 1ae", "T", "Y", "F", "5", "", "HIGH ST", "", "manchester", "MANCHESTER", "GREATER MANCHESTER", "A", "A"],
        ["{B}", 450000, "2021-03-01 00:00", "M1 1AE", "T", "Y", "F", "5", "", "HIGH ST", "", "MANCHESTER", "MANCHESTER", "GREATER MANCHESTER", "A", "A"],
        ["{C}", 5000, "2021-12-31 00:00", "", "O", "N", "U", "1", "", "", "", "LEEDS", "LEEDS", "WEST YORKSHIRE", "B", "A"],
        ["{D}", 100000, "2021-01-01 00:00", "LS1 1AA", "D", "N", "F", "2", "", "", "", "LEEDS", "LEEDS", "WEST YORKSHIRE", "A", "D"],
    ]


def _raw_df():
    return pd.DataFrame(_rows(), columns=RAW_COLUMNS).replace("", None)


def test_clean_frame_rules():
    out = clean_frame(_raw_df()).set_index("transaction_id")
    assert list(out.index) == ["{A}", "{B}", "{C}"]          # duplicate removed, status D removed
    assert out.loc["{B}", "postcode"] == "M1 1AE"
    assert out.loc["{B}", "town_city"] == "MANCHESTER"
    assert out.loc["{A}", "postcode_district"] == "L1" and out.loc["{A}", "postcode_area"] == "L"
    assert out.loc["{A}", "property_type_name"] == "Flat" and out.loc["{A}", "tenure_name"] == "Leasehold"
    assert bool(out.loc["{B}", "is_new_build"]) is True
    assert bool(out.loc["{C}", "is_residential"]) is False
    assert bool(out.loc["{C}", "is_standard_sale"]) is False
    assert bool(out.loc["{C}", "is_price_plausible"]) is False
    assert out.loc["{C}", "postcode"] is None or pd.isna(out.loc["{C}", "postcode"])
    assert out.loc["{A}", "year"] == 2021 and out.loc["{A}", "quarter"] == 2
    assert out.loc["{B}", "price_band"] == "250k-500k"


def test_duckdb_sql_matches_pandas(tmp_path):
    """The SQL implementation must agree with the pandas implementation on every column."""
    csv = tmp_path / "pp-test.csv"
    pd.DataFrame(_rows(), columns=RAW_COLUMNS).to_csv(csv, index=False, header=False)

    sql_out = duckdb.sql(clean_sql_for(csv)).df().sort_values("transaction_id").reset_index(drop=True)
    pd_out = clean_frame(_raw_df()).sort_values("transaction_id").reset_index(drop=True)

    assert list(sql_out["transaction_id"]) == list(pd_out["transaction_id"])
    for col in ["price", "postcode", "postcode_district", "postcode_area", "town_city", "year", "month",
                "property_type_name", "tenure_name", "is_new_build", "is_residential", "is_standard_sale",
                "price_band", "is_price_plausible"]:
        left = sql_out[col].where(sql_out[col].notna(), None).tolist()
        right = pd_out[col].where(pd_out[col].notna(), None).tolist()
        assert [str(v) for v in left] == [str(v) for v in right], col

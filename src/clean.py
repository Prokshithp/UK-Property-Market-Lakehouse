"""
Step 2 - CLEAN (silver layer)

Turns the raw yearly CSVs into a typed, de-duplicated, feature-enriched Parquet dataset
partitioned by year (data/processed/sales/year=YYYY/*.parquet).

Two implementations of the SAME rules live here on purpose:

* `clean_year_duckdb()`  - production path. DuckDB streams the CSV, applies the rules in SQL
                           and writes Parquet in parallel. Handles millions of rows in seconds.
* `clean_frame()`        - pandas path. Identical logic on a DataFrame; used by the unit tests
                           and by the pandas / multiprocessing / Dask engines in the benchmark.

Incremental: a year is re-processed only if its raw file changed (SHA-256 from the ingest
manifest) or its partition is missing.
"""
from __future__ import annotations

import json
import logging
import re
import shutil
from pathlib import Path

import duckdb
import pandas as pd

from config import (
    INGEST_MANIFEST,
    MAX_PLAUSIBLE_PRICE,
    MIN_PLAUSIBLE_PRICE,
    PROCESSED_DIR,
    PROPERTY_TYPES,
    RAW_COLUMNS,
    SILVER_DIR,
    TENURES,
    YEARS,
    raw_csv_path,
)

log = logging.getLogger(__name__)
SILVER_MANIFEST = PROCESSED_DIR / "_silver_manifest.json"

POSTCODE_RE = re.compile(r"^([A-Z]{1,2})(\d[A-Z\d]?)\s*(\d[A-Z]{2})$")


# --------------------------------------------------------------------------- #
# Pure helper functions (unit-tested)
# --------------------------------------------------------------------------- #
def normalise_postcode(pc: str | float | None) -> str | None:
    """' l1  8jq ' -> 'L1 8JQ'; invalid / missing -> None."""
    if not isinstance(pc, str):
        return None
    compact = re.sub(r"\s+", "", pc.upper())
    if len(compact) < 5 or len(compact) > 7:
        return None
    formatted = f"{compact[:-3]} {compact[-3:]}"
    return formatted if POSTCODE_RE.match(formatted) else None


def postcode_district(pc: str | float | None) -> str | None:
    """'L1 8JQ' -> 'L1' (the outward code)."""
    return pc.split(" ")[0] if isinstance(pc, str) and pc else None


def postcode_area(pc: str | float | None) -> str | None:
    """'L1 8JQ' -> 'L' (the letters at the start)."""
    if not isinstance(pc, str) or not pc:
        return None
    m = re.match(r"^[A-Z]{1,2}", pc)
    return m.group(0) if m else None


def price_band(price: int | float) -> str:
    if price < 100_000:
        return "<100k"
    if price < 250_000:
        return "100k-250k"
    if price < 500_000:
        return "250k-500k"
    if price < 1_000_000:
        return "500k-1m"
    return "1m+"


# --------------------------------------------------------------------------- #
# pandas implementation
# --------------------------------------------------------------------------- #
def read_raw_csv_pandas(path: Path, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, header=None, names=RAW_COLUMNS,
                       dtype={"price": "int64"}, keep_default_na=False, na_values=[""], **kwargs)


def clean_frame(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()

    # 1. Keep only live records (monthly files also carry C=changed / D=deleted rows)
    df = df[df["record_status"].fillna("A") == "A"]

    # 2. De-duplicate on the natural key
    df = df.drop_duplicates(subset="transaction_id")

    # 3. Types
    df["price"] = pd.to_numeric(df["price"], errors="coerce").astype("int64")
    df["date_of_transfer"] = pd.to_datetime(df["date_of_transfer"].str.slice(0, 10), format="%Y-%m-%d")

    # 4. Postcode normalisation + geography hierarchy
    df["postcode"] = df["postcode"].map(normalise_postcode)
    df["postcode_district"] = df["postcode"].map(postcode_district)
    df["postcode_area"] = df["postcode"].map(postcode_area)

    # 5. Text standardisation
    for col in ("town_city", "district", "county", "street", "locality"):
        df[col] = df[col].fillna("").str.strip().str.upper().replace("", None)

    # 6. Derived attributes
    df["year"] = df["date_of_transfer"].dt.year.astype("int32")
    df["month"] = df["date_of_transfer"].dt.month.astype("int8")
    df["quarter"] = df["date_of_transfer"].dt.quarter.astype("int8")
    df["property_type_name"] = df["property_type"].map(PROPERTY_TYPES).fillna("Other")
    df["tenure_name"] = df["duration"].map(TENURES).fillna("Unknown")
    df["is_new_build"] = (df["old_new"] == "Y")
    df["is_residential"] = df["property_type"].isin(["D", "S", "T", "F"])
    df["is_standard_sale"] = df["ppd_category_type"] == "A"
    df["price_band"] = df["price"].map(price_band)
    df["is_price_plausible"] = df["price"].between(MIN_PLAUSIBLE_PRICE, MAX_PLAUSIBLE_PRICE)

    return df.drop(columns=["record_status"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# DuckDB implementation (production path)
# --------------------------------------------------------------------------- #
def _csv_relation_sql(path: Path) -> str:
    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in RAW_COLUMNS)
    return (f"read_csv('{path.as_posix()}', header=false, columns={{{cols}}}, "
            f"quote='\"', escape='\"', null_padding=true, ignore_errors=true)")


CLEAN_SQL_TEMPLATE = """
WITH live AS (
    SELECT * FROM {source}
    WHERE coalesce(record_status, 'A') = 'A'
),
dedup AS (
    SELECT * FROM live
    QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY date_of_transfer DESC) = 1
),
typed AS (
    SELECT
        transaction_id,
        CAST(price AS BIGINT)                                   AS price,
        CAST(substr(date_of_transfer, 1, 10) AS DATE)          AS date_of_transfer,
        CASE WHEN regexp_matches(upper(regexp_replace(postcode, '\\s+', '', 'g')), '^[A-Z]{{1,2}}[0-9][A-Z0-9]?[0-9][A-Z]{{2}}$')
             THEN regexp_replace(upper(regexp_replace(postcode, '\\s+', '', 'g')), '^(.*)(.{{3}})$', '\\1 \\2')
             END                                                AS postcode,
        property_type, old_new, duration, paon, saon,
        nullif(upper(trim(street)),   '')                       AS street,
        nullif(upper(trim(locality)), '')                       AS locality,
        nullif(upper(trim(town_city)), '')                      AS town_city,
        nullif(upper(trim(district)), '')                       AS district,
        nullif(upper(trim(county)),   '')                       AS county,
        ppd_category_type
    FROM dedup
)
SELECT
    *,
    split_part(postcode, ' ', 1)                                AS postcode_district,
    regexp_extract(postcode, '^[A-Z]{{1,2}}')                   AS postcode_area,
    CAST(year(date_of_transfer) AS INTEGER)                     AS year,
    CAST(month(date_of_transfer) AS TINYINT)                    AS month,
    CAST(quarter(date_of_transfer) AS TINYINT)                  AS quarter,
    CASE property_type WHEN 'D' THEN 'Detached' WHEN 'S' THEN 'Semi-Detached'
                       WHEN 'T' THEN 'Terraced' WHEN 'F' THEN 'Flat' ELSE 'Other' END AS property_type_name,
    CASE duration WHEN 'F' THEN 'Freehold' WHEN 'L' THEN 'Leasehold' ELSE 'Unknown' END AS tenure_name,
    old_new = 'Y'                                               AS is_new_build,
    property_type IN ('D','S','T','F')                          AS is_residential,
    ppd_category_type = 'A'                                     AS is_standard_sale,
    CASE WHEN price < 100000 THEN '<100k' WHEN price < 250000 THEN '100k-250k'
         WHEN price < 500000 THEN '250k-500k' WHEN price < 1000000 THEN '500k-1m' ELSE '1m+' END AS price_band,
    price BETWEEN {min_price} AND {max_price}                   AS is_price_plausible
FROM typed
"""


def clean_sql_for(path: Path) -> str:
    return CLEAN_SQL_TEMPLATE.format(source=_csv_relation_sql(path),
                                     min_price=MIN_PLAUSIBLE_PRICE, max_price=MAX_PLAUSIBLE_PRICE)


def _load_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def clean_year_duckdb(year: int, con: duckdb.DuckDBPyConnection | None = None) -> int:
    """Clean one raw year and (re)write its Parquet partition. Returns rows written."""
    src = raw_csv_path(year)
    partition = SILVER_DIR / f"year={year}"
    if partition.exists():
        shutil.rmtree(partition)
    partition.mkdir(parents=True, exist_ok=True)

    own = con is None
    con = con or duckdb.connect()
    try:
        # Keep sales whose transfer year matches the file year (files are built by year of transfer)
        sql = clean_sql_for(src)
        con.execute(f"""
            COPY (SELECT * EXCLUDE (year) FROM ({sql}) WHERE year = {year})
            TO '{partition.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD, PER_THREAD_OUTPUT true, ROW_GROUP_SIZE 500000)
        """)
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{partition.as_posix()}/*.parquet')").fetchone()[0]
    finally:
        if own:
            con.close()
    return rows


def run(force: bool = False) -> dict[int, int]:
    raw_manifest = _load_json(INGEST_MANIFEST).get("files", {})
    silver_manifest = _load_json(SILVER_MANIFEST)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    written: dict[int, int] = {}
    con = duckdb.connect()
    for year in YEARS:
        if not raw_csv_path(year).exists():
            log.warning("pp-%s.csv missing - skipping", year)
            continue
        raw_sha = raw_manifest.get(str(year), {}).get("sha256")
        partition_ok = (SILVER_DIR / f"year={year}").exists()
        if partition_ok and not force and silver_manifest.get(str(year), {}).get("raw_sha256") == raw_sha:
            log.info("year=%s unchanged - skipping", year)
            continue
        rows = clean_year_duckdb(year, con)
        written[year] = rows
        silver_manifest[str(year)] = {"rows": rows, "raw_sha256": raw_sha}
        SILVER_MANIFEST.write_text(json.dumps(silver_manifest, indent=2), encoding="utf-8")
        log.info("year=%s -> %s rows written", year, f"{rows:,}")
    con.close()

    total_bytes = sum(p.stat().st_size for p in SILVER_DIR.rglob("*.parquet"))
    log.info("Silver layer: %s partitions, %.0f MB Parquet", len(list(SILVER_DIR.glob("year=*"))), total_bytes / 1e6)
    return written


def silver_glob() -> str:
    return (SILVER_DIR / "*" / "*.parquet").as_posix()

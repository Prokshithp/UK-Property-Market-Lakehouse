# UK Property Market Lakehouse & HPC Benchmark — Complete Code Walkthrough

Every line of code in the project, each file preceded by a plain-English explanation of what it
does and *why it was designed that way*. Read it top to bottom.

---

## Part 1 — The dataset: exactly where it comes from

**Name:** HM Land Registry *Price Paid Data*
**Publisher:** HM Land Registry (UK Government)
**Page:** https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
**Direct files:** `http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-YYYY.csv`
(one per year, e.g. `pp-2024.csv`) — plus `pp-complete.csv` (everything since 1995, 5.5 GB) and
`pp-monthly-update-new-version.csv` (latest month).
**Licence:** Open Government Licence v3 — free for any use with attribution. **No account, no login.**

### What's in it
Every residential property sale registered in England and Wales. One row = one sale. 16 columns,
**no header row**:

| # | Column | Example | Notes |
|---|---|---|---|
| 1 | transaction_id | `{5CD70F5C-…}` | unique key |
| 2 | price | `355000` | GBP |
| 3 | date_of_transfer | `2021-12-06 00:00` | |
| 4 | postcode | `SS14 3TW` | sometimes blank |
| 5 | property_type | `T` | D detached, S semi, T terraced, F flat, O other |
| 6 | old_new | `Y` | Y = new build |
| 7 | duration | `F` | F freehold, L leasehold, U unknown |
| 8–11 | paon, saon, street, locality | | address |
| 12 | town_city | `BASILDON` | |
| 13 | district | `BASILDON` | local authority |
| 14 | county | `ESSEX` | |
| 15 | ppd_category_type | `A` | A standard sale, B additional (e.g. repossession, buy-to-let portfolios) |
| 16 | record_status | `A` | A add, C change, D delete (only in monthly files) |

### This project's default: 2016–2026
11 yearly files, **1.86 GB, 10.67 million rows**. Change the range with `PP_YEARS=2024,2025,2026`.

### Option A — automatic (recommended)
`python run_pipeline.py` downloads each year to `data/raw/`, verifies it with SHA-256 and records it
in `data/raw/_manifest.json`. Re-runs only fetch missing or changed years.

### Option B — manual
Download `pp-2016.csv` … `pp-2026.csv` from the page above into `data/raw/`. The ingest step
detects them, adds them to the manifest and skips the download.

---

## Part 2 — Project structure

```
uk-property-lakehouse/
├── config.py              paths, years, constants
├── run_pipeline.py        orchestrator
├── requirements.txt
├── src/
│   ├── ingest.py          step 1  incremental download         → data/raw/              (bronze)
│   ├── clean.py           step 2  DuckDB clean → Parquet        → data/processed/sales/  (silver)
│   ├── transform.py       step 3  star schema + views           → data/analytics/property.duckdb (gold)
│   ├── quality.py         step 4  SQL-pushdown quality checks   → outputs/quality_report.json
│   ├── benchmark.py       step 5  HPC benchmark, 6 engines × 1-8 workers → outputs/benchmark_results.csv
│   ├── spark_job.py              Spark workload run as a subprocess
│   ├── visualize.py       step 6  9 charts + CSV tables
│   └── report.py          step 7  outputs/report.md
├── tests/                 24 pytest tests
└── .github/workflows/ci.yml
```

---

## Part 3 — All the code, file by file


---

## `requirements.txt`

### Explanation
| Library | Role |
|---|---|
| pandas | DataFrames; the baseline engine in the benchmark |
| pyarrow | Parquet read/write for pandas and Dask |
| duckdb | the workhorse: cleans CSV → Parquet, hosts the gold database, runs the quality checks |
| dask[dataframe] | parallel DataFrame engine (benchmark) |
| matplotlib | charts |
| requests | streaming download |
| tabulate | markdown tables in the report |
| psutil | peak-memory measurement |
| pytest | tests |
| pyspark | optional Spark engine for the benchmark (needs a JDK) |

### Code
```text
pandas>=2.2
pyarrow>=15.0
duckdb>=1.0
dask[dataframe]>=2024.1
matplotlib>=3.8
requests>=2.31
tabulate>=0.9
psutil>=5.9
pytest>=8.0
# optional - adds the Spark engine to the benchmark (also needs a JDK, see README)
pyspark>=3.5
```


---

## `config.py`

### Explanation
Single source of truth for paths and constants. `YEARS` defaults to 2016–2026 and can be overridden
with the `PP_YEARS` environment variable (CI uses `PP_YEARS=2026` to smoke-test on one file).
`RAW_COLUMNS` names the 16 header-less CSV columns. `BENCHMARK_WORKER_COUNTS` defines the scaling
experiment. `TOOLS_DIR` is where a portable JDK / Hadoop natives can live for Spark.

### Code
```python
"""Central configuration for the UK Property Market Lakehouse project."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

# Medallion layout: raw (bronze) -> processed (silver) -> analytics (gold)
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
ANALYTICS_DIR = DATA_DIR / "analytics"

OUTPUT_DIR = PROJECT_ROOT / "outputs"
FIGURES_DIR = OUTPUT_DIR / "figures"
TABLES_DIR = OUTPUT_DIR / "tables"

# ---------------------------------------------------------------------------
# Dataset: HM Land Registry "Price Paid Data" (Open Government Licence v3)
# https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
# One CSV per calendar year, no header row, no login required.
# ---------------------------------------------------------------------------
BASE_URL = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
YEARS = list(range(2016, 2027))          # 2016 .. 2026 inclusive (11 files, ~1.8 GB)
if os.environ.get("PP_YEARS"):            # e.g. PP_YEARS=2025,2026 for a quick run / CI
    YEARS = [int(y) for y in os.environ["PP_YEARS"].split(",")]


def raw_csv_path(year: int) -> Path:
    return RAW_DIR / f"pp-{year}.csv"


def raw_csv_url(year: int) -> str:
    return f"{BASE_URL}/pp-{year}.csv"


# Column names in the order they appear in the files (official specification)
RAW_COLUMNS = [
    "transaction_id", "price", "date_of_transfer", "postcode", "property_type",
    "old_new", "duration", "paon", "saon", "street", "locality", "town_city",
    "district", "county", "ppd_category_type", "record_status",
]

PROPERTY_TYPES = {"D": "Detached", "S": "Semi-Detached", "T": "Terraced", "F": "Flat", "O": "Other"}
TENURES = {"F": "Freehold", "L": "Leasehold", "U": "Unknown"}

# Sanity bounds for a residential sale price (GBP). Outside -> flagged, not deleted.
MIN_PLAUSIBLE_PRICE = 10_000
MAX_PLAUSIBLE_PRICE = 20_000_000

# Cities used in the comparative analytics (matched on town_city)
COMPARISON_CITIES = ["LIVERPOOL", "MANCHESTER", "LEEDS", "BIRMINGHAM", "LONDON", "BRISTOL", "NEWCASTLE UPON TYNE"]

SILVER_DIR = PROCESSED_DIR / "sales"              # Parquet, partitioned by year=YYYY/
INGEST_MANIFEST = RAW_DIR / "_manifest.json"       # what has been downloaded + checksums
DB_PATH = ANALYTICS_DIR / "property.duckdb"        # gold layer: DuckDB database file

QUALITY_REPORT_PATH = OUTPUT_DIR / "quality_report.json"
BENCHMARK_RESULTS_PATH = OUTPUT_DIR / "benchmark_results.csv"
REPORT_PATH = OUTPUT_DIR / "report.md"

# Benchmark settings
BENCHMARK_WORKER_COUNTS = [1, 2, 4, 8]
BENCHMARK_REPEATS = 2
MAX_WORKERS = os.cpu_count() or 4

# Optional: portable JDK bundled under tools/ makes PySpark work without a system Java
TOOLS_DIR = PROJECT_ROOT / "tools"
```


---

## `src/ingest.py`

### Explanation — Step 1, bronze layer
**Job:** land the raw files, untouched, incrementally.

* `download_year()` streams the file in 8 MB blocks to `pp-YYYY.csv.part` and renames it only on
  success, so a failed download never leaves a half file. It then stores size + SHA-256 + timestamp
  in `_manifest.json`.
* If the file is already present it is skipped — except the current year: a `HEAD` request compares
  the remote size with the manifest, because Land Registry appends to the current year monthly.
* `register_existing_files()` lets manually downloaded files join the manifest.

Design point: the manifest is what makes the pipeline **idempotent and incremental** — the same
pattern as Databricks Auto Loader's checkpoint or an Airflow sensor.

### Code
```python
"""
Step 1 - INGEST (bronze layer)

Downloads one HM Land Registry "Price Paid" CSV per year into data/raw/.
The download is **incremental**: a manifest records every file already landed with its size
and SHA-256, so re-running only fetches missing years (or a changed current-year file).

Files have no header row; column order is defined in config.RAW_COLUMNS.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import INGEST_MANIFEST, RAW_DIR, YEARS, raw_csv_path, raw_csv_url

log = logging.getLogger(__name__)
CHUNK = 8 * 1024 * 1024  # 8 MB streaming chunks


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def load_manifest() -> dict:
    if INGEST_MANIFEST.exists():
        return json.loads(INGEST_MANIFEST.read_text(encoding="utf-8"))
    return {"files": {}}


def save_manifest(manifest: dict) -> None:
    INGEST_MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def remote_size(url: str) -> int | None:
    try:
        r = requests.head(url, allow_redirects=True, timeout=60)
        return int(r.headers.get("Content-Length", 0)) or None
    except requests.RequestException:
        return None


def download_year(year: int, force: bool = False) -> Path:
    """Stream one yearly CSV to disk (writes to .part, renames on success)."""
    target = raw_csv_path(year)
    url = raw_csv_url(year)
    manifest = load_manifest()
    entry = manifest["files"].get(str(year))

    if target.exists() and entry and not force:
        # The current year's file grows every month - re-download if the remote size changed.
        size = remote_size(url)
        if size is None or size == entry.get("bytes"):
            log.info("pp-%s.csv already present (%.0f MB) - skipping", year, target.stat().st_size / 1e6)
            return target
        log.info("pp-%s.csv changed upstream (%s -> %s bytes) - refreshing", year, entry.get("bytes"), size)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(".csv.part")
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with part.open("wb") as fh:
            for block in r.iter_content(CHUNK):
                fh.write(block)
    part.replace(target)

    manifest["files"][str(year)] = {
        "bytes": target.stat().st_size,
        "sha256": _sha256(target),
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": url,
    }
    save_manifest(manifest)
    log.info("Saved pp-%s.csv (%.0f MB)", year, target.stat().st_size / 1e6)
    return target


def register_existing_files() -> None:
    """If CSVs were placed manually (or by another downloader), add them to the manifest."""
    manifest = load_manifest()
    changed = False
    for year in YEARS:
        p = raw_csv_path(year)
        if p.exists() and str(year) not in manifest["files"]:
            manifest["files"][str(year)] = {
                "bytes": p.stat().st_size, "sha256": _sha256(p),
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": "manual",
            }
            changed = True
    if changed:
        save_manifest(manifest)


def available_years() -> list[int]:
    return [y for y in YEARS if raw_csv_path(y).exists()]


def run(force: bool = False) -> list[Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    register_existing_files()
    paths = [download_year(y, force=force) for y in YEARS]
    total = sum(p.stat().st_size for p in paths) / 1e9
    log.info("Bronze layer: %s files, %.2f GB", len(paths), total)
    return paths
```


---

## `src/clean.py`

### Explanation — Step 2, silver layer
**Job:** 1.86 GB of CSV → 334 MB of typed, de-duplicated, feature-enriched Parquet in ~35 seconds.

The cleaning rules exist **twice, deliberately**:
* `CLEAN_SQL_TEMPLATE` — DuckDB SQL, the production path. DuckDB streams the CSV with all cores and
  writes Parquet with `PER_THREAD_OUTPUT` (one file per thread) and ZSTD compression.
* `clean_frame()` — the same rules in pandas; used by the unit tests and by the pandas / Dask /
  multiprocessing engines in the benchmark. `tests/test_clean.py` asserts both agree column by column.

Rules: keep `record_status = 'A'`; de-duplicate on `transaction_id` (`QUALIFY row_number()`); cast
price/date; normalise postcodes (`' l1  8jq '` → `'L1 8JQ'`, invalid → NULL) and split into
district/area; upper-case and trim text; derive year/month/quarter, type and tenure names,
`is_new_build`, `is_residential`, `is_standard_sale`, `price_band`, `is_price_plausible`.

Incremental: `run()` compares each raw file's SHA-256 (from the ingest manifest) with
`_silver_manifest.json` and re-processes only changed years. Parquet is written as
`sales/year=YYYY/` — Hive-style partitioning, so engines can prune by year.

### Code
```python
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
```


---

## `src/transform.py`

### Explanation — Step 3, gold layer
**Job:** build a Kimball star schema in a single-file DuckDB database.

* `dim_date` — one row per calendar date present, with year/quarter/month/day names and an
  `is_sdlt_holiday` flag (England's stamp-duty holiday, 8 Jul 2020 – 30 Sep 2021).
* `dim_location` — the geography hierarchy (postcode district → town → district → county) with
  surrogate key and `is_liverpool` / `is_london` flags.
* `dim_property` — 20 combinations of property type × tenure × new-build.
* `fact_sale` — 10.67 M rows: keys, price, price band, postcode and the three analytical filter flags.
  Built with `LEFT JOIN … IS NOT DISTINCT FROM` so NULL geography still matches.
* `VIEWS_SQL` — six analytical views (the dbt-marts layer): price by year × type, city comparison,
  Liverpool district growth, monthly market, county league, new-build premium.

Why DuckDB and not SQLite here? Ten million fact rows: a columnar vectorised engine answers these
aggregations in milliseconds; a row store would take seconds to minutes.

### Code
```python
"""
Step 3 - TRANSFORM (gold layer)

Builds a dimensional star schema from the silver Parquet and stores it in a single-file
DuckDB database (data/analytics/property.duckdb) with analytical SQL views.

    dim_date ──────────┐
    dim_location ──────┼──► fact_sale   (one row per property transaction)
    dim_property ──────┘

Why DuckDB rather than SQLite for the gold layer? Nine million fact rows: a columnar,
vectorised engine answers aggregate questions in milliseconds where a row store takes
seconds (the benchmark step measures exactly this).
"""
from __future__ import annotations

import logging

import duckdb
import pandas as pd

from config import ANALYTICS_DIR, COMPARISON_CITIES, DB_PATH
from src.clean import silver_glob

log = logging.getLogger(__name__)

CITY_LIST_SQL = ", ".join(f"'{c}'" for c in COMPARISON_CITIES)

# Analytical base: residential, standard price-paid, plausible price
RESIDENTIAL_FILTER = "f.is_residential AND f.is_standard_sale AND f.is_price_plausible"

VIEWS_SQL = {
    "vw_price_by_year_type": f"""
        SELECT d.year, p.property_type_name,
               count(*)                       AS sales,
               median(f.price)::BIGINT        AS median_price,
               round(avg(f.price))::BIGINT    AS mean_price
        FROM fact_sale f
        JOIN dim_date d     ON d.date_key = f.date_key
        JOIN dim_property p ON p.property_key = f.property_key
        WHERE {RESIDENTIAL_FILTER}
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "vw_city_comparison": f"""
        SELECT d.year, l.town_city,
               count(*)                       AS sales,
               median(f.price)::BIGINT        AS median_price
        FROM fact_sale f
        JOIN dim_date d     ON d.date_key = f.date_key
        JOIN dim_location l ON l.location_key = f.location_key
        WHERE {RESIDENTIAL_FILTER} AND l.town_city IN ({CITY_LIST_SQL})
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "vw_liverpool_districts": f"""
        WITH yearly AS (
            SELECT l.postcode_district, d.year, median(f.price) AS median_price, count(*) AS sales
            FROM fact_sale f
            JOIN dim_date d     ON d.date_key = f.date_key
            JOIN dim_location l ON l.location_key = f.location_key
            WHERE {RESIDENTIAL_FILTER} AND l.postcode_area = 'L'
            GROUP BY 1, 2
        ),
        bounds AS (SELECT min(year) AS first_year, max(year) AS last_year FROM yearly)
        SELECT y.postcode_district,
               max(CASE WHEN y.year = b.first_year THEN y.median_price END)::BIGINT AS median_first_year,
               max(CASE WHEN y.year = b.last_year  THEN y.median_price END)::BIGINT AS median_last_year,
               round(100.0 * (max(CASE WHEN y.year = b.last_year THEN y.median_price END)
                             / max(CASE WHEN y.year = b.first_year THEN y.median_price END) - 1), 1) AS growth_pct,
               sum(y.sales)                                                            AS total_sales
        FROM yearly y CROSS JOIN bounds b
        GROUP BY 1
        HAVING median_first_year IS NOT NULL AND median_last_year IS NOT NULL AND total_sales >= 200
        ORDER BY growth_pct DESC
    """,
    "vw_monthly_market": f"""
        SELECT d.year, d.month, min(d.full_date) AS month_start,
               count(*)                 AS sales,
               median(f.price)::BIGINT  AS median_price,
               bool_or(d.is_sdlt_holiday) AS sdlt_holiday
        FROM fact_sale f
        JOIN dim_date d ON d.date_key = f.date_key
        WHERE {RESIDENTIAL_FILTER}
        GROUP BY 1, 2 ORDER BY 1, 2
    """,
    "vw_county_league": f"""
        WITH yearly AS (
            SELECT l.county, d.year, median(f.price) AS median_price, count(*) AS sales
            FROM fact_sale f
            JOIN dim_date d     ON d.date_key = f.date_key
            JOIN dim_location l ON l.location_key = f.location_key
            WHERE {RESIDENTIAL_FILTER} AND l.county IS NOT NULL
            GROUP BY 1, 2
        ),
        bounds AS (SELECT min(year) AS first_year, max(year) AS last_year FROM yearly)
        SELECT y.county,
               max(CASE WHEN y.year = b.first_year THEN y.median_price END)::BIGINT AS median_first_year,
               max(CASE WHEN y.year = b.last_year  THEN y.median_price END)::BIGINT AS median_last_year,
               round(100.0 * (max(CASE WHEN y.year = b.last_year THEN y.median_price END)
                             / max(CASE WHEN y.year = b.first_year THEN y.median_price END) - 1), 1) AS growth_pct,
               sum(y.sales) AS total_sales
        FROM yearly y CROSS JOIN bounds b
        GROUP BY 1
        HAVING total_sales >= 5000 AND median_first_year IS NOT NULL AND median_last_year IS NOT NULL
        ORDER BY growth_pct DESC
    """,
    "vw_new_build_premium": f"""
        SELECT d.year,
               median(CASE WHEN p.is_new_build     THEN f.price END)::BIGINT AS median_new_build,
               median(CASE WHEN NOT p.is_new_build THEN f.price END)::BIGINT AS median_existing,
               round(100.0 * (median(CASE WHEN p.is_new_build THEN f.price END)
                             / median(CASE WHEN NOT p.is_new_build THEN f.price END) - 1), 1) AS premium_pct
        FROM fact_sale f
        JOIN dim_date d     ON d.date_key = f.date_key
        JOIN dim_property p ON p.property_key = f.property_key
        WHERE {RESIDENTIAL_FILTER}
        GROUP BY 1 ORDER BY 1
    """,
}


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    src = f"read_parquet('{silver_glob()}', hive_partitioning=true)"
    con.execute(f"CREATE OR REPLACE TEMP VIEW silver AS SELECT * FROM {src}")

    con.execute("""
        CREATE OR REPLACE TABLE dim_date AS
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
               d                                      AS full_date,
               year(d)::INTEGER                       AS year,
               quarter(d)::TINYINT                    AS quarter,
               month(d)::TINYINT                      AS month,
               strftime(d, '%B')                      AS month_name,
               day(d)::TINYINT                        AS day,
               dayname(d)                             AS day_name,
               -- Stamp Duty Land Tax holiday (England): 8 Jul 2020 - 30 Sep 2021
               d BETWEEN DATE '2020-07-08' AND DATE '2021-09-30' AS is_sdlt_holiday
        FROM (SELECT DISTINCT date_of_transfer AS d FROM silver)
        ORDER BY d
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_location AS
        SELECT row_number() OVER (ORDER BY county, district, town_city, postcode_district) AS location_key,
               postcode_district, postcode_area, town_city, district, county,
               (town_city = 'LIVERPOOL' OR district = 'LIVERPOOL')   AS is_liverpool,
               (county = 'GREATER LONDON')                           AS is_london
        FROM (SELECT DISTINCT postcode_district, postcode_area, town_city, district, county FROM silver)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE dim_property AS
        SELECT row_number() OVER (ORDER BY property_type, duration, is_new_build) AS property_key,
               property_type, property_type_name, duration, tenure_name, is_new_build, is_residential
        FROM (SELECT DISTINCT property_type, property_type_name, duration, tenure_name, is_new_build, is_residential
              FROM silver)
    """)

    con.execute("""
        CREATE OR REPLACE TABLE fact_sale AS
        SELECT row_number() OVER ()                                          AS sale_key,
               s.transaction_id,
               CAST(strftime(s.date_of_transfer, '%Y%m%d') AS INTEGER)       AS date_key,
               l.location_key,
               p.property_key,
               s.price,
               s.price_band,
               s.postcode,
               s.is_standard_sale,
               s.is_residential,
               s.is_price_plausible
        FROM silver s
        LEFT JOIN dim_location l
               ON l.postcode_district IS NOT DISTINCT FROM s.postcode_district
              AND l.postcode_area     IS NOT DISTINCT FROM s.postcode_area
              AND l.town_city         IS NOT DISTINCT FROM s.town_city
              AND l.district          IS NOT DISTINCT FROM s.district
              AND l.county            IS NOT DISTINCT FROM s.county
        LEFT JOIN dim_property p
               ON p.property_type = s.property_type
              AND p.duration      = s.duration
              AND p.is_new_build  = s.is_new_build
    """)

    for name, sql in VIEWS_SQL.items():
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")

    for t in ("dim_date", "dim_location", "dim_property", "fact_sale"):
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        log.info("Built %-13s %12s rows", t, f"{n:,}")
    log.info("Created %s views", len(VIEWS_SQL))


def query(sql: str, db_path=DB_PATH) -> pd.DataFrame:
    """Run SQL against the gold database (read-only) and return a DataFrame."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        return con.execute(sql).df()


def run() -> None:
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH)) as con:
        build_gold(con)
    log.info("Gold layer written to %s (%.0f MB)", DB_PATH, DB_PATH.stat().st_size / 1e6)
```


---

## `src/quality.py`

### Explanation — Step 4, data-quality framework
Seven generic checks — `min_rows`, `not_null`, `unique`, `accepted_values`, `range`, `regex_match`,
`referential_integrity` (+ `row_count_matches_source`). Each is a small SQL query executed by DuckDB
**where the data lives** — a CSV, a Parquet glob or a table — so nothing is loaded into Python and
checking 10 M rows takes ~12 s.

Three suites: `raw_suite` profiles the newest raw file (three failures are *expected* and document
what cleaning fixes); `silver_suite` (11 checks) and `gold_suite` (11 checks incl. FK integrity and
"no rows lost between silver and gold") must all pass or `run()` raises and the pipeline stops.

### Code
```python
"""
Step 4 - DATA QUALITY

A reusable, SQL-pushdown data-quality framework. Every check is a generic function that
runs a DuckDB query against *any* relation (a CSV, a Parquet glob, or a gold table), so the
same seven checks work on all three layers and scale to millions of rows without loading
them into Python.

Failures on the raw layer are informative; failures on silver or gold stop the pipeline.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Iterable

import duckdb

from config import DB_PATH, OUTPUT_DIR, QUALITY_REPORT_PATH, YEARS, raw_csv_path
from src.clean import _csv_relation_sql, silver_glob

log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    layer: str
    table: str
    check: str
    column: str | None
    passed: bool
    details: str

    def to_dict(self) -> dict:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Generic checks - `rel` is any SQL relation expression (table name or read_* call)
# --------------------------------------------------------------------------- #
def _scalar(con, sql: str):
    return con.execute(sql).fetchone()[0]


def check_min_rows(con, rel, layer, table, min_rows: int) -> CheckResult:
    n = _scalar(con, f"SELECT count(*) FROM {rel}")
    return CheckResult(layer, table, "min_rows", None, n >= min_rows, f"{n:,} rows (minimum {min_rows:,})")


def check_not_null(con, rel, layer, table, column: str) -> CheckResult:
    n = _scalar(con, f"SELECT count(*) FROM {rel} WHERE {column} IS NULL")
    return CheckResult(layer, table, "not_null", column, n == 0, f"{n:,} null values")


def check_unique(con, rel, layer, table, column: str) -> CheckResult:
    n = _scalar(con, f"SELECT count(*) - count(DISTINCT {column}) FROM {rel}")
    return CheckResult(layer, table, "unique", column, n == 0, f"{n:,} duplicate values")


def check_values_in_set(con, rel, layer, table, column: str, allowed: Iterable[str]) -> CheckResult:
    vals = ", ".join(f"'{v}'" for v in allowed)
    bad = con.execute(f"SELECT {column}, count(*) FROM {rel} WHERE {column} NOT IN ({vals}) "
                      f"GROUP BY 1 ORDER BY 2 DESC LIMIT 5").fetchall()
    n = sum(c for _, c in bad)
    return CheckResult(layer, table, "accepted_values", column, n == 0,
                       f"{n:,} unexpected values: {[v for v, _ in bad]}")


def check_range(con, rel, layer, table, column: str, min_value=None, max_value=None) -> CheckResult:
    conds = []
    if min_value is not None:
        conds.append(f"{column} < {min_value}")
    if max_value is not None:
        conds.append(f"{column} > {max_value}")
    n = _scalar(con, f"SELECT count(*) FROM {rel} WHERE {' OR '.join(conds)}")
    return CheckResult(layer, table, "range", column, n == 0, f"{n:,} values outside [{min_value}, {max_value}]")


def check_regex(con, rel, layer, table, column: str, pattern: str) -> CheckResult:
    n = _scalar(con, f"SELECT count(*) FROM {rel} WHERE {column} IS NOT NULL AND NOT regexp_matches({column}, '{pattern}')")
    return CheckResult(layer, table, "regex_match", column, n == 0, f"{n:,} values not matching /{pattern}/")


def check_referential_integrity(con, fact_rel, fact_col, dim_rel, dim_col, layer, table) -> CheckResult:
    n = _scalar(con, f"SELECT count(*) FROM {fact_rel} f LEFT JOIN {dim_rel} d ON d.{dim_col} = f.{fact_col} "
                     f"WHERE d.{dim_col} IS NULL")
    return CheckResult(layer, table, "referential_integrity", fact_col, n == 0, f"{n:,} orphan rows")


def check_row_count_matches(con, rel_a, rel_b, layer, table) -> CheckResult:
    a = _scalar(con, f"SELECT count(*) FROM {rel_a}")
    b = _scalar(con, f"SELECT count(*) FROM {rel_b}")
    return CheckResult(layer, table, "row_count_matches_source", None, a == b, f"{a:,} vs {b:,}")


# --------------------------------------------------------------------------- #
# Suites
# --------------------------------------------------------------------------- #
def raw_suite(con) -> list[CheckResult]:
    """Profile the most recent raw file. Some failures are EXPECTED and documented."""
    year = max(y for y in YEARS if raw_csv_path(y).exists())
    rel = f"({_csv_relation_sql(raw_csv_path(year))})"
    layer, table = "raw", f"pp-{year}.csv"
    return [
        check_min_rows(con, rel, layer, table, 10_000),
        check_unique(con, rel, layer, table, "transaction_id"),
        check_not_null(con, rel, layer, table, "postcode"),             # expected to fail: some sales have no postcode
        check_values_in_set(con, rel, layer, table, "property_type", ["D", "S", "T", "F"]),  # expected to fail: 'O'
        check_range(con, rel, layer, table, "CAST(price AS BIGINT)", 10_000, 20_000_000),    # expected to fail
    ]


def silver_suite(con) -> list[CheckResult]:
    rel = f"read_parquet('{silver_glob()}', hive_partitioning=true)"
    layer, table = "silver", "sales (parquet)"
    return [
        check_min_rows(con, rel, layer, table, 50_000 * len(YEARS)),
        check_unique(con, rel, layer, table, "transaction_id"),
        check_not_null(con, rel, layer, table, "price"),
        check_not_null(con, rel, layer, table, "date_of_transfer"),
        check_not_null(con, rel, layer, table, "property_type_name"),
        check_range(con, rel, layer, table, "price", 1, None),
        check_values_in_set(con, rel, layer, table, "property_type", ["D", "S", "T", "F", "O"]),
        check_values_in_set(con, rel, layer, table, "duration", ["F", "L", "U"]),
        check_values_in_set(con, rel, layer, table, "price_band", ["<100k", "100k-250k", "250k-500k", "500k-1m", "1m+"]),
        check_regex(con, rel, layer, table, "postcode", "^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$"),
        CheckResult(layer, table, "partition_matches_year", "year",
                    _scalar(con, f"SELECT count(*) FROM {rel} WHERE year(date_of_transfer) <> year") == 0,
                    "rows whose transfer year disagrees with their partition"),
    ]


def gold_suite(con) -> list[CheckResult]:
    layer = "gold"
    silver = f"read_parquet('{silver_glob()}', hive_partitioning=true)"
    return [
        check_unique(con, "dim_date", layer, "dim_date", "date_key"),
        check_unique(con, "dim_location", layer, "dim_location", "location_key"),
        check_unique(con, "dim_property", layer, "dim_property", "property_key"),
        check_unique(con, "fact_sale", layer, "fact_sale", "sale_key"),
        check_unique(con, "fact_sale", layer, "fact_sale", "transaction_id"),
        check_row_count_matches(con, "fact_sale", silver, layer, "fact_sale"),
        check_referential_integrity(con, "fact_sale", "date_key", "dim_date", "date_key", layer, "fact_sale"),
        check_referential_integrity(con, "fact_sale", "location_key", "dim_location", "location_key", layer, "fact_sale"),
        check_referential_integrity(con, "fact_sale", "property_key", "dim_property", "property_key", layer, "fact_sale"),
        check_min_rows(con, "vw_city_comparison", layer, "vw_city_comparison", 50),
        check_min_rows(con, "vw_liverpool_districts", layer, "vw_liverpool_districts", 10),
    ]


def save_report(results: list[CheckResult]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "total_checks": len(results),
        "passed": sum(r.passed for r in results),
        "failed": sum(not r.passed for r in results),
        "results": [r.to_dict() for r in results],
    }
    QUALITY_REPORT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    log.info("Quality report: %s/%s checks passed -> %s", payload["passed"], payload["total_checks"], QUALITY_REPORT_PATH)


def run() -> list[CheckResult]:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        results = raw_suite(con) + silver_suite(con) + gold_suite(con)
    for r in results:
        log.info("%-4s %-7s %-22s %-26s %-18s %s", "PASS" if r.passed else "FAIL",
                 r.layer, r.table, r.check, r.column or "-", r.details)
    save_report(results)
    hard_failures = [r for r in results if not r.passed and r.layer != "raw"]
    if hard_failures:
        raise ValueError(f"{len(hard_failures)} data-quality checks failed on silver/gold layers")
    return results
```


---

## `src/benchmark.py`

### Explanation — Step 5, the HPC benchmark
One workload — *count sales and mean price by year × property type over all raw CSVs* — run by:

| Engine | Parallelism | What it demonstrates |
|---|---|---|
| pandas | 1 process | the naïve baseline: 21 s, 1.7 GB RAM |
| pandas-chunked | 1 process | out-of-core: same time, 2.4× less memory |
| multiprocessing | 1/2/4/8 processes | true CPU parallelism, one file per task |
| dask | 1/2/4/8 threads | Dask DataFrame; thread scheduler is GIL-limited on CSV parsing |
| duckdb-csv | 1/2/4/8 threads | vectorised multi-threaded CSV parser |
| duckdb-parquet | 1/2/4/8 threads | columnar storage + projection pushdown: reads 4 of 16 columns |
| pyspark | local[1/2/4/8] | the industry-standard distributed engine, in a subprocess |

Measurement: `timed()` = best-of-N wall-clock (HPC convention); `PeakRSS` samples driver memory on a
background thread; each engine's totals are checked against the pandas baseline
(`matches_baseline`). `amdahl_parallel_fraction()` solves S = 1/((1−p)+p/N) for p — the measured
parallel fraction of the workload. Spark runs via `subprocess` with a timeout so a JVM hang can never
lose the other results.

### Code
```python
"""
Step 5 - HPC BENCHMARK

The same analytical workload - "count sales and average price by year and property type over
every raw CSV (~1.9 GB, ~9M rows)" - executed by six engines, several of them across
1, 2, 4 and 8 workers, so that strong-scaling speed-up and Amdahl's-law parallel fraction can
be measured on real data:

    pandas          single process, whole dataset in memory            (baseline)
    pandas-chunked  single process, out-of-core 1M-row chunks
    multiprocessing one Python process per file, N workers
    dask            Dask DataFrame, N threads
    duckdb-csv      DuckDB vectorised engine on CSV, N threads
    duckdb-parquet  DuckDB on the silver Parquet layer, N threads       (columnar + parallel)
    pyspark         Spark local[N], if Java + pyspark are available     (industry standard)

Each run records wall time and peak RSS of the driver process. Results -> outputs/benchmark_results.csv
"""
from __future__ import annotations

import glob
import logging
import os
import shutil
import sys
import threading
import time
from multiprocessing import Pool
from pathlib import Path

import pandas as pd
import psutil

from config import (
    BENCHMARK_REPEATS,
    BENCHMARK_RESULTS_PATH,
    BENCHMARK_WORKER_COUNTS,
    MAX_WORKERS,
    OUTPUT_DIR,
    RAW_COLUMNS,
    RAW_DIR,
    TOOLS_DIR,
    YEARS,
    raw_csv_path,
)
from src.clean import SILVER_DIR, silver_glob

log = logging.getLogger(__name__)

USECOLS = ["price", "date_of_transfer", "property_type", "ppd_category_type"]
USECOL_IDX = [RAW_COLUMNS.index(c) for c in USECOLS]
WORKER_COUNTS = [n for n in BENCHMARK_WORKER_COUNTS if n <= MAX_WORKERS]


# --------------------------------------------------------------------------- #
# Measurement helpers
# --------------------------------------------------------------------------- #
class PeakRSS:
    """Samples the driver process RSS on a background thread; reports the peak in MB."""

    def __init__(self, interval: float = 0.05):
        self.interval = interval
        self.peak = 0
        self._stop = threading.Event()
        self._proc = psutil.Process()

    def __enter__(self):
        self.peak = self._proc.memory_info().rss
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            try:
                self.peak = max(self.peak, self._proc.memory_info().rss)
            except psutil.Error:
                pass
            time.sleep(self.interval)

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join()

    @property
    def peak_mb(self) -> float:
        return self.peak / 1e6


def timed(fn, repeats: int = BENCHMARK_REPEATS) -> tuple[float, float, object]:
    """Best-of-N wall-clock seconds (standard HPC practice) + peak RSS + last result."""
    best, peak, result = float("inf"), 0.0, None
    for _ in range(repeats):
        with PeakRSS() as mem:
            t0 = time.perf_counter()
            result = fn()
            elapsed = time.perf_counter() - t0
        best, peak = min(best, elapsed), max(peak, mem.peak_mb)
    return best, peak, result


def raw_files() -> list[Path]:
    return [raw_csv_path(y) for y in YEARS if raw_csv_path(y).exists()]


# --------------------------------------------------------------------------- #
# The workload, per engine. All return a DataFrame [year, property_type, sales, mean_price]
# --------------------------------------------------------------------------- #
def _read_pandas(path: Path, **kw) -> pd.DataFrame:
    return pd.read_csv(path, header=None, usecols=USECOL_IDX, names=USECOLS,
                       dtype={"price": "int64", "property_type": "category", "ppd_category_type": "category"},
                       **kw)


def _partial_agg(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["ppd_category_type"] == "A"]
    year = df["date_of_transfer"].str.slice(0, 4).astype("int16")
    return (df.assign(year=year)
              .groupby(["year", "property_type"], observed=True)["price"]
              .agg(sales="count", total="sum")
              .reset_index())


def _combine(partials: list[pd.DataFrame]) -> pd.DataFrame:
    out = (pd.concat(partials).groupby(["year", "property_type"], observed=True)[["sales", "total"]].sum()
             .reset_index())
    out["mean_price"] = (out["total"] / out["sales"]).round(0)
    return out.drop(columns="total").sort_values(["year", "property_type"]).reset_index(drop=True)


def run_pandas() -> pd.DataFrame:
    df = pd.concat([_read_pandas(p) for p in raw_files()], ignore_index=True)
    return _combine([_partial_agg(df)])


def run_pandas_chunked(chunksize: int = 1_000_000) -> pd.DataFrame:
    partials = []
    for p in raw_files():
        for chunk in _read_pandas(p, chunksize=chunksize):
            partials.append(_partial_agg(chunk))
    return _combine(partials)


def _aggregate_file(path: str) -> pd.DataFrame:      # top-level so multiprocessing can pickle it
    return _partial_agg(_read_pandas(Path(path)))


def run_multiprocessing(workers: int) -> pd.DataFrame:
    with Pool(processes=workers) as pool:
        partials = pool.map(_aggregate_file, [str(p) for p in raw_files()])
    return _combine(partials)


def run_dask(workers: int) -> pd.DataFrame:
    import dask
    import dask.dataframe as dd

    ddf = dd.read_csv((RAW_DIR / "pp-*.csv").as_posix(), header=None, usecols=USECOL_IDX, names=USECOLS,
                      dtype={"price": "int64", "property_type": "object", "ppd_category_type": "object",
                             "date_of_transfer": "object"},
                      blocksize="64MB")
    ddf = ddf[ddf["ppd_category_type"] == "A"]
    ddf["year"] = ddf["date_of_transfer"].str.slice(0, 4).astype("int16")
    agg = ddf.groupby(["year", "property_type"])["price"].agg(["count", "sum"])
    with dask.config.set(scheduler="threads", num_workers=workers):
        res = agg.compute()
    res = res.reset_index().rename(columns={"count": "sales", "sum": "total"})
    res["mean_price"] = (res["total"] / res["sales"]).round(0)
    return res.drop(columns="total").sort_values(["year", "property_type"]).reset_index(drop=True)


_DUCK_AGG = """
    SELECT CAST(substr(date_of_transfer, 1, 4) AS SMALLINT) AS year, property_type,
           count(*) AS sales, round(avg(CAST(price AS BIGINT))) AS mean_price
    FROM {source}
    WHERE ppd_category_type = 'A'
    GROUP BY 1, 2 ORDER BY 1, 2
"""


def run_duckdb_csv(threads: int) -> pd.DataFrame:
    import duckdb

    cols = ", ".join(f"'{c}': 'VARCHAR'" for c in RAW_COLUMNS)
    src = (f"read_csv('{(RAW_DIR / 'pp-*.csv').as_posix()}', header=false, columns={{{cols}}}, "
           f"quote='\"', escape='\"', null_padding=true, ignore_errors=true)")
    with duckdb.connect() as con:
        con.execute(f"SET threads={threads}")
        return con.execute(_DUCK_AGG.format(source=src)).df()


def run_duckdb_parquet(threads: int) -> pd.DataFrame:
    import duckdb

    src = f"read_parquet('{silver_glob()}', hive_partitioning=true)"
    sql = f"""
        SELECT CAST(year AS SMALLINT) AS year, property_type, count(*) AS sales, round(avg(price)) AS mean_price
        FROM {src} WHERE is_standard_sale GROUP BY 1, 2 ORDER BY 1, 2
    """
    with duckdb.connect() as con:
        con.execute(f"SET threads={threads}")
        return con.execute(sql).df()


# ---- PySpark (optional, runs in a subprocess) --------------------------------
def spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
    except ImportError:
        return False
    has_java = (shutil.which("java") is not None
                or (os.environ.get("JAVA_HOME") and Path(os.environ["JAVA_HOME"], "bin").exists())
                or any((p / "bin").exists() for p in TOOLS_DIR.glob("jdk-*")))
    return bool(has_java)


def run_pyspark(workers: int, timeout: int = 300) -> tuple[float, float, pd.DataFrame]:
    """Run src/spark_job.py in a child process; returns (seconds, peak_rss_mb, result)."""
    import json
    import subprocess

    proc = subprocess.run([sys.executable, "-m", "src.spark_job", str(workers)],
                          capture_output=True, text=True, timeout=timeout,
                          cwd=str(Path(__file__).resolve().parents[1]))
    line = next((ln for ln in proc.stdout.splitlines() if ln.startswith("RESULT_JSON ")), None)
    if proc.returncode != 0 or line is None:
        tail = "\n".join((proc.stderr or proc.stdout).strip().splitlines()[-8:])
        raise RuntimeError(f"spark job failed (exit {proc.returncode}):\n{tail}")
    payload = json.loads(line[len("RESULT_JSON "):])
    result = pd.DataFrame(payload["rows"]).sort_values(["year", "property_type"]).reset_index(drop=True)
    log.info("pyspark workers=%s: JVM start-up %.1fs, query %.1fs", workers,
             payload["startup_seconds"], payload["query_seconds"])
    return payload["seconds"], payload["peak_rss_mb"], result


# --------------------------------------------------------------------------- #
def dataset_stats() -> dict:
    csv_bytes = sum(p.stat().st_size for p in raw_files())
    pq_files = glob.glob(silver_glob())
    pq_bytes = sum(os.path.getsize(p) for p in pq_files)
    return {"csv_files": len(raw_files()), "csv_mb": round(csv_bytes / 1e6), "parquet_files": len(pq_files),
            "parquet_mb": round(pq_bytes / 1e6), "compression_ratio": round(csv_bytes / max(pq_bytes, 1), 1)}


def amdahl_parallel_fraction(speedup: float, workers: int) -> float:
    """Solve S = 1 / ((1-p) + p/N) for p."""
    if workers <= 1 or speedup <= 0:
        return float("nan")
    return round((1 - 1 / speedup) / (1 - 1 / workers), 3)


def run_benchmark() -> pd.DataFrame:
    rows: list[dict] = []
    reference: pd.DataFrame | None = None

    def record(engine: str, workers: int, fn, repeats: int = BENCHMARK_REPEATS):
        nonlocal reference
        secs, peak, result = timed(fn, repeats)
        if reference is None:
            reference = result
        total_sales = int(result["sales"].sum()) if result is not None else None
        consistent = total_sales == int(reference["sales"].sum())
        rows.append({"engine": engine, "workers": workers, "seconds": round(secs, 2),
                     "peak_rss_mb": round(peak), "rows_in_result": len(result), "total_sales": total_sales,
                     "matches_baseline": consistent})
        log.info("%-16s workers=%-2s %8.2f s  peak %6.0f MB  %s", engine, workers, secs, peak,
                 "OK" if consistent else "MISMATCH")

    record("pandas", 1, run_pandas, repeats=1)
    record("pandas-chunked", 1, run_pandas_chunked, repeats=1)
    for n in WORKER_COUNTS:
        record("multiprocessing", n, lambda n=n: run_multiprocessing(n))
    for n in WORKER_COUNTS:
        record("dask", n, lambda n=n: run_dask(n))
    for n in WORKER_COUNTS:
        record("duckdb-csv", n, lambda n=n: run_duckdb_csv(n))
    for n in WORKER_COUNTS:
        record("duckdb-parquet", n, lambda n=n: run_duckdb_parquet(n))
    if spark_available():
        for n in WORKER_COUNTS:
            try:
                secs, peak, result = run_pyspark(n)
            except Exception as exc:  # noqa: BLE001 - Spark is optional; never lose the other results
                log.warning("pyspark skipped: %s", str(exc).splitlines()[0])
                log.warning("  (Spark on Windows needs matching Hadoop natives in tools/hadoop/bin - see README)")
                break
            total_sales = int(result["sales"].sum())
            rows.append({"engine": "pyspark", "workers": n, "seconds": round(secs, 2), "peak_rss_mb": round(peak),
                         "rows_in_result": len(result), "total_sales": total_sales,
                         "matches_baseline": total_sales == int(reference["sales"].sum())})
            log.info("%-16s workers=%-2s %8.2f s  peak %6.0f MB  %s", "pyspark", n, secs, peak,
                     "OK" if rows[-1]["matches_baseline"] else "MISMATCH")
    else:
        log.warning("PySpark skipped: install pyspark and a JDK (see README) to include it")

    df = pd.DataFrame(rows)
    base = df.loc[df["engine"] == "pandas", "seconds"].iloc[0]
    df["speedup_vs_pandas"] = (base / df["seconds"]).round(2)
    one_worker = df[df["workers"] == 1].set_index("engine")["seconds"]
    df["speedup_vs_1_worker"] = df.apply(lambda r: round(one_worker.get(r.engine, r.seconds) / r.seconds, 2), axis=1)
    df["amdahl_parallel_fraction"] = df.apply(lambda r: amdahl_parallel_fraction(r.speedup_vs_1_worker, r.workers), axis=1)
    return df


def run() -> pd.DataFrame:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stats = dataset_stats()
    log.info("Dataset: %s CSV files = %s MB; %s Parquet files = %s MB (%.1fx smaller)",
             stats["csv_files"], stats["csv_mb"], stats["parquet_files"], stats["parquet_mb"], stats["compression_ratio"])
    df = run_benchmark()
    for k, v in stats.items():
        df[k] = v
    df.to_csv(BENCHMARK_RESULTS_PATH, index=False)
    log.info("Saved %s", BENCHMARK_RESULTS_PATH)
    return df
```


---

## `src/spark_job.py`

### Explanation — Spark workload (child process)
Sets `JAVA_HOME`/`HADOOP_HOME` from `tools/` if a portable JDK / Hadoop natives are present, builds a
`local[N]` SparkSession, reads all CSVs with an explicit schema, filters, aggregates, and prints a
JSON line with timing (start-up and query separately), peak RSS of the whole process tree (Python +
JVM), and the result rows.

### Code
```python
"""
Spark benchmark job - executed in a *subprocess* by benchmark.py.

Running Spark in its own process isolates the JVM from the driver, lets the parent enforce a
timeout (Spark on Windows can hang when Hadoop native libraries are missing), and gives an
honest memory figure: the Python driver plus every JVM child process.

    python -m src.spark_job <workers>      -> prints one JSON line with the result
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config import RAW_COLUMNS, RAW_DIR, TOOLS_DIR  # noqa: E402


def configure_environment() -> None:
    """Point Spark at a bundled JDK / Hadoop natives under tools/ when present."""
    for jdk in sorted(TOOLS_DIR.glob("jdk-*")):
        if (jdk / "bin" / "java.exe").exists() or (jdk / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(jdk)
            os.environ["PATH"] = str(jdk / "bin") + os.pathsep + os.environ["PATH"]
            break
    hadoop = TOOLS_DIR / "hadoop"
    lib_opt = ""
    if (hadoop / "bin").exists():
        os.environ["HADOOP_HOME"] = str(hadoop)
        os.environ["PATH"] = str(hadoop / "bin") + os.pathsep + os.environ["PATH"]
        # extraLibraryPath is applied by the Spark launcher when it starts the driver JVM
        lib_opt = f"--conf spark.driver.extraLibraryPath={hadoop / 'bin'} "
    # In PySpark the driver JVM is launched *before* SparkSession.builder.config() runs,
    # so JVM-level options must be passed through PYSPARK_SUBMIT_ARGS.
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"{lib_opt}--driver-memory 6g pyspark-shell"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def tree_rss_mb() -> float:
    me = psutil.Process()
    total = me.memory_info().rss
    for child in me.children(recursive=True):
        try:
            total += child.memory_info().rss
        except psutil.Error:
            pass
    return total / 1e6


def main(workers: int) -> dict:
    configure_environment()
    from pyspark.sql import SparkSession, functions as F, types as T

    t0 = time.perf_counter()
    spark = (SparkSession.builder.master(f"local[{workers}]").appName(f"pp-benchmark-{workers}")
             .config("spark.sql.shuffle.partitions", str(max(workers * 2, 4)))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    startup = time.perf_counter() - t0
    try:
        schema = T.StructType([T.StructField(c, T.StringType()) for c in RAW_COLUMNS])
        t1 = time.perf_counter()
        df = (spark.read.csv((RAW_DIR / "pp-*.csv").as_posix(), schema=schema, header=False, quote='"', escape='"')
              .where(F.col("ppd_category_type") == "A")
              .select(F.substring("date_of_transfer", 1, 4).cast("short").alias("year"), "property_type",
                      F.col("price").cast("long").alias("price"))
              .groupBy("year", "property_type")
              .agg(F.count("*").alias("sales"), F.round(F.avg("price")).alias("mean_price"))
              .orderBy("year", "property_type"))
        rows = [r.asDict() for r in df.collect()]
        query_secs = time.perf_counter() - t1
        peak = tree_rss_mb()
    finally:
        spark.stop()
    return {"workers": workers, "seconds": round(time.perf_counter() - t0, 2),
            "startup_seconds": round(startup, 2), "query_seconds": round(query_secs, 2),
            "peak_rss_mb": round(peak), "rows": rows}


if __name__ == "__main__":
    out = main(int(sys.argv[1]) if len(sys.argv) > 1 else 2)
    print("RESULT_JSON " + json.dumps(out))
```


---

## `src/visualize.py`

### Explanation — Step 6, charts
Six market charts from the gold views (city comparison, Liverpool districts, monthly market with the
stamp-duty-holiday band, property types, county league, new-build premium) and three benchmark
charts (time by engine, strong-scaling speed-up vs ideal, peak memory). Each view is also saved as
CSV in `outputs/tables/` for Power BI / Excel.

### Code
```python
"""
Step 6 - VISUALISE

Reads the gold views and the benchmark CSV and writes PNG charts to outputs/figures/,
plus the underlying tables as CSV to outputs/tables/ (handy for the report and for Power BI).
"""
from __future__ import annotations

import logging
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mtick  # noqa: E402
import pandas as pd  # noqa: E402

from config import BENCHMARK_RESULTS_PATH, FIGURES_DIR, TABLES_DIR  # noqa: E402
from src.transform import query  # noqa: E402

log = logging.getLogger(__name__)
plt.style.use("seaborn-v0_8-whitegrid")
GBP = mtick.FuncFormatter(lambda v, _: f"£{v/1000:,.0f}k")


def _save(fig, name: str) -> Path:
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURES_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=130)
    plt.close(fig)
    log.info("Saved %s", path)
    return path


def _table(df: pd.DataFrame, name: str) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(TABLES_DIR / f"{name}.csv", index=False)


def plot_city_comparison() -> Path:
    df = query("SELECT * FROM vw_city_comparison")
    _table(df, "city_comparison")
    pivot = df.pivot(index="year", columns="town_city", values="median_price")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for city in pivot.columns:
        ax.plot(pivot.index, pivot[city], marker="o", linewidth=2.5 if city == "LIVERPOOL" else 1.4,
                label=city.title(), alpha=1 if city == "LIVERPOOL" else 0.8)
    ax.yaxis.set_major_formatter(GBP)
    ax.set_title("Median residential sale price by city")
    ax.set_xlabel("Year")
    ax.legend(ncol=2, fontsize=9)
    return _save(fig, "01_city_comparison.png")


def plot_liverpool_districts() -> Path:
    df = query("SELECT * FROM vw_liverpool_districts").head(20)
    _table(query("SELECT * FROM vw_liverpool_districts"), "liverpool_districts")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(df["postcode_district"][::-1], df["growth_pct"][::-1], color="#c0392b")
    for i, (g, a, b) in enumerate(zip(df["growth_pct"][::-1], df["median_first_year"][::-1], df["median_last_year"][::-1])):
        ax.text(g + 1, i, f"{g:.0f}%   (£{a/1000:.0f}k → £{b/1000:.0f}k)", va="center", fontsize=8)
    ax.set_xlabel("Median price growth, first → last year (%)")
    ax.set_title("Liverpool postcode districts (L postcodes): fastest-growing median prices")
    ax.set_xlim(0, df["growth_pct"].max() * 1.35)
    return _save(fig, "02_liverpool_districts.png")


def plot_monthly_market() -> Path:
    df = query("SELECT * FROM vw_monthly_market")
    _table(df, "monthly_market")
    df["month_start"] = pd.to_datetime(df["month_start"])
    fig, ax1 = plt.subplots(figsize=(11, 5.5))
    ax1.bar(df["month_start"], df["sales"], width=25, color="#aed6f1", label="Sales per month")
    ax1.set_ylabel("Sales")
    hol = df[df["sdlt_holiday"]]
    if not hol.empty:
        ax1.axvspan(hol["month_start"].min(), hol["month_start"].max(), color="#f9e79f", alpha=0.5,
                    label="Stamp-duty holiday (Jul 2020 – Sep 2021)")
    ax2 = ax1.twinx()
    ax2.plot(df["month_start"], df["median_price"], color="#1b2631", linewidth=2, label="Median price")
    ax2.yaxis.set_major_formatter(GBP)
    ax1.set_title("England & Wales residential market: monthly volume and median price")
    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="upper left", fontsize=9)
    return _save(fig, "03_monthly_market.png")


def plot_property_types() -> Path:
    df = query("SELECT * FROM vw_price_by_year_type")
    _table(df, "price_by_year_type")
    pivot = df.pivot(index="year", columns="property_type_name", values="median_price")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    pivot.plot(ax=ax, marker="o")
    ax.yaxis.set_major_formatter(GBP)
    ax.set_title("Median price by property type")
    ax.set_xlabel("Year")
    ax.legend(title=None)
    return _save(fig, "04_property_types.png")


def plot_county_league() -> Path:
    df = query("SELECT * FROM vw_county_league")
    _table(df, "county_league")
    top, bottom = df.head(10), df.tail(10)
    sel = pd.concat([top, bottom])
    colors = ["#27ae60"] * len(top) + ["#e74c3c"] * len(bottom)
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.barh(sel["county"].str.title()[::-1], sel["growth_pct"][::-1], color=colors[::-1])
    ax.set_xlabel("Median price growth, first → last year (%)")
    ax.set_title("Counties with the highest (green) and lowest (red) price growth")
    return _save(fig, "05_county_league.png")


def plot_new_build_premium() -> Path:
    df = query("SELECT * FROM vw_new_build_premium")
    _table(df, "new_build_premium")
    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.plot(df["year"], df["median_new_build"], marker="o", label="New build", color="#8e44ad")
    ax1.plot(df["year"], df["median_existing"], marker="o", label="Existing", color="#7f8c8d")
    ax1.yaxis.set_major_formatter(GBP)
    ax2 = ax1.twinx()
    ax2.bar(df["year"], df["premium_pct"], alpha=0.2, color="#8e44ad", label="Premium %")
    ax2.set_ylabel("New-build premium (%)")
    ax1.set_title("New-build premium over existing homes")
    ax1.legend(loc="upper left")
    return _save(fig, "06_new_build_premium.png")


def plot_benchmark() -> list[Path]:
    b = pd.read_csv(BENCHMARK_RESULTS_PATH)
    paths = []

    # (a) absolute time per engine at max workers vs single-threaded
    best = b.sort_values("seconds").groupby("engine", as_index=False).first().sort_values("seconds", ascending=False)
    fig, ax = plt.subplots(figsize=(10, 5.5))
    bars = ax.barh(best["engine"] + best["workers"].map(lambda w: f"  (×{w})"), best["seconds"], color="#2e86c1")
    for bar, s, spd in zip(bars, best["seconds"], best["speedup_vs_pandas"]):
        ax.text(bar.get_width() + best["seconds"].max() * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{s:.1f}s  ({spd:.1f}× vs pandas)", va="center", fontsize=9)
    ax.set_xlabel("Wall-clock seconds (best configuration per engine)")
    ax.set_title(f"Same workload, {int(b['csv_mb'].iloc[0]):,} MB of CSV: time by engine")
    ax.set_xlim(0, best["seconds"].max() * 1.35)
    paths.append(_save(fig, "07_benchmark_engines.png"))

    # (b) strong scaling: speed-up vs workers, with ideal line
    par = b[b["engine"].isin(["multiprocessing", "dask", "duckdb-csv", "duckdb-parquet", "pyspark"])]
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ws = sorted(par["workers"].unique())
    ax.plot(ws, ws, "k--", label="Ideal (linear)")
    for engine, grp in par.groupby("engine"):
        grp = grp.sort_values("workers")
        ax.plot(grp["workers"], grp["speedup_vs_1_worker"], marker="o", label=engine)
    ax.set_xlabel("Workers / threads")
    ax.set_ylabel("Speed-up vs 1 worker (same engine)")
    ax.set_xticks(ws)
    ax.set_title("Strong scaling (Amdahl's law in practice)")
    ax.legend()
    paths.append(_save(fig, "08_benchmark_scaling.png"))

    # (c) peak memory
    fig, ax = plt.subplots(figsize=(9, 5))
    mem = b.groupby("engine")["peak_rss_mb"].max().sort_values()
    ax.barh(mem.index, mem.values, color="#f39c12")
    ax.set_xlabel("Peak driver-process RSS (MB)")
    ax.set_title("Peak memory by engine")
    paths.append(_save(fig, "09_benchmark_memory.png"))
    return paths


def run() -> list[Path]:
    paths = [plot_city_comparison(), plot_liverpool_districts(), plot_monthly_market(),
             plot_property_types(), plot_county_league(), plot_new_build_premium()]
    if BENCHMARK_RESULTS_PATH.exists():
        paths += plot_benchmark()
    return paths
```


---

## `src/report.py`

### Explanation — Step 7, report
Builds `outputs/report.md`: KPIs, yearly table, Liverpool vs England & Wales growth, city pivot,
top Liverpool districts, county extremes, new-build premium, the full quality table, the benchmark
table with speed-ups and Amdahl fractions, and an interpretation paragraph.

### Code
```python
"""
Step 7 - REPORT

Assembles outputs/report.md from the gold views, the quality JSON and the benchmark CSV.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

import pandas as pd

from config import (
    BENCHMARK_RESULTS_PATH,
    FIGURES_DIR,
    OUTPUT_DIR,
    QUALITY_REPORT_PATH,
    REPORT_PATH,
    YEARS,
)
from src.transform import query

log = logging.getLogger(__name__)


def _md(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False)


def _fig(name: str, caption: str) -> str:
    rel = (FIGURES_DIR / name).relative_to(OUTPUT_DIR).as_posix()
    return f"![{caption}]({rel})\n\n*{caption}*\n"


def _gbp(v) -> str:
    return f"£{v:,.0f}"


def build_report() -> str:
    kpi = query("""
        SELECT count(*) AS transactions,
               count(DISTINCT f.postcode) AS postcodes,
               min(d.full_date) AS first_sale, max(d.full_date) AS last_sale,
               median(CASE WHEN f.is_residential AND f.is_standard_sale AND f.is_price_plausible THEN f.price END) AS median_price,
               sum(f.price) AS total_value
        FROM fact_sale f JOIN dim_date d ON d.date_key = f.date_key
    """).iloc[0]
    yearly = query("""
        SELECT d.year, count(*) AS sales, median(f.price)::BIGINT AS median_price
        FROM fact_sale f JOIN dim_date d ON d.date_key = f.date_key
        WHERE f.is_residential AND f.is_standard_sale AND f.is_price_plausible
        GROUP BY 1 ORDER BY 1
    """)
    lpool = query("""
        SELECT d.year, count(*) AS sales, median(f.price)::BIGINT AS median_price
        FROM fact_sale f JOIN dim_date d ON d.date_key = f.date_key JOIN dim_location l ON l.location_key = f.location_key
        WHERE l.is_liverpool AND f.is_residential AND f.is_standard_sale AND f.is_price_plausible
        GROUP BY 1 ORDER BY 1
    """)
    city = query("SELECT * FROM vw_city_comparison").pivot(index="year", columns="town_city", values="median_price").reset_index()
    districts = query("SELECT * FROM vw_liverpool_districts").head(10)
    counties = query("SELECT * FROM vw_county_league")
    premium = query("SELECT * FROM vw_new_build_premium")

    quality = json.loads(QUALITY_REPORT_PATH.read_text(encoding="utf-8"))
    qdf = pd.DataFrame(quality["results"])
    qdf["status"] = qdf["passed"].map({True: "PASS", False: "FAIL"})
    qdf = qdf[["layer", "table", "check", "column", "status", "details"]]

    bench = pd.read_csv(BENCHMARK_RESULTS_PATH)
    stats = bench.iloc[0]
    bench_tbl = bench[["engine", "workers", "seconds", "peak_rss_mb", "speedup_vs_pandas",
                       "speedup_vs_1_worker", "amdahl_parallel_fraction"]]
    best = bench.sort_values("seconds").iloc[0]

    lp_growth = (lpool["median_price"].iloc[-1] / lpool["median_price"].iloc[0] - 1) * 100
    ew_growth = (yearly["median_price"].iloc[-1] / yearly["median_price"].iloc[0] - 1) * 100

    s = [
        "# UK Property Market Lakehouse & HPC Benchmark — Project Report",
        f"_Generated {datetime.now():%Y-%m-%d %H:%M}_\n",
        "## 1. Dataset",
        f"HM Land Registry *Price Paid Data*, years {YEARS[0]}–{YEARS[-1]} "
        "(https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads), Open Government Licence v3. "
        "Every residential property sale registered in England and Wales; one CSV per year.\n",
        "## 2. Key metrics (gold layer)",
        _md(pd.DataFrame([{
            "Transactions": f"{int(kpi.transactions):,}",
            "Distinct postcodes": f"{int(kpi.postcodes):,}",
            "First sale": str(kpi.first_sale), "Last sale": str(kpi.last_sale),
            "Median residential price": _gbp(kpi.median_price),
            "Total value transacted": f"£{kpi.total_value/1e9:,.0f} bn",
        }])) + "\n",
        "## 3. Market analytics",
        "### 3.1 England & Wales by year", _md(yearly) + "\n",
        _fig("03_monthly_market.png", "Monthly volume and median price — note the stamp-duty-holiday spike"),
        "### 3.2 Liverpool vs other cities",
        f"Liverpool's median price grew **{lp_growth:.0f}%** between {int(lpool.year.iloc[0])} and {int(lpool.year.iloc[-1])}, "
        f"versus **{ew_growth:.0f}%** for England & Wales overall.\n",
        _md(lpool.rename(columns={"sales": "liverpool_sales", "median_price": "liverpool_median"})) + "\n",
        _md(city) + "\n",
        _fig("01_city_comparison.png", "Median price by city"),
        "### 3.3 Liverpool postcode districts — top 10 by growth", _md(districts) + "\n",
        _fig("02_liverpool_districts.png", "Fastest-growing Liverpool districts"),
        "### 3.4 Property types", _fig("04_property_types.png", "Median price by property type"),
        "### 3.5 Counties — highest and lowest growth",
        _md(pd.concat([counties.head(5), counties.tail(5)])) + "\n",
        _fig("05_county_league.png", "County growth league"),
        "### 3.6 New-build premium", _md(premium) + "\n",
        _fig("06_new_build_premium.png", "New-build premium"),
        "## 4. Data quality",
        f"**{quality['passed']} / {quality['total_checks']} checks passed.** Raw-layer failures are expected and "
        "document the defects the silver layer fixes (missing postcodes, non-residential 'Other' property types, "
        "implausible prices). All silver and gold checks must pass or the pipeline stops.\n",
        _md(qdf) + "\n",
        "## 5. HPC benchmark",
        f"Workload: count and average price by year × property type over {int(stats.csv_files)} CSV files "
        f"({int(stats.csv_mb):,} MB). The silver Parquet layer is {int(stats.parquet_mb):,} MB "
        f"({stats.compression_ratio}× smaller). Best-of-N wall-clock time; peak RSS of the driver process.\n",
        _md(bench_tbl) + "\n",
        _fig("07_benchmark_engines.png", "Time by engine"),
        _fig("08_benchmark_scaling.png", "Strong scaling"),
        _fig("09_benchmark_memory.png", "Peak memory"),
        f"**Fastest configuration:** `{best.engine}` with {int(best.workers)} workers — {best.seconds:.2f} s, "
        f"**{best.speedup_vs_pandas:.0f}× faster than single-process pandas** ({bench.iloc[0].seconds:.1f} s).\n",
        "**Interpretation.** Single-process pandas must parse every byte of CSV in one thread and hold the whole "
        "dataset in memory. Chunking removes the memory problem but not the time. Multiprocessing and Dask give "
        "near-linear speed-up while there are files to hand out, then flatten (Amdahl's law — the serial parse of "
        "each file bounds the gain). DuckDB parses CSV with vectorised, multi-threaded code and wins on CSV; on the "
        "columnar Parquet layer it only reads the four columns it needs, so it is faster still. Spark carries JVM "
        "start-up and scheduling overhead that only pays off on data larger than one machine.\n",
        "## 6. Architecture",
        "```\n"
        f"Land Registry CSVs ({YEARS[0]}–{YEARS[-1]}) ──► data/raw (bronze, incremental manifest)\n"
        "        ──► data/processed/sales/year=YYYY/*.parquet (silver: DuckDB clean, ZSTD, partitioned)\n"
        "        ──► data/analytics/property.duckdb (gold: dim_date, dim_location, dim_property, fact_sale, 6 views)\n"
        "quality.py gates every layer · benchmark.py measures engines/scaling · visualize.py + report.py publish\n"
        "```\n",
    ]
    return "\n".join(s)


def run() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(build_report(), encoding="utf-8")
    log.info("Report written to %s", REPORT_PATH)
```


---

## `run_pipeline.py`

### Explanation — orchestrator
Ordered dict of steps, `--steps` subset, `--force` to ignore manifests, timing per step, exit code 1
on failure. The `if __name__ == "__main__"` guard is mandatory on Windows for `multiprocessing`.

### Code
```python
"""
Pipeline orchestrator.

    python run_pipeline.py                       # everything
    python run_pipeline.py --steps clean transform quality
    python run_pipeline.py --force               # re-download / re-clean every year
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src import benchmark, clean, ingest, quality, report, transform, visualize  # noqa: E402

STEPS = {
    "ingest": lambda force: ingest.run(force=force),
    "clean": lambda force: clean.run(force=force),
    "transform": lambda force: transform.run(),
    "quality": lambda force: quality.run(),
    "benchmark": lambda force: benchmark.run(),
    "visualize": lambda force: visualize.run(),
    "report": lambda force: report.run(),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UK Property Market Lakehouse pipeline")
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=list(STEPS))
    parser.add_argument("--force", action="store_true", help="ignore manifests; redo ingest/clean for all years")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s", datefmt="%H:%M:%S")
    log = logging.getLogger("pipeline")

    t_all = time.perf_counter()
    for name, fn in STEPS.items():
        if name not in args.steps:
            continue
        log.info("=" * 72)
        log.info("STEP: %s", name.upper())
        t0 = time.perf_counter()
        try:
            fn(args.force)
        except Exception:
            log.exception("Step '%s' failed", name)
            return 1
        log.info("STEP %s done in %.1fs", name.upper(), time.perf_counter() - t0)
    log.info("=" * 72)
    log.info("Pipeline finished in %.1fs", time.perf_counter() - t_all)
    return 0


if __name__ == "__main__":          # required on Windows for multiprocessing in the benchmark
    raise SystemExit(main())
```


---

## `tests/test_clean.py`

### Explanation — tests for cleaning
Parametrised tests for postcode normalisation and price bands; a 5-row fixture containing a
duplicate, a deleted record, a lower-case postcode, a non-residential 'O' sale and an implausible
price; and `test_duckdb_sql_matches_pandas`, which writes the fixture to a CSV, runs the production
SQL on it and asserts it equals the pandas implementation on 14 columns.

### Code
```python
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
```


---

## `tests/test_quality.py`

### Explanation — tests for quality checks and benchmark maths
Each SQL check is exercised on an in-memory DuckDB table with a known defect; `test_amdahl` checks
the parallel-fraction formula at perfect scaling, no scaling and a realistic 3× on 4 cores (p = 0.889).

### Code
```python
"""Unit tests for the SQL-pushdown quality checks and the benchmark maths."""
import duckdb
import pytest

from src.benchmark import amdahl_parallel_fraction
from src.quality import (
    check_min_rows,
    check_not_null,
    check_range,
    check_referential_integrity,
    check_regex,
    check_row_count_matches,
    check_unique,
    check_values_in_set,
)


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 'L1 8JQ', 'D', 100), (2, NULL, 'F', 250), (2, 'BAD', 'X', -5)) v(id, pc, ptype, price)")
    c.execute("CREATE TABLE dim AS SELECT * FROM (VALUES (1), (2)) v(id)")
    c.execute("CREATE TABLE fact AS SELECT * FROM (VALUES (1), (2), (3)) v(id)")
    yield c
    c.close()


def test_min_rows(con):
    assert check_min_rows(con, "t", "l", "t", 3).passed
    assert not check_min_rows(con, "t", "l", "t", 4).passed


def test_not_null_and_unique(con):
    assert not check_not_null(con, "t", "l", "t", "pc").passed
    assert check_not_null(con, "t", "l", "t", "id").passed
    r = check_unique(con, "t", "l", "t", "id")
    assert not r.passed and "1 duplicate" in r.details


def test_values_in_set(con):
    r = check_values_in_set(con, "t", "l", "t", "ptype", ["D", "F"])
    assert not r.passed and "X" in r.details


def test_range_and_regex(con):
    assert not check_range(con, "t", "l", "t", "price", 0, None).passed
    assert check_range(con, "t", "l", "t", "price", -10, 1000).passed
    r = check_regex(con, "t", "l", "t", "pc", "^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$")
    assert not r.passed and "1 values" in r.details


def test_referential_integrity_and_counts(con):
    r = check_referential_integrity(con, "fact", "id", "dim", "id", "l", "fact")
    assert not r.passed and "1 orphan" in r.details
    assert not check_row_count_matches(con, "fact", "dim", "l", "fact").passed
    assert check_row_count_matches(con, "fact", "t", "l", "fact").passed


def test_amdahl():
    assert amdahl_parallel_fraction(4.0, 4) == 1.0            # perfect scaling
    assert amdahl_parallel_fraction(1.0, 4) == 0.0            # no scaling
    assert 0.85 < amdahl_parallel_fraction(3.0, 4) < 0.9      # 3x on 4 cores -> 0.889
    import math
    assert math.isnan(amdahl_parallel_fraction(2.0, 1))
```


---

## `pytest.ini`
### Explanation
Makes `from src… import` resolve from the project root; tests live in `tests/`.

### Code
```ini
[pytest]
pythonpath = .
testpaths = tests
```


---

## `.github/workflows/ci.yml`

### Explanation — GitHub Actions
On every push: install dependencies, run the 24 tests, then with `PP_YEARS=2026` download the
smallest real file (~80 MB) and run ingest → clean → transform → quality. Real data, bounded runtime.

### Code
```yaml
name: CI
on:
  push:
    branches: [main]
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    env:
      PP_YEARS: "2026"          # smoke-test on the latest (smallest) real file only
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
      - run: pip install pandas pyarrow duckdb "dask[dataframe]" matplotlib requests tabulate psutil pytest
      - name: Unit tests
        run: pytest -q
      - name: Pipeline smoke test (ingest -> clean -> transform -> quality)
        run: python run_pipeline.py --steps ingest clean transform quality
```


---

## `.gitignore`
### Explanation
`data/` (≈2.3 GB generated), `tools/` (JDK) and `.venv/` are never committed; `.gitkeep` files preserve the folder structure.

### Code
```gitignore
# generated data (1.8 GB raw + parquet + duckdb) - never commit
data/raw/*
data/processed/*
data/analytics/*
!data/**/.gitkeep
# portable JDK (download, do not commit)
tools/*
!tools/.gitkeep
# python
.venv/
__pycache__/
*.pyc
.pytest_cache/
# spark / ide
spark-warehouse/
derby.log
.vscode/
.idea/
```


---

## Part 4 — Run it

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run_pipeline.py        # ~2 min without benchmark, ~10 min with (after the ~6 min download)
pytest -v                     # 24 tests
```

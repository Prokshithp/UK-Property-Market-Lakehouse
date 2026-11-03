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

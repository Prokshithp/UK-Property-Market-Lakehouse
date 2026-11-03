# UK Property Market Lakehouse & HPC Benchmark

![CI](https://img.shields.io/badge/CI-GitHub%20Actions-blue) ![Python](https://img.shields.io/badge/python-3.12-blue) ![Data](https://img.shields.io/badge/data-10.7M%20rows%20%7C%201.86%20GB-green)

A local-first lakehouse over **every property sale in England & Wales from 2016 to 2026**
(10.7 million transactions, 1.86 GB of raw CSV from HM Land Registry), built in Python with
DuckDB, Parquet, Dask and Spark — and a **high-performance-computing benchmark** that measures
how the same workload scales across engines and cores.

```
Land Registry CSVs ──► bronze  data/raw/pp-YYYY.csv                 incremental, SHA-256 manifest
                   ──► silver  data/processed/sales/year=YYYY/      DuckDB clean → ZSTD Parquet, partitioned
                   ──► gold    data/analytics/property.duckdb       star schema + 6 analytical SQL views

quality.py gates every layer · benchmark.py (pandas / multiprocessing / Dask / DuckDB / Spark × 1-8 cores)
visualize.py + report.py publish charts, tables and a markdown report
```

## Why this project

| Senior data-engineering concept | Where it is demonstrated |
|---|---|
| Genuine data volume | 10.7M rows / 1.86 GB processed end-to-end on a laptop in ~2 minutes |
| Medallion / lakehouse architecture | bronze → silver → gold folders, Parquet partitioned by year |
| Incremental loading | manifests with SHA-256 — only new or changed years are re-processed |
| Distributed & parallel compute | multiprocessing, Dask, DuckDB threads, Spark `local[N]` benchmarked at 1/2/4/8 workers |
| Performance engineering | strong-scaling curves, Amdahl parallel fraction, peak-RSS, CSV vs Parquet (5.6× smaller) |
| Dimensional modelling | `dim_date`, `dim_location`, `dim_property`, `fact_sale`, 6 views (the "dbt marts") |
| Data quality as a gate | 7 generic SQL-pushdown checks, 27 assertions, silver/gold failures stop the pipeline |
| Testing & CI | 24 pytest tests incl. SQL-vs-pandas equivalence; GitHub Actions smoke-tests one real year |
| Domain analytics | Liverpool vs UK cities, L-postcode district growth, stamp-duty-holiday effect, new-build premium |

## The dataset

**HM Land Registry – Price Paid Data** · Open Government Licence v3 · no login required
<https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads>

One CSV per year, no header. Columns: transaction id, price, date, postcode, property type
(D/S/T/F/O), new-build flag, tenure (F/L), address fields, district, county, PPD category, record status.

| Years | Files | Size | Rows |
|---|---|---|---|
| 2016–2026 (default) | 11 | 1.86 GB | 10.67 M |

**You download nothing by hand** — `python run_pipeline.py` fetches each year (≈4–8 min on a
normal connection) and skips years already present. To work with fewer years:

```powershell
$env:PP_YEARS = "2024,2025,2026"     # macOS/Linux: export PP_YEARS=2024,2025,2026
python run_pipeline.py
```

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1                # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py                      # download → clean → model → quality → benchmark → charts → report
pytest -v
```

Run subsets: `python run_pipeline.py --steps clean transform quality` · force a full rebuild: `--force`.

### Optional: enable the Spark engine
PySpark needs Java. Either install any JDK 17 and set `JAVA_HOME`, **or** drop a portable JDK
into `tools/` (the benchmark auto-detects `tools/jdk-*`):

```powershell
curl -L -o jdk.zip "https://api.adoptium.net/v3/binary/latest/17/ga/windows/x64/jdk/hotspot/normal/eclipse?project=jdk"
Expand-Archive jdk.zip tools\ ; Remove-Item jdk.zip
```
Without Java the benchmark simply skips Spark and logs a warning.

## What you get

| Deliverable | Location |
|---|---|
| Raw dataset (bronze) | `data/raw/pp-2016.csv … pp-2026.csv` + `_manifest.json` |
| Cleaned Parquet (silver) | `data/processed/sales/year=YYYY/*.parquet` (334 MB, ZSTD) |
| SQL database (gold) | `data/analytics/property.duckdb` — 4 tables, 6 views |
| Data-quality report | `outputs/quality_report.json` |
| HPC benchmark results | `outputs/benchmark_results.csv` + figures 07–09 |
| Analytics tables | `outputs/tables/*.csv` |
| Charts | `outputs/figures/*.png` (9) |
| Report | `outputs/report.md` |

Query the gold layer from Python:

```python
from src.transform import query
query("SELECT * FROM vw_city_comparison WHERE town_city = 'LIVERPOOL'")
```
or open `property.duckdb` with DBeaver / the DuckDB CLI.

## Star schema

```
dim_date (3.9k) ──────┐
dim_location (9.2k) ──┼──► fact_sale (10.67 M)
dim_property (20) ────┘
views: vw_price_by_year_type · vw_city_comparison · vw_liverpool_districts
       vw_monthly_market · vw_county_league · vw_new_build_premium
```

## Project layout

```
uk-property-lakehouse/
├── config.py            paths, years, constants (PP_YEARS override)
├── run_pipeline.py      orchestrator
├── src/
│   ├── ingest.py        incremental download with manifest + SHA-256
│   ├── clean.py         cleaning rules in DuckDB SQL *and* pandas (tested equal)
│   ├── transform.py     star schema + views in DuckDB
│   ├── quality.py       SQL-pushdown quality framework
│   ├── benchmark.py     six engines × worker counts, Amdahl analysis
│   ├── visualize.py     9 charts + CSV tables
│   └── report.py        markdown report
├── tests/               24 unit tests
├── .github/workflows/ci.yml
├── GUIDE.md             step-by-step learning guide
├── CODE_WALKTHROUGH.md  every file explained with full source
└── PROJECT_DOCUMENT.md  formal project document
```

## Mapping to production

| Here | In production |
|---|---|
| DuckDB / Dask / Spark local | Databricks / EMR / Synapse Spark |
| Parquet partitioned by year | Delta Lake with partitioning + Z-order |
| Manifest-driven incremental loads | Auto Loader / Airflow sensors |
| DuckDB star schema + views | Snowflake / BigQuery + dbt |
| `quality.py` | dbt tests / Great Expectations / Soda |
| `run_pipeline.py` | Airflow / Databricks Workflows |

## Citation
Contains HM Land Registry data © Crown copyright and database right 2026. This data is licensed
under the Open Government Licence v3.0.

## Dataset Source

- **Source:** HM Land Registry
- **Dataset:** Price Paid Data
- **Coverage:** England and Wales
- **Official Link:** https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

**About**

End-to-end UK property market data engineering platform implementing Bronze-Silver-Gold lakehouse architecture, incremental data ingestion, Parquet transformations, dimensional modelling, data quality validation, analytical SQL views and high-performance computing benchmarks using Python, DuckDB, Dask and Apache Spark.

**Author & Ownership**

**Author and Project Owner: Prokshith**

This project is original work created and maintained by Prokshith.

Copyright and Usage Notice

**© 2026 Prokshith. All rights reserved.**

This repository is publicly accessible for viewing and learning purposes. Public availability does not grant permission to copy, reproduce, modify, redistribute, republish, or commercially use the project's original source code, documentation, architecture, or other materials.

**Prior written permission from the project owner is required before copying, reusing, modifying, or redistributing any part of this project.

Please respect the author's intellectual property and acknowledge the project owner when referring to this work.**

**Third-Party Data and Licensing**

The HM Land Registry Price Paid Data used in this project remains subject to its own Open Government Licence v3.0. The project ownership notice does not override or change the original licence terms applying to third-party data.

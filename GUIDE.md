# Step-by-Step Guide

Work through these in order. Each step says **what to run**, **what to read**, and **what you
should see**. Budget: about 2 hours to run and understand everything.

---

## Step 0 – Set up (10 min)

1. Open the `uk-property-lakehouse` folder in VS Code (**File → Open Folder**).
2. Terminal (**Ctrl + `**):
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
   (If `python` is not found on Windows, use the full path, e.g. `& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" -m venv .venv`.)
3. **Ctrl+Shift+P → Python: Select Interpreter → `.venv`**.
4. Optional but recommended – enable Spark: download a portable JDK into `tools/` (command in README).

**Read:** `requirements.txt`. Note there are two "big data" engines here that the healthcare
project did not have: **Dask** and **PySpark**, plus **DuckDB** as the SQL warehouse.

---

## Step 1 – Configuration

**Read:** `config.py`

* `YEARS = 2016..2026` – which yearly files to process. `PP_YEARS=2025,2026` in the environment
  overrides it (used by CI and for quick experiments).
* `BASE_URL` – the Land Registry's public S3 bucket; `raw_csv_url(2024)` builds a file URL.
* `RAW_COLUMNS` – the files have **no header row**; this list gives the 16 columns their names.
* `MIN/MAX_PLAUSIBLE_PRICE` – sanity bounds; outside values are *flagged*, not deleted.
* `BENCHMARK_WORKER_COUNTS = [1, 2, 4, 8]` – the strong-scaling experiment.

---

## Step 2 – Ingest (bronze)

```powershell
python run_pipeline.py --steps ingest
```
**See:** 11 files in `data/raw/` (1.86 GB) and `data/raw/_manifest.json`.

**Read:** `src/ingest.py`
* `download_year()` streams a file in 8 MB blocks to `.part`, renames on success (never a
  half-written file), then records bytes + SHA-256 in the manifest.
* Re-running does a `HEAD` request for the current year only – the 2026 file grows monthly, so
  it is refreshed when its size changes. Older years are skipped instantly. **That is an
  incremental load.**

**Try:** open `_manifest.json`. Delete the entry for 2026 and re-run – only 2026 downloads.

---

## Step 3 – Clean (silver)

```powershell
python run_pipeline.py --steps clean
```
**See:** `year=2016 -> 1,046,796 rows written` … 11 partitions, **334 MB Parquet** (5.6× smaller
than CSV), in ~35 seconds. Look in `data/processed/sales/year=2021/` – several Parquet files,
one per DuckDB thread (`PER_THREAD_OUTPUT`).

**Read:** `src/clean.py` – it contains the same rules **twice**:
* `CLEAN_SQL_TEMPLATE` – the production path in DuckDB SQL: keep live records, de-duplicate with
  `QUALIFY row_number()`, cast types, normalise postcodes with regex, derive year/quarter,
  type names, flags, price band.
* `clean_frame()` – identical rules in pandas, used by the unit tests and by the pandas / Dask /
  multiprocessing engines in the benchmark.
* `tests/test_clean.py::test_duckdb_sql_matches_pandas` proves both produce the same output.

**Why DuckDB for cleaning?** pandas would need ~8 GB RAM and minutes; DuckDB streams the CSV in
parallel and writes Parquet directly. This is the "right tool for the volume" decision.

**Try:**
```powershell
python -c "import duckdb; print(duckdb.sql(\"SELECT year, count(*) FROM read_parquet('data/processed/sales/*/*.parquet', hive_partitioning=true) GROUP BY 1 ORDER BY 1\"))"
```

---

## Step 4 – Transform (gold star schema)

```powershell
python run_pipeline.py --steps transform
```
**See:** `dim_date 3,918 · dim_location 9,182 · dim_property 20 · fact_sale 10,666,409` and
`data/analytics/property.duckdb`.

**Read:** `src/transform.py`
* `dim_date` has an `is_sdlt_holiday` flag (stamp-duty holiday, Jul 2020–Sep 2021) – a domain
  attribute in the date dimension, classic Kimball.
* `dim_location` is the geography hierarchy postcode district → town → district → county with
  `is_liverpool` / `is_london` flags.
* `fact_sale` carries price, keys, and the three analytical filters as booleans.
* `VIEWS_SQL` – six views = the business logic (dbt-marts equivalent). Read
  `vw_liverpool_districts`: a CTE computes per-district yearly medians, then pivots first/last
  year and growth %.

**Try:** `python -c "from src.transform import query; print(query('SELECT * FROM vw_city_comparison WHERE year IN (2016, 2026)'))"`

---

## Step 5 – Quality

```powershell
python run_pipeline.py --steps quality
```
**See:** 27 checks, 24 pass. The 3 raw failures are deliberate – missing postcodes, non-residential
`O` property types, implausible prices – they show what the silver layer fixes.

**Read:** `src/quality.py`. Each check is a tiny SQL query run by DuckDB **where the data is**
(CSV, Parquet or table) – nothing is pulled into Python. This is how you test 10 million rows in
12 seconds. `gold_suite` includes `row_count_matches_source` (no rows lost between silver and
gold) and referential integrity on all three foreign keys.

**Try breaking it:** in `transform.py` change the `dim_property` join to drop `is_new_build`
and re-run `--steps transform quality` – the pipeline fails on referential integrity. Undo.

---

## Step 6 – HPC benchmark (the heart of the project)

```powershell
python run_pipeline.py --steps benchmark
```
Takes 5–10 minutes. **See:** one log line per configuration, e.g.
`duckdb-parquet workers=8 0.4 s`, and `outputs/benchmark_results.csv`.

**Read:** `src/benchmark.py`
* The workload is identical for every engine: *count sales and average price by year × property
  type over all 1.86 GB*. Every engine's answer is checked against the pandas baseline
  (`matches_baseline`).
* `PeakRSS` samples memory on a background thread.
* `timed()` takes best-of-N – standard HPC practice.
* `amdahl_parallel_fraction()` solves Amdahl's law for *p* from the measured speed-up – this is
  the HPC theory from the degree applied to real numbers.

**Talk about it like this:** *"On a cold cache pandas parsed 1.86 GB on one core in 21 s using
1.7 GB RAM; warm, 7.6 s. Chunking cuts memory 2.5× but not time. Multiprocessing/Dask scale to about
1.5–1.7× and flatten – process start-up and the per-file parse are the serial part (Amdahl p ≈ 0.45).
DuckDB is vectorised and multi-threaded so it wins on CSV (2.5 s); on Parquet it reads only 4 of 16
columns and finishes in 0.13 s – 59× faster than pandas. Spark is slowest on one laptop (18.5 s)
because ~5 s of JVM start-up and scheduling overhead only amortise on multi-machine data."*

---

## Step 7 – Visualise + report

```powershell
python run_pipeline.py --steps visualize report
```
**See:** 9 PNGs in `outputs/figures/`, CSV tables in `outputs/tables/`, `outputs/report.md`
(Ctrl+Shift+V to preview).

Look especially at `03_monthly_market.png` – the stamp-duty-holiday spike and the June 2021 /
Sept 2021 cliff edges are visible in the data – and `02_liverpool_districts.png`.

---

## Step 8 – Full run and tests

```powershell
python run_pipeline.py      # all steps; ~12 min incl. benchmark, ~2 min without
pytest -v                   # 24 tests
```

---

## Step 9 – GitHub

```powershell
git init
git add .
git commit -m "UK property market lakehouse and HPC benchmark"
git branch -M main
git remote add origin https://github.com/<your-user>/uk-property-lakehouse.git
git push -u origin main
```
`.gitignore` excludes `data/`, `tools/` and `.venv/` (≈3 GB). **Commit `outputs/`** so visitors
see the charts and report immediately. The Actions tab will run the tests and a one-year
pipeline smoke test.

---

## Step 10 – Extend it (pick one)

| Idea | Shows |
|---|---|
| Add Delta Lake (`deltalake` package) for the silver layer with time travel | lakehouse ACID |
| Join ONS postcode → latitude/longitude and draw a Liverpool price heat-map | geospatial |
| Monthly incremental merge from `pp-monthly-update-new-version.csv` (handles C/D records) | CDC / upserts |
| dbt-duckdb project replacing `VIEWS_SQL` | dbt |
| Airflow DAG wrapping the seven steps | orchestration |
| Repeat the benchmark on a cloud VM with 32 cores and extend the scaling curve | HPC |

---

## One-paragraph pitch

> "I built a lakehouse over every property sale in England and Wales since 2016 – 10.7 million
> transactions, 1.9 GB of government open data. Raw files land incrementally with a checksum
> manifest, DuckDB cleans them into partitioned Parquet in 35 seconds, and a star schema with
> analytical views serves Liverpool-versus-UK market analysis. A SQL-pushdown quality framework
> gates each layer. Because my master's was in Big Data and HPC, I benchmarked the same workload
> on pandas, multiprocessing, Dask, DuckDB and Spark across 1–8 cores and fitted Amdahl's law to
> the results – which is how I justify engine and file-format choices with evidence rather than
> opinion."

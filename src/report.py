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

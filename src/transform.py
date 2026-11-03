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

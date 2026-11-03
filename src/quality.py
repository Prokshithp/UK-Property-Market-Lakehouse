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

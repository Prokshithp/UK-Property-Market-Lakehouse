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

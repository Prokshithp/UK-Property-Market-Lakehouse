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

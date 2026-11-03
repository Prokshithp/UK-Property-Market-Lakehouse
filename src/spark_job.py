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

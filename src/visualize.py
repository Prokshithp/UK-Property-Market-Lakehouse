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

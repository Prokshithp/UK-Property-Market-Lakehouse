"""
Pipeline orchestrator.

    python run_pipeline.py                       # everything
    python run_pipeline.py --steps clean transform quality
    python run_pipeline.py --force               # re-download / re-clean every year
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from src import benchmark, clean, ingest, quality, report, transform, visualize  # noqa: E402

STEPS = {
    "ingest": lambda force: ingest.run(force=force),
    "clean": lambda force: clean.run(force=force),
    "transform": lambda force: transform.run(),
    "quality": lambda force: quality.run(),
    "benchmark": lambda force: benchmark.run(),
    "visualize": lambda force: visualize.run(),
    "report": lambda force: report.run(),
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="UK Property Market Lakehouse pipeline")
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=list(STEPS))
    parser.add_argument("--force", action="store_true", help="ignore manifests; redo ingest/clean for all years")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s", datefmt="%H:%M:%S")
    log = logging.getLogger("pipeline")

    t_all = time.perf_counter()
    for name, fn in STEPS.items():
        if name not in args.steps:
            continue
        log.info("=" * 72)
        log.info("STEP: %s", name.upper())
        t0 = time.perf_counter()
        try:
            fn(args.force)
        except Exception:
            log.exception("Step '%s' failed", name)
            return 1
        log.info("STEP %s done in %.1fs", name.upper(), time.perf_counter() - t0)
    log.info("=" * 72)
    log.info("Pipeline finished in %.1fs", time.perf_counter() - t_all)
    return 0


if __name__ == "__main__":          # required on Windows for multiprocessing in the benchmark
    raise SystemExit(main())

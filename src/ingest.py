"""
Step 1 - INGEST (bronze layer)

Downloads one HM Land Registry "Price Paid" CSV per year into data/raw/.
The download is **incremental**: a manifest records every file already landed with its size
and SHA-256, so re-running only fetches missing years (or a changed current-year file).

Files have no header row; column order is defined in config.RAW_COLUMNS.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from config import INGEST_MANIFEST, RAW_DIR, YEARS, raw_csv_path, raw_csv_url

log = logging.getLogger(__name__)
CHUNK = 8 * 1024 * 1024  # 8 MB streaming chunks


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def load_manifest() -> dict:
    if INGEST_MANIFEST.exists():
        return json.loads(INGEST_MANIFEST.read_text(encoding="utf-8"))
    return {"files": {}}


def save_manifest(manifest: dict) -> None:
    INGEST_MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def remote_size(url: str) -> int | None:
    try:
        r = requests.head(url, allow_redirects=True, timeout=60)
        return int(r.headers.get("Content-Length", 0)) or None
    except requests.RequestException:
        return None


def download_year(year: int, force: bool = False) -> Path:
    """Stream one yearly CSV to disk (writes to .part, renames on success)."""
    target = raw_csv_path(year)
    url = raw_csv_url(year)
    manifest = load_manifest()
    entry = manifest["files"].get(str(year))

    if target.exists() and entry and not force:
        # The current year's file grows every month - re-download if the remote size changed.
        size = remote_size(url)
        if size is None or size == entry.get("bytes"):
            log.info("pp-%s.csv already present (%.0f MB) - skipping", year, target.stat().st_size / 1e6)
            return target
        log.info("pp-%s.csv changed upstream (%s -> %s bytes) - refreshing", year, entry.get("bytes"), size)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix(".csv.part")
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with part.open("wb") as fh:
            for block in r.iter_content(CHUNK):
                fh.write(block)
    part.replace(target)

    manifest["files"][str(year)] = {
        "bytes": target.stat().st_size,
        "sha256": _sha256(target),
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": url,
    }
    save_manifest(manifest)
    log.info("Saved pp-%s.csv (%.0f MB)", year, target.stat().st_size / 1e6)
    return target


def register_existing_files() -> None:
    """If CSVs were placed manually (or by another downloader), add them to the manifest."""
    manifest = load_manifest()
    changed = False
    for year in YEARS:
        p = raw_csv_path(year)
        if p.exists() and str(year) not in manifest["files"]:
            manifest["files"][str(year)] = {
                "bytes": p.stat().st_size, "sha256": _sha256(p),
                "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "source": "manual",
            }
            changed = True
    if changed:
        save_manifest(manifest)


def available_years() -> list[int]:
    return [y for y in YEARS if raw_csv_path(y).exists()]


def run(force: bool = False) -> list[Path]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    register_existing_files()
    paths = [download_year(y, force=force) for y in YEARS]
    total = sum(p.stat().st_size for p in paths) / 1e9
    log.info("Bronze layer: %s files, %.2f GB", len(paths), total)
    return paths

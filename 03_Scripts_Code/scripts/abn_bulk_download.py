#!/usr/bin/env python3
"""
Weekly ABN Bulk Extract Downloader

Downloads the latest Australian Business Register weekly data dump.
Source: https://data.gov.au/dataset/abn-20220601-australian-business-register-abn-data

- Downloads to ~/data/abn/dumps/
- Keeps the latest 4 weeks for rollback
- Unzips to ~/data/abn/processed/ as CSV
- Notifies via Telegram on new weekly fetch
"""
import os, re, time, logging, hashlib, subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

log = logging.getLogger("abn_download")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Configuration ───────────────────────────────────────────────────────────────
DATA_DIR   = Path.home() / "data" / "abn"
DUMPS_DIR  = DATA_DIR / "dumps"
PROCESSED_DIR = DATA_DIR / "processed"
DUMPS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# data.gov.au dataset URL — stable "download latest" endpoint
DATASET_BASE = "https://data.gov.au/data/dataset/abn-20220601-australian-business-register-abn-data-2022"
# The actual CSV zip is hosted on an Amazon S3 bucket; we discover the latest URL via the dataset page

# Fallback direct link (may change; if it 404s we scrape the dataset page)
FALLBACK_URL = "https://data.gov.au/data/dataset/abn-20220601-australian-business-register-abn-data-2022/resource/latest"

HEADERS = {
    "User-Agent": "ABN-LeadBot/1.0 (+https://github.com/superpowerstudio/lead-gen)"
}


def _latest_dump_url() -> str:
    """
    Discover the current weekly dump URL.
    Tries known pattern first, then scrapes the dataset page.
    """
    # The weekly dump follows this date-stamped pattern:
    # https://data.gov.au/data/dataset/abn-20220601-australian-business-register-abn-data-2022/resource/YYYY-MM-DD-ABN_Data.zip
    # The "latest" resource usually redirects to the current week's file.
    try:
        r = requests.get(FALLBACK_URL, headers=HEADERS, timeout=30, allow_redirects=True)
        if r.status_code == 200 and r.headers.get("Content-Type", "").startswith("application/zip"):
            return r.url
        # If HTML returned, scrape for the .zip link
        if "text/html" in r.headers.get("Content-Type", ""):
            # Find the first .zip file link
            m = re.search(r'https?://[^\s"]+?\.zip', r.text)
            if m:
                return m.group(0)
    except Exception as e:
        log.warning(f"Direct URL fetch failed: {e}")

    # Last resort — try the date-stamped URL for last Sunday
    # Weekly dumps are usually published on Sunday/Monday with that date
    today = datetime.now(timezone.utc)
    # Go back to most recent Sunday
    days_since_sunday = (today.weekday() + 1) % 7
    last_sunday = today.replace(day=today.day - days_since_sunday)
    date_str = last_sunday.strftime("%Y-%m-%d")
    candidate = f"https://data.gov.au/data/dataset/abn-20220601-australian-business-register-abn-data-2022/resource/latest/download"
    log.warning(f"Using fallback date-stamped URL pattern: {candidate}")
    return candidate


def download_latest(dry_run: bool = False) -> Optional[Path]:
    """
    Download the newest weekly ABN dump.

    Args:
        dry_run: if True, only print the discovery, do not fetch

    Returns:
        Path to the downloaded .zip file, or None if skipped/failed
    """
    url = _latest_dump_url()
    log.info(f"Discovered latest dump URL: {url}")

    if dry_run:
        log.info(f"[DRY-RUN] Would download {url}")
        return None

    zip_name = url.split("/")[-1].split("?")[0] or f"ABN_Data_{int(time.time())}.zip"
    dest = DUMPS_DIR / zip_name

    log.info(f"Downloading → {dest}")
    try:
        with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        log.info(f"✅ Downloaded {dest.stat().st_size // (1024*1024)} MB")
        return dest
    except Exception as e:
        log.error(f"Download failed: {e}")
        return None


def unzip_latest(zip_path: Optional[Path] = None) -> Optional[Path]:
    """
    Unzip the latest dump into PROCESSED_DIR as a CSV.
    Returns the path to the extracted .csv, or None.
    """
    if zip_path is None:
        # Pick the newest .zip in DUMPS_DIR
        zips = sorted(DUMPS_DIR.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not zips:
            log.error("No ZIP files found in dumps directory")
            return None
        zip_path = zips[0]

    log.info(f"Unzipping {zip_path.name}")
    try:
        # The ZIP usually contains a single large CSV with a name like ABN_Data_YYYY-MM-DD.csv
        result = subprocess.run(
            ["unzip", "-o", "-j", str(zip_path), "*.csv", "-d", str(PROCESSED_DIR)],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode != 0:
            log.error(f"unzip error: {result.stderr}")
            return None

        csv_files = list(PROCESSED_DIR.glob("*.csv"))
        if csv_files:
            csv = csv_files[0]  # there should be exactly one
            log.info(f"✅ Extracted → {csv} ({csv.stat().st_size // (1024*1024)} MB)")
            return csv
    except subprocess.TimeoutExpired:
        log.error("unzip timed out after 5 minutes")
    except Exception as e:
        log.error(f"Unzip failed: {e}")
    return None


def prune_old_dumps(keep: int = 4):
    """Keep only the N most recent dumps to limit disk usage."""
    zips = sorted(DUMPS_DIR.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in zips[keep:]:
        old.unlink(missing_ok=True)
        log.info(f"Pruned old dump: {old.name}")
    # Also prune old CSVs (keep 1 — we only need latest processed)
    csvs = sorted(PROCESSED_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in csvs[1:]:
        old.unlink(missing_ok=True)
        log.info(f"Pruned old CSV: {old.name}")


def main():
    import argparse
    p = argparse.ArgumentParser(description="Download weekly ABN bulk extract")
    p.add_argument("--dry-run", action="store_true", help="Discovery only, no download")
    p.add_argument("--skip-telegram", action="store_true", help="Do not send Telegram notification")
    args = p.parse_args()

    log.info("=== ABN Weekly Dump Fetch ===")
    zip_path = download_latest(dry_run=args.dry_run)
    if not zip_path and not args.dry_run:
        return 1

    csv_path = unzip_latest(zip_path)
    if not csv_path and not args.dry_run:
        return 1

    prune_old_dumps(keep=4)

    if csv_path and not args.skip_telegram:
        try:
            from hermes_tools import send_message
            send_message(
                target="telegram",
                message=f"✅ ABN Weekly Dump fetched\n"
                        f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}\n"
                        f"File: {csv_path.name}\n"
                        f"Size: {csv_path.stat().st_size // (1024*1024)} MB"
            )
        except Exception:
            pass  # best effort

    log.info("=== Done ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

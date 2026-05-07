#!/usr/bin/env python3
"""
Weekly ABN-Lead Pipeline Orchestrator

End-to-end weekly update:
1. Download latest ABN bulk dump
2. Extract trade leads per target city
3. Enrich with phone/email via secondary sources (Yellow Pages / Google fallback)
4. Upsert to Supabase with ingestion_log audit trail
5. Send Telegram summary + new ABN count

Designed for cron: run every Monday 02:00 UTC
"""
import os, json, argparse, logging, sys, time, subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("weekly_pipeline")

# ── Target markets ───────────────────────────────────────────────────────────────
TARGET_CITIES = [
    {"city": "Sydney",   "state": "NSW",  "suburbs": ["sydney", "parramatta", "newcastle", "wollongong"]},
    {"city": "Melbourne","state": "VIC",  "suburbs": ["melbourne", "geelong", "ballarat"]},
    {"city": "Brisbane", "state": "QLD",  "suburbs": ["brisbane", "gold coast", "sunshine coast"]},
    {"city": "Perth",    "state": "WA",   "suburbs": ["perth", "mandurah"]},
    {"city": "Adelaide", "state": "SA",   "suburbs": ["adelaide"]},
    # Expand as needed
]

# Derived commands
DOWNLOADER = [sys.executable, str(PROJECT_ROOT / "scripts" / "abn_bulk_download.py"), ""]
EXTRACTOR_BASE = [sys.executable, str(PROJECT_ROOT / "scripts" / "abn_lead_extractor.py")]
ENRICHER = [sys.executable, str(PROJECT_ROOT / "abn_enrichment.py")]  # existing phone/email enricher (if available)
IMPORT_SCRIPT = [sys.executable, str(PROJECT_ROOT / "scripts" / "import_leads.py")]  # will create


def run(cmd: List[str], timeout: int = 600) -> subprocess.CompletedProcess:
    """Run a subprocess, log output, return result."""
    log.info(f"▶ {' '.join(cmd)}")
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=str(PROJECT_ROOT))
    duration = time.time() - start
    log.info(f"⏱  {cmd[0]} exited {result.returncode} in {duration:.0f}s")
    if result.stdout:
        for line in result.stdout.splitlines()[-10:]:
            log.debug(f"  STDOUT: {line}")
    if result.stderr:
        for line in result.stderr.splitlines()[-5:]:
            log.warning(f"  STDERR: {line}")
    return result


def weekly_pipeline(dry_run: bool = False, send_telegram: bool = True) -> Dict:
    """
    Execute the full weekly ABN lead generation pipeline.

    Returns a summary dict for reporting.
    """
    summary = {
        "run_date": datetime.now(timezone.utc).isoformat(),
        "steps": {},
        "total_new_leads": 0,
        "errors": [],
    }

    # ── Step 1: Download weekly dump ────────────────────────────────────────────
    log.info("═══ STEP 1: Download weekly ABN dump ═══")
    r = run(DOWNLOADER + (["--dry-run"] if dry_run else []), timeout=300)
    summary["steps"]["download"] = {"rc": r.returncode, "stdout": r.stdout[-500:]}
    if r.returncode != 0 and not dry_run:
        summary["errors"].append("ABN download failed")
        return summary

    # ── Step 2: Extract per city ────────────────────────────────────────────────
    all_leads = []
    extract_stats = {}
    for target in TARGET_CITIES:
        city, state = target["city"], target["state"]
        log.info(f"═══ STEP 2.{state}: Extracting {city} ═══")
        out_json = PROJECT_ROOT / "raw_leads" / f"abn_{city.lower()}_{datetime.now(timezone.utc):%Y%m%d}.json"
        cmd = EXTRACTOR_BASE + [
            "--city", city,
            "--state", state,
            "--output", str(out_json),
            "--limit", "500",
        ]
        r = run(cmd, timeout=120)
        extract_stats[city] = {"rc": r.returncode, "file": str(out_json)}
        if r.returncode == 0 and out_json.exists():
            batch = json.loads(out_json.read_text())
            all_leads.extend(batch)
            extract_stats[city]["count"] = len(batch)
            log.info(f"→ {len(batch):,} raw leads from {city}")
        else:
            summary["errors"].append(f"Extract failed for {city}")

    summary["steps"]["extract"] = extract_stats
    summary["total_new_leads"] = len(all_leads)

    if not all_leads:
        log.warning("No leads extracted from any city — pipeline ends here")
        return summary

    # ── Step 3: Enrich (phone + email) ─────────────────────────────────────────
    # Optional: if abn_enrichment.py works for these names, run it.
    # For now we skip because ABN name→contact requires separate Yellow Pages lookup
    # TODO: integrate enrichment here
    log.info("═══ STEP 3: Enrichment (skipped — coming soon) ═══")

    # ── Step 4: Save consolidated JSON ─────────────────────────────────────────
    consolidated_path = PROJECT_ROOT / "data" / f"weekly_leads_{datetime.now(timezone.utc):%Y%m%d}.json"
    consolidated_path.parent.mkdir(parents=True, exist_ok=True)
    consolidated_path.write_text(json.dumps(all_leads, indent=2))
    log.info(f"✅ Consolidated {len(all_leads):,} leads → {consolidated_path}")

    # ── Step 5: Upsert to Supabase ──────────────────────────────────────────────
    log.info("═══ STEP 4: Database import ═══")
    if not dry_run:
        # Assumes a simple import script exists
        r = run(IMPORT_SCRIPT + [str(consolidated_path)], timeout=300)
        summary["steps"]["import"] = {"rc": r.returncode}
        if r.returncode != 0:
            summary["errors"].append("Database import failed")
    else:
        log.info("[DRY-RUN] Skipping database import")
        summary["steps"]["import"] = {"rc": 0, "note": "skipped dry-run"}

    # ── Step 6: Telegram notification ───────────────────────────────────────────
    if send_telegram and not dry_run:
        try:
            from hermes_tools import send_message
            msg = (
                f"📥 Weekly ABN Lead Update\n"
                f"Date: {datetime.now(timezone.utc):%Y-%m-%d}\n"
                f"Total new leads: {len(all_leads):,}\n"
                f"Per city:\n"
            )
            for city, stats in extract_stats.items():
                msg += f"  • {city}: {stats.get('count','?')}\n"
            send_message(target="telegram", message=msg)
        except Exception as e:
            log.warning(f"Telegram send failed: {e}")

    log.info(f"✅ Pipeline complete — {len(all_leads):,} leads")
    return summary


def main():
    p = argparse.ArgumentParser(description="Weekly ABN lead pipeline orchestrator")
    p.add_argument("--dry-run", action="store_true", help="Download only, no DB import")
    p.add_argument("--no-telegram", action="store_true", help="Skip Telegram notification")
    args = p.parse_args()

    summary = weekly_pipeline(dry_run=args.dry_run, send_telegram=not args.no_telegram)
    print(json.dumps(summary, indent=2))
    return 0 if not summary["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

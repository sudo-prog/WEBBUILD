#!/usr/bin/env python3
"""
ABN Bulk Extract → Lead Extractor (pure stdlib, no pandas)

Filters the weekly ABN dump for high-intent B2B leads:
- Industry: trades (plumbing, electrical, building, etc.)
- Location: Australian capital cities / priority suburbs
- Has no website (no URL field, or domain field empty)
- Active status (already guaranteed by weekly dump)

Exports JSON in the qualified_leads schema used by the rest of the pipeline.
"""
import os, re, json, csv, argparse, logging, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR     = PROJECT_ROOT / "data" / "abn"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR   = PROJECT_ROOT / "raw_leads"

log = logging.getLogger("abn_extractor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ── Trade industry keywords → precise categories ────────────────────────────────
CATEGORY_MAP = {
    "plumber":        ["plumber", "plumbing", "drain", "blocked drain", "leak detection", "hot water"],
    "electrician":    ["electrician", "electrical", "sparky", "wiring", "switchboard", "lighting"],
    "builder":        ["builder", "carpenter", "construction", "renovation", "extensions", "home builder"],
    "painter":        ["painter", "painting", "decorator", "wallpaper", "stripping"],
    "roofer":         ["roofer", "roofing", "tiling", "guttering", "downpipes", "metal roof"],
    "air conditioning": ["air conditioning", "hvac", "ducted", "split system", "cooling", "evaporative"],
    "kitchen":        ["kitchen", "bathroom", "joinery", "cabinet", "benchtop", "outdoor kitchen"],
    "flooring":       ["flooring", "tiles", "laminate", "carpet", "polished concrete", "timber floor"],
    "solar":          ["solar", "solar panel", "photovoltaic", "pv", "solar power", "battery"],
    "pest control":   ["pest control", "termite", "exterminator", "rodent", "fumigation"],
    "gardener":       ["gardener", "landscaper", "tree", "lawn mowing", "hedge trimming", "garden design"],
    "mechanic":       ["mechanic", "auto repair", "vehicle", "car service", "mechanical", "tyre"],
}

KEYWORD_TO_CATEGORY = {}
for cat, kws in CATEGORY_MAP.items():
    for kw in kws:
        KEYWORD_TO_CATEGORY[kw] = cat

# ── Helpers ──────────────────────────────────────────────────────────────────────
PLACEHOLDER_WEB = re.compile(r"^(?:https?://)?(?:www\.)?(?:n/?a|none|null|not available)$", re.I)

def _has_website(record: Dict) -> bool:
    url = record.get("WebsiteAddress", "").strip()
    return bool(url) and not PLACEHOLDER_WEB.match(url)

def _detect_category(name: str, trading: str = "") -> str:
    text = f"{name} {trading}".lower()
    for kw, cat in KEYWORD_TO_CATEGORY.items():
        if kw in text:
            return cat
    return "trade"


def extract_leads(
    csv_path: Path,
    output_city: str,
    state_code: str,
    max_results: int = 1000,
) -> List[Dict]:
    """
    Stream the ABN CSV and extract tradespeople with no website.

    Args:
        csv_path: Path to the latest ABN_Data_*.csv
        output_city: Target city name (output schema)
        state_code: Two-letter state code (e.g. "NSW")
        max_results: cap returned leads

    Returns:
        List of lead dicts ready for JSON export
    """
    log.info(f"Streaming {csv_path} …")
    leads = []
    matched_state = 0
    matched_no_web = 0
    matched_trade = 0

    with open(csv_path, newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        # Normalise fieldnames to handle variations
        fieldnames = [n.lower().strip() for n in reader.fieldnames or []]
        col_map = {name: orig for name, orig in zip(fieldnames, reader.fieldnames or [])}
        # Ensure required columns exist
        required = {"abn", "entityname", "state"}
        missing = required - set(fieldnames)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        for row in reader:
            # ── State filter ──────────────────────────────────────────────────────────
            state_val = row.get(col_map.get("state","State"), "").strip().upper()
            if state_val != state_code.upper():
                continue
            matched_state += 1

            # ── Website filter ───────────────────────────────────────────────────────
            website = row.get(col_map.get("websiteaddress","WebsiteAddress"), "").strip()
            if website and not PLACEHOLDER_WEB.match(website):
                continue
            matched_no_web += 1

            # ── Entity type filter (exclude individuals without business structure) ───
            entity_type = row.get(col_map.get("entitytype","EntityType"), "").strip()
            skip_entity = {"individual", "individual - sole trader"}.__contains__(entity_type.lower())
            if skip_entity:
                continue

            # ── Trade detection ──────────────────────────────────────────────────────
            entity_name = row.get(col_map.get("entityname","EntityName"), "").strip()
            trading     = row.get(col_map.get("tradingnames","TradingNames"), "").strip()
            if not _detect_category(entity_name, trading):
                continue
            matched_trade += 1

            # ── Build lead ────────────────────────────────────────────────────────────
            category     = _detect_category(entity_name, trading)
            postcode     = row.get(col_map.get("postcode","Postcode"), "").strip() or None
            address      = row.get(col_map.get("address","Address"), row.get(col_map.get("addressline1",""), "")).strip() or None

            lead = {
                "business_name":  entity_name,
                "category":       category,
                "phone":          None,
                "email":          None,
                "website":        None,
                "city":           output_city.title(),
                "state":          state_code.upper(),
                "suburb":         None,
                "postcode":       postcode,
                "address_full":   address,
                "source":         f"abn_bulk_{datetime.now(timezone.utc):%Y-%m-%d}",
                "abn":            row.get(col_map.get("abn","ABN"), "").strip(),
                "abn_status":     "active",
                "lead_score":     None,
                "needs_review":   False,
                "enriched_at":    datetime.now(timezone.utc).isoformat(),
            }
            leads.append(lead)

            if len(leads) >= max_results:
                break

    log.info(f"Filter stats — state: {matched_state:,} | no-website: {matched_no_web:,} | trade: {matched_trade:,}")
    log.info(f"Prepared {len(leads):,} lead records (capped at {max_results:,})")
    return leads


def main():
    p = argparse.ArgumentParser(description="Extract trades leads from weekly ABN bulk dump (stdlib CSV)")
    p.add_argument("--city", required=True, help="Target city name (output)")
    p.add_argument("--state", required=True, help="State code (NSW, VIC, QLD, …)")
    p.add_argument("--csv", type=Path, help="Path to ABN_Data_*.csv (defaults to latest in processed/)")
    p.add_argument("--output", type=Path, default=None, help="Output JSON file")
    p.add_argument("--limit", type=int, default=500, help="Max leads to emit (default 500)")
    args = p.parse_args()

    csv_path = args.csv
    if not csv_path:
        candidates = sorted(PROCESSED_DIR.glob("ABN_Data_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            log.error("No ABN CSV found — run scripts/abn_bulk_download.py first")
            return 1
        csv_path = candidates[0]

    if not csv_path.exists():
        log.error(f"CSV not found: {csv_path}")
        return 1

    leads = extract_leads(csv_path, args.city, args.state.upper(), max_results=args.limit)

    if not leads:
        log.warning("No leads extracted — try broadening keywords")
        return 1

    output_path = args.output or OUTPUT_DIR / f"abn_{args.city.lower()}_{datetime.now(timezone.utc):%Y%m%d}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(leads, indent=2))
    log.info(f"✅ Wrote {len(leads)} leads → {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

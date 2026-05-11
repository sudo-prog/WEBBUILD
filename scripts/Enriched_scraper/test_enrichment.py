#!/usr/bin/env python3
"""
test_enrichment.py — Quick sanity-check for each free enrichment source.

Run this first to confirm the sources are reachable before kicking off a
batch of thousands of leads.

Usage:
  python test_enrichment.py
  python test_enrichment.py --business "Sydney Plumbing Services" --city Sydney --state NSW
"""
import sys, argparse, json
from pathlib import Path

# Import from sibling script
sys.path.insert(0, str(Path(__file__).parent))
from enrich_contacts_free import (
    lookup_abn_api,
    enrich_via_duckduckgo,
    enrich_via_whitepages,
    enrich_via_truelocal,
    build_dork_query,
)

# ── Hardcoded test cases (real known businesses) ──────────────────────────────
TEST_CASES = [
    {
        "business_name": "Bluey's Plumbing Services",
        "trading_name":  "Bluey's Plumbing",
        "city":          "Sydney",
        "state":         "NSW",
        "abn":           "51824753556",   # Mastercraft Roofing — just to test API shape
        "category":      "plumber",
    },
    {
        "business_name": "Fitzgerald Electrical",
        "trading_name":  "Fitzgerald Electrical",
        "city":          "Melbourne",
        "state":         "VIC",
        "abn":           None,
        "category":      "electrician",
    },
]


def section(title: str):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print(f"{'─'*55}")


def test_source(label: str, fn, *args) -> dict:
    print(f"\n  [{label}]", end=" ", flush=True)
    try:
        result = fn(*args)
        phone = result.get("phone", "—")
        email = result.get("email", "—")
        print(f"phone={phone}  email={email}")
        if result:
            for k, v in result.items():
                print(f"    {k}: {v}")
        return result
    except Exception as e:
        print(f"ERROR: {e}")
        return {}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--business", default=None, help="Custom business name to test")
    p.add_argument("--city",     default="Sydney")
    p.add_argument("--state",    default="NSW")
    p.add_argument("--abn",      default=None)
    args = p.parse_args()

    cases = TEST_CASES
    if args.business:
        cases = [{
            "business_name": args.business,
            "trading_name":  args.business,
            "city":          args.city,
            "state":         args.state,
            "abn":           args.abn,
            "category":      "trade",
        }]

    for case in cases:
        name = case["trading_name"] or case["business_name"]
        section(f'{name} ({case["city"]}, {case["state"]})')

        # 1. Show dork query
        query = build_dork_query(name, case["city"], case["state"])
        print(f"\n  DDG query that will be used:")
        print(f"  {query}\n")

        # 2. ABN API
        if case.get("abn"):
            test_source("ABN Lookup API", lookup_abn_api, case["abn"])
        else:
            print("  [ABN Lookup API] skipped — no ABN provided")

        # 3. DuckDuckGo dork
        test_source("DuckDuckGo dork", enrich_via_duckduckgo, name, case["city"], case["state"])

        # 4. White Pages
        test_source("White Pages AU", enrich_via_whitepages, name, case["state"])

        # 5. True Local
        test_source("True Local AU", enrich_via_truelocal, name, case["city"])

    print(f"\n{'='*55}")
    print("Test complete. If all sources show data (or graceful 'none found'),")
    print("the enricher is working. Run batch with:")
    print()
    print("  python run_enrichment_batch.py --city sydney --limit 50 --dry-run")
    print("  python run_enrichment_batch.py --city sydney --limit 200")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()

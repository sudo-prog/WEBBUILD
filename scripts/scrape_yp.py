#!/usr/bin/env python3
"""
Stage 1: Yellow Pages directory scraper (email omitted — YP doesn't expose it publicly).
Collects: business_name, phone, address, category, city, state, website_present yes/no.
Output: raw_leads/yp_<city>_raw_<timestamp>.json
"""
import json, time, sys, asyncio, re, argparse
from pathlib import Path
from typing import List, Dict

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: pip install playwright && playwright install chromium")
    sys.exit(1)

CATEGORIES = ["plumber", "electrician", "builder", "painter", "carpenter",
              "roofer", "air conditioning", "kitchen", "flooring", "solar"]

# Selectors (updated 2026)
LISTING_SEL = 'div.v-card'
NAME_SEL     = 'h2.n a.business-name'
PHONE_SEL    = 'div.phones.phone.primary'
ADDR_STREET  = 'div.street-address'
ADDR_LOC     = 'div.locality'
# External website: any http link that's not YellowPages-owned
WEBSITE_SEL  = 'a[href^="http"]'

EXCLUDED_DOMAINS = ['yellowpages.com.au', 'yellow.com.au', 'thryv.com.au', 'directoryselect.com']


def extract_listings(page) -> List[Dict]:
    results = []
    cards = page.query_selector_all(LISTING_SEL)
    print(f"  Found {len(cards)} listing cards")
    for card in cards:
        try:
            name_el = card.query_selector(NAME_SEL)
            name = name_el.inner_text().strip() if name_el else ""
            if not name:
                continue

            phone_el = card.query_selector(PHONE_SEL)
            phone = phone_el.inner_text().strip() if phone_el else ""

            street_el = card.query_selector(ADDR_STREET)
            loc_el = card.query_selector(ADDR_LOC)
            address = ""
            if street_el and loc_el:
                address = f"{street_el.inner_text().strip()}, {loc_el.inner_text().strip()}"

            # Check for external website presence
            has_website = False
            link_els = card.query_selector_all(WEBSITE_SEL)
            for el in link_els:
                href = el.get_attribute('href') or ''
                href_low = href.lower()
                if any(excl in href_low for excl in EXCLUDED_DOMAINS):
                    continue
                if any(skip in href_low for skip in ['privacy','terms','login','account','my.yellow','facebook','twitter']):
                    continue
                # Non-YP external link found → they advertise a site
                has_website = True
                break

            results.append({
                "business_name": name,
                "phone": phone,
                "address": address,
                "has_website": has_website,
            })
        except Exception:
            continue
    return results


def scrape_city(city: str, state: str, max_pages: int = 3) -> List[Dict]:
    location = f"{city.title()} {state}"
    all_leads = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")

        for category in CATEGORIES:
            print(f"[{city}] {category}...", end="", flush=True)
            url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={location}&pageNumber=1"
            try:
                page.goto(url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(2000)
                cat_leads = extract_listings(page)
                # No website filter at this stage — we need to see raw count
                print(f" {len(cat_leads)} listings")
                for lead in cat_leads:
                    lead["category"] = category
                    lead["city"] = city
                    lead["state"] = state
                all_leads.extend(cat_leads)
            except Exception as e:
                print(f" ERROR: {e}")

            page.wait_for_timeout(1000)  # polite delay

        browser.close()
    return all_leads


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("city")
    parser.add_argument("state")
    parser.add_argument("--pages", type=int, default=1, help="Number of result pages to scrape (default 1)")
    args = parser.parse_args()

    raw = scrape_city(args.city, args.state, max_pages=args.pages)
    ts = int(time.time())
    out = Path(f"raw_leads/yp_{args.city.lower()}_{args.state.lower()}_raw_{ts}.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(raw, indent=2))
    print(f"\n✓ Wrote {len(raw)} raw listings → {out}")

#!/usr/bin/env python3
import json, time, sys, re, random, argparse
from pathlib import Path
from typing import List, Dict
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ─── Trade categories (subset available on Yellow Pages) ────────────────────────
CATEGORIES = [
    "plumber", "electrician", "builder", "painter", "carpenter",
    "roofer", "air conditioning", "kitchen", "flooring", "solar"
]

CITIES = [
    ("Sydney",    "NSW"),
    ("Melbourne", "VIC"),
    ("Brisbane",  "QLD"),
    ("Perth",     "WA"),
    ("Adelaide",  "SA"),
    ("Canberra",  "ACT"),
    ("Hobart",    "TAS"),
    ("Darwin",    "NT"),
]

OUTPUT_DIR = Path("/home/thinkpad/Projects/supabase_australia/raw_leads")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Selectors (as of 2026)
LISTING_SEL = 'div.v-card'
NAME_SEL     = 'h2.n a.business-name'
PHONE_SEL    = 'div.phones.phone.primary'
ADDR_STREET  = 'div.street-address'
ADDR_LOC     = 'div.locality'
WEBSITE_SEL  = 'a[href^="http"]'
EXCLUDED_DOMAINS = ['yellowpages.com.au', 'yellow.com.au', 'thryv.com.au', 'directoryselect.com']
SKIP_PATTERNS = ['privacy','terms','login','account','my.yellow','facebook','twitter']

def has_external_website(card) -> bool:
    """Return True if listing contains an external (non-YP) website link."""
    try:
        for el in card.query_selector_all(WEBSITE_SEL):
            href = (el.get_attribute('href') or '').lower()
            if any(excl in href for excl in EXCLUDED_DOMAINS):
                continue
            if any(skip in href for skip in SKIP_PATTERNS):
                continue
            return True
    except Exception:
        pass
    return False

def scrape_category(page, city, state, category, max_pages: int = 1) -> List[Dict]:
    location = f"{city.title()} {state}"
    results = []
    for page_num in range(1, max_pages + 1):
        url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={location}&pageNumber={page_num}"
        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(random.randint(1500, 2500))
            cards = page.query_selector_all(LISTING_SEL)
            if not cards:
                print(f"  [{city}|{category}] page {page_num}: no listings")
                break
            print(f"  [{city}|{category}] page {page_num}: {len(cards)} listings")
            for card in cards:
                try:
                    name_el = card.query_selector(NAME_SEL)
                    name = name_el.inner_text().strip() if name_el else ""
                    if not name:
                        continue
                    phone_el = card.query_selector(PHONE_SEL)
                    phone = phone_el.inner_text().strip() if phone_el else ""
                    street_el = card.query_selector(ADDR_STREET)
                    loc_el    = card.query_selector(ADDR_LOC)
                    address = ""
                    if street_el and loc_el:
                        address = f"{street_el.inner_text().strip()}, {loc_el.inner_text().strip()}"
                    has_web = has_external_website(card)
                    results.append({
                        "business_name": name,
                        "phone": phone,
                        "address": address,
                        "has_website": has_web,
                        "category": category,
                        "city": city,
                        "state": state,
                        "source": "yellowpages.com.au",
                    })
                except Exception:
                    continue
            # Brief pause between pages
            if page_num < max_pages:
                page.wait_for_timeout(random.randint(1000, 2000))
        except PlaywrightTimeoutError:
            print(f"  Timeout on {url}")
            break
        except Exception as e:
            print(f"  Error on {url}: {e}")
            break
    return results

def main():
    parser = argparse.ArgumentParser(description="Scrape Yellow Pages for trades (all cities)")
    parser.add_argument('--pages', type=int, default=1, help='Number of result pages per query (default 1)')
    parser.add_argument('--delay', type=float, default=2.0, help='Extra delay between city/category queries (seconds)')
    args = parser.parse_args()

    all_leads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
        try:
            for city, state in CITIES:
                print(f"\n=== {city}, {state} ===")
                for category in CATEGORIES:
                    leads = scrape_category(page, city, state, category, max_pages=args.pages)
                    all_leads.extend(leads)
                    # Polite extra delay
                    time.sleep(args.delay)
        finally:
            browser.close()

    # Deduplicate by business_name + city (simple key)
    seen = set()
    uniq = []
    for lead in all_leads:
        key = (lead['business_name'].lower(), lead['city'].lower())
        if key not in seen:
            seen.add(key)
            uniq.append(lead)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"yp_all_{ts}.json"
    out_path.write_text(json.dumps(uniq, indent=2))
    print(f"\n✅ Scraped {len(all_leads)} listings → {len(uniq)} unique → {out_path.name}")
    # Summary
    no_web = sum(1 for l in uniq if not l['has_website'])
    print(f"   No-website leads: {no_web}")
    print(f"   By city:")
    for city,_ in CITIES:
        cnt = sum(1 for l in uniq if l['city'] == city)
        no_web_ct = sum(1 for l in uniq if l['city'] == city and not l['has_website'])
        print(f"     {city}: {cnt} total, {no_web_ct} no-website")

if __name__ == '__main__':
    main()

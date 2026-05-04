#!/usr/bin/env python3
import json, time, sys, re, random, argparse
from pathlib import Path
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CITIES = ["Brisbane", "Sydney", "Gold Coast", "Melbourne", "Darwin", "Perth"]
STATES = {"Brisbane":"QLD", "Sydney":"NSW", "Gold Coast":"QLD", "Melbourne":"VIC", "Darwin":"NT", "Perth":"WA"}

CATEGORIES = [
    "plumber","gas fitter","plumbing","hot water","drainage","septic","roof plumber",
    "electrician","electrical contractor","solar electrician","auto electrician",
    "industrial electrician","residential electrician","commercial electrician",
    "builder","construction manager","site manager","project manager",
    "construction supervisor","site supervisor","foreman","building supervisor",
    "boilermaker","welder","metal fabricator","structural steel","welding contractor",
    "air conditioning","refrigeration","hvac","refrigeration mechanic",
    "heating and cooling","ducting","mechanical services",
    "concreter","concrete contractor","concreting","flooring","floor sander",
    "epoxy flooring","polished concrete","tiling",
    "carpenter","joiner","cabinet maker",
    "mechanic","tradesperson","trades assistant","handyman","maintenance trades"
]

OUT_DIR = Path("/home/thinkpad/Projects/supabase_australia/raw_leads/yellow_pages_batch")
OUT_DIR.mkdir(parents=True, exist_ok=True)

LISTING_SEL = 'div.v-card'
NAME_SEL     = 'h2.n a.business-name'
PHONE_SEL    = 'div.phones.phone.primary'
ADDR_STREET  = 'div.street-address'
ADDR_LOC     = 'div.locality'
WEBSITE_SEL  = 'a[href^="http"]'
EXCLUDED_DOMAINS = ['yellowpages.com.au','yellow.com.au','thryv.com.au','directoryselect.com']
SKIP_PATTERNS    = ['privacy','terms','login','account','my.yellow','facebook','twitter']

def has_external_website(card):
    try:
        for el in card.query_selector_all(WEBSITE_SEL):
            href = (el.get_attribute('href') or '').lower()
            if any(excl in href for excl in EXCLUDED_DOMAINS): continue
            if any(skip in href for skip in SKIP_PATTERNS): continue
            return True
    except Exception: pass
    return False

def scrape_category(page, city, state, category, max_pages: int = 2):
    location = f"{city} {state}"
    results = []
    for page_num in range(1, max_pages+1):
        url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={location}&pageNumber={page_num}"
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(random.randint(1200,2000))
            cards = page.query_selector_all(LISTING_SEL)
            if not cards: break
            for card in cards:
                try:
                    name_el = card.query_selector(NAME_SEL)
                    name = name_el.inner_text().strip() if name_el else ""
                    if not name: continue
                    phone_el = card.query_selector(PHONE_SEL)
                    phone = phone_el.inner_text().strip() if phone_el else ""
                    street_el = card.query_selector(ADDR_STREET)
                    loc_el    = card.query_selector(ADDR_LOC)
                    address = ""
                    suburb  = ""
                    if street_el and loc_el:
                        address = f"{street_el.inner_text().strip()}, {loc_el.inner_text().strip()}"
                        suburb  = loc_el.inner_text().strip()
                    listing_type = "featured" if card.query_selector(".featured-label") else "basic"
                    desc_el = card.query_selector(".listing-description")
                    description = desc_el.inner_text().strip() if desc_el else ""
                    has_web = has_external_website(card)
                    results.append({
                        "business_name":    name,
                        "phone":            phone,
                        "address":          address,
                        "suburb":           suburb,
                        "yp_listing_type":  listing_type,
                        "yp_featured":      listing_type == "featured",
                        "yp_description":   description,
                        "yp_url":           has_web,
                        "yp_last_updated":  datetime.now().isoformat(),
                        "category":         category,
                        "city":             city,
                        "state":            state,
                        "source":           "yellowpages.com.au"
                    })
                except Exception: continue
            time.sleep(random.uniform(1.0, 2.0))
        except PlaywrightTimeoutError:
            print(f"  Timeout {city}/{category} page {page_num}")
            break
        except Exception as e:
            print(f"  Error {city}/{category}: {e}")
            break
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pages', type=int, default=2, help="Pages per category (default 2)")
    args = parser.parse_args()

    all_leads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        )
        try:
            for city in CITIES:
                state = STATES[city]
                print(f"\n=== {city} ({state}) ===")
                for category in CATEGORIES:
                    leads = scrape_category(page, city, state, category, max_pages=args.pages)
                    all_leads.extend(leads)
                    print(f"  {category}: {len(leads)}")
        finally:
            browser.close()

    seen = set()
    uniq = []
    for lead in all_leads:
        key = (lead['business_name'].lower(), lead['city'].lower())
        if key not in seen:
            seen.add(key)
            uniq.append(lead)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUT_DIR / f"yp_batch_{ts}.jsonl"
    with open(out_path, 'w') as f:
        for rec in uniq: f.write(json.dumps(rec) + '\n')
    print(f"\n✅ {len(uniq)} unique YP listings → {out_path}")
    from collections import Counter
    print("City distribution:", Counter(r['city'] for r in uniq))

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
import json, time, sys, re, random, argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CATEGORIES = [
    "plumber", "electrician", "builder", "painter", "carpenter",
    "roofer", "air conditioning", "kitchen", "flooring", "solar"
]

OUTPUT_DIR = Path("/home/thinkpad/Projects/supabase_australia/raw_leads/yellow_pages")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LISTING_SEL = 'div.v-card'
NAME_SEL     = 'h2.n a.business-name'
PHONE_SEL    = 'div.phones.phone.primary'
ADDR_STREET  = 'div.street-address'
ADDR_LOC     = 'div.locality'
WEBSITE_SEL  = 'a[href^="http"]'
EXCLUDED_DOMAINS = ['yellowpages.com.au','yellow.com.au','thryv.com.au','directoryselect.com']
SKIP_PATTERNS = ['privacy','terms','login','account','my.yellow','facebook','twitter']

def has_external_website(card) -> bool:
    try:
        for el in card.query_selector_all(WEBSITE_SEL):
            href = (el.get_attribute('href') or '').lower()
            if any(excl in href for excl in EXCLUDED_DOMAINS): continue
            if any(skip in href for skip in SKIP_PATTERNS): continue
            return True
    except Exception:
        pass
    return False

def scrape_city(page, city, state, max_pages: int = 1) -> list:
    location = f"{city.title()} {state}"
    results = []
    for page_num in range(1, max_pages+1):
        url = f"https://www.yellowpages.com.au/search/listings?clue=&locationClue={location}&pageNumber={page_num}"  # general search for all categories? We'll filter by category below.
        # Actually we need per category to get trade relevance
        # We'll iterate categories outside
        pass
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', required=True)
    parser.add_argument('--state', required=True)
    parser.add_argument('--pages', type=int, default=1)
    parser.add_argument('--delay', type=float, default=2.0)
    args = parser.parse_args()

    all_leads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
        try:
            for category in CATEGORIES:
                url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={args.city.title()} {args.state}&pageNumber=1"
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    page.wait_for_timeout(random.randint(1500,2500))
                    cards = page.query_selector_all(LISTING_SEL)
                    print(f"  {category}: {len(cards)} listings")
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
                            if street_el and loc_el:
                                address = f"{street_el.inner_text().strip()}, {loc_el.inner_text().strip()}"
                            has_web = has_external_website(card)
                            all_leads.append({
                                "business_name": name,
                                "phone": phone,
                                "address": address,
                                "has_website": has_web,
                                "category": category,
                                "city": args.city,
                                "state": args.state,
                                "source": "yellowpages.com.au",
                            })
                        except Exception:
                            continue
                    time.sleep(args.delay)
                except PlaywrightTimeoutError:
                    print(f"  Timeout for {category}")
                    continue
                except Exception as e:
                    print(f"  Error in {category}: {e}")
                    continue
        finally:
            browser.close()

    # Deduplicate by name+city
    seen = set()
    uniq = []
    for lead in all_leads:
        key = (lead['business_name'].lower(), lead['city'].lower())
        if key not in seen:
            seen.add(key)
            uniq.append(lead)

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"yp_{args.city.lower()}_{args.state.lower()}_{ts}.json"
    out_path.write_text(json.dumps(uniq, indent=2))
    no_web = sum(1 for l in uniq if not l['has_website'])
    print(f"\n✅ {args.city}: {len(uniq)} unique ({no_web} no-website) → {out_path.name}")

if __name__ == '__main__':
    main()

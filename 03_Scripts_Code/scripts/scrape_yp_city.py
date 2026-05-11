#!/usr/bin/env python3
import json, time, sys, re, random, argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

CATEGORIES = [
    "plumber", "electrician", "builder", "painter", "carpenter",
    "roofer", "air conditioning", "kitchen", "flooring", "solar"
]

TRADE_KEYWORDS = [
    'plumb', 'elec', 'build', 'paint', 'carpet', 'roof', 'air', 'cond', 'kitchen', 'floor', 'solar',
    'construct', 'renov', 'repair', 'service', 'contractor', 'install', 'heat', 'cool', 'bath', 'tile'
]

def contains_trade_keyword(name):
    name_lower = name.lower()
    for kw in TRADE_KEYWORDS:
        if kw in name_lower:
            return True
    return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--city', required=True)
    parser.add_argument('--state', required=True)
    parser.add_argument('--pages', type=int, default=1)
    parser.add_argument('--delay', type=float, default=2.0)
    args = parser.parse_args()

    all_leads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, channel="chromium")
        page = browser.new_page(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
)
        try:
            for category in CATEGORIES:
                url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={args.city.title()} {args.state}&pageNumber=1"
                try:
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    page.wait_for_timeout(random.randint(1500,2500))
                    cards = page.query_selector_all('div.v-card')
                    print(f"  {category}: {len(cards)} listings")
                    for card in cards:
                        try:
                            name_el = card.query_selector('h2.n a.business-name')
                            name = name_el.inner_text().strip() if name_el else ""
                            if not name: continue
                            
                            # DEBUG: Print business name
                            print(f"    Found: {name}")
                            
                            if not contains_trade_keyword(name):
                                continue
                            
                            phone_el = card.query_selector('div.phones.phone.primary')
                            phone = phone_el.inner_text().strip() if phone_el else ""
                            street_el = card.query_selector('div.street-address')
                            loc_el    = card.query_selector('div.locality')
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
                except Exception:
                    print(f"  Error in {category}")
                    continue
        finally:
            browser.close()

    # Deduplicate
    seen = set()
    uniq = []
    for lead in all_leads:
        key = (lead['business_name'].lower(), lead['city'].lower())
        if key not in seen:
            seen.add(key)
            uniq.append(lead)
    
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = Path(f"raw_leads/yellow_pages/yp_{args.city.lower()}_{args.state.lower()}_{ts}.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(uniq, indent=2))
    no_web = sum(1 for l in uniq if not l['has_website'])
    print(f"\n✅ {args.city}: {len(uniq)} unique ({no_web} no-website) → {out_path.name}")

if __name__ == '__main__':
    main()

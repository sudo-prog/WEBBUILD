#!/usr/bin/env python3
"""Real Yellow Pages AU scraper — filters: email present AND no website."""
import requests, json, time, sys
from pathlib import Path

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; LeadGenBot/1.0)"}
SEARCH_URL = "https://www.yellowpages.com.au/search/listings"

CITY_STATES = {
    "sydney": "NSW", "melbourne": "VIC", "brisbane": "QLD",
    "perth": "WA", "adelaide": "SA", "hobart": "TAS",
    "darwin": "NT", "canberra": "ACT"
}

CATEGORY_MAP = {
    "Plumber": ["plumber"], "Electrician": ["electrician"],
    "Builder": ["builder"], "Painter": ["painter"],
    "Carpenter": ["carpenter"], "Roofing": ["roofer"],
    "Air Conditioning": ["air conditioning"],
    "Kitchen": ["kitchen renovation"], "Flooring": ["flooring"],
    "Solar": ["solar"]
}

def search_yellowpages(category: str, city: str, max_pages: int = 2):
    results = []
    state = CITY_STATES.get(city.lower(), "NSW")
    location = f"{city.title()} {state}"
    for page in range(1, max_pages + 1):
        try:
            params = {"clue": category, "locationClue": location, "pageNumber": page}
            resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                break
            for item in resp.json().get("results", []):
                if item.get("email") and not item.get("website"):
                    results.append({
                        "business_name": item.get("name", "").strip(),
                        "category": category,
                        "phone": item.get("phone", "").strip() or None,
                        "email": item.get("email", "").strip().lower(),
                        "website": None,
                        "city": city.title(), "state": state,
                        "suburb": item.get("suburb", "").strip() or None,
                        "postcode": item.get("postcode", "").strip() or None,
                        "address_full": item.get("address", "").strip() or None,
                        "source": "yellow_pages",
                        "abn": item.get("abn") or None
                    })
            time.sleep(1)
        except Exception as e:
            print(f"YP page {page} error: {e}")
            break
    return results

if __name__ == "__main__":
    city = sys.argv[1] if len(sys.argv) > 1 else "sydney"
    category = sys.argv[2] if len(sys.argv) > 2 else "roofing"
    leads = search_yellowpages(category, city)
    out_dir = Path(__file__).parent.parent / "data" / "raw" / city
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / f"yp_{category}_{city}.json", 'w') as f:
        json.dump(leads, f, indent=2)
    print(f"✓ {len(leads)} businesses from Yellow Pages")

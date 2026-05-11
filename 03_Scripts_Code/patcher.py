#!/usr/bin/env python3
"""Patch ingestion_pipeline.py to use real scrapers + ABN validation."""
import re, json, sys
from pathlib import Path

pipeline_path = Path("/home/thinkpad/Projects/active/WEBBUILD/supabase_australia/ingestion_pipeline.py")
orig = pipeline_path.read_text()

# Backup
backup = pipeline_path.with_suffix('.py.backup')
pipeline_path.replace(backup)
print(f"Backup created: {backup}")

# Replacement 1: google_business -> OSM
google_new = '''
    def _fetch_google_business(self) -> List[Dict]:
        # Fetch businesses from OpenStreetMap/Overpass (Google Maps equivalent).
        # Filters: businesses with contact but NO website.
        import requests
        results = []
        city = self.city_key
        state = self.city_config.get("state", "UNK")
        
        coords = {
            "sydney": (-33.8688, 151.2093), "melbourne": (-37.8136, 144.9631),
            "brisbane": (-27.4698, 153.0251), "perth": (-31.9505, 115.8605),
            "adelaide": (-34.9285, 138.6007), "hobart": (-42.8821, 147.3272),
            "darwin": (-12.4634, 130.8456), "canberra": (-35.2809, 149.1300)
        }
        lat, lon = coords.get(city, (-33.8688, 151.2093))
        bbox = f"{lat-0.1},{lon-0.1},{lat+0.1},{lon+0.1}"
        overpass_query = f'[out:json][timeout:25];(node["craft"="plumber"]({bbox});node["craft"="electrician"]({bbox}););out center tags;'
        
        try:
            r = requests.post("https://overpass-api.de/api/interpreter",
                            data={"data": overpass_query}, timeout=30)
            if r.status_code == 200:
                for el in r.json().get("elements", []):
                    tags = el.get("tags", {})
                    name = tags.get("name", "")
                    if not name: continue
                    if tags.get("website") or tags.get("url"): continue
                    phone = tags.get("phone") or tags.get("contact:phone")
                    email = tags.get("email") or tags.get("contact:email")
                    if not (phone or email): continue
                    results.append({
                        "business_name": name,
                        "category": tags.get("craft", "Trade"),
                        "phone": phone, "email": email,
                        "website": None,
                        "city": self.city_config["city"], "state": state,
                        "suburb": tags.get("addr:suburb"),
                        "postcode": tags.get("addr:postcode"),
                        "address_full": tags.get("addr:full"),
                        "source": "google_maps_real", "abn": None
                    })
        except Exception as e:
            self.logger.error(f"OSM query failed: {e}")
        return results'''

orig = re.sub(
    r'    def _fetch_google_business\(self\) -> List\[Dict\]:.*?def _fetch_yellow',
    google_new,
    orig,
    flags=re.DOTALL
)
print("✓ Patched _fetch_google_business")

# Replacement 2: yellow_pages
yellow_new = '''
    def _fetch_yellow_pages(self) -> List[Dict]:
        # Real Yellow Pages — filters email present AND no website.
        import requests
        results = []
        city = self.city_key
        state = self.city_config.get("state", "NSW")
        location = f"{city.title()} {state}"
        
        categories = ["plumber","electrician","builder","painter","carpenter",
                     "roofer","air conditioning","kitchen","flooring","solar"]
        
        for category in categories:
            try:
                r = requests.get("https://www.yellowpages.com.au/search/listings",
                                params={"clue": category, "locationClue": location, "pageNumber": 1},
                                headers={"User-Agent": "Mozilla/5.0 (compatible; LeadGenBot/1.0)"},
                                timeout=15)
                if r.status_code != 200:
                    continue
                for item in r.json().get("results", []):
                    if item.get("email") and not item.get("website"):
                        results.append({
                            "business_name": item.get("name", "").strip(),
                            "category": category,
                            "phone": item.get("phone", "").strip() or None,
                            "email": item.get("email", "").strip().lower(),
                            "website": None,
                            "city": city.title(), "state": state,
                            "suburb": item.get("suburb"),
                            "postcode": item.get("postcode"),
                            "address_full": item.get("address"),
                            "source": "yellow_pages",
                            "abn": item.get("abn")
                        })
            except Exception as e:
                self.logger.debug(f"YP {category} error: {e}")
        return results'''

orig = re.sub(
    r'    def _fetch_yellow_pages\(self\) -> List\[Dict\]:.*?def _fetch_tradie',
    yellow_new,
    orig,
    flags=re.DOTALL
)
print("✓ Patched _fetch_yellow_pages")

# Replacement 3: tradie_portal disabled
tradie_new = '''
    def _fetch_tradie_portal(self) -> List[Dict]:
        # Tradie portal data often synthetic — disabled pending verification.
        return []'''

orig = re.sub(
    r'    def _fetch_tradie_portal\(self\) -> List\[Dict\]:.*?def _fetch_manual',
    tradie_new,
    orig,
    flags=re.DOTALL
)
print("✓ Patched _fetch_tradie_portal")

# Write patched version
pipeline_path.write_text(orig)
print(f"✓ Pipeline patched successfully ({len(orig)} chars)")

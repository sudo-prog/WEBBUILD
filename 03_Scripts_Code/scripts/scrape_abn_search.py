#!/usr/bin/env python3
"""
ABN Lookup Search Scraper — searches by business name + location, extracts table rows.
Gets: ABN, business_name, entity_type, location (postcode+state), active_status.
Does NOT include website (not exposed on public UI).
"""
import json, time, sys, asyncio, re, argparse, requests
from pathlib import Path
from typing import List, Dict

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("ERROR: pip install playwright && playwright install chromium")
    sys.exit(1)

# Trade keywords to search — combined with city/state
TRADES = ["plumber", "electrician", "builder", "painter", "carpenter",
          "roofer", "air conditioning", "kitchen", "flooring", "solar"]

TARGET_CITIES = {
    "Sydney":   {"state": "NSW", "postcodes": ["2000", "2001", "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027", "2028", "2029", "2030", "2031", "2032", "2033", "2034", "2035", "2036", "2037", "2038", "2039", "2040", "2041", "2042", "2043", "2044", "2045", "2046", "2047", "2048", "2049", "2050", "2060", "2061", "2062", "2063", "2064", "2065", "2066", "2067", "2068", "2069", "2070", "2071", "2072", "2073", "2074", "2075", "2076", "2077", "2078", "2079", "2080", "2081", "2082", "2083", "2084", "2085", "2086", "2087", "2088", "2089", "2090", "2091", "2092", "2093", "2094", "2095", "2096", "2097", "2098", "2099", "2100", "2101", "2102", "2103", "2104", "2105", "2106", "2107", "2108", "2109", "2110", "2111", "2112", "2113", "2114", "2115", "2116", "2117", "2118", "2119", "2120", "2121", "2122", "2123", "2124", "2125", "2126", "2127", "2128", "2129", "2130", "2131", "2132", "2133", "2134", "2135", "2136", "2137", "2138", "2139", "2140", "2141", "2142", "2143", "2144", "2145", "2146", "2147", "2148", "2150", "2151", "2152", "2153", "2154", "2155", "2156", "2157", "2158", "2159", "2160", "2161", "2162", "2163", "2164", "2165", "2166", "2167", "2168", "2169", "2170", "2171", "2172", "2173", "2174", "2175", "2176", "2177", "2178", "2179", "2180", "2181", "2182", "2183", "2184", "2185", "2186", "2187", "2188", "2189", "2190", "2191", "2192", "2193", "2194", "2195", "2196", "2197", "2198", "2199"]},
    "Melbourne": {"state": "VIC", "postcodes": ["3000", "3001", "3002", "3003", "3004", "3005", "3006", "3008", "3010", "3011", "3012", "3013", "3015", "3016", "3017", "3018", "3019", "3020", "3021", "3022", "3023", "3024", "3025", "3026", "3027", "3028", "3029", "3030", "3031", "3032", "3033", "3034", "3035", "3036", "3037", "3038", "3039", "3040", "3041", "3042", "3043", "3044", "3045", "3046", "3047", "3048", "3049", "3050", "3051", "3052", "3053", "3054", "3055", "3056", "3057", "3058", "3059", "3060", "3061", "3062", "3063", "3064", "3065", "3066", "3067", "3068", "3069", "3070", "3071", "3072", "3073", "3074", "3075", "3076", "3077", "3078", "3079", "3080", "3081", "3082", "3083", "3084", "3085", "3086", "3087", "3088", "3089", "3090", "3091", "3093", "3094", "3095", "3096", "3097", "3098", "3099", "3100", "3101", "3102", "3103", "3104", "3105", "3106", "3107", "3108", "3109", "3110", "3111", "3112", "3113", "3114", "3115", "3116", "3117", "3118", "3119", "3120", "3121", "3122", "3123", "3124", "3125", "3126", "3127", "3128", "3129", "3130", "3131", "3132", "3133", "3134", "3135", "3136", "3137", "3138", "3139", "3140", "3141", "3142", "3143", "3144", "3145", "3146", "3147", "3148", "3149", "3150", "3151", "3152", "3153", "3154", "3155", "3156", "3157", "3158", "3159", "3160", "3161", "3162", "3163", "3164", "3165", "3166", "3167", "3168", "3169", "3170", "3171", "3172", "3173", "3174", "3175", "3176", "3177", "3178", "3179", "3180", "3181", "3182", "3183", "3184", "3185", "3186", "3187", "3188", "3189", "3190", "3191", "3192", "3193", "3194", "3195", "3196", "3197", "3198", "3199"]},
    "Brisbane": {"state": "QLD", "postcodes": ["4000", "4001", "4002", "4003", "4004", "4005", "4006", "4007", "4008", "4009", "4010", "4011", "4012", "4013", "4014", "4015", "4016", "4017", "4018", "4019", "4020", "4021", "4022", "4025", "4027", "4028", "4029", "4030", "4031", "4032", "4033", "4034", "4035", "4036", "4037", "4051", "4053", "4054", "4055", "4056", "4057", "4058", "4059", "4060", "4061", "4062", "4063", "4064", "4065", "4066", "4067", "4068", "4069", "4070", "4071", "4072", "4073", "4074", "4075", "4076", "4077", "4078", "4101", "4102", "4103", "4104", "4105", "4106", "4107", "4108", "4109", "4110", "4111", "4112", "4113", "4114", "4115", "4116", "4117", "4118", "4119", "4120", "4121", "4122", "4123", "4124", "4125", "4126", "4127", "4128", "4129", "4130", "4131", "4132", "4152", "4153", "4154", "4155", "4156", "4157", "4158", "4159", "4160", "4161", "4162", "4163", "4164", "4165", "4166", "4167", "4168", "4169", "4170", "4171", "4172", "4173", "4174", "4175", "4176", "4177", "4178", "4179", "4180", "4181", "4182", "4183", "4184", "4185", "4205", "4207", "4208", "4209", "4210", "4211", "4212", "4213", "4214", "4215", "4216", "4217", "4218", "4219", "4220", "4221", "4222", "4223", "4224", "4225", "4226", "4227", "4228", "4229", "4270", "4271", "4272", "4273", "4274", "4275", "4276", "4277", "4278", "4279", "4280", "4281", "4282", "4283", "4284", "4285", "4286", "4287", "4288", "4300", "4301", "4302", "4303", "4304", "4305", "4306", "4307", "4309", "4310", "4311", "4312", "4313", "4314", "4315", "4316", "4317", "4318", "4319", "4320", "4321", "4322", "4323", "4325", "4326", "4500", "4501", "4502", "4503", "4504", "4505", "4506", "4507", "4508", "4509", "4510", "4511", "4512", "4513", "4514", "4515", "4516", "4517", "4518", "4519", "4520", "4550", "4551", "4552", "4553", "4554", "4555", "4556", "4557", "4558", "4559", "4560", "4561", "4562", "4563", "4564", "4565", "4566", "4567", "4568", "4569", "4570", "4571", "4572", "4573", "4574", "4575", "4576", "4577", "4578", "4579", "4580", "4600", "4601", "4605", "4606", "4607", "4608", "4609", "4610", "4611", "4612", "4613", "4614", "4615", "4616", "4617", "4618"]},
    "Perth":    {"state": "WA",  "postcodes": ["6000", "6001", "6002", "6003", "6004", "6005", "6006", "6007", "6008", "6009", "6010", "6011", "6012", "6014", "6015", "6016", "6017", "6018", "6019", "6020", "6021", "6022", "6023", "6024", "6025", "6026", "6027", "6028", "6029", "6030", "6031", "6032", "6033", "6034", "6035", "6036", "6037", "6038", "6039", "6040", "6041", "6042", "6043", "6044", "6045", "6046", "6047", "6048", "6049", "6050", "6051", "6052", "6053", "6054", "6055", "6056", "6057", "6058", "6059", "6060", "6061", "6062", "6063", "6064", "6065", "6066", "6067", "6068", "6069", "6070", "6071", "6072", "6073", "6074", "6076", "6077", "6078", "6079", "6080", "6081", "6082", "6083", "6084", "6085", "6086", "6087", "6088", "6089", "6090", "6100", "6101", "6102", "6103", "6104", "6105", "6106", "6107", "6108", "6109", "6110", "6111", "6112", "6151", "6152", "6153", "6154", "6155", "6156", "6157", "6158", "6159", "6160", "6161", "6162", "6163", "6164", "6165", "6166", "6167", "6168", "6169", "6170", "6171", "6172", "6173", "6174", "6175", "6176", "6177", "6178", "6179", "6180", "6181", "6182", "6183", "6184", "6185", "6186", "6187", "6188", "6189", "6190", "6191", "6192", "6193", "6194", "6195", "6196", "6197", "6198", "6199"]},
    "Adelaide": {"state": "SA",  "postcodes": ["5000", "5001", "5002", "5003", "5004", "5005", "5006", "5007", "5008", "5009", "5010", "5011", "5012", "5013", "5014", "5015", "5016", "5017", "5018", "5019", "5020", "5021", "5022", "5023", "5024", "5025", "5026", "5027", "5028", "5029", "5030", "5031", "5032", "5033", "5034", "5035", "5036", "5037", "5038", "5039", "5040", "5041", "5042", "5043", "5044", "5045", "5046", "5047", "5048", "5049", "5050", "5051", "5052", "5053", "5054", "5055", "5056", "5057", "5058", "5059", "5060", "5061", "5062", "5063", "5064", "5065", "5066", "5067", "5068", "5069", "5070", "5071", "5072", "5073", "5074", "5075", "5076", "5077", "5078", "5079", "5080", "5081", "5082", "5083", "5084", "5085", "5086", "5087", "5088", "5089", "5090", "5091", "5092", "5093", "5094", "5095", "5096", "5097", "5098", "5099", "5100", "5101", "5102", "5103", "5104", "5105", "5106", "5107", "5108", "5109", "5110", "5111", "5112", "5113", "5114", "5115", "5116", "5117", "5118", "5119", "5120", "5121", "5122", "5123", "5124", "5125", "5126", "5127", "5128", "5129", "5130", "5131", "5132", "5133", "5134", "5135", "5136", "5137", "5138", "5139", "5140", "5141", "5142", "5143", "5144", "5145", "5146", "5147", "5148", "5149", "5150", "5151", "5152", "5153", "5154", "5155", "5156", "5157", "5158", "5159", "5160", "5161", "5162", "5163", "5164", "5165", "5166", "5167", "5168", "5169", "5170", "5171", "5172", "5173", "5174", "5175", "5176", "5177", "5178", "5179", "5180", "5181", "5182", "5183", "5184", "5185", "5186", "5187", "5188", "5189", "5190", "5191", "5192", "5193", "5194", "5195", "5196", "5197", "5198", "5199"]},
}

def is_in_target_city(postcode_str: str, city_key: str) -> bool:
    """Check if postcode belongs to target city's postcode list."""
    postcode_clean = ''.join(filter(str.isdigit, postcode_str))[:4]
    return postcode_clean in TARGET_CITIES.get(city_key, {}).get("postcodes", [])


def parse_search_results(html: str) -> List[Dict]:
    """
    Parse the ABN search results table using BeautifulSoup (not regex).
    Replaced regex-based parsing which broke silently on nested tags.
    """
    leads = []
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("[WARN] BeautifulSoup not installed — falling back to lxml")
        try:
            from lxml.html import fromstring
            soup = fromstring(html)
            # Use basic XPath as fallback
            return _parse_with_lxml(html)
        except ImportError:
            print("[WARN] Neither BeautifulSoup nor lxml available — using regex fallback")
            return _parse_with_regex(html)

    soup = BeautifulSoup(html, 'html.parser')
    # Find all table rows (after header)
    rows = soup.select('table tbody tr, table tr')
    print(f"  Found {len(rows)} result rows in table")

    for row in rows:
        try:
            cells = row.find_all('td')
            if len(cells) < 4:
                continue

            # ABN: first cell contains a link
            abn_link = cells[0].find('a', href=True)
            if not abn_link:
                continue
            abn_match = re.search(r'/ABN/View\?abn=(\d+)', abn_link['href'])
            if not abn_match:
                continue
            abn = abn_match.group(1)
            # Format as 11-digit with spaces
            abn_fmt = f"{abn[:2]} {abn[2:5]} {abn[5:8]} {abn[8:11]}"

            # Active status check
            active_span = cells[0].find('span', class_=re.compile(r'active', re.I))
            if not active_span:
                continue  # skip inactive

            # Business name from second cell
            name = cells[1].get_text(strip=True)

            # Entity type from third cell
            entity_type = cells[2].get_text(strip=True)

            # Location from fourth cell: "4171 QLD"
            location = cells[3].get_text(strip=True)
            loc_parts = location.split()
            postcode = ''.join(filter(str.isdigit, loc_parts[0])) if loc_parts else ""
            state = loc_parts[-1].upper() if len(loc_parts) > 1 else ""

            leads.append({
                "abn": abn_fmt,
                "business_name": name,
                "entity_type": entity_type,
                "postcode": postcode,
                "state": state,
                "location_raw": location,
            })
        except Exception as e:
            continue

    return leads


def _parse_with_lxml(html: str) -> List[Dict]:
    """Fallback lxml HTML parser when BeautifulSoup unavailable."""
    from lxml.html import fromstring
    doc = fromstring(html)
    leads = []
    for row in doc.xpath('//table//tr[td]'):
        cells = row.xpath('td')
        if len(cells) < 4:
            continue
        try:
            abn_text = cells[0].text_content().strip()
            abn_digits = ''.join(filter(str.isdigit, abn_text))
            if len(abn_digits) < 11:
                continue
            abn_fmt = f"{abn_digits[:2]} {abn_digits[2:5]} {abn_digits[5:8]} {abn_digits[8:11]}"
            name = cells[1].text_content().strip()
            entity_type = cells[2].text_content().strip()
            location = cells[3].text_content().strip()
            loc_parts = location.split()
            postcode = ''.join(filter(str.isdigit, loc_parts[0])) if loc_parts else ""
            state = loc_parts[-1].upper() if len(loc_parts) > 1 else ""
            leads.append({
                "abn": abn_fmt, "business_name": name,
                "entity_type": entity_type, "postcode": postcode,
                "state": state, "location_raw": location,
            })
        except Exception:
            continue
    return leads


def _parse_with_regex(html: str) -> List[Dict]:
    """Original regex fallback for when neither parser is available."""
    leads = []
    rows = re.findall(r'<tr>\s*<td>.*?</td>\s*<td>.*?</td>\s*<td>.*?</td>\s*<td>.*?</td>\s*</tr>', html, re.DOTALL)
    for row in rows:
        try:
            abn_match = re.search(r'<a[^>]*?/ABN/View\?abn=(\d+)', row, re.I)
            if not abn_match:
                continue
            abn = abn_match.group(1)
            abn_fmt = f"{abn[:2]} {abn[2:5]} {abn[5:8]} {abn[8:11]}"
            active = 1 if re.search(r'<span[^>]*?class=["\'][^"\']*?active', row, re.I) else 0
            if not active:
                continue
            tds = re.findall(r'<td[^>]*?>(.*?)</td>', row, re.DOTALL)
            if len(tds) < 4:
                continue
            name = re.sub(r'<[^>]+>', '', tds[1]).strip()
            entity_type = re.sub(r'<[^>]+>', '', tds[2]).strip()
            location = re.sub(r'<[^>]+>', '', tds[3]).strip()
            loc_parts = location.split()
            postcode = ''.join(filter(str.isdigit, loc_parts[0])) if loc_parts else ""
            state = loc_parts[-1].upper() if len(loc_parts) > 1 else ""
            leads.append({
                "abn": abn_fmt, "business_name": name,
                "entity_type": entity_type, "postcode": postcode,
                "state": state, "location_raw": location,
            })
        except Exception:
            continue
    return leads


def has_next_page(html: str) -> bool:
    """Check if there is a 'Next' pagination link."""
    return bool(re.search(r'Next\s*</a>|<a[^>]*?pageNumber=\d+[^>]*?>Next', html, re.I))


def get_next_page_url(html: str) -> str | None:
    """Extract the next page URL from pagination."""
    match = re.search(r'<a[^>]*?href="([^"]*pageNumber=\d+)"[^>]*?>Next', html, re.I)
    if match:
        link = match.group(1)
        if not link.startswith('http'):
            link = "https://abr.business.gov.au" + link
        return link
    return None


def search_abn(page, query: str) -> List[Dict]:
    """Perform an ABN lookup search and collect all pages of results."""
    base_url = "https://abr.business.gov.au/ABN/Search"
    url = f"{base_url}?SearchText={requests.utils.quote(query)}"
    print(f"  GET {url}")
    page.goto(url, wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(2000)
    
    all_leads = []
    page_num = 1
    while True:
        html = page.content()
        leads = parse_search_results(html)
        print(f"  Page {page_num}: {len(leads)} leads")
        all_leads.extend(leads)
        
        if not has_next_page(html):
            break
        
        next_url = get_next_page_url(html)
        if not next_url:
            break
        page.goto(next_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        page_num += 1
        if page_num > 10:  # safety cap
            print("  [WARN] Reached 10 pages, stopping")
            break
    
    return all_leads


def scrape_city(city: str, state: str, max_trades: int = None) -> List[Dict]:
    """
    For a target city, search all trades and collect ABN-matched leads.
    Filters by postcode belonging to city.
    """
    all_leads = []
    city_key = city.title()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
        page = context.new_page()
        
        for trade in (TRADES[:max_trades] if max_trades else TRADES):
            query = f"{trade} {city_key} {state}"
            print(f"\n[{city}] Searching: '{query}'")
            try:
                leads = search_abn(page, query)
                # Filter by postcode (city boundary check)
                city_postcodes = set(TARGET_CITIES.get(city_key, {}).get("postcodes", []))
                filtered = [l for l in leads if l['postcode'] in city_postcodes]
                print(f"  → {len(filtered)} leads in {city_key} after postcode filter (from {len(leads)} total)")
                for l in filtered:
                    l["category"] = trade
                    l["city"] = city_key
                all_leads.extend(filtered)
            except Exception as e:
                print(f"  ERROR: {e}")
            time.sleep(1.0)  # polite delay between searches
        
        browser.close()
    return all_leads


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("city", help="City name (e.g. Sydney)")
    parser.add_argument("state", help="State code (e.g. NSW)")
    parser.add_argument("--trades", type=int, default=None, help="Number of trade categories to scrape (default: all)")
    args = parser.parse_args()

    print(f"=== ABN Lookup Scraper: {args.city}, {args.state} ===")
    leads = scrape_city(args.city, args.state, max_trades=args.trades)
    
    # Deduplicate by ABN
    seen = set()
    unique = []
    for l in leads:
        if l['abn'] not in seen:
            seen.add(l['abn'])
            unique.append(l)
    
    ts = int(time.time())
    out = Path(f"raw_leads/abn_{args.city.lower()}_{args.state.lower()}_{ts}.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(unique, indent=2))
    print(f"\n✓ {len(unique)} unique ABN leads → {out}")

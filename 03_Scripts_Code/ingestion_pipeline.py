#!/usr/bin/env python3
"""
Australian Capital Cities Lead Ingestion Pipeline — PostgreSQL direct version
8 Cities: Sydney, Melbourne, Brisbane, Perth, Adelaide, Hobart, Darwin, Canberra

Uses psycopg2 for direct DB access (no Supabase client needed).
"""
from abn_validator import verify as verify_abn

import os, sys, json, uuid, argparse, logging, csv
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path

from postgrest import SyncPostgrestClient

PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================
# Configuration
# ============================================
@dataclass
class Config:
    db_host: str = '127.0.0.1'
    db_port: int = 6543
    db_name: str = 'postgres'
    db_user: str = 'postgres'
    db_password: str = ''
    batch_size: int = 100
    dry_run: bool = False
    log_level: str = "INFO"

    @classmethod
    def from_file(cls, path: str = 'config/settings.json') -> 'Config':
        with open(path) as f:
            data = json.load(f)
        db = data.get('postgres', {})
        ing = data.get('ingestion', {})
        return cls(
            db_host=db.get('host', '127.0.0.1'),
            db_port=db.get('port', 6543),
            db_name=db.get('database', 'postgres'),
            db_user=db.get('user', 'postgres'),
            db_password=db.get('password', ''),
            batch_size=ing.get('batch_size', 100),
            dry_run=ing.get('dry_run', False),
            log_level=ing.get('log_level', 'INFO')
        )

    @classmethod
    def from_env(cls) -> 'Config':
        return cls(
            db_host=os.getenv('PG_HOST', '127.0.0.1'),
            db_port=int(os.getenv('PG_PORT', '6543')),
            db_name=os.getenv('PG_DATABASE', 'postgres'),
            db_user=os.getenv('PG_USER', 'postgres'),
            db_password=os.getenv('PG_PASSWORD', ''),
            batch_size=int(os.getenv('BATCH_SIZE', '100')),
            dry_run=os.getenv('DRY_RUN', 'false').lower() == 'true',
            log_level=os.getenv('LOG_LEVEL', 'INFO')
        )

# ============================================
# Logging
# ============================================
def setup_logging(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger('ingestion')
    logger.setLevel(getattr(logging, level.upper()))
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

# ============================================
# Database wrapper
# ============================================
class Database:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.client = None
        if not self.config.dry_run:
            import os
            supabase_url = os.getenv("SUPABASE_URL")
            anon_key = os.getenv("SUPABASE_ANON_KEY")
            if not supabase_url or not anon_key:
                self.logger.warning('SUPABASE_URL or SUPABASE_ANON_KEY not set — DB ops will be skipped')
            else:
                self.client = SyncPostgrestClient(supabase_url, headers={"Authorization": f"Bearer {anon_key}"})
                self.logger.info(f'Connected to Supabase at {supabase_url}')
        else:
            self.logger.info('[DRY-RUN] No DB connection')

    def insert_leads(self, leads: List[Dict], batch_id: str) -> Dict[str, int]:
        if self.config.dry_run:
            self.logger.info(f"[DRY-RUN] Would insert {len(leads)} leads")
            return {'inserted': len(leads), 'updated': 0, 'skipped': 0, 'failed': 0}

        if not self.client:
            return {'inserted': 0, 'updated': 0, 'skipped': 0, 'failed': len(leads)}

        stats = {'inserted': 0, 'updated': 0, 'skipped': 0, 'failed': 0}
        try:
            result = self.client.table('leads').upsert(leads, on_conflict='business_name,city').execute()
            stats['inserted'] = len(result.data)
        except Exception as e:
            self.logger.error(f'Failed to insert leads: {e}')
            stats['failed'] = len(leads)
        return stats

    def log_ingestion(self, log_entry: Dict) -> Optional[str]:
        if self.config.dry_run:
            log_id = str(uuid.uuid4())
            self.logger.info(f"[DRY-RUN] Would log ingestion: {log_entry['source_name']} id={log_id}")
            return log_id

        try:
            result = self.client.table('ingestion_log').insert(log_entry).execute()
            log_id = str(result.data[0]['id']) if result.data else str(uuid.uuid4())
            return log_id
        except Exception as e:
            self.logger.error(f'Failed to create ingestion_log: {e}')
            return None

    def update_ingestion(self, log_id: str, updates: Dict):
        if self.config.dry_run:
            return
        try:
            self.client.table('ingestion_log').update(updates).eq('id', log_id).execute()
        except Exception as e:
            self.logger.error(f'Failed to update ingestion_log: {e}')

# ============================================
# Lead validation
# ============================================
AUSTRALIAN_STATES = {'NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT'}

def validate_lead(raw: Dict, city_config: Dict, logger: logging.Logger) -> Optional[Dict]:
    errors = []
    business_name = (raw.get('business_name') or '').strip()
    if not business_name:
        return None
    category = (raw.get('category') or '').strip()
    if not category:
        return None

    lead_id = raw.get('lead_id')
    if not lead_id:
        slug = business_name.lower().replace(' ', '-')[:50]
        lead_id = f"{city_config.get('state', 'UNK').lower()}-{slug}-{str(uuid.uuid4())[:8]}"

    state = raw.get('state') or city_config.get('state', '')
    if state not in AUSTRALIAN_STATES:
        errors.append(f"Invalid state: {state}")

    city = raw.get('city') or city_config.get('city', '')
    country = raw.get('country') or 'Australia'

    email = raw.get('email', '').strip() if raw.get('email') else None
    if email and '@' not in email:
        errors.append(f"Invalid email: {email}")
        email = None

    # ============================================
    # NEW: Website rejection — businesses with websites are disqualified
    # ============================================
    website = (raw.get('website') or '').strip()
    if website and website not in ['', 'N/A', 'n/a', 'null', 'None']:
        logger.debug(f"Lead '{business_name}' has website '{website}' — rejecting")
        return None

    phone = raw.get('phone', '').strip() if raw.get('phone') else None
    if phone:
        phone = phone.replace(' ', '')

    lead_score = raw.get('lead_score')
    if lead_score is None:
        score = 50
        if email: score += 10
        if phone: score += 10
        if raw.get('website'): score += 10
        if raw.get('rating'): score += int(float(raw.get('rating', 0)) * 5)
        lead_score = min(100, score)
    else:
        try:
            lead_score = int(lead_score)
            lead_score = max(0, min(100, lead_score))
        except (ValueError, TypeError):
            lead_score = 50

    # ============================================
    # ABN cross-reference with Australian Business Register
    # Zero-trust: ABN must be valid AND active; any error = REJECT
    # ============================================
    abn = raw.get('abn')
    if abn:
        is_verified, abn_details = verify_abn(business_name, state, abn)
        if is_verified:
            lead_score = min(100, lead_score + 15) if lead_score else 65
            official_name = abn_details.get('entity_name', '')
            if official_name and len(official_name) > len(business_name):
                business_name = official_name
        else:
            logger.debug(f"Lead '{business_name}' ABN {abn} failed verification (reason: {abn_details.get('error','unknown')})")
            return None

    lat = raw.get('geo_lat')
    lng = raw.get('geo_lng')
    if lat and lng:
        try:
            lat = float(lat)
            lng = float(lng)
            if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                lat = lng = None
        except (ValueError, TypeError):
            lat = lng = None

    # Ensure services is a string, not a list (psycopg2 cannot insert lists into text columns)
    services = raw.get('services')
    if isinstance(services, list):
        services = ",".join(str(s) for s in services)
    elif not isinstance(services, str):
        services = None

    if errors:
        for err in errors:
            logger.debug(f"Lead '{business_name}': {err}")
        return None

    now = datetime.now(timezone.utc)
    return {
        'lead_id': lead_id,
        'source': raw.get('source', 'manual'),
        'ingestion_batch_id': str(uuid.uuid4()),
        'business_name': business_name,
        'abn': raw.get('abn'),
        'category': category,
        'subcategory': raw.get('subcategory'),
        'services': services,
        'phone': phone,
        'mobile': raw.get('mobile'),
        'email': email,
        'website': raw.get('website'),
        'country': country,
        'state': state,
        'city': city,
        'suburb': raw.get('suburb'),
        'postcode': raw.get('postcode'),
        'address_full': raw.get('address_full'),
        'geo_lat': lat,
        'geo_lng': lng,
        'years_in_business': raw.get('years_in_business'),
        'employee_count': raw.get('employee_count'),
        'rating': float(raw['rating']) if raw.get('rating') else None,
        'review_count': int(raw.get('review_count', 0)),
        'lead_score': lead_score,
        'tier': raw.get('tier', 'standard'),
        'is_active': bool(raw.get('is_active', True)),
        'first_seen_at': raw.get('first_seen_at') or now.isoformat(),
        'last_verified_at': raw.get('last_verified_at'),
        'created_at': now.isoformat(),
        'updated_at': now.isoformat(),
    }

# ============================================
# City fetchers
# ============================================
def load_city_config(city_key: str) -> Dict[str, str]:
    config_path = PROJECT_ROOT / 'config' / 'settings.json'
    # Fallback to root-level config if not found in scripts directory
    if not config_path.exists():
        config_path = Path(__file__).parent.parent / 'config' / 'settings.json'
    if config_path.exists():
        with open(config_path) as f:
            data = json.load(f)
        if city_key in data.get('cities', {}):
            city_data = data['cities'][city_key].copy()
            city_data['city'] = city_data.get('city', city_key.title())
            return city_data
    fallback = {
        'sydney':    {'state': 'NSW', 'city': 'Sydney'},
        'melbourne': {'state': 'VIC', 'city': 'Melbourne'},
        'brisbane':  {'state': 'QLD', 'city': 'Brisbane'},
        'perth':     {'state': 'WA',  'city': 'Perth'},
        'adelaide':  {'state': 'SA',  'city': 'Adelaide'},
        'hobart':    {'state': 'TAS', 'city': 'Hobart'},
        'darwin':    {'state': 'NT',  'city': 'Darwin'},
        'canberra':  {'state': 'ACT',  'city': 'Canberra'},
    }
    return fallback.get(city_key, {'state': 'UNK', 'city': city_key.title()})

class CityFetcher:
    def __init__(self, city_key: str, logger: logging.Logger):
        self.city_key = city_key
        self.logger = logger
        self.city_config = load_city_config(city_key)

    def fetch_all(self, source: Optional[str] = None) -> List[Dict]:
        sources = [source] if source else ['google_business', 'yellow_pages', 'manual']
        all_leads = []
        for src in sources:
            try:
                method = getattr(self, f'_fetch_{src}', None)
                if method:
                    leads = method()
                    all_leads.extend(leads)
                    self.logger.info(f"Fetched {len(leads)} leads from {src} ({self.city_key})")
                else:
                    self.logger.warning(f"Unknown source: {src}")
            except Exception as e:
                self.logger.error(f"Source {src} failed: {e}")
        return all_leads

    def _fetch_google_business(self) -> List[Dict]:
        """Fetch from OpenStreetMap Overpass (Google Maps equivalent).
        Strict filter: contact present and NO website."""
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
        overpass_query = f"""[out:json][timeout:25];
(
  node["craft"="plumber"]({bbox});
  node["craft"="electrician"]({bbox});
  node["craft"="carpenter"]({bbox});
  node["craft"="painter"]({bbox});
);
out center tags;"""
        try:
            # Overpass API is picky about Accept-Encoding — newer requests lib sends br,zstd which causes 406
            r = requests.post("https://overpass-api.de/api/interpreter",
                            data={"data": overpass_query},
                            headers={
                                "Accept": "*/*",
                                "Accept-Encoding": "gzip, deflate",
                                "User-Agent": "curl/8.0",
                                "Content-Type": "application/x-www-form-urlencoded",
                            },
                            timeout=30)
            if r.status_code == 200:
                for el in r.json().get("elements", []):
                    tags = el.get("tags", {})
                    name = tags.get("name", "")
                    if not name:
                        continue
                    if tags.get("website") or tags.get("url"):
                        continue
                    phone = tags.get("phone") or tags.get("contact:phone")
                    email = tags.get("email") or tags.get("contact:email")
                    if not (phone or email):
                        continue
                    results.append({
                        "business_name": name,
                        "category": tags.get("craft", "Trade"),
                        "phone": phone, "email": email, "website": None,
                        "city": self.city_config["city"], "state": state,
                        "suburb": tags.get("addr:suburb"),
                        "postcode": tags.get("addr:postcode"),
                        "address_full": tags.get("addr:full"),
                        "source": "google_maps_real", "abn": None
                    })
            else:
                self.logger.warning(f"OSM API returned {r.status_code}: {r.text[:200]}")
        except Exception as e:
            self.logger.error(f"OSM query failed: {e}")
        return results

    def _fetch_yellow_pages(self) -> List[Dict]:
        """Real Yellow Pages AU scraper — Playwright (bypasses Cloudflare).
        Has a short timeout — if scraper unavailable, returns empty."""
        import subprocess, json
        city = self.city_key
        state = self.city_config.get("state", "NSW")

        # Check if the scraper script exists and playwright is available
        scraper_path = PROJECT_ROOT / "scripts" / "scrape_yp_playwright.py"
        if not scraper_path.exists():
            self.logger.warning(f"YP scraper not found at {scraper_path}")
            return []

        # Try with system python first (more likely to have playwright)
        python_candidates = [sys.executable, "/usr/bin/python3", "/usr/bin/python"]
        for python_bin in python_candidates:
            if not os.path.isfile(python_bin):
                continue
            # Quick check if playwright is installed
            try:
                import subprocess
                r = subprocess.run(
                    [python_bin, "-c", "import playwright; print('ok')"],
                    capture_output=True, text=True, timeout=5
                )
                if r.returncode == 0:
                    break
            except Exception:
                continue
        else:
            self.logger.warning("Playwright not available — skipping YP scraper")
            return []

        cmd = [python_bin, str(scraper_path), city, state]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                self.logger.error(f"YP scraper failed: {result.stderr[:200]}")
                return []
            if not result.stdout.strip():
                self.logger.warning(f"YP scraper returned empty output")
                return []
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            self.logger.error(f"YP scraper output not valid JSON: {e}")
            return []
        except subprocess.TimeoutExpired:
            self.logger.warning(f"YP scraper timed out (60s)")
            return []
        except Exception as e:
            self.logger.error(f"YP scraper error: {e}")
            return []

        results = []
        for item in data:
            results.append({
                "business_name": item.get("name", "").strip(),
                "category": item.get("category", "Trade"),
                "phone": item.get("phone") or None,
                "email": (item.get("email") or "").strip().lower() or None,
                "website": None,
                "city": city.title(),
                "state": state,
                "suburb": item.get("suburb"),
                "postcode": None,
                "address_full": None,
                "source": "yellow_pages_playwright",
                "abn": None,
            })
        return results

    def _fetch_tradie_portal(self) -> List[Dict]:
        """Tradie portal data is often gated/synthetic — disabled."""
        return []

    def _fetch_manual(self) -> List[Dict]:
        # Try both possible locations for input CSVs
        for base in [PROJECT_ROOT, PROJECT_ROOT.parent]:
            csv_path = base / 'data' / 'inputs' / f'{self.city_key}_leads.csv'
            if csv_path.exists():
                leads = []
                with open(csv_path, newline='', encoding='utf-8') as f:
                    for row in csv.DictReader(f):
                        row['source'] = 'manual'
                        leads.append(row)
                self.logger.info(f"Loaded {len(leads)} leads from {csv_path}")
                return leads
        return []

# ============================================
# Orchestrator
# ============================================
class IngestionOrchestrator:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.db = Database(config, logger)

    def run_city(self, city_key: str, source: Optional[str] = None) -> Dict[str, Any]:
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Starting ingestion: city={city_key}, source={source or 'all'}")
        self.logger.info(f"{'='*60}")

        city_config = load_city_config(city_key)
        fetcher = CityFetcher(city_key, self.logger)

        log_id = None
        if not self.config.dry_run:
            log_entry = {
                'batch_id': str(uuid.uuid4()),
                'source_name': source or 'multi_source',
                'city_target': city_config['city'],
                'state_target': city_config['state'],
                'record_count': 0,
                'status': 'running',
                'source_config': {'city': city_key, 'source': source}
            }
            log_id = self.db.log_ingestion(log_entry)

        start_time = datetime.now(timezone.utc)
        raw_leads = fetcher.fetch_all(source)
        fetch_count = len(raw_leads)

        validated = []
        for raw in raw_leads:
            cleaned = validate_lead(raw, city_config, self.logger)
            if cleaned:
                validated.append(cleaned)
            else:
                self.logger.debug(f"Invalid lead skipped: {raw.get('business_name','Unknown')}")

        valid_count = len(validated)
        stats = self.db.insert_leads(validated, log_id or 'dry-run')

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        status = 'completed' if stats['failed'] == 0 else 'partial' if stats['inserted'] > 0 else 'failed'

        if log_id and not self.config.dry_run:
            self.db.update_ingestion(log_id, {
                'record_count': fetch_count,
                'records_inserted': stats['inserted'],
                'records_updated': stats.get('updated', 0),
                'records_skipped': fetch_count - valid_count,
                'records_failed': stats['failed'],
                'status': status,
                'duration_seconds': int(duration),
                'completed_at': datetime.now(timezone.utc).isoformat()
            })

        self.logger.info(f"City {city_key}: {fetch_count} fetched, {valid_count} valid, {stats['inserted']} inserted, {stats['failed']} failed in {duration:.1f}s")
        return {
            'city': city_key,
            'fetched': fetch_count,
            'valid': valid_count,
            'inserted': stats['inserted'],
            'updated': stats.get('updated', 0),
            'failed': stats['failed'],
            'duration_seconds': duration,
            'log_id': log_id,
            'status': status
        }

    def run_all(self) -> List[Dict[str, Any]]:
        cities = ['sydney','melbourne','brisbane','perth','adelaide','hobart','darwin','canberra']
        results = []
        for city in cities:
            try:
                res = self.run_city(city)
                results.append(res)
            except Exception as e:
                self.logger.error(f"City {city} crashed: {e}")
                results.append({'city': city, 'status': 'error', 'error': str(e)})
        return results

# ============================================
# CLI
# ============================================
def main():
    parser = argparse.ArgumentParser(
        description='Australian leads ingestion pipeline — 8 capital cities (psycopg2)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --city sydney
  %(prog)s --all
  %(prog)s --city melbourne --source yellow_pages
  %(prog)s --city brisbane --dry-run
        """
    )
    parser.add_argument('--city', choices=['sydney','melbourne','brisbane','perth','adelaide','hobart','darwin','canberra'])
    parser.add_argument('--source', choices=['google_business','yellow_pages','tradie_portal','manual','government_portal'])
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--config', default='config/settings.json')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG','INFO','WARNING','ERROR'])

    args = parser.parse_args()
    if not args.city and not args.all:
        parser.error("Must specify either --city or --all")

    logger = setup_logging(args.log_level)
    logger.info("Australian Leads Ingestion Pipeline (psycopg2) starting")

    # Try config from scripts directory first, then root
    config_path = PROJECT_ROOT / args.config
    if not config_path.exists():
        config_path = Path(__file__).parent.parent / args.config
    if config_path.exists():
        config = Config.from_file(str(config_path))
        logger.info(f"Loaded config from {config_path}")
    else:
        config = Config.from_env()
        logger.info("Loaded config from environment variables")

    config.dry_run = config.dry_run or args.dry_run
    if not config.db_password and not config.dry_run:
        logger.error("DB password not set (check config/settings.json or PG_PASSWORD env var)")
        sys.exit(1)

    orchestrator = IngestionOrchestrator(config, logger)

    if args.all:
        results = orchestrator.run_all()
        print_summary(results)
    else:
        result = orchestrator.run_city(args.city, args.source)
        print_summary([result])

def print_summary(results: List[Dict]):
    print("\n" + "="*60)
    print("INGESTION SUMMARY")
    print("="*60)
    total_fetched = sum(r.get('fetched', 0) for r in results)
    total_inserted = sum(r.get('inserted', 0) for r in results)
    total_failed = sum(r.get('failed', 0) for r in results)
    for r in results:
        icon = "✅" if r.get('status') == 'completed' else "⚠️" if r.get('status') == 'partial' else "❌"
        print(f"{icon} {r['city']:12s}  fetched={r.get('fetched',0):4d}  valid={r.get('valid',0):4d}  inserted={r.get('inserted',0):4d}  failed={r.get('failed',0):4d}")
    print(f"\nTOTAL: fetched={total_fetched}, inserted={total_inserted}, failed={total_failed}")
    print("="*60)

if __name__ == '__main__':
    main()
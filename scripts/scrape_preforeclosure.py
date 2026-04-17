"""
BatchData Pre-Foreclosure Scraper
Runs Mon/Thu at 6am CDT — Sedgwick KS, Harris TX, Clark NV
Cost: ~$0.06/result
"""

import json, os, re, hashlib, logging
from datetime import datetime, timezone
from pathlib import Path
import requests

logging.basicConfig(level=logging.INFO, format='%(levelname)-7s %(message)s')
log = logging.getLogger('preforeclosure')

DATA_DIR  = Path(__file__).parent.parent / 'data'
API_KEY   = os.environ.get('BATCHDATA_API_KEY', '')
API_URL   = 'https://api.batchdata.com/api/v1/property/search'
PAGE_SIZE = 100  # max per request

COUNTIES = [
    {'query': 'Sedgwick County, KS', 'key': 'sedgwick', 'city': 'Wichita',    'state': 'KS'},
    {'query': 'Harris County, TX',   'key': 'harris',   'city': 'Houston',    'state': 'TX'},
    {'query': 'Clark County, NV',    'key': 'clark',    'city': 'Las Vegas',  'state': 'NV'},
]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_id(*parts):
    s = '|'.join(str(p or '') for p in parts).lower()
    return hashlib.md5(s.encode()).hexdigest()[:16]

def fetch_preforeclosures(query, skip=0):
    """Fetch one page of pre-foreclosure results."""
    payload = {
        'searchCriteria': {
            'query': query,
            'quickLists': ['preforeclosure']
        },
        'options': {
            'take': PAGE_SIZE,
            'skip': skip
        }
    }
    resp = requests.post(API_URL,
        json=payload,
        headers={
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        },
        timeout=30
    )
    log.info(f'  BatchData HTTP {resp.status_code} — body length: {len(resp.text)} — first 200: {resp.text[:200]}')
    if not resp.ok:
        log.error(f'  BatchData error: {resp.text[:500]}')
    resp.raise_for_status()
    return resp.json()

def parse_property(prop, county_key, city, state):
    """Convert a BatchData property record to INTEL lead format."""
    addr    = prop.get('address', {})
    owner   = prop.get('owner', {})
    fc      = prop.get('foreclosure', {})
    val     = prop.get('valuation', {})
    listing = prop.get('listing', {})
    intel   = prop.get('intel', {})

    street     = addr.get('street', '') or addr.get('houseNumber', '')
    full_addr  = f"{street}, {addr.get('city', city)} {addr.get('state', state)}"
    owner_name = owner.get('fullName', '') or 'SEE COUNTY RECORDS'

    # Format amount — use open lien balance or auction min bid
    open_lien = prop.get('openLien', {})
    amount = None
    if fc.get('auctionMinimumBidAmount'):
        amount = f"${fc['auctionMinimumBidAmount']:,.2f}"
    elif open_lien.get('totalOpenLienBalance'):
        amount = f"${open_lien['totalOpenLienBalance']:,.2f}"

    auction_date = fc.get('auctionDate', '')
    if auction_date:
        try:
            auction_date = datetime.fromisoformat(auction_date.replace('Z','+00:00')).strftime('%Y-%m-%d')
        except: pass

    notes_parts = []
    if fc.get('status'):          notes_parts.append(fc['status'])
    if fc.get('auctionDate'):     notes_parts.append(f"Auction: {auction_date}")
    if fc.get('auctionLocation'): notes_parts.append(f"@ {fc['auctionLocation']}, {fc.get('auctionCity','')}")
    if fc.get('trusteeName'):     notes_parts.append(f"Trustee: {fc['trusteeName']}")
    if fc.get('caseNumber'):      notes_parts.append(f"Case: {fc['caseNumber']}")
    if val.get('estimatedValue'): notes_parts.append(f"Est. Value: ${val['estimatedValue']:,}")
    if listing.get('propertyType'): notes_parts.append(f"Type: {listing['propertyType']}")

    return {
        'id':          make_id(county_key, 'preforeclosure', street, owner_name),
        'county':      county_key,
        'type':        'pre-foreclosure',
        'owner':       owner_name,
        'address':     full_addr,
        'amount':      amount,
        'filingDate':  fc.get('filingDate', fc.get('recordingDate', '')),
        'caseNumber':  fc.get('caseNumber', ''),
        'phone':       None,
        'score':       min(int(intel.get('salePropensity', 50)), 100) if intel.get('salePropensity') else 50,
        'scrapedAt':   now_iso(),
        'propertyType': listing.get('propertyType', ''),
        'bedrooms':    listing.get('bedroomCount'),
        'estimatedValue': val.get('estimatedValue'),
        'auctionDate': auction_date,
        'lender':      fc.get('currentLenderName', ''),
        'notes':       ' | '.join(notes_parts),
        'source':      'BatchData Pre-Foreclosure',
    }

def load_existing(county_key):
    """Load existing leads from the county's pre-foreclosure file."""
    path = DATA_DIR / f'leads-{county_key}-preforeclosure.json'
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
                return {l['id']: l for l in data.get('leads', [])}
        except: pass
    return {}

def save(county_key, leads, total_found):
    path = DATA_DIR / f'leads-{county_key}-preforeclosure.json'
    data = {
        'lastUpdated': now_iso(),
        'totalLeads':  len(leads),
        'totalFound':  total_found,
        'source':      'BatchData',
        'leads':       leads
    }
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)
    log.info(f'  Saved {len(leads)} leads → {path.name}')
    return data

def scrape_county(county):
    key   = county['key']
    query = county['query']
    city  = county['city']
    state = county['state']

    log.info(f'\n{"="*50}')
    log.info(f'{query.upper()}')
    log.info(f'{"="*50}')

    existing = load_existing(key)
    new_leads = {}
    total_found = 0
    skip = 0

    while True:
        try:
            data = fetch_preforeclosures(query, skip)
            props = data.get('results', {}).get('properties', [])
            meta  = data.get('meta', {}).get('results', {})
            total_found = meta.get('resultsFound', 0)

            log.info(f'  Page skip={skip}: {len(props)} results (total found: {total_found})')

            for prop in props:
                lead = parse_property(prop, key, city, state)
                new_leads[lead['id']] = lead

            if len(props) < PAGE_SIZE or skip + PAGE_SIZE >= total_found:
                break
            skip += PAGE_SIZE

        except Exception as e:
            log.error(f'  Error fetching page skip={skip}: {e}')
            break

    # Merge with existing (keep any manually traced phones)
    merged = {}
    for lid, lead in new_leads.items():
        if lid in existing and existing[lid].get('phone'):
            lead['phone'] = existing[lid]['phone']  # preserve traced phones
        merged[lid] = lead

    leads_list = sorted(merged.values(), key=lambda l: l.get('score', 0), reverse=True)
    save(key, leads_list, total_found)
    log.info(f'  → {len(leads_list)} pre-foreclosure leads')
    return len(leads_list)

def main():
    log.info('='*60)
    log.info('BatchData Pre-Foreclosure Scraper')
    log.info(f'Run time: {now_iso()}')
    log.info('='*60)

    if not API_KEY:
        log.error('BATCHDATA_API_KEY not set')
        return

    totals = {}
    for county in COUNTIES:
        try:
            totals[county['key']] = scrape_county(county)
        except Exception as e:
            log.error(f'{county["key"]} failed: {e}')
            totals[county['key']] = 0

    log.info('\n' + '='*60)
    log.info('PRE-FORECLOSURE SCRAPE COMPLETE')
    for k, v in totals.items():
        log.info(f'  {k.upper()}: {v}')
    log.info(f'  TOTAL: {sum(totals.values())}')
    log.info('='*60)

if __name__ == '__main__':
    main()

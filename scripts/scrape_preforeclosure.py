"""
BatchData Pre-Foreclosure Scraper
Runs Mon/Thu at 6am CDT — Sedgwick KS, Harris TX, Clark NV, Shelby TN
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
PAGE_SIZE = 25  # lower to stay within balance; increase once balance grows

COUNTIES = [
    {'query': 'Sedgwick County, KS', 'key': 'sedgwick', 'city': 'Wichita',    'state': 'KS'},
    {'query': 'Harris County, TX',   'key': 'harris',   'city': 'Houston',    'state': 'TX'},
    {'query': 'Clark County, NV',    'key': 'clark',    'city': 'Las Vegas',  'state': 'NV'},
    {'query': 'Shelby County, TN',   'key': 'shelby',   'city': 'Memphis',    'state': 'TN'},
]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# firstSeenDate/lastSeenDate are compared against the browser's LOCAL "today" in
# INTEL, and Lawton browses from Central time — so stamp the run date in Central
# so a 6am CDT run lines up with the same calendar day in the UI.
try:
    from zoneinfo import ZoneInfo
    CENTRAL = ZoneInfo('America/Chicago')
except Exception:
    CENTRAL = None

def today_str():
    """Run date as YYYY-MM-DD in Central time (falls back to UTC)."""
    if CENTRAL:
        return datetime.now(CENTRAL).strftime('%Y-%m-%d')
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')

def make_id(*parts):
    s = '|'.join(str(p or '') for p in parts).lower()
    return hashlib.md5(s.encode()).hexdigest()[:16]

def _norm(s):
    """Collapse to lowercase alphanumerics + single spaces for stable hashing."""
    return re.sub(r'[^a-z0-9]+', ' ', str(s or '').lower()).strip()

def lead_id(county_key, address_str):
    """Deterministic, owner-independent lead ID.

    Keyed only on county + normalized street address (NOT owner name, which
    flips between a real name and 'SEE COUNTY RECORDS' between runs, and NOT the
    raw street/houseNumber fallback). Fresh records and migrated existing records
    both run through this against the same assembled address string, so the same
    property always resolves to the same ID.
    """
    return make_id(county_key, 'pre-foreclosure', _norm(address_str))

def fetch_preforeclosures(query, skip=0, min_recording_date=None):
    """Fetch one page of pre-foreclosure results.

    If min_recording_date is provided, only returns records with
    foreclosure.recordingDate >= min_recording_date to avoid paying
    for duplicates we already have.
    """
    search_criteria = {
        'query': query,
        'quickLists': ['preforeclosure']
    }
    if min_recording_date:
        search_criteria['foreclosure'] = {
            'recordingDate': { 'minDate': min_recording_date }
        }

    payload = {
        'searchCriteria': search_criteria,
        'options': {
            'take': PAGE_SIZE,
            'skip': skip
        }
    }
    log.info(f'  Calling BatchData API — key ends in: ...{API_KEY[-6:] if API_KEY else "NOT SET"}')
    if min_recording_date:
        log.info(f'  Filter: recordingDate >= {min_recording_date}')
    try:
        resp = requests.post(API_URL,
            json=payload,
            headers={
                'Authorization': f'Bearer {API_KEY}',
                'Content-Type': 'application/json'
            },
            timeout=30
        )
        log.info(f'  HTTP {resp.status_code} — body length: {len(resp.text)} — preview: {resp.text[:300]}')
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        log.error(f'  Request failed: {type(e).__name__}: {e}')
        raise
    except Exception as e:
        log.error(f'  Unexpected error: {type(e).__name__}: {e}')
        raise

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

    run_date = today_str()
    return {
        'id':          lead_id(county_key, full_addr),
        'county':      county_key,
        'type':        'pre-foreclosure',
        'owner':       owner_name,
        'address':     full_addr,
        'amount':      amount,
        'filingDate':  fc.get('filingDate', fc.get('recordingDate', '')),
        'recordingDate': fc.get('recordingDate', fc.get('filingDate', '')),
        'caseNumber':  fc.get('caseNumber', ''),
        'phone':       None,
        'score':       min(int(intel.get('salePropensity', 50)), 100) if intel.get('salePropensity') else 50,
        'scrapedAt':   now_iso(),
        # firstSeenDate is the authoritative "new" signal (set once, preserved on
        # merge). lastSeenDate is refreshed every run BatchData returns this record.
        'firstSeenDate': run_date,
        'lastSeenDate':  run_date,
        'propertyType': listing.get('propertyType', ''),
        'bedrooms':    listing.get('bedroomCount'),
        'estimatedValue': val.get('estimatedValue'),
        'auctionDate': auction_date,
        'lender':      fc.get('currentLenderName', ''),
        'notes':       ' | '.join(notes_parts),
        'source':      'BatchData Pre-Foreclosure',
    }

def load_existing(county_key):
    """Load existing leads, re-keyed under the current deterministic ID scheme.

    Older files were keyed on an owner-dependent ID. We re-derive each lead's ID
    from its stored address so it matches what fresh records now produce, stash
    the prior ID as ``legacyId`` (so INTEL can carry over contacted/phone flags),
    and backfill firstSeenDate/lastSeenDate from prior data so nothing already on
    file gets falsely flagged as "new today" on the first run after this change.
    """
    path = DATA_DIR / f'leads-{county_key}-preforeclosure.json'
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception:
        return {}

    migrated = {}
    for lead in data.get('leads', []):
        old_id = lead.get('id', '')
        new_id = lead_id(lead.get('county', county_key), lead.get('address', ''))

        if old_id and old_id != new_id and 'legacyId' not in lead:
            lead['legacyId'] = old_id
        lead['id'] = new_id

        # Backfill first/last seen for records written before these existed.
        # Prefer any existing stamp, then the prior scrape date, then filing date.
        seen_proxy = (lead.get('scrapedAt', '') or '')[:10] \
            or (lead.get('recordingDate', '') or '')[:10] \
            or (lead.get('filingDate', '') or '')[:10]
        if not lead.get('firstSeenDate'):
            lead['firstSeenDate'] = seen_proxy or today_str()
        if not lead.get('lastSeenDate'):
            lead['lastSeenDate'] = seen_proxy or lead['firstSeenDate']

        # If two legacy rows collapse to the same property, keep the earliest first-seen.
        if new_id in migrated:
            prev = migrated[new_id]
            if lead['firstSeenDate'] and prev.get('firstSeenDate'):
                lead['firstSeenDate'] = min(lead['firstSeenDate'], prev['firstSeenDate'])
            if not lead.get('phone') and prev.get('phone'):
                lead['phone'] = prev['phone']
        migrated[new_id] = lead

    return migrated

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
    fetch_succeeded = False  # track if we got at least one successful page
    paging_error    = False  # a page threw mid-pagination → pull is incomplete

    # NOTE: we deliberately do a FULL pull every run (no min_recording_date /
    # incremental filter). Auto-prune needs to know which existing records are
    # STILL in the active pre-foreclosure pull, and the only way to know a record
    # has left (sold / cured / auctioned) is to fetch the full current set and see
    # it's absent. This costs more per run than the old incremental fetch, but it
    # is required for cured properties to drop out of the JSON.
    min_recording_date = None

    while True:
        try:
            data = fetch_preforeclosures(query, skip, min_recording_date)
            props = data.get('results', {}).get('properties', [])
            meta  = data.get('meta', {}).get('results', {})
            total_found = meta.get('resultsFound', 0)
            fetch_succeeded = True  # at least one page worked

            log.info(f'  Page skip={skip}: {len(props)} results (total found: {total_found})')

            for prop in props:
                lead = parse_property(prop, key, city, state)
                new_leads[lead['id']] = lead

            if len(props) < PAGE_SIZE or skip + PAGE_SIZE >= total_found:
                break
            skip += PAGE_SIZE

        except Exception as e:
            log.error(f'  Error fetching page skip={skip}: {e}')
            paging_error = True
            break

    # If fetch completely failed, preserve existing data rather than wiping it
    if not fetch_succeeded and existing:
        log.warning(f'  ⚠ Fetch failed — preserving {len(existing)} existing leads for {key}')
        log.info(f'  → {len(existing)} pre-foreclosure leads (preserved from last successful run)')
        return len(existing)

    # ── Auto-prune decision ─────────────────────────────────────────────────
    # Prune ONLY on a clean, COMPLETE full pull: at least one page succeeded, no
    # page errored mid-way, and we actually got results back. When those hold we
    # rebuild the county from ONLY what this run returned, so any property no
    # longer in pre-foreclosure (cured / sold / auctioned) drops from the JSON
    # entirely — main view and archive alike.
    #
    # Guards that fall back to the additive merge (keep existing, add new):
    #   • paging_error   → the pull is partial, pruning could wipe real leads
    #   • new_leads == 0 → a clean run returning zero for a county that normally
    #                      has hundreds is almost certainly an API anomaly, not a
    #                      genuinely empty county; never wipe everything on that.
    run_date = today_str()
    prune = fetch_succeeded and not paging_error and len(new_leads) > 0
    if not prune and existing:
        reason = 'incomplete pull' if paging_error else ('zero results' if not new_leads else 'n/a')
        log.warning(f'  ⚠ Skipping auto-prune for {key} ({reason}) — existing leads preserved')

    merged = {} if prune else dict(existing)  # prune → rebuild fresh; else additive
    for lid, lead in new_leads.items():
        prior = existing.get(lid)
        if prior:
            # Known property re-returned by BatchData: it is NOT new today.
            # Preserve the original first-seen date and any manually traced phone,
            # and carry the legacyId so the UI keeps matching prior flags.
            lead['firstSeenDate'] = prior.get('firstSeenDate') or lead.get('firstSeenDate')
            if prior.get('legacyId') and 'legacyId' not in lead:
                lead['legacyId'] = prior['legacyId']
            if prior.get('phone'):
                lead['phone'] = prior['phone']
        # We physically saw this record in the API results this run.
        lead['lastSeenDate'] = run_date
        merged[lid] = lead

    if prune:
        pruned = len(existing) - len([k for k in merged if k in existing])
        log.info(f'  Prune: {len(existing)} existing → {len(merged)} kept '
                 f'({len(new_leads)} in fresh pull, {pruned} dropped from active pull)')
    else:
        log.info(f'  Merge: {len(existing)} existing + {len(new_leads)} fetched = {len(merged)} total')

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

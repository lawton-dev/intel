#!/usr/bin/env python3
"""
RentCast new-listings + price-drop scraper.

Hits RentCast /listings/sale, pulls active listings in target Wichita zips,
and splits them into two lead types written to two JSON files per market:
  data/leads-{market}-new-listing.json  → listed 0-7 days ago
  data/leads-{market}-price-drop.json    → had a price drop event in last 30 days

Runs in GitHub Actions, costs ~$0.07/request on Foundation plan ($74/mo for
1,000 requests). With limit=500 per call, one call per zip is plenty.

Env: RENTCAST_API_KEY (GitHub secret)
"""

import json
import logging
import os
import sys
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ─── Config ─────────────────────────────────────────────────
API_URL  = 'https://api.rentcast.io/v1/listings/sale'
API_KEY  = os.environ.get('RENTCAST_API_KEY', '')
DATA_DIR = Path(__file__).parent.parent / 'data'

NEW_LISTING_MAX_DAYS = 7     # 0-7 days = "new"
PRICE_DROP_LOOKBACK  = 30    # consider price drops in last 30 days
STALE_MIN_DAYS       = 90    # active 90+ days = "stale"
STALE_MAX_DAYS       = 180   # cap at 180 to avoid ancient listings
FAILED_MAX_DAYS_AGO  = 14    # listing removed in last 14 days = "failed"

# Statuses that mean the listing ended for a "good" reason (sale in progress or sold)
# We DON'T want these in the failed bucket — only true expires/withdraws/cancellations
EXCLUDED_END_STATUSES = {
    'pending',
    'sale pending',
    'under contract',
    'contingent',
    'active under contract',
    'accepting backup',
    'accepting backup offers',
    'sold',
    'closed',
    'leased',
    'rented',
}

# Property types we care about (no condos, townhouses, manufactured, or land)
# RentCast enum: Single Family, Condo, Townhouse, Manufactured, Multi-Family, Apartment, Land
ALLOWED_PROPERTY_TYPES = {
    'Single Family',
    'Multi-Family',
    'Apartment',
}

# Markets we scrape. Add more as we expand.
# Each market = (key, list_of_zips, city, state)
MARKETS = {
    'wichita': {
        'city':  'Wichita',
        'state': 'KS',
        'zips':  ['67037', '67060', '67203', '67204', '67207', '67208',
                  '67209', '67211', '67212', '67213', '67214', '67217',
                  '67218', '67219', '67220', '67226', '67230', '67235'],
        'county': 'sedgwick',
    },
    'clark': {
        'city':  'Las Vegas',
        'state': 'NV',
        'zips':  ['89101', '89104', '89106', '89107', '89108', '89110',
                  '89115', '89121', '89122', '89142', '89146', '89147',
                  '89156', '89030', '89031', '89032', '89015', '89014'],
        'county': 'clark',
    },
    'harris': {
        'city':  'Houston',
        'state': 'TX',
        'zips':  ['77002', '77003', '77004', '77009', '77011', '77016',
                  '77020', '77021', '77026', '77028', '77033', '77035',
                  '77045', '77051', '77087', '77088', '77091', '77093'],
        'county': 'harris',
    },
}

logging.basicConfig(level=logging.INFO, format='%(levelname)s\t%(message)s')
log = logging.getLogger()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def _get_latest_status(listing):
    """Return the most recent status string from a listing's history, or None.
    Checks event, listingType, and status fields in that priority order.
    """
    history = listing.get('history') or {}
    if not isinstance(history, dict) or not history:
        return None
    try:
        latest_key = max(history.keys())
        latest = history[latest_key] or {}
        for key in ('event', 'listingType', 'status'):
            v = (latest.get(key) or '').strip()
            if v:
                return v
    except Exception:
        return None
    return None


def _is_excluded_status(listing):
    """Check if a listing ended for a non-failure reason (sold, pending, etc.).
    Returns True if we should SKIP this listing from the failed bucket.

    Aggressive: scans all top-level string fields AND every history entry's
    event/listingType/status fields for any excluded keyword.
    """
    def contains_excluded(v):
        if not v: return False
        v = str(v).strip().lower()
        return any(excl in v for excl in EXCLUDED_END_STATUSES)

    # Check key top-level fields
    for key in ('mlsStatus', 'listingType', 'status', 'event', 'lastEvent'):
        if contains_excluded(listing.get(key)):
            return True

    # Check EVERY history entry, not just the latest — if it was EVER pending,
    # there's likely a sale in motion
    history = listing.get('history') or {}
    if isinstance(history, dict):
        for date_str, entry in history.items():
            if not isinstance(entry, dict):
                continue
            for key in ('event', 'listingType', 'status'):
                if contains_excluded(entry.get(key)):
                    return True

    return False


def fetch_listings(zip_code, status='Active', days_old=None):
    """Fetch sale listings for one zip with optional filters.

    status: 'Active' or 'Inactive'
    days_old: optional string like '90:180' for a range, or None for all
    """
    params = {
        'zipCode': zip_code,
        'status':  status,
        'limit':   500,
    }
    if days_old:
        params['daysOld'] = days_old

    label = f'{status}{" daysOld="+days_old if days_old else ""}'
    log.info(f'  Calling RentCast for zip {zip_code} [{label}] — key: ...{API_KEY[-6:] if API_KEY else "NOT SET"}')
    try:
        resp = requests.get(API_URL,
            params=params,
            headers={'X-Api-Key': API_KEY, 'Accept': 'application/json'},
            timeout=30
        )
        log.info(f'  HTTP {resp.status_code} — body length: {len(resp.text)}')
        if resp.status_code == 401:
            log.error('  401 — RENTCAST_API_KEY missing or invalid')
            return []
        if resp.status_code == 429:
            log.warning('  429 — rate limited, skipping this zip')
            return []
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get('listings', [])
    except requests.exceptions.RequestException as e:
        log.error(f'  Request failed: {type(e).__name__}: {e}')
        return []


def parse_history_for_price_drop(history):
    """Look at the history dict for any price-decrease event in the last 30 days.

    History is keyed by date string (YYYY-MM-DD), each value has price + event.
    A price drop = a 'Sale Listing' event where the price went DOWN vs. the
    previous chronological entry.

    Returns: (had_drop, drop_amount, drop_date, original_price, current_price)
    """
    if not history or not isinstance(history, dict):
        return False, 0, None, None, None

    # Sort by date ascending
    entries = []
    for date_str, evt in history.items():
        try:
            d = datetime.fromisoformat(date_str)
            entries.append((d, evt))
        except Exception:
            continue
    entries.sort(key=lambda x: x[0])

    if len(entries) < 2:
        return False, 0, None, None, None

    cutoff = datetime.now() - timedelta(days=PRICE_DROP_LOOKBACK)
    had_drop = False
    drop_amount = 0
    drop_date = None
    original_price = entries[0][1].get('price')
    current_price = entries[-1][1].get('price')

    for i in range(1, len(entries)):
        prev_price = entries[i - 1][1].get('price')
        curr_price = entries[i][1].get('price')
        if prev_price and curr_price and curr_price < prev_price:
            # Price went down
            if entries[i][0] >= cutoff:
                had_drop = True
                drop_amount = prev_price - curr_price
                drop_date = entries[i][0].strftime('%Y-%m-%d')

    return had_drop, drop_amount, drop_date, original_price, current_price


def parse_listing(listing, county_key, lead_type, drop_info=None):
    """Build an INTEL-shaped lead from a RentCast listing record."""
    addr = listing.get('formattedAddress', '')
    city = listing.get('city', '')
    state = listing.get('state', '')
    zip_code = listing.get('zipCode', '')
    listed_date = listing.get('listedDate', '')
    days_on_market = listing.get('daysOnMarket', 0)
    price = listing.get('price', 0)

    agent = listing.get('listingAgent') or {}
    office = listing.get('listingOffice') or {}

    # Stable lead ID — must be DETERMINISTIC across runs (Python's hash() is randomized,
    # which previously caused duplicates on every run). Use MD5 of address+type.
    rentcast_id = listing.get('id', '')
    # Hash on address (most stable identifier) rather than rentcast_id which may change
    hash_input = f"{addr}|{lead_type}".lower()
    lead_id = f'rc-{lead_type[:2]}-' + hashlib.md5(hash_input.encode()).hexdigest()[:10]

    base = {
        'id':            lead_id,
        'county':        county_key,
        'type':          lead_type,
        'owner':         f"Listing Agent: {agent.get('name', 'Unknown')}",
        'address':       addr,
        'amount':        f"${price:,}" if price else None,
        'filingDate':    listed_date,
        'caseNumber':    listing.get('mlsNumber', ''),
        'phone':         agent.get('phone'),
        'agentName':     agent.get('name', ''),
        'agentPhone':    agent.get('phone', ''),
        'agentEmail':    agent.get('email', ''),
        'officeName':    office.get('name', ''),
        'score':         50,  # computed in frontend
        'scrapedAt':     now_iso(),
        'propertyType':  listing.get('propertyType', ''),
        'bedrooms':      listing.get('bedrooms'),
        'bathrooms':     listing.get('bathrooms'),
        'sqft':          listing.get('squareFootage'),
        'lotSize':       listing.get('lotSize'),
        'yearBuilt':     listing.get('yearBuilt'),
        'listingPrice':  price,
        'mlsStatus':     listing.get('status', ''),
        'mlsName':       listing.get('mlsName', ''),
        'daysOnMarket':  days_on_market,
        'listedDate':    listed_date,
        'zipCode':       zip_code,
        'rentcastId':    rentcast_id,
        'source':        'RentCast',
    }

    if lead_type == 'price-drop' and drop_info:
        had_drop, drop_amount, drop_date, original_price, current_price = drop_info
        pct = round((drop_amount / original_price) * 100, 1) if original_price else 0
        base['priceReduced']       = True
        base['priceDropAmount']    = drop_amount
        base['priceDropDate']      = drop_date
        base['priceDropPct']       = pct
        base['originalPrice']      = original_price
        base['currentPrice']       = current_price
        base['notes']              = (f"Price dropped ${drop_amount:,} ({pct}%) "
                                       f"on {drop_date} · {days_on_market}d on market "
                                       f"· now ${current_price:,}")
    elif lead_type == 'stale-listing':
        base['notes'] = (f"Active {days_on_market}d on market · ${price:,}"
                          f"{' · ' + str(listing.get('bedrooms')) + 'bd' if listing.get('bedrooms') else ''}")
        base['highEquity']    = False  # we don't know without skip trace
        base['vacant']        = False
        base['absenteeOwner'] = False
    elif lead_type == 'failed-listing':
        removed = listing.get('removedDate', '')
        days_since_failed = 0
        if removed:
            try:
                rd = datetime.fromisoformat(removed.replace('Z', '+00:00'))
                days_since_failed = (datetime.now(timezone.utc) - rd).days
            except Exception:
                pass
        # Pull the actual most-recent status from history (not hardcoded "EXPIRED")
        actual_status = _get_latest_status(listing) or 'INACTIVE'
        base['daysSinceFailed'] = days_since_failed
        base['removedDate']     = removed
        base['mlsStatus']       = actual_status.upper()
        base['notes']           = (f"{actual_status} {days_since_failed}d ago · "
                                    f"was ${price:,} · {days_on_market}d on market")
        base['highEquity']      = False
        base['vacant']          = False
        base['absenteeOwner']   = False
    else:
        base['notes'] = (f"Listed {days_on_market}d ago · ${price:,}"
                          f"{' · ' + str(listing.get('bedrooms')) + 'bd' if listing.get('bedrooms') else ''}")

    return base


def load_existing(market_key, lead_type):
    """Read existing JSON for this market+type. Empty dict if none.

    Also dedupes by address — earlier scraper versions used Python's randomized
    hash() which generated different IDs for the same property each run, causing
    duplicates in the saved file. We re-key by address+type to collapse those.

    For failed-listing files: also drops any existing entry whose mlsStatus
    matches an excluded keyword (cleans up old "EXPIRED"-labeled pendings).
    """
    path = DATA_DIR / f'leads-{market_key}-{lead_type}.json'
    if not path.exists():
        return {}
    try:
        with open(path) as f:
            d = json.load(f)
        leads = d.get('leads', [])

        # Re-key everything by deterministic ID (address-based hash)
        # If multiple old records collide on the new ID, keep the one with phone data
        deduped = {}
        excluded = 0
        for lead in leads:
            addr = lead.get('address', '')
            if not addr:
                continue

            # For failed listings: drop any entry whose status matches excluded keywords
            # (cleans up old data where pending/sold were saved as "EXPIRED")
            if lead_type == 'failed-listing':
                status_str = str(lead.get('mlsStatus') or '').lower()
                notes_str  = str(lead.get('notes') or '').lower()
                if any(excl in status_str or excl in notes_str for excl in EXCLUDED_END_STATUSES):
                    excluded += 1
                    continue

            hash_input = f"{addr}|{lead_type}".lower()
            new_id = f'rc-{lead_type[:2]}-' + hashlib.md5(hash_input.encode()).hexdigest()[:10]
            lead['id'] = new_id  # rewrite to new stable ID

            if new_id in deduped:
                # Collision — keep the better of the two
                existing = deduped[new_id]
                if lead.get('phone') and not existing.get('phone'):
                    deduped[new_id] = lead  # new one has phone, prefer it
                # else keep existing
            else:
                deduped[new_id] = lead

        if len(deduped) < len(leads):
            log.info(f'  Cleaned {len(leads) - len(deduped)} {lead_type} entries from existing file ({excluded} status-excluded, rest duplicates)')

        return deduped
    except Exception as e:
        log.warning(f'  Could not read {path}: {e}')
        return {}


def save(market_key, lead_type, leads_list, raw_count):
    """Write JSON to disk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f'leads-{market_key}-{lead_type}.json'
    with open(path, 'w') as f:
        json.dump({
            'lastUpdated': now_iso(),
            'totalLeads':  len(leads_list),
            'totalFound':  raw_count,
            'source':      'RentCast',
            'leadType':    lead_type,
            'market':      market_key,
            'leads':       leads_list,
        }, f, indent=2)
    log.info(f'  Saved {len(leads_list)} {lead_type} leads → {path.name}')


def scrape_market(key, info):
    """Scrape all zips for one market: new + price-drop + stale + failed buckets."""
    log.info(f'\n{"=" * 50}')
    log.info(f'{key.upper()} — {info["city"]}, {info["state"]} ({len(info["zips"])} zips)')
    log.info('=' * 50)

    existing_new    = load_existing(key, 'new-listing')
    existing_drop   = load_existing(key, 'price-drop')
    existing_stale  = load_existing(key, 'stale-listing')
    existing_failed = load_existing(key, 'failed-listing')

    new_leads, drop_leads, stale_leads, failed_leads = {}, {}, {}, {}
    total_active    = 0
    total_inactive  = 0
    fetch_succeeded = False

    # ── Pass 1: ACTIVE listings (covers new + price-drop + stale all at once) ────
    # One call per zip returns up to 500 active listings; we filter client-side
    # so the cost is the same regardless of how many lead buckets we make from it.
    for zip_code in info['zips']:
        listings = fetch_listings(zip_code, status='Active')
        if listings:
            fetch_succeeded = True

        # Filter to allowed property types (skip condos, land, etc.)
        listings = [l for l in listings if l.get('propertyType') in ALLOWED_PROPERTY_TYPES]
        total_active += len(listings)

        for listing in listings:
            days = listing.get('daysOnMarket', 9999) or 9999

            # New listing — 0-7 days
            if days <= NEW_LISTING_MAX_DAYS:
                lead = parse_listing(listing, info['county'], 'new-listing')
                new_leads[lead['id']] = lead

            # Stale listing — 90-180 days, still active
            if STALE_MIN_DAYS <= days <= STALE_MAX_DAYS:
                lead = parse_listing(listing, info['county'], 'stale-listing')
                stale_leads[lead['id']] = lead

            # Price drop — any active listing with a drop in last 30 days
            history = listing.get('history') or {}
            drop_info = parse_history_for_price_drop(history)
            if drop_info[0]:
                lead = parse_listing(listing, info['county'], 'price-drop', drop_info)
                drop_leads[lead['id']] = lead

        log.info(f'  zip {zip_code} ACTIVE: {len(listings)} listings')

    # ── Pass 2: INACTIVE listings filtered to last 14 days (failed listings) ────
    # daysOld here means how many days ago the listing first went up — not what we want.
    # We pull all inactive and filter by removedDate client-side.
    for zip_code in info['zips']:
        listings = fetch_listings(zip_code, status='Inactive')
        if listings:
            fetch_succeeded = True

        # Filter to allowed property types
        listings = [l for l in listings if l.get('propertyType') in ALLOWED_PROPERTY_TYPES]
        total_inactive += len(listings)

        excluded_count = 0
        prior_failed_count = len(failed_leads)

        for listing in listings:
            # Only keep if removedDate is within FAILED_MAX_DAYS_AGO
            removed = listing.get('removedDate')
            if not removed:
                continue
            try:
                rd = datetime.fromisoformat(removed.replace('Z', '+00:00'))
                days_ago = (datetime.now(timezone.utc) - rd).days
                if days_ago > FAILED_MAX_DAYS_AGO or days_ago < 0:
                    continue
            except Exception:
                continue

            # Skip if the listing ended for a non-failure reason (pending/sold/etc.)
            # Check both top-level mlsStatus and the most recent history entry
            if _is_excluded_status(listing):
                excluded_count += 1
                continue

            lead = parse_listing(listing, info['county'], 'failed-listing')
            failed_leads[lead['id']] = lead

        log.info(f'  zip {zip_code} INACTIVE: {len(listings)} pulled, {excluded_count} excluded (pending/sold/etc.) → {len([k for k in failed_leads if k.startswith("rc-fa-")]) - prior_failed_count} kept')

        log.info(f'  zip {zip_code} INACTIVE: {len(listings)} (kept within 14d window)')

    # Preserve existing data on total fetch failure
    if not fetch_succeeded:
        log.warning(f'  ⚠ All fetches failed — preserving existing data for {key}')
        return 0, 0, 0, 0

    # ── Merge each bucket with existing (preserve traced phones across runs) ──
    def merge(new_set, existing):
        out = dict(existing)
        for lid, lead in new_set.items():
            if lid in existing and existing[lid].get('phone'):
                lead['phone'] = existing[lid]['phone']
            out[lid] = lead
        return out

    merged_new    = merge(new_leads,    existing_new)
    merged_drop   = merge(drop_leads,   existing_drop)
    merged_stale  = merge(stale_leads,  existing_stale)
    # Failed listings: REBUILD FRESH each run — don't merge with existing.
    # The 14-day rolling window means leads drop off naturally; merging keeps
    # stale entries from old scraper versions that mislabeled pending as expired.
    # Preserve phones from existing failed leads though.
    merged_failed = {}
    for lid, lead in failed_leads.items():
        if lid in existing_failed and existing_failed[lid].get('phone'):
            lead['phone'] = existing_failed[lid]['phone']
        merged_failed[lid] = lead

    save(key, 'new-listing',    sorted(merged_new.values(),   key=lambda l: l.get('daysOnMarket', 999)),        total_active)
    save(key, 'price-drop',     sorted(merged_drop.values(),  key=lambda l: l.get('priceDropPct', 0), reverse=True), total_active)
    save(key, 'stale-listing',  sorted(merged_stale.values(), key=lambda l: l.get('daysOnMarket', 0), reverse=True), total_active)
    save(key, 'failed-listing', sorted(merged_failed.values(),key=lambda l: l.get('daysSinceFailed', 999)),     total_inactive)

    return len(merged_new), len(merged_drop), len(merged_stale), len(merged_failed)


def main():
    log.info('=' * 60)
    log.info('RentCast Listings Scraper')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

    if not API_KEY:
        log.error('RENTCAST_API_KEY not set — exiting')
        sys.exit(1)

    totals = {}
    for key, info in MARKETS.items():
        n, d, s, f = scrape_market(key, info)
        totals[key] = (n, d, s, f)

    log.info('\n' + '=' * 60)
    log.info('LISTINGS SCRAPE COMPLETE')
    grand = [0, 0, 0, 0]
    for k, (n, d, s, f) in totals.items():
        log.info(f'  {k.upper()}: {n} new · {d} drops · {s} stale · {f} failed')
        grand[0] += n; grand[1] += d; grand[2] += s; grand[3] += f
    log.info(f'  TOTAL: {grand[0]} new · {grand[1]} drops · {grand[2]} stale · {grand[3]} failed')
    log.info('=' * 60)


if __name__ == '__main__':
    main()

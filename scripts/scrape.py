#!/usr/bin/env python3
"""
INTEL Scraper v5 — Multi-County Lead Intelligence
==================================================
Scrapes all active counties and writes separate JSON files.

Counties:
  - Sedgwick, KS  → data/leads-sedgwick.json
  - Harris, TX    → data/leads-harris.json
  - Shelby, TN    → data/leads-shelby.json
  - Clark, NV     → data/leads-clark.json
  - Maricopa, AZ  → data/leads-maricopa.json

Also writes data/leads.json (Sedgwick alias for backwards compat)
"""

import json, re, hashlib, time, logging, io
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
log = logging.getLogger('intel')

DATA_DIR = Path(__file__).parent.parent / 'data'

# ── Helpers ───────────────────────────────────────────────────────────────────
def make_id(*parts):
    return hashlib.md5('|'.join(str(p) for p in parts).encode()).hexdigest()[:12]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fmt_amount(s):
    if not s: return None
    clean = re.sub(r'[^\d.]', '', str(s).replace(',', ''))
    try:
        v = float(clean)
        return f'${v:,.2f}' if v > 0 else None
    except: return None

def norm_addr(raw, city='', state=''):
    if not raw: return ''
    a = re.sub(r'\s+', ' ', raw).strip()

    # Strip trailing zip codes (5 or 9 digit) embedded by county records
    a = re.sub(r'\s*\d{5}(?:-?\d{4})?\s*$', '', a).strip()

    # Strip trailing state abbreviation if already present (e.g. "...KS" or "... KS")
    a = re.sub(r'\s+[A-Z]{2}\s*$', '', a).strip()

    # Strip trailing city name if already present (case-insensitive)
    if city and re.search(re.escape(city), a, re.IGNORECASE):
        # Remove the city and anything after it (city was embedded mid-string from county format)
        a = re.sub(re.escape(city) + r'.*$', '', a, flags=re.IGNORECASE).strip().rstrip(',').strip()

    # Append clean city, state suffix
    if city or state:
        suffix = ', ' + ' '.join(filter(None, [city, state]))
        a = a + suffix

    return a

def lead(county, ltype, owner, address, amount=None, date=None, case=None, notes=None):
    return {
        'id':         make_id(county, ltype, owner, address),
        'county':     county,
        'type':       ltype,
        'owner':      (owner or '').upper().strip(),
        'address':    address or '',
        'amount':     fmt_amount(amount),
        'filingDate': date,
        'caseNumber': case,
        'notes':      notes or '',
        'scrapedAt':  now_iso(),
    }

def dedup(leads):
    seen, out = set(), []
    for l in leads:
        if l['id'] not in seen:
            seen.add(l['id'])
            out.append(l)
    return out

def save(county, leads):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f'leads-{county}.json'

    # Load existing leads and keep those scraped within the last 7 days
    existing = []
    if path.exists():
        try:
            with open(path) as f:
                old_data = json.load(f)
            cutoff = datetime.now(timezone.utc).timestamp() - (7 * 24 * 3600)
            for l in old_data.get('leads', []):
                try:
                    scraped_ts = datetime.fromisoformat(l['scrapedAt']).timestamp()
                    if scraped_ts >= cutoff:
                        existing.append(l)
                except Exception:
                    existing.append(l)  # keep if we can't parse date
            log.info(f'  Loaded {len(existing)} existing leads (≤7 days) from {path.name}')
        except Exception as e:
            log.warning(f'  Could not load existing leads: {e}')

    # Merge: new leads take precedence (freshen scrapedAt), old leads fill the rest
    new_ids = {l['id'] for l in leads}
    merged = leads + [l for l in existing if l['id'] not in new_ids]

    order = {'tax-foreclosure':0,'probate':1,'state-warrant':2,'tax-delinquent':3}
    merged = dedup(merged)
    merged.sort(key=lambda l: order.get(l['type'], 9))

    output = {
        'lastUpdated': now_iso(),
        'county':      county,
        'totalLeads':  len(merged),
        'sources': {
            'tax_delinquent':  len([l for l in merged if l['type']=='tax-delinquent']),
            'tax_foreclosure': len([l for l in merged if l['type']=='tax-foreclosure']),
            'probate':         len([l for l in merged if l['type']=='probate']),
            'state_warrant':   len([l for l in merged if l['type']=='state-warrant']),
        },
        'leads': merged,
    }
    with open(path, 'w') as f:
        json.dump(output, f, indent=2)
    log.info(f'  Saved {len(merged)} total leads ({len(leads)} new + {len(merged)-len(leads)} retained) → {path}')
    return output


# ══════════════════════════════════════════════════════════════════════════════
# SEDGWICK COUNTY, KS
# ══════════════════════════════════════════════════════════════════════════════
def scrape_sedgwick(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('SEDGWICK COUNTY, KS')
    log.info('='*50)

    # 1. Delinquent Real Estate Taxes (A-Z sweep)
    log.info('  Scraping tax delinquent (A-Z)...')
    try:
        page.goto('https://ssc.sedgwickcounty.org/propertytax/delinquenciesintro.aspx',
                  wait_until='networkidle', timeout=30000)
        page.click('input[type="submit"]', timeout=5000)
        page.wait_for_load_state('networkidle', timeout=15000)

        seen = set()
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            try:
                page.fill('input[name="ctl00$mainContentPlaceHolder$keywordsTextBox_TextBox"]', letter)
                page.click('input[name="ctl00$mainContentPlaceHolder$searchButton"]')
                page.wait_for_load_state('networkidle', timeout=15000)
                for table in page.query_selector_all('table'):
                    for row in table.query_selector_all('tr')[1:]:
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue
                        owner = cells[0]
                        if re.match(r'^(name|owner|taxpayer|address|total)', owner, re.I): continue
                        if len(owner) < 3: continue
                        addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), '')
                        amt  = next((c for c in cells if re.match(r'^\$[\d,]+', c)), '')
                        uid  = make_id('sedgwick', 'td', owner, addr)
                        if uid in seen: continue
                        seen.add(uid)
                        leads.append(lead('sedgwick','tax-delinquent', owner,
                                         norm_addr(addr,'Wichita','KS'), amt,
                                         notes='Real estate tax delinquent — Sedgwick County Treasurer'))
                time.sleep(0.4)
            except: continue
        log.info(f'  → {len(leads)} tax delinquent')
    except Exception as e:
        log.warning(f'  x Sedgwick tax delinquent: {e}')

    # Reset page state before next navigation
    try:
        page.goto('about:blank', wait_until='domcontentloaded', timeout=5000)
    except: pass
    time.sleep(2)

    # 2. Tax Foreclosure Auction (seasonal)
    try:
        page.goto('https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/',
                  wait_until='networkidle', timeout=30000)
        text = page.inner_text('body')
        if 'concluded' not in text.lower():
            date_m = re.search(r'(\w+ \d{1,2},?\s*202\d)', text)
            auction_date = date_m.group(1) if date_m else None
            pdfs = [a.get_attribute('href') for a in page.query_selector_all('a[href*=".pdf"]')]
            if auction_date or pdfs:
                leads.append(lead('sedgwick','tax-foreclosure',
                                  'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                                  'Sedgwick County, KS', None, auction_date,
                                  notes='Tax foreclosure auction. Visit sedgwickcounty.org/treasurer'))
    except Exception as e:
        log.warning(f'  x Sedgwick tax foreclosure: {e}')

    # 3. KDOR State Tax Warrants — Playwright only (JS required to populate table)
    log.info('  Scraping KDOR warrants...')
    sw_count = 0

    def parse_kdor_html(html, county_filter):
        from bs4 import BeautifulSoup
        found = []
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            return found
        rows = table.find_all('tr')
        log.info(f'    KDOR table rows: {len(rows)}')
        hdr_done = False
        for row in rows:
            cells = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
            if not cells or not cells[0]: continue
            if not hdr_done:
                hdr_done = True
                if any(h.lower() in ('name','taxpayer','county','amount') for h in cells):
                    continue
            name, addr, county_col, amt, case_num = (cells+['','','','',''])[:5]
            if not name or len(name) < 2: continue
            if county_col and county_col.strip() and county_filter not in county_col.lower(): continue
            parts = re.split(r'\xa0{2,}|\s{3,}', name)
            owner = parts[0].strip()
            address = parts[1].strip() if len(parts) > 1 else addr
            found.append((owner, address, amt, case_num))
        return found

    try:
        for wtype, lbl in [('i','individual'),('b','business')]:
            url = f'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch?type={wtype}'
            page.goto(url, wait_until='networkidle', timeout=25000)
            time.sleep(2)
            html = page.content()
            matches = parse_kdor_html(html, 'sedgwick')
            log.info(f'    [{lbl}] matches found: {len(matches)}')
            for owner, address, amt, case_num in matches:
                leads.append(lead('sedgwick','state-warrant', owner,
                                  norm_addr(address,'Wichita','KS'), amt, None, case_num,
                                  notes=f'Kansas DOR state tax warrant ({lbl})'))
                sw_count += 1
    except Exception as e:
        log.warning(f'  x Sedgwick warrants: {e}')
    log.info(f'  → {sw_count} state warrants')

    return save('sedgwick', leads)


# ══════════════════════════════════════════════════════════════════════════════
# HARRIS COUNTY, TX  (Houston)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_harris(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('HARRIS COUNTY, TX')
    log.info('='*50)

    # 1. Tax Sale Listing (public, fully posted)
    log.info('  Scraping tax sale listing...')
    try:
        page.goto('https://www.hctax.net/Property/listings/taxsalelisting',
                  wait_until='networkidle', timeout=30000)
        text = page.inner_text('body')

        # Parse property listings — each entry has an account number and address
        # Pattern: "Account No: XXXXXXXXX ... More Commonly Known As, ADDRESS"
        addr_matches = re.findall(
            r'MORE COMMONLY KNOWN AS[,\s]+([^\.]+?)(?:\.|ACCOUNT|LOT|TRACT|$)',
            text.upper()
        )
        acct_matches = re.findall(r'ACCOUNT (?:NO|NUMBER)[:\s#]+([0-9\-]+)', text.upper())

        for i, addr in enumerate(addr_matches):
            addr = addr.strip()
            if not addr or len(addr) < 5: continue
            case = acct_matches[i] if i < len(acct_matches) else None
            leads.append(lead('harris','tax-foreclosure',
                              'SEE HARRIS COUNTY RECORDS',
                              addr + ', Houston TX', None, None, case,
                              notes='Harris County tax sale listing — hctax.net'))

        log.info(f'  → {len(leads)} tax sale properties')
    except Exception as e:
        log.warning(f'  x Harris tax sale: {e}')

    # 2. Delinquent Tax Search (A-Z sweep on hctax.net)
    log.info('  Scraping delinquent tax search...')
    td_count = 0
    try:
        page.goto('https://www.hctax.net/Property/DelinquentTax',
                  wait_until='networkidle', timeout=30000)

        seen = set()
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            try:
                # Find and fill search box
                for sel in ['input[name*="search"]','input[name*="name"]',
                            'input[type="text"]','#txtOwnerName']:
                    try:
                        page.fill(sel, letter, timeout=3000)
                        break
                    except: continue

                # Submit
                for sel in ['input[type="submit"]','button[type="submit"]','#btnSearch']:
                    try:
                        page.click(sel, timeout=3000)
                        page.wait_for_load_state('networkidle', timeout=12000)
                        break
                    except: continue

                for table in page.query_selector_all('table'):
                    for row in table.query_selector_all('tr')[1:]:
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue
                        owner = cells[0]
                        if re.match(r'^(name|owner|acct|account)', owner, re.I): continue
                        if len(owner) < 3: continue
                        addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), '')
                        amt  = next((c for c in cells if re.match(r'^\$[\d,]+', c)), '')
                        uid  = make_id('harris','td', owner, addr)
                        if uid in seen: continue
                        seen.add(uid)
                        leads.append(lead('harris','tax-delinquent', owner,
                                         norm_addr(addr,'Houston','TX'), amt,
                                         notes='Real estate tax delinquent — Harris County Tax Office'))
                        td_count += 1
                time.sleep(0.4)
            except: continue

        log.info(f'  → {td_count} tax delinquent')
    except Exception as e:
        log.warning(f'  x Harris tax delinquent: {e}')

    # 3. Probate Records — Harris County Clerk
    log.info('  Scraping probate records...')
    pr_count = 0
    try:
        page.goto('https://www.cclerk.hctx.net/applications/websearch/RP.aspx',
                  wait_until='networkidle', timeout=20000)

        # Search for "Estate" document type
        for sel_sel in ['select[name*="type"]','select[id*="type"]','#docType']:
            try:
                page.select_option(sel_sel, label='LIS PENDENS', timeout=3000)
                break
            except:
                try:
                    page.select_option(sel_sel, index=1, timeout=2000)
                except: continue

        text = page.inner_text('body')
        for name in re.findall(r'Estate of\s+([A-Z][A-Z\s,\.]{3,50}?)(?:\n|\r|$)', text):
            name = name.strip().rstrip(',.')
            if len(name) > 3:
                leads.append(lead('harris','probate', f'Estate of {name}',
                                  'Houston TX — run skip trace for address', None, None, None,
                                  notes='Probate filing — Harris County Clerk'))
                pr_count += 1
        log.info(f'  → {pr_count} probate leads')
    except Exception as e:
        log.warning(f'  x Harris probate: {e}')

    return save('harris', leads)


# ══════════════════════════════════════════════════════════════════════════════
# SHELBY COUNTY, TN  (Memphis)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_shelby(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('SHELBY COUNTY, TN')
    log.info('='*50)

    # 1. Delinquent Tax Lookup
    log.info('  Scraping delinquent taxes...')
    try:
        page.goto('https://www.shelbycountytrustee.com/103/Tax-Look-Up',
                  wait_until='networkidle', timeout=30000)

        seen = set()
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            try:
                for sel in ['input[name*="name"]','input[name*="search"]',
                            'input[type="text"]','#txtName']:
                    try:
                        page.fill(sel, letter, timeout=3000)
                        break
                    except: continue

                for sel in ['input[type="submit"]','button[type="submit"]','#btnSearch']:
                    try:
                        page.click(sel, timeout=3000)
                        page.wait_for_load_state('networkidle', timeout=12000)
                        break
                    except: continue

                for table in page.query_selector_all('table'):
                    for row in table.query_selector_all('tr')[1:]:
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue
                        owner = cells[0]
                        if re.match(r'^(name|owner|taxpayer|parcel)', owner, re.I): continue
                        if len(owner) < 3: continue
                        addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), '')
                        amt  = next((c for c in cells if re.match(r'^\$[\d,]+', c)), '')
                        uid  = make_id('shelby','td', owner, addr)
                        if uid in seen: continue
                        seen.add(uid)
                        leads.append(lead('shelby','tax-delinquent', owner,
                                         norm_addr(addr,'Memphis','TN'), amt,
                                         notes='Real estate tax delinquent — Shelby County Trustee'))
                time.sleep(0.4)
            except: continue

        log.info(f'  → {len([l for l in leads if l["type"]=="tax-delinquent"])} tax delinquent')
    except Exception as e:
        log.warning(f'  x Shelby tax delinquent: {e}')

    # 2. Probate Court Records
    log.info('  Scraping probate records...')
    try:
        page.goto('https://www.shelbycountytn.gov/3666/How-to-Search-for-Documents',
                  wait_until='networkidle', timeout=20000)

        # Navigate to actual probate search
        for a in page.query_selector_all('a'):
            href = a.get_attribute('href') or ''
            txt  = (a.inner_text() or '').lower()
            if 'probate' in txt and ('search' in txt or 'case' in txt):
                try:
                    page.goto(href if href.startswith('http') else 'https://www.shelbycountytn.gov' + href,
                              wait_until='networkidle', timeout=15000)
                    break
                except: continue

        text = page.inner_text('body')
        seen = set()
        for name in re.findall(r'(?:Estate of|In re[:\s]+)\s*([A-Z][A-Z\s,\.]{3,50}?)(?:\n|\r|Docket|Case)', text):
            name = name.strip().rstrip(',.')
            if len(name) > 3 and name not in seen:
                seen.add(name)
                leads.append(lead('shelby','probate', f'Estate of {name}',
                                  'Memphis TN — run skip trace for address', None, None, None,
                                  notes='Probate filing — Shelby County. NOTE: TN 2025 wholesaling disclosure law applies.'))

        log.info(f'  → {len([l for l in leads if l["type"]=="probate"])} probate leads')
    except Exception as e:
        log.warning(f'  x Shelby probate: {e}')

    # 3. Register of Deeds — Lis Pendens
    log.info('  Scraping Register of Deeds (Lis Pendens)...')
    try:
        page.goto('https://search.register.shelby.tn.us/search/index.php',
                  wait_until='networkidle', timeout=20000)

        # Select Lis Pendens document type if available
        for sel in ['select[name*="type"]','select[name*="doc"]','#docType']:
            try:
                page.select_option(sel, label='LIS PENDENS', timeout=3000)
                break
            except:
                try:
                    page.select_option(sel, value='LP', timeout=2000)
                except: continue

        # Submit search
        for sel in ['input[type="submit"]','button[type="submit"]']:
            try:
                page.click(sel, timeout=3000)
                page.wait_for_load_state('networkidle', timeout=12000)
                break
            except: continue

        for table in page.query_selector_all('table'):
            for row in table.query_selector_all('tr')[1:]:
                cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                if len(cells) < 2 or not cells[0]: continue
                owner = cells[0]
                if len(owner) < 3 or re.match(r'^(name|grantor|grantee)', owner, re.I): continue
                addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), 'Memphis TN')
                leads.append(lead('shelby','lis-pendens', owner,
                                  norm_addr(addr,'Memphis','TN'), None, None, None,
                                  notes='Lis Pendens — Shelby County Register of Deeds. NOTE: TN 2025 wholesaling disclosure law applies.'))

        log.info(f'  → {len([l for l in leads if l["type"]=="lis-pendens"])} lis pendens')
    except Exception as e:
        log.warning(f'  x Shelby lis pendens: {e}')

    return save('shelby', leads)


# ══════════════════════════════════════════════════════════════════════════════
# CLARK COUNTY, NV  (Las Vegas)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_clark(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('CLARK COUNTY, NV')
    log.info('='*50)

    # 1. Delinquent Tax Properties (published on treasurer site)
    log.info('  Scraping delinquent tax list...')
    try:
        page.goto('https://www.clarkcountynv.gov/government/elected_officials/county_treasurer/real-property-tax-information',
                  wait_until='networkidle', timeout=30000)
        text = page.inner_text('body')

        # Look for link to delinquent list
        for a in page.query_selector_all('a'):
            href = a.get_attribute('href') or ''
            txt  = (a.inner_text() or '').lower()
            if ('delinquent' in txt or 'delinquent' in href.lower()) and \
               ('list' in txt or 'list' in href.lower() or '.pdf' in href.lower()):
                full = href if href.startswith('http') else 'https://www.clarkcountynv.gov' + href
                try:
                    page.goto(full, wait_until='networkidle', timeout=20000)
                    break
                except: continue

        text = page.inner_text('body')
        seen = set()
        for line in text.splitlines():
            line = line.strip()
            # Look for lines with address patterns
            if re.search(r'\d+\s+[NSEW]?\s*\w+\s+(?:St|Ave|Blvd|Dr|Ct|Pl|Rd|Ln|Way|Ter)', line, re.I):
                amt = re.search(r'\$[\d,]+\.?\d*', line)
                uid = make_id('clark','td', line[:60])
                if uid in seen: continue
                seen.add(uid)
                leads.append(lead('clark','tax-delinquent',
                                  'SEE CLARK COUNTY RECORDS',
                                  line[:100] + ('' if 'NV' in line else ', Las Vegas NV'),
                                  amt.group(0) if amt else None,
                                  notes='Real estate tax delinquent — Clark County Treasurer'))

        log.info(f'  → {len(leads)} tax delinquent')
    except Exception as e:
        log.warning(f'  x Clark tax delinquent: {e}')

    # 2. Tax Auction Info
    log.info('  Scraping tax auction...')
    try:
        page.goto('https://www.clarkcountynv.gov/government/elected_officials/county_treasurer/real_property_tax_auction_information.php',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        date_m = re.search(r'(\w+ \d{1,2},?\s*202\d)', text)
        auction_date = date_m.group(1) if date_m else None
        if auction_date:
            leads.append(lead('clark','tax-foreclosure',
                              'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                              'Clark County, NV', None, auction_date,
                              notes=f'Clark County tax auction. Date: {auction_date}'))
            log.info(f'  → Tax auction posted: {auction_date}')
    except Exception as e:
        log.warning(f'  x Clark tax auction: {e}')

    # 3. Tax Liens/Warrants (Recorder)
    log.info('  Scraping recorded liens...')
    try:
        page.goto('https://www.clarkcountynv.gov/government/elected_officials/county_recorder/liens.php',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        seen = set()
        for line in text.splitlines():
            line = line.strip()
            if len(line) < 5: continue
            amt_m = re.search(r'\$[\d,]+\.?\d*', line)
            if amt_m and re.search(r'[A-Z]{2,}', line):
                uid = make_id('clark','sw', line[:60])
                if uid in seen: continue
                seen.add(uid)
                leads.append(lead('clark','state-warrant',
                                  line.split('  ')[0][:60].strip(),
                                  'Clark County NV — verify address',
                                  amt_m.group(0),
                                  notes='Tax lien/warrant — Clark County Recorder'))

        log.info(f'  → {len([l for l in leads if l["type"]=="state-warrant"])} liens/warrants')
    except Exception as e:
        log.warning(f'  x Clark liens: {e}')

    return save('clark', leads)


# ══════════════════════════════════════════════════════════════════════════════
# MARICOPA COUNTY, AZ  (Phoenix)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_maricopa(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('MARICOPA COUNTY, AZ')
    log.info('='*50)

    # 1. Delinquent Parcels via GIS/Treasurer API
    log.info('  Scraping delinquent parcels...')
    try:
        # Maricopa publishes delinquent parcels — try the GIS data endpoint
        import urllib.request, json as jsonlib

        # Try the ArcGIS REST API behind their GIS map
        api_url = ('https://gis.maricopa.gov/arcgis/rest/services/TSR/LienDelinquentParcel/MapServer/0/query'
                   '?where=1%3D1&outFields=*&f=json&resultRecordCount=1000')
        req = urllib.request.Request(api_url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = jsonlib.loads(resp.read())
            features = data.get('features', [])
            log.info(f'  GIS API returned {len(features)} delinquent parcels')

            for feat in features:
                attrs = feat.get('attributes', {})
                # Common field names in Maricopa GIS
                owner   = (attrs.get('OWNER_NAME') or attrs.get('OwnerName') or
                           attrs.get('OWNNAME') or 'SEE MARICOPA RECORDS')
                address = (attrs.get('SITUS_ADDRESS') or attrs.get('SitusAddress') or
                           attrs.get('ADDRESS') or attrs.get('ADDR') or '')
                amount  = (attrs.get('TOTAL_DUE') or attrs.get('TotalDue') or
                           attrs.get('AMOUNT_DUE') or attrs.get('TAX_DUE') or '')
                parcel  = (attrs.get('PARCEL_NO') or attrs.get('ParcelNo') or
                           attrs.get('APN') or '')
                city    = attrs.get('SITUS_CITY') or 'Phoenix'

                if address:
                    address = f"{address}, {city} AZ"

                leads.append(lead('maricopa','tax-delinquent', owner,
                                  address or 'Maricopa County AZ',
                                  str(amount) if amount else None, None, parcel,
                                  notes='Delinquent parcel — Maricopa County Treasurer GIS'))

            log.info(f'  → {len(leads)} delinquent parcels from GIS')

        except Exception as e:
            log.warning(f'  GIS API failed ({e}), trying web scrape...')
            # Fallback: scrape the treasurer page
            page.goto('https://treasurer.maricopa.gov/', wait_until='networkidle', timeout=30000)
            for a in page.query_selector_all('a'):
                href = a.get_attribute('href') or ''
                txt  = (a.inner_text() or '').lower()
                if 'delinquent' in txt or 'overdue' in txt or 'lien' in txt:
                    full = href if href.startswith('http') else 'https://treasurer.maricopa.gov' + href
                    try:
                        page.goto(full, wait_until='networkidle', timeout=15000)
                        break
                    except: continue

            text = page.inner_text('body')
            seen = set()
            for line in text.splitlines():
                line = line.strip()
                amt_m = re.search(r'\$[\d,]+\.?\d*', line)
                addr_m = re.search(r'\d+\s+\w+.*?(?:St|Ave|Dr|Rd|Blvd|Ln|Way|Ct)', line, re.I)
                if amt_m and addr_m:
                    uid = make_id('maricopa','td', line[:60])
                    if uid in seen: continue
                    seen.add(uid)
                    leads.append(lead('maricopa','tax-delinquent',
                                      'SEE MARICOPA COUNTY RECORDS',
                                      addr_m.group(0) + ', Phoenix AZ',
                                      amt_m.group(0),
                                      notes='Delinquent property — Maricopa County Treasurer'))
            log.info(f'  → {len(leads)} leads from web scrape')

    except Exception as e:
        log.warning(f'  x Maricopa delinquent: {e}')

    # 2. Tax Lien Sale (annual — February)
    log.info('  Scraping tax lien sale info...')
    try:
        page.goto('https://treasurer.maricopa.gov/', wait_until='networkidle', timeout=30000)
        text = page.inner_text('body')
        date_m = re.search(r'(?:auction|sale|lien)[^\n]*(\w+ \d{1,2},?\s*202\d)', text, re.I)
        auction_date = date_m.group(1) if date_m else None

        if auction_date:
            leads.append(lead('maricopa','tax-foreclosure',
                              'ANNUAL TAX LIEN SALE — SEE MARICOPA TREASURER',
                              'Maricopa County, AZ', None, auction_date,
                              notes=f'Maricopa County annual tax lien auction. Date: {auction_date}. treasurer.maricopa.gov'))
            log.info(f'  → Tax lien sale posted: {auction_date}')
    except Exception as e:
        log.warning(f'  x Maricopa tax sale: {e}')

    # 3. Probate — Maricopa Superior Court
    log.info('  Scraping probate...')
    try:
        page.goto('https://apps.superiorcourt.maricopa.gov/docket/',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        seen = set()
        for name in re.findall(r'Estate of\s+([A-Z][A-Z\s,\.]{3,50}?)(?:\n|\r|$|Case)', text):
            name = name.strip().rstrip(',.')
            if len(name) > 3 and name not in seen:
                seen.add(name)
                leads.append(lead('maricopa','probate', f'Estate of {name}',
                                  'Maricopa County AZ — run skip trace', None, None, None,
                                  notes='Probate filing — Maricopa Superior Court'))
        log.info(f'  → {len([l for l in leads if l["type"]=="probate"])} probate leads')
    except Exception as e:
        log.warning(f'  x Maricopa probate: {e}')

    return save('maricopa', leads)


# ══════════════════════════════════════════════════════════════════════════════
# KDOR WARRANTS HELPER — reusable for any KS county
# ══════════════════════════════════════════════════════════════════════════════
def scrape_kdor_warrants(page, county_key, county_name, city, state='KS'):
    """Pull KS DOR state tax warrants via Playwright (JS required to populate table)."""
    leads = []

    def parse_kdor_html(html):
        from bs4 import BeautifulSoup
        found = []
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table: return found
        hdr_done = False
        for row in table.find_all('tr'):
            cells = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
            if not cells or not cells[0]: continue
            if not hdr_done:
                hdr_done = True
                if any(h.lower() in ('name','taxpayer','county','amount') for h in cells): continue
            name, addr, county_col, amt, case_num = (cells+['','','','',''])[:5]
            if not name or len(name) < 2: continue
            if county_col and county_col.strip() and county_name.lower() not in county_col.lower(): continue
            parts = re.split(r'\xa0{2,}|\s{3,}', name)
            owner = parts[0].strip()
            address = parts[1].strip() if len(parts) > 1 else addr
            found.append((owner, address, amt, case_num))
        return found

    try:
        for wtype in ['i', 'b']:
            url = f'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch?type={wtype}'
            page.goto(url, wait_until='networkidle', timeout=25000)
            time.sleep(2)
            matches = parse_kdor_html(page.content())
            for owner, address, amt, case_num in matches:
                leads.append(lead(county_key, 'state-warrant', owner,
                                  norm_addr(address, city, state), amt, None, case_num,
                                  notes=f'Kansas DOR state tax warrant'))
    except Exception as e:
        log.warning(f'  x KDOR warrants ({county_name}): {e}')

    log.info(f'  → {len(leads)} KDOR warrants')
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# HARVEY COUNTY, KS  (Newton)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_harvey(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('HARVEY COUNTY, KS')
    log.info('='*50)

    # 1. Delinquent tax search via CIC Hosting portal
    log.info('  Scraping tax delinquent...')
    try:
        page.goto('https://ks1355.cichosting.com/ttp/Tax/Search/search_tax.aspx',
                  wait_until='networkidle', timeout=30000)
        seen = set()
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            try:
                for sel in ['input[name*="name"]','input[name*="Name"]','input[type="text"]','#txtName']:
                    try:
                        page.fill(sel, letter, timeout=3000)
                        break
                    except: continue
                for sel in ['input[type="submit"]','button[type="submit"]','#btnSearch']:
                    try:
                        page.click(sel, timeout=3000)
                        page.wait_for_load_state('networkidle', timeout=12000)
                        break
                    except: continue
                for table in page.query_selector_all('table'):
                    for row in table.query_selector_all('tr')[1:]:
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue
                        owner = cells[0]
                        if re.match(r'^(name|owner|taxpayer)', owner, re.I) or len(owner) < 3: continue
                        addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), '')
                        amt  = next((c for c in cells if re.match(r'^\$[\d,]+', c)), '')
                        uid  = make_id('harvey','td', owner, addr)
                        if uid in seen: continue
                        seen.add(uid)
                        leads.append(lead('harvey','tax-delinquent', owner,
                                         norm_addr(addr,'Newton','KS'), amt,
                                         notes='Real estate tax delinquent — Harvey County Treasurer'))
                time.sleep(0.4)
            except: continue
        log.info(f'  → {len(leads)} tax delinquent')
    except Exception as e:
        log.warning(f'  x Harvey tax delinquent: {e}')

    # 2. Tax foreclosure auction (seasonal)
    try:
        page.goto('https://www.harveycounty.com/departments/treasurer/taxes.html',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        date_m = re.search(r'(\w+ \d{1,2},?\s*202\d)', text)
        if date_m and 'foreclosure' in text.lower():
            leads.append(lead('harvey','tax-foreclosure',
                              'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                              'Harvey County, KS', None, date_m.group(1),
                              notes='Harvey County tax foreclosure auction. Visit harveycounty.com'))
    except Exception as e:
        log.warning(f'  x Harvey tax foreclosure: {e}')

    # 3. KDOR state warrants
    log.info('  Scraping KDOR warrants...')
    leads += scrape_kdor_warrants(page, 'harvey', 'Harvey', 'Newton')

    return save('harvey', leads)


# ══════════════════════════════════════════════════════════════════════════════
# BUTLER COUNTY, KS  (El Dorado)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_butler(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('BUTLER COUNTY, KS')
    log.info('='*50)

    # 1. Delinquent tax listing (published on bucoks.gov in August/October)
    log.info('  Scraping delinquent tax listing...')
    try:
        page.goto('https://www.bucoks.gov/501/Real-Estate-Taxes',
                  wait_until='networkidle', timeout=30000)
        # Look for link to the delinquent list PDF or page
        for a in page.query_selector_all('a'):
            href = a.get_attribute('href') or ''
            txt  = (a.inner_text() or '').lower()
            if 'delinquent' in txt or 'delinquent' in href.lower():
                full = href if href.startswith('http') else 'https://www.bucoks.gov' + href
                try:
                    page.goto(full, wait_until='networkidle', timeout=15000)
                    break
                except: continue

        text = page.inner_text('body')
        seen = set()
        # Parse name + amount patterns from published list
        for line in text.splitlines():
            line = line.strip()
            if len(line) < 5: continue
            amt_m = re.search(r'\$[\d,]+\.?\d*', line)
            name_m = re.search(r'^([A-Z][A-Z\s,\.]{3,40})', line)
            if amt_m and name_m:
                owner = name_m.group(1).strip().rstrip(',')
                uid = make_id('butler','td', owner, line[:40])
                if uid in seen: continue
                seen.add(uid)
                leads.append(lead('butler','tax-delinquent', owner,
                                  'Butler County, KS — run skip trace for address',
                                  amt_m.group(0),
                                  notes='Real estate tax delinquent — Butler County Treasurer'))
        log.info(f'  → {len(leads)} tax delinquent')
    except Exception as e:
        log.warning(f'  x Butler tax delinquent: {e}')

    # 2. Tax foreclosure info
    try:
        page.goto('https://www.bucoks.gov/501/Real-Estate-Taxes',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        date_m = re.search(r'(\w+ \d{1,2},?\s*202\d)', text)
        if date_m and ('foreclosure' in text.lower() or 'auction' in text.lower()):
            leads.append(lead('butler','tax-foreclosure',
                              'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                              'Butler County, KS', None, date_m.group(1),
                              notes='Butler County tax foreclosure. Visit bucoks.gov'))
    except Exception as e:
        log.warning(f'  x Butler tax foreclosure: {e}')

    # 3. KDOR state warrants
    log.info('  Scraping KDOR warrants...')
    leads += scrape_kdor_warrants(page, 'butler', 'Butler', 'El Dorado')

    return save('butler', leads)


# ══════════════════════════════════════════════════════════════════════════════
# SUMNER COUNTY, KS  (Wellington)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_sumner(page):
    leads = []
    log.info('\n' + '='*50)
    log.info('SUMNER COUNTY, KS')
    log.info('='*50)

    # 1. Delinquent tax search via PublicAccessNow portal
    log.info('  Scraping tax delinquent...')
    try:
        page.goto('https://ks-search-sumner.publicaccessnow.com/tax/',
                  wait_until='networkidle', timeout=30000)
        seen = set()
        for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            try:
                for sel in ['input[name*="name"]','input[name*="Name"]',
                            'input[name*="last"]','input[type="text"]']:
                    try:
                        page.fill(sel, letter, timeout=3000)
                        break
                    except: continue
                for sel in ['input[type="submit"]','button[type="submit"]',
                            'button:has-text("Search")']:
                    try:
                        page.click(sel, timeout=3000)
                        page.wait_for_load_state('networkidle', timeout=12000)
                        break
                    except: continue
                for table in page.query_selector_all('table'):
                    for row in table.query_selector_all('tr')[1:]:
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue
                        owner = cells[0]
                        if re.match(r'^(name|owner|taxpayer)', owner, re.I) or len(owner) < 3: continue
                        addr = next((c for c in cells[1:] if re.search(r'\d+\s+\w+', c)), '')
                        amt  = next((c for c in cells if re.match(r'^\$[\d,]+', c)), '')
                        uid  = make_id('sumner','td', owner, addr)
                        if uid in seen: continue
                        seen.add(uid)
                        leads.append(lead('sumner','tax-delinquent', owner,
                                         norm_addr(addr,'Wellington','KS'), amt,
                                         notes='Real estate tax delinquent — Sumner County Treasurer'))
                time.sleep(0.4)
            except: continue
        log.info(f'  → {len(leads)} tax delinquent')
    except Exception as e:
        log.warning(f'  x Sumner tax delinquent: {e}')

    # 2. Tax foreclosure / sheriff sale
    try:
        page.goto('https://www.sumnersheriff.net/divisions/civil-process/sheriff-sales/',
                  wait_until='networkidle', timeout=20000)
        text = page.inner_text('body')
        seen = set()
        for line in text.splitlines():
            line = line.strip()
            if re.search(r'\d+\s+\w+.*?(St|Ave|Dr|Rd|Blvd|Ln)', line, re.I) and len(line) > 10:
                uid = make_id('sumner','tf', line[:60])
                if uid in seen: continue
                seen.add(uid)
                leads.append(lead('sumner','tax-foreclosure',
                                  'SEE SHERIFF SALE LISTING',
                                  line[:80] + ', Wellington KS',
                                  notes='Sumner County Sheriff Sale — sumnersheriff.net'))
        log.info(f'  → {len([l for l in leads if l["type"]=="tax-foreclosure"])} sheriff sales')
    except Exception as e:
        log.warning(f'  x Sumner sheriff sales: {e}')

    # 3. KDOR state warrants
    log.info('  Scraping KDOR warrants...')
    leads += scrape_kdor_warrants(page, 'sumner', 'Sumner', 'Wellington')

    return save('sumner', leads)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    log.info('=' * 60)
    log.info('INTEL Scraper v5 — Multi-County')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

    results = {}

    def new_page(browser):
        """Create a fresh page with standard settings."""
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            viewport={'width':1280,'height':900},
        )
        return ctx.new_page()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=['--no-sandbox','--disable-setuid-sandbox',
                  '--disable-dev-shm-usage','--disable-gpu']
        )

        # Fresh page per county — prevents navigation bleed between counties
        for county, fn in [
            ('sedgwick', scrape_sedgwick),
            ('harris',   scrape_harris),
            ('shelby',   scrape_shelby),
            ('clark',    scrape_clark),
            ('maricopa', scrape_maricopa),
            ('harvey',   scrape_harvey),
            ('butler',   scrape_butler),
            ('sumner',   scrape_sumner),
        ]:
            page = new_page(browser)
            try:
                results[county] = fn(page)
            except Exception as e:
                log.error(f'County {county} failed entirely: {e}')
                results[county] = {'totalLeads': 0, 'sources': {}}
            finally:
                try: page.context.close()
                except: pass

        browser.close()

    # Also write leads.json as Sedgwick alias (backwards compat)
    import shutil
    shutil.copy(DATA_DIR / 'leads-sedgwick.json', DATA_DIR / 'leads.json')

    # Write a master index
    index = {
        'lastUpdated': now_iso(),
        'counties': {
            k: {'totalLeads': v['totalLeads'], 'sources': v.get('sources', {})}
            for k, v in results.items()
        }
    }
    with open(DATA_DIR / 'index.json', 'w') as f:
        json.dump(index, f, indent=2)

    log.info('\n' + '=' * 60)
    log.info('ALL COUNTIES COMPLETE')
    total = sum(v['totalLeads'] for v in results.values())
    log.info(f'Total leads across all counties: {total}')
    for county, data in results.items():
        log.info(f'  {county.upper()}: {data["totalLeads"]}')
    log.info('=' * 60)

if __name__ == '__main__':
    main()

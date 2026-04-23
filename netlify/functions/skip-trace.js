// ── INTEL · Skip Trace via BatchData ────────────────────────────────────────
// Confirmed response path: result.data[0].persons[0].phones[].number
// Name format from county records: "PATRICK Y GAYNOR" → first=PATRICK, last=GAYNOR

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const API_KEY = process.env.BATCHDATA_API_KEY;
  if (!API_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'BatchData not configured' }) };
  }

  let body;
  try { body = JSON.parse(event.body); }
  catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { owner, address, county } = body;
  if (!owner && !address) {
    return { statusCode: 400, body: JSON.stringify({ error: 'owner or address required' }) };
  }

  // ── Parse address ──────────────────────────────────────────
  // Handles formats like:
  //   "123 Main St, Houston TX 77002"
  //   "123 Main St, Las Vegas NV"
  //   "123 Main St, Wichita, KS 67202"  (extra comma)
  //   "123 main st, las vegas nv 89121" (lowercase)
  const fullAddr = String(address || '').trim();
  const parts    = fullAddr.split(',').map(s => s.trim()).filter(Boolean);

  const street = parts[0] || '';

  // Everything after the street is city/state/zip — may be one or two comma-separated chunks
  const tail = parts.slice(1).join(' ').toUpperCase().trim();

  // Match: CITY (any words) STATE (2 letters) optional ZIP (5 digits)
  const csMatch = tail.match(/^(.+?)\s+([A-Z]{2})(?:\s+(\d{5}))?/);

  let city  = csMatch ? csMatch[1].trim() : '';
  let state = csMatch ? csMatch[2] : '';
  let zip   = csMatch?.[3] || '';

  // Fallbacks if regex failed — try to infer from county
  if (!state) {
    // County → state mapping for known markets
    const COUNTY_STATE = {
      sedgwick: 'KS', harvey: 'KS', butler: 'KS', sumner: 'KS',
      harris: 'TX', tarrant: 'TX', dallas: 'TX',
      clark: 'NV', maricopa: 'AZ', shelby: 'TN',
    };
    state = COUNTY_STATE[(county || '').toLowerCase()] || 'KS';
  }
  if (!city) {
    city = parts[parts.length - 1]?.split(/\s+/)[0] || 'Unknown';
  }
  if (!zip) {
    const zm = fullAddr.match(/\b(\d{5})\b/);
    if (zm) zip = zm[1];
  }

  // ── Parse owner name ───────────────────────────────────────
  // County records come as "PATRICK Y GAYNOR" (FIRST MIDDLE LAST)
  // or "GAYNOR PATRICK Y" — we'll try both orders
  const cleanName = (owner || '').replace(/^Estate of\s+/i, '').trim();
  const nameParts = cleanName.split(/\s+/).filter(Boolean);

  // Primary attempt: treat as FIRST [MIDDLE] LAST
  const firstName1 = nameParts[0] || '';
  const lastName1  = nameParts[nameParts.length - 1] || '';

  // Fallback: treat as LAST FIRST (some county records)
  const lastName2  = nameParts[0] || '';
  const firstName2 = nameParts.slice(1).join(' ') || '';

  // ── Extract phones + property type from confirmed response structure ────
  function extractData(data) {
    const phones = [];
    let propertyType = null;
    let bedrooms = null;
    let units = null;

    const records = data?.result?.data || [];
    records.forEach(rec => {
      // Property characteristics
      const prop = rec?.property || {};
      const chars = prop?.characteristics || prop?.building || {};
      propertyType = propertyType ||
        prop?.landUse || prop?.propertyType || prop?.useCode ||
        chars?.useCode || chars?.landUse || chars?.propertyType ||
        prop?.summary?.propClass || null;
      bedrooms = bedrooms || chars?.bedrooms || chars?.bedsCount || null;
      units    = units    || chars?.unitsCount || chars?.unitCount || prop?.units || null;

      // Phones
      (rec?.persons || []).forEach(person => {
        (person?.phones || []).forEach(ph => {
          const num = ph.number || ph.phone || ph.phoneNumber;
          if (num && String(num).replace(/\D/g,'').length >= 10) {
            phones.push({
              number:    formatPhone(String(num)),
              type:      ph.type || 'unknown',
              rank:      ph.rank || 99,
              reachable: ph.reachable || false,
              dnc:       ph.dnc || false,
              tcpa:      ph.tcpa || false,
              carrier:   ph.carrier || '',
            });
          }
        });
      });
    });

    // Sort: by rank, prefer mobile + reachable
    phones.sort((a,b) => {
      if (a.reachable && !b.reachable) return -1;
      if (b.reachable && !a.reachable) return 1;
      if (a.type === 'Mobile' && b.type !== 'Mobile') return -1;
      if (b.type === 'Mobile' && a.type !== 'Mobile') return 1;
      return a.rank - b.rank;
    });

    return { phones, propertyType, bedrooms, units };
  }

  async function doTrace(firstName, lastName) {
    const res = await fetch('https://api.batchdata.com/api/v3/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{
          propertyAddress: { street, city, state, zip },
          person:          { firstName, lastName },
        }]
      }),
    });
    return res.json();
  }

  try {
    // Attempt 1: FIRST LAST order
    const data1   = await doTrace(firstName1, lastName1);
    const res1    = extractData(data1);
    if (res1.phones.length > 0) {
      return success(res1, 'first-last');
    }

    // Attempt 2: LAST FIRST order (county record format)
    const data2   = await doTrace(firstName2, lastName2);
    const res2    = extractData(data2);
    if (res2.phones.length > 0) {
      return success(res2, 'last-first');
    }

    // Attempt 3: Address only — get current owner's phone regardless of name
    const res3raw = await fetch('https://api.batchdata.com/api/v3/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{ propertyAddress: { street, city, state, zip } }]
      }),
    });
    const data3 = await res3raw.json();
    const res3  = extractData(data3);
    if (res3.phones.length > 0) {
      return success(res3, 'address-only');
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        success: false,
        phone: null,
        message: 'No phone found',
        _debug: {
          version: 'v2-county-aware',
          parsed: { street, city, state, zip },
          attempts: ['first-last', 'last-first', 'address-only'],
          responseSnippet: JSON.stringify(data3).substring(0, 300),
        }
      })
    };

  } catch (err) {
    console.error('Skip trace error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Skip trace failed', detail: err.message })
    };
  }
};

function success({ phones, propertyType, bedrooms, units }, method) {
  return {
    statusCode: 200,
    body: JSON.stringify({
      success:      true,
      phone:        phones[0].number,
      allPhones:    phones,
      propertyType: propertyType || null,
      bedrooms:     bedrooms || null,
      units:        units || null,
      method,
    })
  };
}

function formatPhone(raw) {
  const digits = raw.replace(/\D/g, '').replace(/^1/, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  return raw;
}

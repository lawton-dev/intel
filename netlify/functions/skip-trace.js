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

  const { owner, address } = body;
  if (!owner && !address) {
    return { statusCode: 400, body: JSON.stringify({ error: 'owner or address required' }) };
  }

  // ── Parse address ──────────────────────────────────────────
  const addrParts = (address || '').split(',');
  const street    = (addrParts[0] || '').trim();
  const cityState = (addrParts[1] || '').trim();

  // Extract city, state, zip — strip zip+4
  const csMatch = cityState.match(/^(.*?)\s+([A-Z]{2})\s*(\d{5})?\d*/);
  const city    = csMatch ? csMatch[1].trim() : 'Wichita';
  const state   = csMatch ? csMatch[2] : 'KS';
  let   zip     = csMatch?.[3] || '';
  if (!zip) {
    const zm = (address || '').match(/(\d{5})\d*/);
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

  // ── Extract phones from confirmed response structure ────────
  function extractPhones(data) {
    const phones = [];
    const records = data?.result?.data || [];
    records.forEach(rec => {
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

    // Sort: by rank (BatchData already ranks them), prefer mobile + reachable
    phones.sort((a,b) => {
      if (a.reachable && !b.reachable) return -1;
      if (b.reachable && !a.reachable) return 1;
      if (a.type === 'Mobile' && b.type !== 'Mobile') return -1;
      if (b.type === 'Mobile' && a.type !== 'Mobile') return 1;
      return a.rank - b.rank;
    });

    return phones;
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
    const phones1 = extractPhones(data1);
    if (phones1.length > 0) {
      return success(phones1, 'first-last');
    }

    // Attempt 2: LAST FIRST order (county record format)
    const data2   = await doTrace(firstName2, lastName2);
    const phones2 = extractPhones(data2);
    if (phones2.length > 0) {
      return success(phones2, 'last-first');
    }

    // Attempt 3: Address only — get current owner's phone regardless of name
    const res3 = await fetch('https://api.batchdata.com/api/v3/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{ propertyAddress: { street, city, state, zip } }]
      }),
    });
    const data3   = await res3.json();
    const phones3 = extractPhones(data3);
    if (phones3.length > 0) {
      return success(phones3, 'address-only');
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ success: false, phone: null, message: 'No phone found' })
    };

  } catch (err) {
    console.error('Skip trace error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Skip trace failed', detail: err.message })
    };
  }
};

function success(phones, method) {
  return {
    statusCode: 200,
    body: JSON.stringify({
      success:   true,
      phone:     phones[0].number,
      allPhones: phones,
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

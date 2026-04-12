// ── INTEL · Skip Trace via BatchData ────────────────────────────────────────
// Endpoint: POST https://api.batchdata.com/api/v3/property/skip-trace
// Response: flat fields — PHONE1, PHONE2, etc. (not nested persons array)

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

  // Strip zip+4 (672032921 → 67203, 67203-2921 → 67203)
  const csMatch = cityState.match(/^(.*?)\s+([A-Z]{2})\s*(\d{5})?\d*/);
  const city    = csMatch ? csMatch[1].trim() : 'Wichita';
  const state   = csMatch ? csMatch[2] : 'KS';
  let   zip     = csMatch?.[3] || '';

  // Also try pulling zip from end of full address string
  if (!zip) {
    const zipMatch = address.match(/(\d{5})\d*/);
    if (zipMatch) zip = zipMatch[1];
  }

  // ── Parse owner name ───────────────────────────────────────
  const cleanName = (owner || '').replace(/^Estate of\s+/i, '').trim();
  const nameParts = cleanName.split(/\s+/);
  // County records: LAST FIRST format
  const lastName  = nameParts[0] || '';
  const firstName = nameParts.slice(1).join(' ') || '';

  // ── Helper: extract phones from BatchData v3 response ──────
  function extractPhones(data) {
    const phones = [];
    // v3 returns results array with flat phone fields
    const results = data?.results || data?.data || [];
    const record  = Array.isArray(results) ? results[0] : results;

    if (!record) return phones;

    // Flat phone fields: PHONE1, PHONE2, PHONE3 or phone1, phone2...
    for (let i = 1; i <= 10; i++) {
      const num = record[`PHONE${i}`] || record[`phone${i}`] ||
                  record[`Phone${i}`] || record[`phoneNumber${i}`];
      if (num && String(num).replace(/\D/g,'').length >= 10) {
        phones.push({
          number: formatPhone(String(num)),
          type:   record[`PHONE${i}_TYPE`] || record[`phone${i}Type`] || 'unknown',
          score:  record[`PHONE${i}_SCORE`] || 0,
        });
      }
    }

    // Also check nested phones array if present
    const nested = record?.phones || record?.phoneNumbers || [];
    nested.forEach(ph => {
      const num = ph.phone || ph.phoneNumber || ph.number || ph;
      if (num && String(num).replace(/\D/g,'').length >= 10) {
        phones.push({
          number: formatPhone(String(num)),
          type:   ph.type || ph.phoneType || 'unknown',
          score:  ph.score || ph.confidenceScore || 0,
        });
      }
    });

    // Sort: mobile first, then by score
    phones.sort((a,b) => {
      if (a.type?.toLowerCase().includes('mobile') && !b.type?.toLowerCase().includes('mobile')) return -1;
      if (b.type?.toLowerCase().includes('mobile') && !a.type?.toLowerCase().includes('mobile')) return 1;
      return (b.score - a.score);
    });

    return phones;
  }

  // ── BatchData v3 skip trace request ────────────────────────
  async function doSkipTrace(requestBody) {
    const res = await fetch('https://api.batchdata.com/api/v3/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify(requestBody),
    });
    return res.json();
  }

  try {
    // Attempt 1: Full address + name (zip cleaned to 5 digits)
    const data1 = await doSkipTrace({
      requests: [{
        propertyAddress: { street, city, state, zip },
        person:          { firstName, lastName },
      }]
    });
    console.log('Attempt 1 response:', JSON.stringify(data1).slice(0, 500));
    const phones1 = extractPhones(data1);
    if (phones1.length > 0) {
      return { statusCode: 200, body: JSON.stringify({ success: true, phone: phones1[0].number, allPhones: phones1, method: 'address+name' }) };
    }

    // Attempt 2: Address only (no name — let BatchData find current owner)
    const data2 = await doSkipTrace({
      requests: [{
        propertyAddress: { street, city, state, zip },
      }]
    });
    console.log('Attempt 2 response:', JSON.stringify(data2).slice(0, 500));
    const phones2 = extractPhones(data2);
    if (phones2.length > 0) {
      return { statusCode: 200, body: JSON.stringify({ success: true, phone: phones2[0].number, allPhones: phones2, method: 'address-only' }) };
    }

    // Attempt 3: Swap name order (FIRST LAST instead of LAST FIRST)
    if (nameParts.length >= 2) {
      const data3 = await doSkipTrace({
        requests: [{
          propertyAddress: { street, city, state, zip },
          person: {
            firstName: nameParts.slice(0, -1).join(' '),
            lastName:  nameParts[nameParts.length - 1],
          },
        }]
      });
      console.log('Attempt 3 response:', JSON.stringify(data3).slice(0, 500));
      const phones3 = extractPhones(data3);
      if (phones3.length > 0) {
        return { statusCode: 200, body: JSON.stringify({ success: true, phone: phones3[0].number, allPhones: phones3, method: 'name-swapped' }) };
      }
    }

    return { statusCode: 200, body: JSON.stringify({ success: false, phone: null, message: 'No phone found after 3 attempts' }) };

  } catch (err) {
    console.error('Skip trace error:', err);
    return { statusCode: 500, body: JSON.stringify({ error: 'Skip trace failed', detail: err.message }) };
  }
};

function formatPhone(raw) {
  const digits = raw.replace(/\D/g, '').replace(/^1/, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  return raw;
}

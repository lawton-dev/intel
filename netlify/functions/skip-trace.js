// ── INTEL · Skip Trace via BatchData ────────────────────────────────────────
// Fix 1: Strip zip+4 digits before sending (672032921 → 67203)
// Fix 2: Name-only fallback if address+name returns no phone

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
  let   zip       = (addrParts[2] || '').trim();

  // FIX 1: Strip zip+4 (e.g. 672032921 → 67203, 67203-2921 → 67203)
  zip = zip.replace(/[-\s]?\d{4}$/, '').trim();
  // Also strip from street if embedded (e.g. "1628 N PORTER AVE, WICHITA, KS 672032921")
  // Sometimes the zip is the last part of cityState instead
  const zipFromCityState = cityState.match(/(\d{5})(?:\d{4})?$/);
  if (!zip && zipFromCityState) {
    zip = zipFromCityState[1];
  }

  const csMatch = cityState.match(/^(.*?)\s+([A-Z]{2})\s*\d*/);
  const city    = csMatch ? csMatch[1].trim() : 'Wichita';
  const state   = csMatch ? csMatch[2] : 'KS';

  // ── Parse owner name ───────────────────────────────────────
  // Strip "Estate of" prefix for probate leads
  const cleanName  = (owner || '').replace(/^Estate of\s+/i, '').trim();
  const nameParts  = cleanName.split(/\s+/);
  // BatchData works best with firstName / lastName split
  // County records are usually "LAST FIRST M" format
  const lastName   = nameParts[0] || '';
  const firstName  = nameParts.slice(1).join(' ') || '';

  // ── Helper: extract phones from BatchData response ─────────
  function extractPhones(data) {
    const phones = [];
    const result  = data?.results?.[0] || data?.result || data;
    const persons = result?.persons || result?.owner?.persons || result?.owners || [];

    persons.forEach(p => {
      const phoneSources = p.phones || p.phoneNumbers || p.contactInfo?.phones || [];
      phoneSources.forEach(ph => {
        const num = ph.phone || ph.phoneNumber || ph.number || (typeof ph === 'string' ? ph : null);
        if (num && num.replace(/\D/g,'').length >= 10) {
          phones.push({
            number: formatPhone(num),
            type:   ph.phoneType || ph.type || 'unknown',
            score:  ph.confidenceScore || ph.score || 0,
          });
        }
      });
    });

    // Sort: mobile first, then by confidence score
    phones.sort((a,b) => {
      if (a.type === 'mobile' && b.type !== 'mobile') return -1;
      if (b.type === 'mobile' && a.type !== 'mobile') return 1;
      return (b.score - a.score);
    });

    return phones;
  }

  // ── Attempt 1: Full address + name ─────────────────────────
  try {
    const res1 = await fetch('https://api.batchdata.com/api/v1/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{
          propertyAddress: {
            street: street,
            city:   city,
            state:  state,
            zip:    zip,
          },
          person: {
            firstName: firstName,
            lastName:  lastName,
          }
        }]
      })
    });

    const data1  = await res1.json();
    const phones1 = extractPhones(data1);

    if (phones1.length > 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          success:   true,
          phone:     phones1[0].number,
          allPhones: phones1,
          method:    'address+name',
        })
      };
    }

    // ── Attempt 2: Name + city/state only (no address) ────────
    console.log('Attempt 1 returned no phones — trying name-only fallback');

    const res2 = await fetch('https://api.batchdata.com/api/v1/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{
          propertyAddress: {
            city:  city,
            state: state,
            zip:   zip,
          },
          person: {
            firstName: firstName,
            lastName:  lastName,
          }
        }]
      })
    });

    const data2  = await res2.json();
    const phones2 = extractPhones(data2);

    if (phones2.length > 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          success:   true,
          phone:     phones2[0].number,
          allPhones: phones2,
          method:    'name-only',
        })
      };
    }

    // ── Attempt 3: Swap first/last name (some records are FIRST LAST) ──
    if (nameParts.length >= 2) {
      const swapFirst = nameParts.slice(0, -1).join(' ');
      const swapLast  = nameParts[nameParts.length - 1];

      const res3 = await fetch('https://api.batchdata.com/api/v1/property/skip-trace', {
        method: 'POST',
        headers: {
          'Content-Type':  'application/json',
          'Authorization': `Bearer ${API_KEY}`,
        },
        body: JSON.stringify({
          requests: [{
            propertyAddress: {
              street: street,
              city:   city,
              state:  state,
              zip:    zip,
            },
            person: {
              firstName: swapFirst,
              lastName:  swapLast,
            }
          }]
        })
      });

      const data3  = await res3.json();
      const phones3 = extractPhones(data3);

      if (phones3.length > 0) {
        return {
          statusCode: 200,
          body: JSON.stringify({
            success:   true,
            phone:     phones3[0].number,
            allPhones: phones3,
            method:    'name-swapped',
          })
        };
      }
    }

    // All attempts exhausted
    return {
      statusCode: 200,
      body: JSON.stringify({
        success: false,
        phone:   null,
        message: 'No phone found after 3 attempts',
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

function formatPhone(raw) {
  const digits = raw.replace(/\D/g, '').replace(/^1/, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  return raw;
}

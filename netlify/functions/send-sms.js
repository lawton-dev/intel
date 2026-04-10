// ── INTEL · SlickText SMS Function ──────────────────────────────────────────
// Sends a one-off outbound text to a lead's phone number via SlickText API.

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const SLICK_API_KEY  = process.env.SLICK_API_KEY;
  const SLICK_ACCOUNT  = process.env.SLICK_ACCOUNT;
  const FROM_TEXTWORD  = process.env.SLICK_TEXTWORD; // your textword keyword

  if (!SLICK_API_KEY || !SLICK_ACCOUNT) {
    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, error: 'SlickText not configured' })
    };
  }

  let body;
  try { body = JSON.parse(event.body); }
  catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { phone, message } = body;

  if (!phone || !message) {
    return { statusCode: 400, body: JSON.stringify({ error: 'phone and message required' }) };
  }

  // Normalize phone to 10 digits
  const clean = phone.replace(/\D/g, '').replace(/^1/, '');
  if (clean.length !== 10) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid phone number' }) };
  }

  try {
    // SlickText: send a message to a specific number
    const res = await fetch(`https://api.slicktext.com/v1/accounts/${SLICK_ACCOUNT}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + Buffer.from(SLICK_API_KEY + ':').toString('base64'),
      },
      body: JSON.stringify({
        from: FROM_TEXTWORD,
        to:   clean,
        body: message,
      })
    });

    const data = await res.json();

    if (res.ok) {
      return {
        statusCode: 200,
        body: JSON.stringify({ success: true, messageId: data.id || data.sid })
      };
    } else {
      console.error('SlickText error:', data);
      return {
        statusCode: 422,
        body: JSON.stringify({ success: false, error: data.message || 'SlickText send failed' })
      };
    }

  } catch (err) {
    console.error('SMS function error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ success: false, error: 'Internal error' })
    };
  }
};

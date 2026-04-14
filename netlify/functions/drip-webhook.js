// ── INTEL · Drip Webhook Proxy ────────────────────────────────────────────────
// Routes lead data to Zapier drip hook server-side (avoids CORS issues)

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  let body;
  try { body = JSON.parse(event.body); }
  catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  try {
    const res = await fetch('https://hooks.zapier.com/hooks/catch/8732778/u7n82ax/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    return {
      statusCode: 200,
      body: JSON.stringify({ success: true, status: res.status })
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message })
    };
  }
};

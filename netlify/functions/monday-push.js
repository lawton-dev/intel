// netlify/functions/monday-push.js
// Proxies the Zapier "push to Monday" webhook so the URL stays server-side.
// Set ZAPIER_MONDAY_WEBHOOK as a Netlify environment variable
// (the full URL like https://hooks.zapier.com/hooks/catch/xxxx/yyyy/).

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      body: JSON.stringify({ error: 'Method not allowed' })
    };
  }

  const webhookUrl = process.env.ZAPIER_MONDAY_WEBHOOK;
  if (!webhookUrl) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'ZAPIER_MONDAY_WEBHOOK not set in Netlify environment variables'
      })
    };
  }

  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: event.body
    });

    // Zapier typically returns text/plain with a status code
    const text = await res.text();

    return {
      statusCode: res.status,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ok: res.ok, zapier_response: text })
    };
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Proxy error: ' + (err.message || 'unknown') })
    };
  }
};

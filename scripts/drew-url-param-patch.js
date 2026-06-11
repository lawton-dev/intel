// ═══════════════════════════════════════════════════════════
// DREW URL PARAM SUPPORT — paste this <script> block right before </body>
// in DREW's index.html. It does NOT touch your existing checkPw() or
// password hash; it just hooks into the unlock event by watching for
// the gate to disappear.
// ═══════════════════════════════════════════════════════════
(function() {
  // 1. Auto-unlock if previously authed this session (so INTEL clicks don't re-prompt)
  try {
    if (sessionStorage.getItem('drew_unlocked') === '1') {
      const gate = document.getElementById('gate');
      if (gate) gate.style.display = 'none';
      document.body.classList.remove('locked');
    }
  } catch(e) {}

  // 2. Watch for the gate to be hidden (= successful unlock). When it goes away,
  //    remember the session unlock and fire the deep-link handler.
  function watchGate() {
    const gate = document.getElementById('gate');
    if (!gate) {
      setTimeout(watchGate, 200);
      return;
    }
    const obs = new MutationObserver(() => {
      const hidden = gate.style.display === 'none' ||
                     window.getComputedStyle(gate).display === 'none';
      if (hidden) {
        try { sessionStorage.setItem('drew_unlocked', '1'); } catch(e) {}
        setTimeout(handleIntelDeepLink, 150);
        obs.disconnect();
      }
    });
    obs.observe(gate, { attributes: true, attributeFilter: ['style', 'class'] });

    // Also fire immediately if the gate is already hidden (sessionStorage path)
    if (gate.style.display === 'none' || window.getComputedStyle(gate).display === 'none') {
      setTimeout(handleIntelDeepLink, 150);
      obs.disconnect();
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', watchGate);
  } else {
    watchGate();
  }

  function handleIntelDeepLink() {
    const params = new URLSearchParams(window.location.search);
    const address = params.get('address');
    if (!address) return;

    const addrInput = document.getElementById('address');
    if (!addrInput) {
      setTimeout(handleIntelDeepLink, 500);
      return;
    }

    addrInput.value = address;
    addrInput.dispatchEvent(new Event('input',  { bubbles: true }));
    addrInput.dispatchEvent(new Event('change', { bubbles: true }));
    addrInput.focus();

    // Prefill optional supplementary fields if INTEL sent them
    const beds   = params.get('beds');
    const baths  = params.get('baths');
    const sqft   = params.get('sqft');
    const asking = params.get('asking');
    if (beds)   trySet('beds',   beds);
    if (baths)  trySet('baths',  baths);
    if (sqft)   trySet('sqft',   sqft);
    if (asking) trySet('asking', asking);

    showIntelBanner(address, params.get('lead_type'));
  }

  function trySet(id, val) {
    const el = document.getElementById(id);
    if (el && !el.value) {
      el.value = val;
      el.dispatchEvent(new Event('input',  { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }
  }

  function showIntelBanner(address, leadType) {
    if (document.getElementById('intelBanner')) return;
    const banner = document.createElement('div');
    banner.id = 'intelBanner';
    banner.style.cssText = 'background:linear-gradient(90deg,rgba(56,139,253,0.10),rgba(56,139,253,0.02));border:1px solid rgba(56,139,253,0.35);border-radius:8px;padding:10px 16px;margin:12px 24px;font-family:DM Mono,monospace;font-size:11px;color:#388bfd;display:flex;align-items:center;gap:10px;';
    banner.innerHTML = '<span style="font-size:14px">⬅</span><span>From INTEL · ' + (leadType ? leadType + ' · ' : '') + '<strong style="color:#fff">' + address + '</strong></span>';
    document.body.insertBefore(banner, document.body.firstChild);
  }
})();

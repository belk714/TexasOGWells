// Cloudflare Worker — RRC PDQ Production Data Proxy
// Searches RRC Production Data Query and returns lease matches + production data
// 
// SETUP:
// 1. Create a Cloudflare Worker named "og-wells-pdq"
// 2. Paste this code
// 3. Deploy — no KV needed
// 4. URL: og-wells-pdq.belk714.workers.dev

export default {
  async fetch(request) {
    const cors = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    if (request.method === 'OPTIONS') return new Response(null, { headers: cors });

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // GET /search?name=WELLNAME&district=08
      if (path === '/search') {
        const name = (url.searchParams.get('name') || '').trim().toUpperCase();
        const district = url.searchParams.get('district') || 'None Selected';
        if (!name) return jr({ error: 'Missing name' }, 400, cors);
        return jr(await searchLeases(name, district), 200, cors);
      }

      // GET /production?lease=26086&district=08&type=Oil
      if (path === '/production') {
        const lease = url.searchParams.get('lease');
        const district = url.searchParams.get('district');
        const type = url.searchParams.get('type') || 'Oil';
        if (!lease || !district) return jr({ error: 'Missing lease or district' }, 400, cors);
        return jr(await getProduction(lease, district, type), 200, cors);
      }

      return jr({ error: 'Use GET /search?name=X or GET /production?lease=X&district=X&type=Oil' }, 404, cors);
    } catch (e) {
      return jr({ error: e.message }, 500, cors);
    }
  }
};

function jr(data, status, cors) {
  return new Response(JSON.stringify(data), {
    status, headers: { ...cors, 'Content-Type': 'application/json' }
  });
}

async function getSession() {
  const resp = await fetch('https://webapps.rrc.texas.gov/PDQ/leaseSearchAction.do', {
    redirect: 'follow', headers: { 'User-Agent': 'Mozilla/5.0' }
  });
  const text = await resp.text();
  const m = text.match(/jsessionid=([^";&\s]+)/);
  if (!m) throw new Error('No session');
  
  // Extract cookies
  const cookies = resp.headers.getAll ? resp.headers.getAll('set-cookie') : [];
  let cookie = '';
  for (const c of cookies) {
    const cm = c.match(/JSESSIONID=([^;]+)/);
    if (cm) { cookie = `JSESSIONID=${cm[1]}`; break; }
  }
  if (!cookie) cookie = `JSESSIONID=${m[1]}`;
  
  return { id: m[1], cookie };
}

async function searchLeases(name, district) {
  const sess = await getSession();
  
  const body = new URLSearchParams({
    leaseSearchCriteria: 'contains',
    leaseSearchValue: name,
    district: district,
    onShoreCounty: 'None Selected',
    offShoreArea: 'None Selected',
    fieldNo: '',
    submit: 'Submit'
  });

  const resp = await fetch(
    `https://webapps.rrc.texas.gov/PDQ/leaseSearchSubmitAction.do;jsessionid=${sess.id}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Cookie': sess.cookie, 'User-Agent': 'Mozilla/5.0' },
      body: body.toString()
    }
  );
  const html = await resp.text();

  const regex = /option value="(\d+)\^0\^([^^]+)\^1\^([OG])\^2\^([^^]+)\^3\^([^"]*)">\(([^)]+)\):([^<]+)/g;
  const leases = [];
  let match;
  while ((match = regex.exec(html)) !== null && leases.length < 50) {
    leases.push({
      leaseNumber: match[1],
      leaseName: match[2].trim(),
      wellType: match[3] === 'O' ? 'Oil' : 'Gas',
      district: match[4].trim(),
      wellNumber: match[5].trim().replace(/^null$/, ''),
      displayId: match[6].trim(),
      displayName: match[7].trim()
    });
  }

  return { query: name, count: leases.length, leases };
}

async function getProduction(lease, district, type) {
  // Need fresh session and go through the lease search → select → production flow
  const sess = await getSession();
  
  // Go to specific lease query page
  await fetch(
    `https://webapps.rrc.texas.gov/PDQ/quickLeaseReportBuilderAction.do;jsessionid=${sess.id}`,
    { headers: { 'Cookie': sess.cookie, 'User-Agent': 'Mozilla/5.0' } }
  );

  // Submit the specific lease query
  const body = new URLSearchParams({
    wellType: type,
    leaseNumber: lease,
    district: district,
    startMonth: '01',
    startYear: '1993',
    endMonth: '12',
    endYear: '2026',
    submit: 'Submit'
  });

  const resp = await fetch(
    `https://webapps.rrc.texas.gov/PDQ/quickLeaseSubmitAction.do;jsessionid=${sess.id}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Cookie': sess.cookie, 'User-Agent': 'Mozilla/5.0' },
      body: body.toString()
    }
  );
  const html = await resp.text();

  // Parse production data from DataGrid table
  const gridMatch = html.match(/class="DataGrid">([\s\S]*?)<\/TABLE>/);
  if (!gridMatch) {
    if (html.includes('No Matches')) {
      return { lease, district, type, error: 'No production data found', data: [] };
    }
    return { lease, district, type, error: 'Could not parse response', data: [] };
  }

  const rows = [];
  const trRegex = /<TR[^>]*>([\s\S]*?)<\/TR>/gi;
  let tr;
  while ((tr = trRegex.exec(gridMatch[1])) !== null) {
    const cells = [];
    const tdRegex = /<TD[^>]*>([\s\S]*?)<\/TD>/gi;
    let td;
    while ((td = tdRegex.exec(tr[1])) !== null) {
      cells.push(td[1].replace(/<[^>]+>/g, '').replace(/&nbsp;/g, '').replace(/,/g, '').trim());
    }
    if (cells.length > 0 && !cells[0].includes('Message')) {
      rows.push(cells);
    }
  }

  // Parse into structured data
  const production = rows.map(r => {
    if (type === 'Oil') {
      return {
        month: r[0] || '',
        oilBBL: parseInt(r[1]) || 0,
        casingheadGasMCF: parseInt(r[2]) || 0,
        wellCount: parseInt(r[3]) || 0,
        operator: r[5] || '',
        fieldName: r[7] || ''
      };
    } else {
      return {
        month: r[0] || '',
        gasMCF: parseInt(r[1]) || 0,
        condensateBBL: parseInt(r[2]) || 0,
        wellCount: parseInt(r[3]) || 0,
        operator: r[5] || '',
        fieldName: r[7] || ''
      };
    }
  }).filter(r => r.month);

  return { lease, district, type, count: production.length, data: production };
}

// Cloudflare Worker — RRC PDQ Production Data Proxy
// Searches RRC Production Data Query by well/lease name and returns matching leases
// 
// SETUP:
// 1. Create a Cloudflare Worker named "og-wells-pdq" (or similar)
// 2. Paste this code in the editor
// 3. Deploy — no KV needed
// 4. Worker URL: og-wells-pdq.belk714.workers.dev

export default {
  async fetch(request) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // GET /search?name=ROGERS&district=08
      if (request.method === 'GET' && path === '/search') {
        const name = (url.searchParams.get('name') || '').trim().toUpperCase();
        const district = url.searchParams.get('district') || 'None Selected';
        const county = url.searchParams.get('county') || 'None Selected';
        
        if (!name) {
          return jsonResp({ error: 'Missing name parameter' }, 400, corsHeaders);
        }

        // Step 1: Get a PDQ session
        const sessionResp = await fetch('https://webapps.rrc.texas.gov/PDQ/leaseSearchAction.do', {
          redirect: 'follow',
          headers: { 'User-Agent': 'Mozilla/5.0' }
        });
        
        // Extract jsessionid from cookies
        const cookies = sessionResp.headers.getAll('set-cookie') || [];
        let jsessionid = '';
        for (const c of cookies) {
          const m = c.match(/JSESSIONID=([^;]+)/);
          if (m) { jsessionid = m[1]; break; }
        }
        
        if (!jsessionid) {
          // Try from URL
          const finalUrl = sessionResp.url;
          const m = finalUrl.match(/jsessionid=([^?&]+)/);
          if (m) jsessionid = m[1];
        }

        if (!jsessionid) {
          return jsonResp({ error: 'Could not establish PDQ session' }, 502, corsHeaders);
        }

        // Step 2: Submit lease name search
        const searchBody = new URLSearchParams({
          leaseSearchCriteria: 'contains',
          leaseSearchValue: name,
          district: district,
          onShoreCounty: county,
          offShoreArea: 'None Selected',
          fieldNo: '',
          submit: 'Submit'
        });

        const searchResp = await fetch(
          `https://webapps.rrc.texas.gov/PDQ/leaseSearchSubmitAction.do;jsessionid=${jsessionid}`,
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/x-www-form-urlencoded',
              'Cookie': `JSESSIONID=${jsessionid}`,
              'User-Agent': 'Mozilla/5.0'
            },
            body: searchBody.toString()
          }
        );

        const html = await searchResp.text();

        // Step 3: Parse results from <option> tags
        // Format: value="leaseNum^0^LEASENAME^1^TYPE^2^DISTRICT^3^WELLNUMBER">(DIST-LEASENUM):LEASENAME
        const regex = /option value="(\d+)\^0\^([^^]+)\^1\^([OG])\^2\^([^^]+)\^3\^([^"]*)">\(([^)]+)\):([^<]+)/g;
        const leases = [];
        let match;
        while ((match = regex.exec(html)) !== null && leases.length < 50) {
          leases.push({
            leaseNumber: match[1],
            leaseName: match[2].trim(),
            wellType: match[3] === 'O' ? 'Oil' : 'Gas',
            district: match[4].trim(),
            wellNumber: match[5].trim(),
            displayId: match[6].trim(),
            displayName: match[7].trim()
          });
        }

        return jsonResp({
          query: name,
          count: leases.length,
          leases: leases,
          pdqSession: jsessionid
        }, 200, corsHeaders);
      }

      // GET /production?lease=281141&district=08&type=G&from=2020-01&to=2025-12
      // Returns a link to view production (can't easily scrape the actual data)
      if (request.method === 'GET' && path === '/production-link') {
        const lease = url.searchParams.get('lease');
        const district = url.searchParams.get('district');
        const type = url.searchParams.get('type') || 'Oil';
        
        if (!lease || !district) {
          return jsonResp({ error: 'Missing lease or district' }, 400, corsHeaders);
        }

        // Build the PDQ direct link info
        return jsonResp({
          lease,
          district,
          type,
          instructions: 'Go to the RRC PDQ Specific Lease Query page, select the well type, enter the lease number and district, then submit.',
          pdqUrl: 'https://webapps.rrc.texas.gov/PDQ/quickLeaseReportBuilderAction.do',
          leaseNumber: lease,
          districtCode: district,
          wellType: type
        }, 200, corsHeaders);
      }

      return jsonResp({ error: 'Not found. Use GET /search?name=WELLNAME' }, 404, corsHeaders);
    } catch (e) {
      return jsonResp({ error: e.message }, 500, corsHeaders);
    }
  }
};

function jsonResp(data, status, corsHeaders) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
}

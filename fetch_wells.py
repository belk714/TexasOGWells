#!/usr/bin/env python3
"""
Fetch real Texas O&G well data with operator names from RRC.
Strategy:
1. Get well coordinates + API numbers from RRC ArcGIS
2. Get operator info from RRC EWA wellbore query (batch by county)
3. Join and output wells.json
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import http.cookiejar
from concurrent.futures import ThreadPoolExecutor, as_completed

# Permian Basin bounding box
BBOX = {"xmin": -104.5, "ymin": 30.5, "xmax": -100.5, "ymax": 33.5}

# RRC ArcGIS endpoint
GIS_URL = "https://gis.rrc.texas.gov/server/rest/services/rrc_public/RRC_Public_Viewer_Srvs/MapServer/1/query"

# RRC EWA endpoint
EWA_BASE = "https://webapps2.rrc.texas.gov/EWA"

# Operator name mapping - map RRC operator names to our display names
OPERATOR_MAPPING = {
    # ExxonMobil / Pioneer Natural Resources (ExxonMobil acquired Pioneer in 2024)
    "PIONEER NATURAL RESOURCES": "ExxonMobil/Pioneer",
    "PIONEER NATURAL RES": "ExxonMobil/Pioneer",
    "PIONEER NATURAL": "ExxonMobil/Pioneer",
    "EXXONMOBIL": "ExxonMobil/Pioneer",
    "EXXON MOBIL": "ExxonMobil/Pioneer",
    "EXXON": "ExxonMobil/Pioneer",
    "XTO ENERGY": "ExxonMobil/Pioneer",
    # ConocoPhillips
    "CONOCOPHILLIPS": "ConocoPhillips",
    "CONOCO PHILLIPS": "ConocoPhillips",
    "CONOCO": "ConocoPhillips",
    "BURLINGTON RESOURCES": "ConocoPhillips",
    # EOG Resources
    "EOG RESOURCES": "EOG",
    "EOG RES": "EOG",
    # Diamondback Energy
    "DIAMONDBACK": "Diamondback",
    "DIAMONDBACK ENERGY": "Diamondback",
    "DIAMONDBACK E&P": "Diamondback",
    "VIPER ENERGY": "Diamondback",
    "ENERGEN": "Diamondback",
    # Devon Energy
    "DEVON ENERGY": "Devon",
    "DEVON": "Devon",
    # Occidental
    "OCCIDENTAL": "Occidental",
    "OXY": "Occidental",
    "OXY USA": "Occidental",
    "ANADARKO": "Occidental",
    "ANADARKO PETROLEUM": "Occidental",
    "ANADARKO E&P": "Occidental",
    # Chevron
    "CHEVRON": "Chevron",
    "CHEVRON U.S.A.": "Chevron",
    "CHEVRON USA": "Chevron",
    # Apache/APA
    "APACHE": "Apache/APA",
    "APA": "Apache/APA",
    "APA CORPORATION": "Apache/APA",
    # Coterra Energy (formerly Cimarex)
    "COTERRA": "Coterra",
    "COTERRA ENERGY": "Coterra",
    "CIMAREX": "Coterra",
    "CIMAREX ENERGY": "Coterra",
    # Callon Petroleum (acquired by APA in 2024, but keep separate for now)
    "CALLON": "Callon",
    "CALLON PETROLEUM": "Callon",
}


def classify_operator(name):
    """Classify an operator name to one of our tracked companies or 'Other'."""
    if not name:
        return "Other"
    upper = name.upper().strip()
    # Direct match
    for key, val in OPERATOR_MAPPING.items():
        if key in upper:
            return val
    return "Other"


def fetch_gis_wells():
    """Fetch well locations from RRC ArcGIS."""
    print("Fetching wells from RRC ArcGIS...")
    all_wells = {}
    
    # Query in grid cells to avoid hitting record limits
    # The ArcGIS service has a maxRecordCount - let's check
    lat_step = 0.5
    lon_step = 0.5
    
    lat = BBOX["ymin"]
    total_fetched = 0
    while lat < BBOX["ymax"]:
        lon = BBOX["xmin"]
        while lon < BBOX["xmax"]:
            # Build envelope
            envelope = json.dumps({
                "xmin": lon, "ymin": lat,
                "xmax": lon + lon_step, "ymax": lat + lat_step
            })
            
            offset = 0
            batch_size = 2000
            while True:
                params = urllib.parse.urlencode({
                    "where": "SYMNUM IN (4,5,6,7)",  # Oil, Gas, Oil/Gas, Other Completion
                    "outFields": "API,GIS_API5,GIS_WELL_NUMBER,GIS_SYMBOL_DESCRIPTION,GIS_LAT83,GIS_LONG83",
                    "returnGeometry": "true",
                    "resultRecordCount": batch_size,
                    "resultOffset": offset,
                    "outSR": "4326",
                    "geometry": envelope,
                    "geometryType": "esriGeometryEnvelope",
                    "inSR": "4326",
                    "spatialRel": "esriSpatialRelIntersects",
                    "f": "json"
                })
                
                url = f"{GIS_URL}?{params}"
                try:
                    with urllib.request.urlopen(url, timeout=30) as resp:
                        data = json.loads(resp.read())
                except Exception as e:
                    print(f"  Error fetching grid cell ({lat},{lon}): {e}")
                    break
                
                features = data.get("features", [])
                if not features:
                    break
                
                for f in features:
                    attrs = f["attributes"]
                    api = str(attrs.get("API", "")).strip()
                    if api and api not in all_wells:
                        geom = f.get("geometry", {})
                        all_wells[api] = {
                            "api": api,
                            "lat": geom.get("y", attrs.get("GIS_LAT83")),
                            "lng": geom.get("x", attrs.get("GIS_LONG83")),
                            "well_num": str(attrs.get("GIS_WELL_NUMBER", "")).strip(),
                            "type": str(attrs.get("GIS_SYMBOL_DESCRIPTION", "")).strip(),
                        }
                
                total_fetched += len(features)
                print(f"  Grid ({lat:.1f},{lon:.1f}): {len(features)} features, total unique: {len(all_wells)}", end="\r")
                
                if len(features) < batch_size:
                    break
                offset += batch_size
                
            lon += lon_step
        lat += lat_step
    
    print(f"\nTotal unique wells from GIS: {len(all_wells)}")
    return all_wells


def get_ewa_session():
    """Establish an EWA session and return (opener, cookies)."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=__import__('ssl').create_default_context())
    )
    # Initialize session
    opener.open(f"{EWA_BASE}/ewaMain.do", timeout=15)
    opener.open(f"{EWA_BASE}/wellboreQueryAction.do?methodToCall=beginWellboreQuery", timeout=15)
    return opener


def query_ewa_by_api(opener, api_prefix, api_suffix):
    """Query EWA wellbore search for a specific API number."""
    data = urllib.parse.urlencode({
        "methodToCall": "search",
        "searchArgs.apiNoPrefixArg": api_prefix,
        "searchArgs.apiNoSuffixArg": api_suffix,
        "searchArgs.districtCodeArg": "",
        "searchArgs.countyCodeArg": "",
        "searchArgs.operatorNoArg": "",
        "searchArgs.leaseNameArg": "",
        "searchArgs.rrcIdNoArg": "",
        "searchArgs.fieldNoArg": "",
        "searchArgs.wellNoArg": "",
        "searchArgs.wellTypeArg": "",
        "searchArgs.apiNoCompleteArg": "",
        "searchArgs.operatorNumbersArg": "",
        "page": "1",
        "pagesize": "50",
    }).encode()
    
    try:
        resp = opener.open(
            f"{EWA_BASE}/wellboreQueryAction.do",
            data=data,
            timeout=15
        )
        html = resp.read().decode("utf-8", errors="replace")
        
        # Extract operator from the results
        # Pattern: title="Operator # XXXXXX">OPERATOR NAME</a>
        operators = re.findall(r'title="Operator # (\d+)">([^<]+)</a>', html)
        if operators:
            return operators[0]  # (operator_no, operator_name)
    except Exception as e:
        pass
    return None


def query_ewa_by_district_county(opener, district, county_code, page=1, pagesize=50):
    """Query EWA for wells by district and county, return list of (api, operator_name)."""
    data = urllib.parse.urlencode({
        "methodToCall": "search",
        "searchArgs.apiNoPrefixArg": county_code,
        "searchArgs.apiNoSuffixArg": "",
        "searchArgs.districtCodeArg": district,
        "searchArgs.countyCodeArg": "",
        "searchArgs.operatorNoArg": "",
        "searchArgs.leaseNameArg": "",
        "searchArgs.rrcIdNoArg": "",
        "searchArgs.fieldNoArg": "",
        "searchArgs.wellNoArg": "",
        "searchArgs.wellTypeArg": "",
        "searchArgs.apiNoCompleteArg": "",
        "searchArgs.operatorNumbersArg": "",
        "page": str(page),
        "pagesize": str(pagesize),
    }).encode()
    
    try:
        resp = opener.open(
            f"{EWA_BASE}/wellboreQueryAction.do",
            data=data,
            timeout=30
        )
        html = resp.read().decode("utf-8", errors="replace")
        
        # Check for too many records error
        if "exceeds the maximum" in html:
            m = re.search(r'(\d+) records found', html)
            count = int(m.group(1)) if m else 0
            return None, count  # Signal too many records
        
        # Extract API numbers and operator names from results
        # The results table has rows with API links and operator links
        results = []
        
        # Find operator entries: title="Operator # XXXXXX">OPERATOR NAME</a>
        operators = re.findall(r'title="Operator # (\d+)">([^<]+)</a>', html)
        
        # Find API numbers in the same table
        apis = re.findall(r'apiNo=(\d{8})', html)
        
        # They should correspond 1:1
        for i, (op_no, op_name) in enumerate(operators):
            if i < len(apis):
                results.append((apis[i], op_name.replace("&amp;", "&")))
        
        return results, len(results)
    except Exception as e:
        print(f"  Error querying EWA: {e}")
        return [], 0


def fetch_operator_data_batch(wells):
    """Fetch operator data for wells using EWA in batches."""
    print("\nFetching operator data from RRC EWA...")
    
    # Group wells by county code (first 3 digits of API)
    by_county = {}
    for api, well in wells.items():
        county = api[:3]
        if county not in by_county:
            by_county[county] = []
        by_county[county].append(api)
    
    print(f"Wells span {len(by_county)} counties")
    
    # For each county, try to query EWA
    api_to_operator = {}
    
    opener = get_ewa_session()
    
    for county, apis in sorted(by_county.items()):
        print(f"\nCounty {county}: {len(apis)} wells")
        
        # First check if we can get all results
        results, count = query_ewa_by_district_county(opener, "", county, page=1, pagesize=50)
        
        if results is None:
            # Too many records - need to narrow search
            print(f"  Too many records ({count}), querying by individual API suffix ranges...")
            
            # Query in suffix ranges
            for suffix_start in range(0, 100000, 1000):
                suffix_str = f"{suffix_start:05d}"
                # Search with API prefix + partial suffix
                # Actually, the EWA doesn't support range queries. Let's try individual lookups.
                pass
            
            # Fall back to individual lookups for a sample
            sample_apis = apis[:100]  # Limit to 100 per county
            for api in sample_apis:
                prefix = api[:3]
                suffix = api[3:]
                result = query_ewa_by_api(opener, prefix, suffix)
                if result:
                    op_no, op_name = result
                    api_to_operator[api] = op_name.replace("&amp;", "&")
                time.sleep(0.2)  # Rate limit
                
            print(f"  Got {sum(1 for a in sample_apis if a in api_to_operator)}/{len(sample_apis)} operators")
        else:
            # Got results, paginate through all
            for api, op_name in results:
                api_to_operator[api] = op_name
            
            if count == 50:  # Might be more pages
                page = 2
                while True:
                    results, cnt = query_ewa_by_district_county(opener, "", county, page=page, pagesize=50)
                    if not results or cnt == 0:
                        break
                    for api, op_name in results:
                        api_to_operator[api] = op_name
                    if cnt < 50:
                        break
                    page += 1
                    time.sleep(0.1)
            
            print(f"  Got {len([a for a in apis if a in api_to_operator])}/{len(apis)} operators")
    
    return api_to_operator


if __name__ == "__main__":
    # Step 1: Get well locations from GIS
    wells = fetch_gis_wells()
    
    if not wells:
        print("No wells found!")
        sys.exit(1)
    
    # Step 2: Get operator data
    api_to_operator = fetch_operator_data_batch(wells)
    
    print(f"\nTotal wells with operator data: {len(api_to_operator)}")
    
    # Step 3: Build output
    output_wells = []
    operator_counts = {}
    
    for api, well in wells.items():
        raw_operator = api_to_operator.get(api, "")
        operator = classify_operator(raw_operator)
        
        output_wells.append({
            "id": api,
            "lat": round(well["lat"], 6),
            "lng": round(well["lng"], 6),
            "operator": operator,
            "type": well["type"],
            "well_num": well["well_num"],
        })
        
        operator_counts[operator] = operator_counts.get(operator, 0) + 1
    
    print(f"\nOperator distribution:")
    for op, count in sorted(operator_counts.items(), key=lambda x: -x[1]):
        print(f"  {op}: {count}")
    
    # Save
    output_path = "/home/openclaw/.openclaw/workspace/texas-og-wells/wells.json"
    with open(output_path, "w") as f:
        json.dump(output_wells, f)
    
    print(f"\nSaved {len(output_wells)} wells to {output_path}")

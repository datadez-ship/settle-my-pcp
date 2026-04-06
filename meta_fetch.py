#!/usr/bin/env python3
"""
Settle My PCP — Meta Ads Fetcher (multi-campaign edition)

Writes:
  data.js                    → all campaigns combined (master dashboard)
  data-{campaign_id}.js      → per-campaign files (affiliate dashboards)
  master-dashboard.html      → master view with campaign tabs
  affiliate-{slug}.html      → per-campaign standalone dashboards

Usage:
  python3 meta_fetch.py                  # last 30 days
  python3 meta_fetch.py --days 14        # last 14 days
  python3 meta_fetch.py --since 2026-03-01 --until 2026-04-01
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌  Missing dependency: pip3 install requests")
    sys.exit(1)

# ─── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = Path(__file__).parent / "config.json"
OUTPUT_DIR  = Path(__file__).parent

API_VERSION      = "v19.0"
BASE_URL         = f"https://graph.facebook.com/{API_VERSION}"
CONVERSION_EVENT = "offsite_conversion.fb_pixel_complete_registration"

INSIGHT_FIELDS = ",".join([
    "campaign_id", "campaign_name",
    "ad_id", "ad_name",
    "spend", "impressions", "reach", "frequency",
    "clicks", "inline_link_clicks", "inline_link_click_ctr",
    "cpm", "cpc", "actions", "cost_per_action_type",
])

# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_config():
    if not CONFIG_PATH.exists():
        print(f"❌  config.json not found at {CONFIG_PATH}")
        sys.exit(1)
    cfg = json.loads(CONFIG_PATH.read_text())
    if not cfg.get("access_token") or "PLACEHOLDER" in cfg.get("access_token", ""):
        print("❌  access_token not set in config.json")
        sys.exit(1)
    return cfg


def api_get_all(url, params):
    records = []
    while url:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
        body = r.json()
        if "error" in body:
            err = body["error"]
            raise RuntimeError(f"Meta API error ({err.get('code')}): {err.get('message')}")
        records.extend(body.get("data", []))
        url    = body.get("paging", {}).get("next")
        params = {}
    return records


def get_action_value(actions, action_type):
    if not actions:
        return 0.0
    for a in actions:
        if a.get("action_type") == action_type:
            return float(a.get("value", 0))
    return 0.0


def slugify(name):
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

# ─── API calls ────────────────────────────────────────────────────────────────

def fetch_campaigns(cfg):
    print("  → Fetching campaigns...")
    rows = api_get_all(
        f"{BASE_URL}/act_{cfg['ad_account_id']}/campaigns",
        {
            "access_token": cfg["access_token"],
            "fields": "id,name,status,effective_status",
            "limit": 500,
        },
    )
    return {r["id"]: r for r in rows}


def fetch_ad_statuses(cfg):
    print("  → Fetching ad statuses...")
    rows = api_get_all(
        f"{BASE_URL}/act_{cfg['ad_account_id']}/ads",
        {
            "access_token": cfg["access_token"],
            "fields": "id,name,effective_status",
            "limit": 500,
        },
    )
    return {r["id"]: r.get("effective_status", "UNKNOWN") for r in rows}


def fetch_insights(cfg, since, until):
    print(f"  → Fetching insights ({since} → {until})...")
    return api_get_all(
        f"{BASE_URL}/act_{cfg['ad_account_id']}/insights",
        {
            "access_token": cfg["access_token"],
            "fields": INSIGHT_FIELDS,
            "time_range": json.dumps({"since": since, "until": until}),
            "level": "ad",
            "limit": 500,
        },
    )

# ─── Processing ───────────────────────────────────────────────────────────────

def process_ads(rows, status_map, campaign_map):
    """
    Aggregate insight rows by ad_id.
    NOTE: Meta returns inline_link_click_ctr already as a percentage value
    (e.g. 1.9270 = 1.93%), so we store it as-is and display without *100.
    """
    buckets = {}
    for row in rows:
        ad_id   = row.get("ad_id", "")
        ad_name = row.get("ad_name", "Unknown")
        camp_id = row.get("campaign_id", "unknown")
        camp_nm = row.get("campaign_name") or (campaign_map.get(camp_id, {}).get("name", "Unknown Campaign"))

        if ad_id not in buckets:
            buckets[ad_id] = {
                "ad_id": ad_id, "name": ad_name,
                "campaign_id": camp_id, "campaign_name": camp_nm,
                "status": "active" if status_map.get(ad_id) == "ACTIVE" else "inactive",
                "spend": 0.0, "impressions": 0, "reach": 0,
                "linkClicks": 0, "conversions": 0.0,
                "_ctr_num": 0.0, "_ctr_den": 0,
            }
        b   = buckets[ad_id]
        imp = int(row.get("impressions", 0))
        # CTR from Meta is already a % (e.g. 1.9270), weight by impressions
        b["spend"]       += float(row.get("spend", 0))
        b["impressions"] += imp
        b["reach"]       += int(row.get("reach", 0))
        b["linkClicks"]  += int(row.get("inline_link_clicks", 0))
        b["conversions"] += get_action_value(row.get("actions"), CONVERSION_EVENT)
        b["_ctr_num"]    += float(row.get("inline_link_click_ctr", 0)) * imp
        b["_ctr_den"]    += imp

    ads = []
    for b in buckets.values():
        spend  = b["spend"]
        convs  = int(b["conversions"])
        imp    = b["impressions"]
        clicks = b["linkClicks"]
        # ctr stored as percentage already (e.g. 1.93), NOT a fraction
        ctr = (b["_ctr_num"] / b["_ctr_den"]) if b["_ctr_den"] > 0 else 0.0
        ads.append({
            "ad_id":         b["ad_id"],
            "name":          b["name"],
            "campaign_id":   b["campaign_id"],
            "campaign_name": b["campaign_name"],
            "status":        b["status"],
            "spend":         round(spend, 2),
            "impressions":   imp,
            "reach":         b["reach"],
            "linkClicks":    clicks,
            "conversions":   convs,
            "ctr":           round(ctr, 4),   # already in % — display as-is
            "cpl":           round(spend / convs, 2) if convs > 0 else None,
            "cpm":           round(spend / imp * 1000, 2) if imp > 0 else 0.0,
            "cpc":           round(spend / clicks, 2) if clicks > 0 else None,
        })

    ads.sort(key=lambda a: a["spend"], reverse=True)
    return ads


def build_campaign_summaries(ads, campaign_map):
    camps = {}
    for ad in ads:
        cid = ad["campaign_id"]
        if cid not in camps:
            status_raw = campaign_map.get(cid, {}).get("effective_status", "UNKNOWN")
            camps[cid] = {
                "campaign_id":   cid,
                "campaign_name": ad["campaign_name"],
                "status":        "active" if status_raw == "ACTIVE" else "inactive",
                "spend": 0.0, "impressions": 0, "reach": 0,
                "linkClicks": 0, "conversions": 0, "ads": [],
            }
        c = camps[cid]
        c["spend"]       += ad["spend"]
        c["impressions"] += ad["impressions"]
        c["reach"]       += ad["reach"]
        c["linkClicks"]  += ad["linkClicks"]
        c["conversions"] += ad["conversions"]
        c["ads"].append(ad)

    result = []
    for c in camps.values():
        sp = c["spend"]; conv = c["conversions"]; clicks = c["linkClicks"]; imp = c["impressions"]
        c["spend"]   = round(sp, 2)
        c["ctr"]     = round(clicks / imp * 100, 4) if imp > 0 else 0.0  # compute from raw, gives %
        c["cpl"]     = round(sp / conv, 2) if conv > 0 else None
        c["cpm"]     = round(sp / imp * 1000, 2) if imp > 0 else 0.0
        result.append(c)

    result.sort(key=lambda c: c["spend"], reverse=True)
    return result

# ─── Output writers ───────────────────────────────────────────────────────────

JS_HEADER = "/* Auto-generated by meta_fetch.py — do not edit manually */\n"

def write_master_data_js(campaigns, since, until):
    payload = {
        "meta": {
            "last_updated": datetime.now().isoformat(),
            "since": since,
            "until": until,
            "mode": "multi_campaign",
        },
        "campaigns": campaigns,
    }
    path = OUTPUT_DIR / "data.js"
    path.write_text(JS_HEADER + f"window.DASHBOARD_DATA = {json.dumps(payload, indent=2)};\n")
    print(f"  ✓ data.js written ({len(campaigns)} campaigns)")
    return path


def write_campaign_data_js(campaign, since, until):
    payload = {
        "meta": {
            "last_updated": datetime.now().isoformat(),
            "since": since,
            "until": until,
            "mode": "single_campaign",
        },
        "campaigns": [campaign],
    }
    path = OUTPUT_DIR / f"data-{campaign['campaign_id']}.js"
    path.write_text(JS_HEADER + f"window.DASHBOARD_DATA = {json.dumps(payload, indent=2)};\n")
    print(f"  ✓ data-{campaign['campaign_id']}.js  ({campaign['campaign_name']})")
    return path


def write_affiliate_html(campaign, since, until):
    """Generate a standalone affiliate dashboard for one campaign."""
    camp_id   = campaign["campaign_id"]
    camp_name = campaign["campaign_name"]
    slug      = slugify(camp_name)
    ads       = campaign["ads"]

    fallback = json.dumps({
        "meta": {"last_updated": None, "since": since, "until": until, "mode": "single_campaign"},
        "campaigns": [campaign],
    })

    # CTR note: a.ctr is already in % format (e.g. 1.93), display directly — no *100
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{camp_name} — Ads Dashboard</title>
<script src="data-{camp_id}.js" onerror="window.__dataLoadFailed=true"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a202c;min-height:100vh}}
  header{{background:#1a202c;color:#fff;padding:20px 32px;display:flex;justify-content:space-between;align-items:center}}
  header h1{{font-size:1.25rem;font-weight:600}}
  header .sub{{font-size:0.8rem;opacity:.6;margin-top:2px}}
  .badge{{padding:4px 12px;border-radius:20px;font-size:0.75rem;font-weight:600}}
  .badge.live{{background:#22543d;color:#68d391}}
  .badge.fallback{{background:#744210;color:#f6ad55}}
  .banner{{background:#fffbeb;border-bottom:2px solid #f6ad55;padding:10px 32px;font-size:0.8rem;color:#92400e;display:none}}
  .kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:16px;padding:24px 32px 0}}
  .kpi{{background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .kpi .label{{font-size:0.72rem;text-transform:uppercase;letter-spacing:.05em;color:#718096;margin-bottom:6px}}
  .kpi .value{{font-size:1.7rem;font-weight:700;color:#1a202c}}
  .kpi .value.good{{color:#276749}}
  .charts{{display:grid;grid-template-columns:2fr 1fr;gap:20px;padding:20px 32px}}
  .chart-box{{background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .chart-box h3{{font-size:0.85rem;font-weight:600;color:#4a5568;margin-bottom:16px;text-transform:uppercase;letter-spacing:.04em}}
  table{{width:100%;border-collapse:collapse;font-size:0.82rem}}
  th{{background:#f7fafc;padding:10px 12px;text-align:left;font-weight:600;color:#4a5568;border-bottom:2px solid #e2e8f0;white-space:nowrap}}
  td{{padding:10px 12px;border-bottom:1px solid #edf2f7;vertical-align:middle}}
  tr:hover td{{background:#f7fafc}}
  .status-pill{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:0.7rem;font-weight:600}}
  .status-pill.active{{background:#c6f6d5;color:#276749}}
  .status-pill.inactive{{background:#fed7d7;color:#9b2c2c}}
  .table-wrap{{background:#fff;border-radius:10px;padding:20px;margin:0 32px 32px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .table-wrap h3{{font-size:0.85rem;font-weight:600;color:#4a5568;margin-bottom:16px;text-transform:uppercase;letter-spacing:.04em}}
  .filters{{display:flex;gap:8px;margin-bottom:16px}}
  .filter-btn{{padding:5px 14px;border-radius:20px;border:1px solid #e2e8f0;background:#fff;cursor:pointer;font-size:0.78rem;color:#4a5568}}
  .filter-btn.active{{background:#1a202c;color:#fff;border-color:#1a202c}}
  footer{{text-align:center;padding:24px;color:#a0aec0;font-size:0.75rem}}
  @media(max-width:768px){{.charts{{grid-template-columns:1fr}}.kpis{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>

<div class="banner" id="fallback-banner">
  ⚠ Showing cached data — data updates automatically each morning
</div>

<header>
  <div>
    <h1>{camp_name}</h1>
    <div class="sub" id="header-sub">Loading…</div>
  </div>
  <span class="badge" id="live-badge">●</span>
</header>

<div class="kpis" id="kpis"></div>

<div class="charts">
  <div class="chart-box">
    <h3>Conversions &amp; Cost per Lead</h3>
    <canvas id="convChart" height="120"></canvas>
  </div>
  <div class="chart-box">
    <h3>Spend by Ad</h3>
    <canvas id="spendChart" height="120"></canvas>
  </div>
</div>

<div class="table-wrap">
  <h3>Ad Performance</h3>
  <div class="filters">
    <button class="filter-btn active" onclick="filterTable('all',this)">All</button>
    <button class="filter-btn" onclick="filterTable('active',this)">Active</button>
    <button class="filter-btn" onclick="filterTable('converting',this)">Converting</button>
  </div>
  <table id="adTable">
    <thead>
      <tr>
        <th>Ad Name</th><th>Status</th><th>Spend</th><th>Impressions</th>
        <th>CTR %</th><th>Conversions</th><th>CPL</th><th>CPM</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<footer id="footer">Last updated: —</footer>

<script>
const FALLBACK = {fallback};

const src = (window.DASHBOARD_DATA && window.DASHBOARD_DATA.campaigns && window.DASHBOARD_DATA.campaigns.length)
  ? window.DASHBOARD_DATA : FALLBACK;
const isLive = src !== FALLBACK && !window.__dataLoadFailed;
const campaign = src.campaigns[0];
const meta = src.meta;
const ads = campaign.ads || [];

document.getElementById('live-badge').textContent = isLive ? '● Live' : '● Cached';
document.getElementById('live-badge').className = 'badge ' + (isLive ? 'live' : 'fallback');
if (!isLive) document.getElementById('fallback-banner').style.display = 'block';

const fmt = d => d ? new Date(d.includes('T') ? d : d+'T00:00:00').toLocaleDateString('en-GB',{{day:'numeric',month:'short',year:'numeric'}}) : '—';
document.getElementById('header-sub').textContent =
  fmt(meta.since) + ' → ' + fmt(meta.until) + '  ·  ' + ads.length + ' ads';
document.getElementById('footer').textContent =
  'Last updated: ' + (meta.last_updated ? new Date(meta.last_updated).toLocaleString('en-GB') : '—');

const total = ads.reduce((a,b) => ({{
  spend: a.spend+b.spend, impressions: a.impressions+b.impressions,
  linkClicks: a.linkClicks+b.linkClicks, conversions: a.conversions+b.conversions
}}), {{spend:0,impressions:0,linkClicks:0,conversions:0}});

const kpis = [
  {{label:'Total Spend',  value:'£'+total.spend.toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}},
  {{label:'Impressions',  value:total.impressions.toLocaleString()}},
  {{label:'Link Clicks',  value:total.linkClicks.toLocaleString()}},
  {{label:'Conversions',  value:total.conversions}},
  {{label:'Avg CTR',      value:(total.impressions>0?(total.linkClicks/total.impressions*100).toFixed(2):0)+'%'}},
  {{label:'Avg CPL',      value:total.conversions>0?'£'+(total.spend/total.conversions).toFixed(2):'—', cls:'good'}},
];
document.getElementById('kpis').innerHTML = kpis.map(k =>
  `<div class="kpi"><div class="label">${{k.label}}</div><div class="value ${{k.cls||''}}">${{k.value}}</div></div>`
).join('');

const top = [...ads].sort((a,b)=>b.spend-a.spend).slice(0,8);
const palette = ['#4299e1','#48bb78','#ed8936','#9f7aea','#f56565','#38b2ac','#d69e2e','#667eea'];

new Chart(document.getElementById('convChart'), {{
  type:'bar',
  data:{{
    labels: top.map(a=>a.name.length>22?a.name.slice(0,22)+'…':a.name),
    datasets:[
      {{type:'bar',label:'Conversions',data:top.map(a=>a.conversions),backgroundColor:'#4299e1',yAxisID:'y'}},
      {{type:'line',label:'CPL (£)',data:top.map(a=>a.cpl),borderColor:'#e53e3e',backgroundColor:'rgba(229,62,62,.1)',yAxisID:'y2',tension:.3,pointRadius:4}},
    ]
  }},
  options:{{responsive:true,interaction:{{mode:'index'}},plugins:{{legend:{{position:'top'}}}},
    scales:{{
      y:{{position:'left',title:{{display:true,text:'Conversions'}}}},
      y2:{{position:'right',title:{{display:true,text:'CPL (£)'}},grid:{{drawOnChartArea:false}}}},
    }}
  }}
}});

new Chart(document.getElementById('spendChart'), {{
  type:'doughnut',
  data:{{
    labels: top.map(a=>a.name.length>18?a.name.slice(0,18)+'…':a.name),
    datasets:[{{data:top.map(a=>a.spend),backgroundColor:palette}}]
  }},
  options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:10}}}}}}}}}}
}});

let currentFilter = 'all';
function filterTable(f, btn) {{
  currentFilter = f;
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  renderTable();
}}
function renderTable() {{
  const filtered = ads.filter(a =>
    currentFilter==='all' ? true :
    currentFilter==='active' ? a.status==='active' :
    a.conversions > 0
  );
  document.getElementById('tableBody').innerHTML = filtered.map(a => `
    <tr>
      <td><strong>${{a.name}}</strong></td>
      <td><span class="status-pill ${{a.status}}">${{a.status}}</span></td>
      <td>£${{a.spend.toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}}</td>
      <td>${{a.impressions.toLocaleString()}}</td>
      <td>${{a.ctr.toFixed(2)}}%</td>
      <td>${{a.conversions}}</td>
      <td>${{a.cpl!=null?'£'+a.cpl.toFixed(2):'—'}}</td>
      <td>${{a.cpm!=null?'£'+a.cpm.toFixed(2):'—'}}</td>
    </tr>`).join('');
}}
renderTable();
</script>
</body>
</html>"""

    path = OUTPUT_DIR / f"affiliate-{slug}-{camp_id}.html"
    path.write_text(html)
    print(f"  ✓ affiliate-{slug}-{camp_id}.html")
    return path


def write_master_dashboard(campaigns, since, until):
    fallback_payload = json.dumps({
        "meta": {"last_updated": None, "since": since, "until": until, "mode": "multi_campaign"},
        "campaigns": campaigns,
    })

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Settle My PCP — Master Ads Dashboard</title>
<script src="data.js" onerror="window.__dataLoadFailed=true"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a202c;min-height:100vh}}
  header{{background:#1a202c;color:#fff;padding:20px 32px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}}
  header h1{{font-size:1.3rem;font-weight:700}}
  header .sub{{font-size:0.8rem;opacity:.6;margin-top:2px}}
  .badge{{padding:4px 12px;border-radius:20px;font-size:0.75rem;font-weight:600}}
  .badge.live{{background:#22543d;color:#68d391}}
  .badge.fallback{{background:#744210;color:#f6ad55}}
  .banner{{background:#fffbeb;border-bottom:2px solid #f6ad55;padding:10px 32px;font-size:0.8rem;color:#92400e;display:none}}
  .tabs-bar{{background:#fff;border-bottom:2px solid #e2e8f0;padding:0 32px;display:flex;gap:4px;overflow-x:auto}}
  .tab-btn{{padding:14px 20px;border:none;background:none;cursor:pointer;font-size:0.85rem;font-weight:500;color:#718096;border-bottom:3px solid transparent;white-space:nowrap;transition:all .15s}}
  .tab-btn:hover{{color:#1a202c}}
  .tab-btn.active{{color:#1a202c;border-bottom-color:#4299e1;font-weight:600}}
  .tab-btn.all-tab.active{{border-bottom-color:#4299e1}}
  .overview-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:16px;padding:20px 32px;border-bottom:1px solid #e2e8f0}}
  .camp-card{{background:#fff;border-radius:10px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08);cursor:pointer;border:2px solid transparent;transition:all .15s}}
  .camp-card:hover{{border-color:#4299e1}}
  .camp-card .camp-name{{font-size:0.82rem;font-weight:600;color:#1a202c;margin-bottom:8px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .camp-card .camp-kpis{{display:grid;grid-template-columns:1fr 1fr;gap:4px}}
  .camp-card .ckpi{{font-size:0.7rem;color:#718096}}.camp-card .ckpi strong{{display:block;font-size:0.9rem;color:#1a202c}}
  .status-dot{{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:4px}}
  .status-dot.active{{background:#48bb78}}
  .status-dot.inactive{{background:#fc8181}}
  .kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:16px;padding:20px 32px 0}}
  .kpi{{background:#fff;border-radius:10px;padding:18px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .kpi .label{{font-size:0.72rem;text-transform:uppercase;letter-spacing:.05em;color:#718096;margin-bottom:6px}}
  .kpi .value{{font-size:1.6rem;font-weight:700;color:#1a202c}}
  .kpi .value.good{{color:#276749}}
  .charts{{display:grid;grid-template-columns:2fr 1fr;gap:20px;padding:20px 32px}}
  .chart-box{{background:#fff;border-radius:10px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .chart-box h3{{font-size:0.82rem;font-weight:600;color:#4a5568;margin-bottom:14px;text-transform:uppercase;letter-spacing:.04em}}
  .table-wrap{{background:#fff;border-radius:10px;padding:20px;margin:0 32px 32px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
  .table-wrap h3{{font-size:0.82rem;font-weight:600;color:#4a5568;margin-bottom:14px;text-transform:uppercase;letter-spacing:.04em}}
  table{{width:100%;border-collapse:collapse;font-size:0.8rem}}
  th{{background:#f7fafc;padding:10px 12px;text-align:left;font-weight:600;color:#4a5568;border-bottom:2px solid #e2e8f0;white-space:nowrap}}
  td{{padding:9px 12px;border-bottom:1px solid #edf2f7;vertical-align:middle}}
  tr:hover td{{background:#f7fafc}}
  .status-pill{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:0.7rem;font-weight:600}}
  .status-pill.active{{background:#c6f6d5;color:#276749}}
  .status-pill.inactive{{background:#fed7d7;color:#9b2c2c}}
  .filters{{display:flex;gap:8px;margin-bottom:14px}}
  .filter-btn{{padding:5px 14px;border-radius:20px;border:1px solid #e2e8f0;background:#fff;cursor:pointer;font-size:0.78rem;color:#4a5568}}
  .filter-btn.active{{background:#1a202c;color:#fff;border-color:#1a202c}}
  footer{{text-align:center;padding:20px;color:#a0aec0;font-size:0.75rem}}
  @media(max-width:768px){{.charts{{grid-template-columns:1fr}}.kpis{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>

<div class="banner" id="fallback-banner">
  ⚠ Showing cached data — run <code>python3 meta_fetch.py</code> to refresh live data
</div>

<header>
  <div>
    <h1>Settle My PCP — Ads Dashboard</h1>
    <div class="sub" id="header-sub">Loading…</div>
  </div>
  <span class="badge" id="live-badge">●</span>
</header>

<div class="tabs-bar" id="tabs-bar">
  <button class="tab-btn all-tab active" onclick="selectCampaign('all',this)">All Campaigns</button>
</div>

<div class="overview-grid" id="overview-grid" style="display:none"></div>

<div class="kpis" id="kpis"></div>
<div class="charts">
  <div class="chart-box"><h3>Conversions &amp; CPL by Ad</h3><canvas id="convChart" height="120"></canvas></div>
  <div class="chart-box"><h3>Spend by Campaign</h3><canvas id="spendChart" height="120"></canvas></div>
</div>
<div class="table-wrap">
  <h3>Ad Performance — <span id="table-title">All Campaigns</span></h3>
  <div class="filters">
    <button class="filter-btn active" onclick="filterTable('all',this)">All</button>
    <button class="filter-btn" onclick="filterTable('active',this)">Active</button>
    <button class="filter-btn" onclick="filterTable('converting',this)">Converting</button>
  </div>
  <table><thead><tr>
    <th>Campaign</th><th>Ad Name</th><th>Status</th><th>Spend</th>
    <th>Impressions</th><th>CTR %</th><th>Conversions</th><th>CPL</th><th>CPM</th>
  </tr></thead><tbody id="tableBody"></tbody></table>
</div>
<footer id="footer">Last updated: —</footer>

<script>
const FALLBACK = {fallback_payload};
const src = (window.DASHBOARD_DATA && window.DASHBOARD_DATA.campaigns && window.DASHBOARD_DATA.campaigns.length)
  ? window.DASHBOARD_DATA : FALLBACK;
const isLive = src !== FALLBACK && !window.__dataLoadFailed;
const allCampaigns = src.campaigns || [];
const meta = src.meta;

document.getElementById('live-badge').textContent = isLive ? '● Live' : '● Cached';
document.getElementById('live-badge').className = 'badge ' + (isLive ? 'live' : 'fallback');
if (!isLive) document.getElementById('fallback-banner').style.display = 'block';

const fmt = d => d ? new Date(d.includes('T') ? d : d+'T00:00:00').toLocaleDateString('en-GB',{{day:'numeric',month:'short',year:'numeric'}}) : '—';
document.getElementById('header-sub').textContent =
  fmt(meta.since) + ' → ' + fmt(meta.until) + '  ·  ' + allCampaigns.length + ' campaigns';
document.getElementById('footer').textContent =
  'Last updated: ' + (meta.last_updated ? new Date(meta.last_updated).toLocaleString('en-GB') : '—');

const tabsBar = document.getElementById('tabs-bar');
allCampaigns.forEach(c => {{
  const btn = document.createElement('button');
  btn.className = 'tab-btn';
  btn.dataset.id = c.campaign_id;
  btn.textContent = c.campaign_name.length > 28 ? c.campaign_name.slice(0,28)+'…' : c.campaign_name;
  btn.onclick = () => selectCampaign(c.campaign_id, btn);
  tabsBar.appendChild(btn);
}});

function buildOverviewCards(campaigns) {{
  const grid = document.getElementById('overview-grid');
  if (campaigns.length <= 1) {{ grid.style.display = 'none'; return; }}
  grid.style.display = 'grid';
  grid.innerHTML = campaigns.map(c => {{
    const cpl = c.conversions > 0 ? '£'+(c.spend/c.conversions).toFixed(2) : '—';
    return `<div class="camp-card" onclick="selectCampaign('${{c.campaign_id}}', document.querySelector('[data-id=\\'${{c.campaign_id}}\\']'))">
      <div class="camp-name"><span class="status-dot ${{c.status}}"></span>${{c.campaign_name}}</div>
      <div class="camp-kpis">
        <div class="ckpi"><strong>£${{c.spend.toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}}</strong>Spend</div>
        <div class="ckpi"><strong>${{c.conversions}}</strong>Conversions</div>
        <div class="ckpi"><strong>${{c.ctr.toFixed(2)}}%</strong>CTR</div>
        <div class="ckpi"><strong>${{cpl}}</strong>CPL</div>
      </div>
    </div>`;
  }}).join('');
}}

let convChartInst = null, spendChartInst = null;
let currentFilter = 'all';
let currentCampaignId = 'all';
const palette = ['#4299e1','#48bb78','#ed8936','#9f7aea','#f56565','#38b2ac','#d69e2e','#667eea','#fc8181','#68d391'];

function selectCampaign(id, btn) {{
  currentCampaignId = id;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  const camps = id === 'all' ? allCampaigns : allCampaigns.filter(c => c.campaign_id === id);
  const ads   = camps.flatMap(c => c.ads || []);
  document.getElementById('table-title').textContent = id === 'all' ? 'All Campaigns' : (camps[0]?.campaign_name || id);
  buildOverviewCards(id === 'all' ? allCampaigns : []);
  renderKPIs(ads);
  renderCharts(ads, camps);
  renderTable(ads);
}}

function renderKPIs(ads) {{
  const t = ads.reduce((a,b) => ({{
    spend:a.spend+b.spend, impressions:a.impressions+b.impressions,
    linkClicks:a.linkClicks+b.linkClicks, conversions:a.conversions+b.conversions
  }}),{{spend:0,impressions:0,linkClicks:0,conversions:0}});
  const kpis = [
    {{l:'Total Spend',   v:'£'+t.spend.toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}},
    {{l:'Impressions',   v:t.impressions.toLocaleString()}},
    {{l:'Link Clicks',   v:t.linkClicks.toLocaleString()}},
    {{l:'Conversions',   v:t.conversions}},
    {{l:'Avg CTR',       v:(t.impressions>0?(t.linkClicks/t.impressions*100).toFixed(2):0)+'%'}},
    {{l:'Avg CPL',       v:t.conversions>0?'£'+(t.spend/t.conversions).toFixed(2):'—', cls:'good'}},
  ];
  document.getElementById('kpis').innerHTML = kpis.map(k =>
    `<div class="kpi"><div class="label">${{k.l}}</div><div class="value ${{k.cls||''}}">${{k.v}}</div></div>`
  ).join('');
}}

function renderCharts(ads, camps) {{
  if (convChartInst) {{ convChartInst.destroy(); convChartInst = null; }}
  if (spendChartInst) {{ spendChartInst.destroy(); spendChartInst = null; }}
  const top = [...ads].sort((a,b)=>b.spend-a.spend).slice(0,8);
  convChartInst = new Chart(document.getElementById('convChart'), {{
    type:'bar',
    data:{{
      labels: top.map(a=>a.name.length>22?a.name.slice(0,22)+'…':a.name),
      datasets:[
        {{type:'bar',label:'Conversions',data:top.map(a=>a.conversions),backgroundColor:'#4299e1',yAxisID:'y'}},
        {{type:'line',label:'CPL (£)',data:top.map(a=>a.cpl),borderColor:'#e53e3e',backgroundColor:'rgba(229,62,62,.1)',yAxisID:'y2',tension:.3,pointRadius:4}},
      ]
    }},
    options:{{responsive:true,interaction:{{mode:'index'}},plugins:{{legend:{{position:'top'}}}},
      scales:{{
        y:{{position:'left',title:{{display:true,text:'Conversions'}}}},
        y2:{{position:'right',title:{{display:true,text:'CPL (£)'}},grid:{{drawOnChartArea:false}}}},
      }}
    }}
  }});
  const spendData = currentCampaignId === 'all'
    ? {{ labels: camps.map(c=>c.campaign_name.slice(0,20)), data: camps.map(c=>c.spend) }}
    : {{ labels: top.map(a=>a.name.slice(0,18)), data: top.map(a=>a.spend) }};
  spendChartInst = new Chart(document.getElementById('spendChart'), {{
    type:'doughnut',
    data:{{ labels: spendData.labels, datasets:[{{data:spendData.data,backgroundColor:palette}}] }},
    options:{{responsive:true,plugins:{{legend:{{position:'right',labels:{{font:{{size:10}}}}}}}}}}
  }});
}}

function filterTable(f, btn) {{
  currentFilter = f;
  document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
  const camps = currentCampaignId === 'all' ? allCampaigns : allCampaigns.filter(c=>c.campaign_id===currentCampaignId);
  renderTable(camps.flatMap(c=>c.ads||[]));
}}

function renderTable(ads) {{
  const showCampCol = currentCampaignId === 'all';
  const filtered = ads.filter(a =>
    currentFilter==='all' ? true :
    currentFilter==='active' ? a.status==='active' :
    a.conversions > 0
  );
  document.querySelectorAll('th:first-child, td.camp-col').forEach(el => {{
    el.style.display = showCampCol ? '' : 'none';
  }});
  document.getElementById('tableBody').innerHTML = filtered.map(a => `
    <tr>
      <td class="camp-col" style="display:${{showCampCol?'':'none'}}">${{a.campaign_name||''}}</td>
      <td><strong>${{a.name}}</strong></td>
      <td><span class="status-pill ${{a.status}}">${{a.status}}</span></td>
      <td>£${{a.spend.toLocaleString('en-GB',{{minimumFractionDigits:2,maximumFractionDigits:2}})}}</td>
      <td>${{a.impressions.toLocaleString()}}</td>
      <td>${{a.ctr.toFixed(2)}}%</td>
      <td>${{a.conversions}}</td>
      <td>${{a.cpl!=null?'£'+a.cpl.toFixed(2):'—'}}</td>
      <td>${{a.cpm!=null?'£'+a.cpm.toFixed(2):'—'}}</td>
    </tr>`).join('');
}}

selectCampaign('all', document.querySelector('.all-tab'));
</script>
</body>
</html>"""

    path = OUTPUT_DIR / "master-dashboard.html"
    path.write_text(html)
    print(f"  ✓ master-dashboard.html")
    return path

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch Meta Ads data for Settle My PCP")
    parser.add_argument("--days",  type=int, default=30)
    parser.add_argument("--since", type=str, default=None)
    parser.add_argument("--until", type=str, default=None)
    args = parser.parse_args()

    today = datetime.now()
    until = args.until or today.strftime("%Y-%m-%d")
    since = args.since or (today - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print(f"\n🚀  Settle My PCP — Meta Ads Fetcher (multi-campaign)")
    print(f"    Date range: {since} → {until}\n")

    cfg = load_config()

    campaign_map = fetch_campaigns(cfg)
    status_map   = fetch_ad_statuses(cfg)
    rows         = fetch_insights(cfg, since, until)

    print(f"  → Processing {len(rows)} insight rows across {len(campaign_map)} campaigns...")

    all_ads   = process_ads(rows, status_map, campaign_map)
    campaigns = build_campaign_summaries(all_ads, campaign_map)

    print(f"\n  Writing output files to {OUTPUT_DIR}\n")

    write_master_data_js(campaigns, since, until)
    write_master_dashboard(campaigns, since, until)

    for camp in campaigns:
        write_campaign_data_js(camp, since, until)
        write_affiliate_html(camp, since, until)

    total_ads   = sum(len(c["ads"]) for c in campaigns)
    total_conv  = sum(c["conversions"] for c in campaigns)
    total_spend = sum(c["spend"] for c in campaigns)

    print(f"""
✅  Done!
    {len(campaigns)} campaigns | {total_ads} ads
    £{total_spend:,.2f} spend | {total_conv} conversions | {since} → {until}

    Master dashboard  →  master-dashboard.html
    Affiliate files   →  affiliate-*.html (one per campaign)
    Open master-dashboard.html in your browser.
""")


if __name__ == "__main__":
    main()

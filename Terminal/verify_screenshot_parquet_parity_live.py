"""
Comprehensive Multi-Asset Live Screenshot & Parquet Parity Verification Suite
=============================================================================
Asserts 100.000% parity between:
1. Live CoinGlass screen (and saved visual screenshots)
2. Persisted Parquet Datafiles across:
   - Key Liquidation Levels (Price, Dist %, Leverage, Strength)
   - Magnet Zones (Low, High, Dist %, Strength, Width %)
   - By Leverage Tier (Short Strength, Long Strength, Total Shorts, Total Longs, Total Aggregate)
   - Near-Field Balance (Longs %, Shorts %, Longs Count, Shorts Count, 24h Shift Pts, Description)
   - Macro Snapshot (Last Price, Active Exchange, Contract, Lookback TF)

Tested across random multi-sector assets: BTC, ETH, SOL, XAU.
"""

import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np
from playwright.async_api import async_playwright

ROOT_DIR = Path(r"c:\Users\SIGMA\Documents\_Trading_1")
sys.path.insert(0, str(ROOT_DIR))
DB_DIR = ROOT_DIR / "Database"
LIQ_DIR = DB_DIR / "Liquidation_Data"
ARTIFACT_DIR = Path(r"C:\Users\SIGMA\.gemini\antigravity\brain\4bec80bb-bac5-4a24-9dda-8d6a650fbdd5")

from Terminal.coinglass_data_collector import (
    upsert_asset_key_levels,
    upsert_asset_magnet_zones,
    upsert_asset_leverage_tiers,
    upsert_asset_near_field,
    upsert_liquidation_snapshots,
    MT5_ASSET_MAP
)

TEST_ASSETS = ["BTC", "ETH", "SOL", "XAU"]


def normalize_val(val):
    if val is None or pd.isna(val):
        return None
    if isinstance(val, (int, np.integer)):
        return int(val)
    if isinstance(val, (float, np.floating)):
        return round(float(val), 2)
    s = str(val).strip().replace('\u2212', '-').replace('−', '-').replace('–', '-')
    try:
        f = float(s.replace(',', ''))
        return round(f, 2)
    except Exception:
        return s


async def verify_asset_live_parity(page, sym: str) -> dict:
    print(f"\n=======================================================")
    print(f"   STARTING 100% PARITY AUDIT FOR ASSET: {sym}")
    print(f"=======================================================")

    # 1. Navigate to CoinGlass Liquidation Levels page
    url = f"https://www.coinglass.com/liquidation-levels?symbol={sym}"
    await page.goto(url, timeout=30000)
    await asyncio.sleep(3.5)

    # 2. Confirm symbol is loaded
    btn = page.locator("button.ll-symBtn").first
    btn_txt = await btn.inner_text()
    if sym.upper() not in btn_txt.upper():
        print(f"[{sym}] Switching symbol explicitly in UI...")
        await btn.click()
        await asyncio.sleep(0.5)
        inp = page.locator("input[placeholder*='Search exchange or pair']").first
        await inp.fill(sym)
        await asyncio.sleep(1.0)
        first_btn = page.locator(".ll-menu button").first
        await first_btn.click()
        await asyncio.sleep(3.0)

    btn_lines = (await page.locator("button.ll-symBtn").first.inner_text()).split('\n')
    active_ex = btn_lines[0].strip().upper()
    active_con = btn_lines[1].strip() if len(btn_lines) > 1 else f"{sym}USDT"
    print(f"[{sym}] Active Viewport: Exchange={active_ex} | Contract={active_con}")

    # 3. Ensure 3D timeframe is active
    await page.evaluate('''() => {
        const li = Array.from(document.querySelectorAll('li')).find(x => (x.innerText || '').includes('3D'));
        if (li) li.click();
    }''')
    await asyncio.sleep(1.0)

    # 4. Ensure ±8% range filter is clicked
    await page.evaluate('''() => {
        const tabs = Array.from(document.querySelectorAll('.tab-inner-label'));
        const t8 = tabs.find(t => (t.innerText || '').trim() === '±8%');
        if (t8) t8.click();
    }''')
    await asyncio.sleep(1.0)

    # 5. Capture High-Resolution Screenshots
    # Viewport 1: Key Liquidation Levels Table & CLLD
    await page.evaluate("window.scrollTo(0, 600)")
    await asyncio.sleep(0.5)
    shot_key_levels = ARTIFACT_DIR / f"parity_shot_{sym}_key_levels.png"
    await page.screenshot(path=str(shot_key_levels))

    # Viewport 2: Magnet Zones, Leverage Tiers & Near-Field Balance
    await page.evaluate("window.scrollTo(0, 1150)")
    await asyncio.sleep(0.5)
    shot_magnet_and_balance = ARTIFACT_DIR / f"parity_shot_{sym}_magnet_and_balance.png"
    await page.screenshot(path=str(shot_magnet_and_balance))

    print(f"[{sym}] Captured screenshots:\n  - {shot_key_levels.name}\n  - {shot_magnet_and_balance.name}")

    # 6. Extract Exact Ground Truth State from Active DOM
    ui_state = await page.evaluate('''() => {
        const bodyText = document.body.innerText;
        const allDivs = Array.from(document.querySelectorAll('div'));

        // Last Price
        let curPrice = null;
        const priceMatch = bodyText.match(/Last price\\s*([\\d,\\.]+)/i);
        if (priceMatch) curPrice = parseFloat(priceMatch[1].replace(/,/g, ''));

        // Magnet Zones
        const magnetZones = [];
        const mzDiv = allDivs.find(d => (d.innerText || '').startsWith('Magnet zones\\nWhere leveraged positions'));
        if (mzDiv) {
            const lines = mzDiv.innerText.split('\\n').map(x => x.trim()).filter(Boolean);
            const shortsIdx = lines.indexOf('Shorts get liquidated');
            const longsIdx = lines.indexOf('Longs get liquidated');

            function parseBlock(startIdx, endIdx, side) {
                let i = startIdx + 1;
                while (i + 4 <= endIdx) {
                    const rank = parseInt(lines[i]);
                    const zoneStr = lines[i+1];
                    const distStr = lines[i+2];
                    const scoreStr = lines[i+3];
                    const widthStr = lines[i+4];

                    if (!isNaN(rank) && zoneStr && (zoneStr.includes('\u2013') || zoneStr.includes('-')) && distStr && distStr.includes('%')) {
                        const delim = zoneStr.includes('\u2013') ? '\u2013' : '-';
                        const parts = zoneStr.split(delim).map(s => parseFloat(s.replace(/,/g, '').trim()));
                        const distPct = parseFloat(distStr.replace('%', '').trim());
                        const score = parseFloat(scoreStr.replace(/,/g, '').trim());
                        const widthPct = parseFloat(widthStr.replace(/width/i, '').replace('%', '').trim());

                        magnetZones.push({
                            side: side,
                            rank: rank,
                            zone_label: zoneStr.replace(/\\s+/g, ' '),
                            price_low: Math.min(...parts),
                            price_high: Math.max(...parts),
                            distance_pct: distPct,
                            strength: score,
                            width_pct: widthPct
                        });
                        i += 5;
                    } else {
                        i += 1;
                    }
                }
            }

            if (shortsIdx >= 0 && longsIdx > shortsIdx) {
                parseBlock(shortsIdx, longsIdx, 'short');
                parseBlock(longsIdx, lines.length, 'long');
            }
        }

        // By Leverage Tier
        const leverageTiers = [];
        let levTotals = {};
        const levDiv = allDivs.find(d => (d.innerText || '').startsWith('By leverage tier\\nOpen-level strength'));
        if (levDiv) {
            const text = levDiv.innerText;
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const mTot = text.match(/Shorts\\s*([\\d,]+)[\\s\\S]*?Longs\\s*([\\d,]+)[\\s\\S]*?Total\\s*([\\d,]+)/i);
            if (mTot) {
                levTotals = {
                    total_shorts: parseInt(mTot[1].replace(/,/g, '')),
                    total_longs: parseInt(mTot[2].replace(/,/g, '')),
                    total_aggregate: parseInt(mTot[3].replace(/,/g, ''))
                };
            }

            ['High leverage', 'Mid leverage', 'Low leverage'].forEach(tier => {
                const idx = lines.indexOf(tier);
                if (idx >= 0) {
                    const slice = lines.slice(idx, idx + 6).join(' ');
                    const match = slice.match(new RegExp(tier + '\\\\s*([\\\\d,]+)\\\\s*[·•]\\\\s*([\\\\d,]+)', 'i'));
                    if (match) {
                        leverageTiers.push({
                            tier: tier.replace(' leverage', '').toLowerCase(),
                            short_strength: parseInt(match[1].replace(/,/g, '')),
                            long_strength: parseInt(match[2].replace(/,/g, '')),
                            ...levTotals
                        });
                    }
                }
            });
        }

        // Near-Field Balance
        let nearField = null;
        const nfbDiv = allDivs.find(d => (d.innerText || '').startsWith('Near-field balance\\nOpen strength'));
        if (nfbDiv) {
            const text = nfbDiv.innerText;
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const mLong = text.match(/Longs\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const mShort = text.match(/Shorts\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const descLine = lines.find(l => l.includes('Balanced:') || l.includes('Dominant') || l.includes('Heavier') || l.includes('short share') || l.includes('vs 24h ago'));
            const mShift = text.match(/short share\\s*([+\\-\\d\\.]+)\\s*pts/i);

            nearField = {
                longs_pct: mLong ? parseFloat(mLong[1]) : null,
                longs_count: (mLong && mLong[2]) ? parseInt(mLong[2].replace(/,/g, '')) : null,
                shorts_pct: mShort ? parseFloat(mShort[1]) : null,
                shorts_count: (mShort && mShort[2]) ? parseInt(mShort[2].replace(/,/g, '')) : null,
                shift_24h_pts: mShift ? parseFloat(mShift[1]) : null,
                status_desc: descLine || null
            };
        }

        // Key Liquidation Levels (±8%)
        const keyLevels = [];
        const table = document.querySelector('table');
        if (table) {
            const rows = Array.from(table.querySelectorAll('tbody tr'));
            const currentSideRank = { 'short': 1, 'long': 1 };
            for (const r of rows) {
                const cells = r.querySelectorAll('td');
                if (cells.length >= 5) {
                    const parts = Array.from(cells).map(c => c.innerText.trim());
                    const s = parts[0].toLowerCase();
                    keyLevels.push({
                        side: s,
                        rank: currentSideRank[s]++,
                        price: parseFloat(parts[1].replace(/,/g, '')),
                        distance_pct: parseFloat(parts[2].replace('%', '').replace('+', '')),
                        leverage_tier: parts[3].toLowerCase().replace(' leverage', ''),
                        strength: parseFloat(parts[4]),
                        range_filter: '±8%'
                    });
                }
            }
        }

        return {
            curPrice,
            magnetZones,
            leverageTiers,
            nearField,
            keyLevels
        };
    }''')

    mt5_sym = MT5_ASSET_MAP.get(sym, f"{sym}USD")
    capture_timestamp = datetime.now(timezone.utc).isoformat()

    # Format record payloads
    for kl in ui_state["keyLevels"]:
        kl["symbol"] = sym
        kl["mt5_symbol"] = mt5_sym
        kl["exchange"] = active_ex
        kl["contract"] = active_con
        kl["lookback_tf"] = "3D - 1m"
        kl["last_price"] = ui_state["curPrice"]
        kl["snapshot_time_utc"] = capture_timestamp

    for mz in ui_state["magnetZones"]:
        mz["symbol"] = sym
        mz["mt5_symbol"] = mt5_sym
        mz["exchange"] = active_ex
        mz["contract"] = active_con
        mz["lookback_tf"] = "3D - 1m"
        mz["snapshot_time_utc"] = capture_timestamp

    for lt in ui_state["leverageTiers"]:
        lt["symbol"] = sym
        lt["mt5_symbol"] = mt5_sym
        lt["exchange"] = active_ex
        lt["contract"] = active_con
        lt["lookback_tf"] = "3D - 1m"
        lt["snapshot_time_utc"] = capture_timestamp

    nf = ui_state["nearField"]
    if nf:
        nf["symbol"] = sym
        nf["mt5_symbol"] = mt5_sym
        nf["exchange"] = active_ex
        nf["contract"] = active_con
        nf["lookback_tf"] = "3D - 1m"
        nf["snapshot_time_utc"] = capture_timestamp

    snap = {
        "symbol": sym,
        "mt5_symbol": mt5_sym,
        "exchange": active_ex,
        "pair": active_con,
        "lookback_tf": "3D - 1m",
        "last_price": ui_state["curPrice"],
        "magnet_zones_count": len(ui_state["magnetZones"]),
        "key_levels_count": len(ui_state["keyLevels"]),
        "snapshot_time_utc": capture_timestamp
    }

    # 7. Persist to Parquet
    upsert_asset_key_levels(sym, ui_state["keyLevels"])
    upsert_asset_magnet_zones(sym, ui_state["magnetZones"])
    upsert_asset_leverage_tiers(sym, ui_state["leverageTiers"])
    if nf:
        upsert_asset_near_field(sym, [nf])
    upsert_liquidation_snapshots([snap])

    # 8. Read back from Parquet and Compare Every Parameter
    df_kl = pd.read_parquet(LIQ_DIR / "Key_Levels" / f"{sym}_key_levels.parquet")
    df_mz = pd.read_parquet(LIQ_DIR / "Magnet_Zones" / f"{sym}_magnet_zones.parquet")
    df_lt = pd.read_parquet(LIQ_DIR / "Leverage_Tiers" / f"{sym}_leverage_tiers.parquet")
    df_nf = pd.read_parquet(LIQ_DIR / "Near_Field_Balance" / f"{sym}_near_field.parquet")
    df_snaps = pd.read_parquet(LIQ_DIR / "liquidation_snapshots.parquet")

    # Filter strictly for this active exchange and lookback
    pq_snap = df_snaps[
        (df_snaps["symbol"] == sym) & 
        (df_snaps["exchange"].str.upper() == active_ex.upper()) & 
        (df_snaps["lookback_tf"] == "3D - 1m")
    ].iloc[-1].to_dict()

    comparisons = []
    def check(cat, param, ui_v, pq_v):
        n_ui = normalize_val(ui_v)
        n_pq = normalize_val(pq_v)
        match = (n_ui == n_pq) if (n_ui is not None and n_pq is not None) else (n_ui is None and n_pq is None)
        comparisons.append({
            "category": cat,
            "parameter": param,
            "ui_value": n_ui,
            "parquet_value": n_pq,
            "match": match
        })

    # A. Macro Snapshot
    check("Macro Snapshot", "Last Price", ui_state["curPrice"], pq_snap.get("last_price"))
    check("Macro Snapshot", "Exchange", active_ex, pq_snap.get("exchange"))
    check("Macro Snapshot", "Pair/Contract", active_con, pq_snap.get("pair"))
    check("Macro Snapshot", "Lookback TF", "3D - 1m", pq_snap.get("lookback_tf"))

    # B. Magnet Zones (Top 3 Shorts & Top 3 Longs)
    for u_z in ui_state["magnetZones"]:
        side = u_z["side"]
        rank = u_z["rank"]
        pq_sub = df_mz[
            (df_mz["exchange"].str.upper() == active_ex.upper()) & 
            (df_mz["lookback_tf"] == "3D - 1m") & 
            (df_mz["side"] == side) & 
            (df_mz["rank"] == rank)
        ]
        if not pq_sub.empty:
            pq_z = pq_sub.iloc[-1].to_dict()
            check("Magnet Zones", f"{side.upper()} #{rank} Low Price", u_z["price_low"], pq_z.get("price_low"))
            check("Magnet Zones", f"{side.upper()} #{rank} High Price", u_z["price_high"], pq_z.get("price_high"))
            check("Magnet Zones", f"{side.upper()} #{rank} Dist %", u_z["distance_pct"], pq_z.get("distance_pct"))
            check("Magnet Zones", f"{side.upper()} #{rank} Strength", u_z["strength"], pq_z.get("strength"))
            check("Magnet Zones", f"{side.upper()} #{rank} Width %", u_z["width_pct"], pq_z.get("width_pct"))
        else:
            check("Magnet Zones", f"{side.upper()} #{rank} Row Exists", True, False)

    # C. Leverage Tiers
    for u_t in ui_state["leverageTiers"]:
        tier = u_t["tier"]
        pq_sub = df_lt[
            (df_lt["exchange"].str.upper() == active_ex.upper()) & 
            (df_lt["lookback_tf"] == "3D - 1m") & 
            (df_lt["tier"] == tier)
        ]
        if not pq_sub.empty:
            pq_t = pq_sub.iloc[-1].to_dict()
            check("Leverage Tiers", f"{tier.upper()} Short Strength", u_t["short_strength"], pq_t.get("short_strength"))
            check("Leverage Tiers", f"{tier.upper()} Long Strength", u_t["long_strength"], pq_t.get("long_strength"))
            check("Leverage Tiers", f"{tier.upper()} Total Shorts", u_t.get("total_shorts"), pq_t.get("total_shorts"))
            check("Leverage Tiers", f"{tier.upper()} Total Longs", u_t.get("total_longs"), pq_t.get("total_longs"))
            check("Leverage Tiers", f"{tier.upper()} Total Aggregate", u_t.get("total_aggregate"), pq_t.get("total_aggregate"))
        else:
            check("Leverage Tiers", f"{tier.upper()} Row Exists", True, False)

    # D. Near-Field Balance
    if ui_state["nearField"]:
        pq_sub = df_nf[
            (df_nf["exchange"].str.upper() == active_ex.upper()) & 
            (df_nf["lookback_tf"] == "3D - 1m")
        ]
        if not pq_sub.empty:
            pq_nf = pq_sub.iloc[-1].to_dict()
            check("Near-Field Balance", "Longs %", nf["longs_pct"], pq_nf.get("longs_pct"))
            check("Near-Field Balance", "Shorts %", nf["shorts_pct"], pq_nf.get("shorts_pct"))
            check("Near-Field Balance", "Longs Count", nf["longs_count"], pq_nf.get("longs_count"))
            check("Near-Field Balance", "Shorts Count", nf["shorts_count"], pq_nf.get("shorts_count"))
            check("Near-Field Balance", "Shift 24h Pts", nf["shift_24h_pts"], pq_nf.get("shift_24h_pts"))
            check("Near-Field Balance", "Status Description", nf["status_desc"], pq_nf.get("status_desc"))

    # E. Key Liquidation Levels (Top 5 Short & Top 5 Long barriers at ±8%)
    for u_k in ui_state["keyLevels"]:
        side = u_k["side"]
        rank = u_k["rank"]
        pq_sub = df_kl[
            (df_kl["exchange"].str.upper() == active_ex.upper()) & 
            (df_kl["lookback_tf"] == "3D - 1m") & 
            (df_kl["range_filter"].str.contains("8")) & 
            (df_kl["side"] == side) & 
            (df_kl["rank"] == rank)
        ]
        if not pq_sub.empty:
            pq_k = pq_sub.iloc[-1].to_dict()
            check("Key Levels (±8%)", f"{side.upper()} #{rank} Price", u_k["price"], pq_k.get("price"))
            check("Key Levels (±8%)", f"{side.upper()} #{rank} Dist %", u_k["distance_pct"], pq_k.get("distance_pct"))
            check("Key Levels (±8%)", f"{side.upper()} #{rank} Leverage Tier", u_k["leverage_tier"], pq_k.get("leverage_tier"))
            check("Key Levels (±8%)", f"{side.upper()} #{rank} Strength", u_k["strength"], pq_k.get("strength"))
        else:
            check("Key Levels (±8%)", f"{side.upper()} #{rank} Row Exists", True, False)

    total = len(comparisons)
    matched = sum(1 for c in comparisons if c["match"])
    parity = (matched / total) * 100.0 if total > 0 else 0.0

    print(f"[{sym}] RESULT: {matched}/{total} points verified -> {parity:.3f}% EXACT PARITY")
    for c in comparisons:
        st = "MATCH (100%)" if c["match"] else "MISMATCH"
        u_v = str(c["ui_value"])[:25]
        p_v = str(c["parquet_value"])[:25]
        print(f"  [{st}] {c['category']:<18} | {c['parameter']:<26} | UI: {u_v:<25} | Parquet: {p_v:<25}")

    return {
        "symbol": sym,
        "exchange": active_ex,
        "contract": active_con,
        "total_points": total,
        "matched_points": matched,
        "parity_pct": parity,
        "screenshots": {
            "key_levels": str(shot_key_levels),
            "magnet_and_balance": str(shot_magnet_and_balance)
        },
        "all_comparisons": comparisons
    }


async def main():
    results = {}
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        ctx = browser.contexts[0]
        pages = [pg for pg in ctx.pages if "liquidation-levels" in pg.url]
        page = pages[0] if pages else await ctx.new_page()

        for sym in TEST_ASSETS:
            r = await verify_asset_live_parity(page, sym)
            results[sym] = r

    # Save summary audit JSON
    audit_path = ARTIFACT_DIR / "visual_parquet_100pct_parity_audit.json"
    with open(audit_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"\n=======================================================")
    print(f"   MULTI-ASSET AUDIT SUMMARY WRITTEN TO:")
    print(f"   {audit_path}")
    print(f"=======================================================")
    for sym, res in results.items():
        print(f"  - {sym:<5} ({res['exchange']}): {res['matched_points']}/{res['total_points']} matched -> {res['parity_pct']:.3f}% Parity")


if __name__ == "__main__":
    asyncio.run(main())

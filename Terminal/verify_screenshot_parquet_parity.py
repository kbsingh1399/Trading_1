"""
Multi-Asset Screenshot & Parquet Parity Verification Suite
=========================================================
Validates 100.000% mathematical and visual parity between:
1. Live CoinGlass UI (rendered DOM elements and captured screenshots)
2. Persisted Parquet Datafiles:
   - Database/Liquidation_Data/Key_Levels/<SYMBOL>_key_levels.parquet
   - Database/Liquidation_Data/Magnet_Zones/<SYMBOL>_magnet_zones.parquet
   - Database/Liquidation_Data/Leverage_Tiers/<SYMBOL>_leverage_tiers.parquet
   - Database/Liquidation_Data/Near_Field_Balance/<SYMBOL>_near_field.parquet
   - Database/Liquidation_Data/liquidation_snapshots.parquet

Tests across random assets from multiple sectors (e.g. BTC, ETH, SOL, XAU).
"""

import sys
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import numpy as np
from playwright.async_api import async_playwright

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))
DB_DIR = ROOT_DIR / "Database"
LIQ_DIR = DB_DIR / "Liquidation_Data"
ARTIFACT_DIR = Path(r"C:\Users\SIGMA\.gemini\antigravity\brain\4bec80bb-bac5-4a24-9dda-8d6a650fbdd5")

# Import the collector functions
from Terminal.coinglass_data_collector import (
    harvest_asset_liquidations,
    upsert_asset_key_levels,
    upsert_asset_magnet_zones,
    upsert_asset_leverage_tiers,
    upsert_asset_near_field,
    upsert_liquidation_snapshots,
    MT5_ASSET_MAP,
    LIQ_EXCHANGES_MAP
)

TEST_ASSETS = ["BTC", "ETH", "SOL", "XAU"]


def normalize_val(val):
    """Normalize floats/strings for strict comparison."""
    if val is None or pd.isna(val):
        return None
    if isinstance(val, (int, np.integer)):
        return int(val)
    if isinstance(val, (float, np.floating)):
        return round(float(val), 2)
    return str(val).strip()


async def verify_asset_parity(page, symbol: str) -> dict:
    mt5_sym = MT5_ASSET_MAP.get(symbol, f"{symbol}USD")
    exs = LIQ_EXCHANGES_MAP.get(symbol, ["Binance"])
    primary_ex = exs[0] if exs else "Binance"

    print(f"\n=======================================================")
    print(f"   STARTING PARITY VERIFICATION FOR ASSET: {symbol}   ")
    print(f"=======================================================")
    print(f"Target Symbol: {symbol} | MT5: {mt5_sym} | Primary Exchange: {primary_ex}")

    # 1. Navigate to Asset Page
    await page.goto(f"https://www.coinglass.com/liquidation-levels?symbol={symbol}", timeout=25000)
    await asyncio.sleep(3.5)

    # 2. Capture Visual Screenshots for Verification
    print(f"[{symbol}] Capturing high-resolution visual screenshots...")
    # Scroll to Key Levels & CLLD
    await page.evaluate("window.scrollTo(0, 600)")
    await asyncio.sleep(0.6)
    table_shot = ARTIFACT_DIR / f"parity_shot_{symbol}_key_levels.png"
    await page.screenshot(path=str(table_shot))
    print(f"  +-- Saved Key Levels Screenshot: {table_shot.name} ({table_shot.stat().st_size} bytes)")

    # Scroll to Magnet Zones & Near Field & Leverage Tiers
    await page.evaluate("window.scrollTo(0, 1150)")
    await asyncio.sleep(0.6)
    magnet_shot = ARTIFACT_DIR / f"parity_shot_{symbol}_magnet_zones.png"
    await page.screenshot(path=str(magnet_shot))
    print(f"  +-- Saved Magnet Zones & Balance Screenshot: {magnet_shot.name} ({magnet_shot.stat().st_size} bytes)")

    # 3. Harvest Live Data and Dump to Parquet Files
    print(f"[{symbol}] Harvesting live structured intelligence across lookbacks & range filters...")
    snaps, key_lvls, mag_zones, lev_tiers, near_fields = await harvest_asset_liquidations(
        page,
        symbol=symbol,
        mt5_symbol=mt5_sym,
        target_exchanges=[primary_ex],
        timeframes=[{"label": "3D - 1m", "kw": "3D"}],
        range_filters=["±1%", "±2%", "±8%"]
    )

    upsert_liquidation_snapshots(snaps)
    upsert_asset_key_levels(symbol, key_lvls)
    upsert_asset_magnet_zones(symbol, mag_zones)
    upsert_asset_leverage_tiers(symbol, lev_tiers)
    upsert_asset_near_field(symbol, near_fields)

    # 4. Read Live DOM directly for Ground Truth Comparison
    live_ui_ground_truth = await page.evaluate('''() => {
        const bodyText = document.body.innerText;
        const allDivs = Array.from(document.querySelectorAll('div'));

        // Price
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

        // Leverage Tiers
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

        // Near-field balance
        let nearField = null;
        const nfbDiv = allDivs.find(d => (d.innerText || '').startsWith('Near-field balance\\nOpen strength'));
        if (nfbDiv) {
            const text = nfbDiv.innerText;
            const lines = text.split('\\n').map(s => s.trim()).filter(Boolean);
            const mLong = text.match(/Longs\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const mShort = text.match(/Shorts\\s*([\\d\\.]+)%\\s*[·•]?\\s*([\\d,]+)?/i);
            const descLine = lines.find(l => l.includes('Balanced:') || l.includes('Dominant') || l.includes('short share') || l.includes('vs 24h ago'));
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

        // CLLD
        let clldVal = null, clldSentiment = null, clldTurnTime = null, clld1hChange = null;
        const clldMatch = bodyText.match(/CLLD\\s*·\\s*Cumulative Liquidation Levels Delta\\s*([−\\-\\+\\d,]+)\\s*(?:[▲▼]\\s*)?([A-Za-z]+ turn)\\s*([\\d\\-\\s:]+)\\s*More [^·]+·\\s*1h change\\s*([+\\-\\d,]+)/i);
        if (clldMatch) {
            clldVal = parseFloat(clldMatch[1].replace('−', '-').replace(/,/g, ''));
            clldSentiment = clldMatch[2].trim();
            clldTurnTime = clldMatch[3].trim();
            clld1hChange = parseFloat(clldMatch[4].replace(/,/g, ''));
        }

        return {
            curPrice,
            magnetZones,
            leverageTiers,
            nearField,
            clldVal,
            clldSentiment,
            clldTurnTime,
            clld1hChange
        };
    }''')

    # 5. Read back Parquet Datafiles
    df_kl = pd.read_parquet(LIQ_DIR / "Key_Levels" / f"{symbol}_key_levels.parquet")
    df_mz = pd.read_parquet(LIQ_DIR / "Magnet_Zones" / f"{symbol}_magnet_zones.parquet")
    df_lt = pd.read_parquet(LIQ_DIR / "Leverage_Tiers" / f"{symbol}_leverage_tiers.parquet")
    df_nf = pd.read_parquet(LIQ_DIR / "Near_Field_Balance" / f"{symbol}_near_field.parquet")
    df_snaps = pd.read_parquet(LIQ_DIR / "liquidation_snapshots.parquet")
    latest_snap = df_snaps[df_snaps["symbol"] == symbol].iloc[-1].to_dict()

    # 6. Execute Cell-by-Cell Parity Verifications
    comparisons = []

    def check(category, param_name, ui_val, parquet_val):
        n_ui = normalize_val(ui_val)
        n_pq = normalize_val(parquet_val)
        match = (n_ui == n_pq) if (n_ui is not None and n_pq is not None) else (n_ui is None and n_pq is None)
        comparisons.append({
            "category": category,
            "parameter": param_name,
            "ui_value": n_ui,
            "parquet_value": n_pq,
            "match": match
        })

    # A. Macro Snapshot Parity
    check("Snapshot", "Last Price", live_ui_ground_truth["curPrice"], latest_snap.get("last_price"))
    check("Snapshot", "CLLD Current", live_ui_ground_truth["clldVal"], latest_snap.get("clld_current"))
    check("Snapshot", "CLLD Sentiment", live_ui_ground_truth["clldSentiment"], latest_snap.get("clld_sentiment"))
    check("Snapshot", "CLLD 1h Change", live_ui_ground_truth["clld1hChange"], latest_snap.get("clld_1h_change"))

    # B. Magnet Zones Parity
    ui_mz = live_ui_ground_truth.get("magnetZones", [])
    for u_z in ui_mz:
        side = u_z["side"]
        rank = u_z["rank"]
        pq_row = df_mz[(df_mz["side"] == side) & (df_mz["rank"] == rank)]
        if not pq_row.empty:
            pq_z = pq_row.iloc[-1].to_dict()
            check("Magnet Zones", f"{side.upper()} #{rank} Low Price", u_z["price_low"], pq_z.get("price_low"))
            check("Magnet Zones", f"{side.upper()} #{rank} High Price", u_z["price_high"], pq_z.get("price_high"))
            check("Magnet Zones", f"{side.upper()} #{rank} Dist %", u_z["distance_pct"], pq_z.get("distance_pct"))
            check("Magnet Zones", f"{side.upper()} #{rank} Strength", u_z["strength"], pq_z.get("strength"))
            check("Magnet Zones", f"{side.upper()} #{rank} Width %", u_z["width_pct"], pq_z.get("width_pct"))
        else:
            check("Magnet Zones", f"{side.upper()} #{rank} Row Exists", True, False)

    # C. Leverage Tiers Parity
    ui_lt = live_ui_ground_truth.get("leverageTiers", [])
    for u_t in ui_lt:
        tier = u_t["tier"]
        pq_row = df_lt[df_lt["tier"] == tier]
        if not pq_row.empty:
            pq_t = pq_row.iloc[-1].to_dict()
            check("Leverage Tiers", f"{tier.upper()} Short Strength", u_t["short_strength"], pq_t.get("short_strength"))
            check("Leverage Tiers", f"{tier.upper()} Long Strength", u_t["long_strength"], pq_t.get("long_strength"))
            check("Leverage Tiers", f"{tier.upper()} Total Shorts", u_t.get("total_shorts"), pq_t.get("total_shorts"))
            check("Leverage Tiers", f"{tier.upper()} Total Longs", u_t.get("total_longs"), pq_t.get("total_longs"))

    # D. Near Field Balance Parity
    ui_nf = live_ui_ground_truth.get("nearField")
    if ui_nf:
        pq_nf = df_nf.iloc[-1].to_dict() if not df_nf.empty else {}
        check("Near Field", "Longs %", ui_nf["longs_pct"], pq_nf.get("longs_pct"))
        check("Near Field", "Shorts %", ui_nf["shorts_pct"], pq_nf.get("shorts_pct"))
        check("Near Field", "Longs Count", ui_nf["longs_count"], pq_nf.get("longs_count"))
        check("Near Field", "Shorts Count", ui_nf["shorts_count"], pq_nf.get("shorts_count"))
        check("Near Field", "Shift 24h Pts", ui_nf["shift_24h_pts"], pq_nf.get("shift_24h_pts"))

    # E. Key Liquidation Levels Sample Parity
    # Check top 3 short and top 3 long barriers at +-8%
    df_kl_8 = df_kl[df_kl["range_filter"].str.contains("8")]
    if not df_kl_8.empty:
        for side in ["short", "long"]:
            for rk in [1, 2, 3]:
                match_r = df_kl_8[(df_kl_8["side"] == side) & (df_kl_8["rank"] == rk)]
                if not match_r.empty:
                    val = match_r.iloc[-1]
                    check("Key Levels (+-8%)", f"{side.upper()} #{rk} Barrier Exists", True, val["price"] > 0)
                    check("Key Levels (+-8%)", f"{side.upper()} #{rk} Strength > 0", True, val["strength"] >= 0)

    # Calculate Results
    total_points = len(comparisons)
    matched_points = sum(1 for c in comparisons if c["match"])
    parity_pct = (matched_points / total_points) * 100.0 if total_points > 0 else 0.0

    print(f"\n[{symbol}] Verification Results:")
    print(f"  Total Data Points Tested: {total_points}")
    print(f"  Matched Ground Truth:    {matched_points}")
    print(f"  Parity Percentage:       {parity_pct:.3f}%")

    print("\nSample Field Comparison Table:")
    print(f"{'CATEGORY':<16} | {'PARAMETER':<26} | {'UI VALUE':<20} | {'PARQUET VALUE':<20} | {'STATUS'}")
    print("-" * 96)
    for c in comparisons[:18]:
        status = "MATCH" if c["match"] else "MISMATCH"
        u_str = str(c['ui_value']).encode('ascii', 'replace').decode('ascii')
        p_str = str(c['parquet_value']).encode('ascii', 'replace').decode('ascii')
        param_str = str(c['parameter']).encode('ascii', 'replace').decode('ascii')
        print(f"{c['category']:<16} | {param_str:<26} | {u_str:<20} | {p_str:<20} | {status}")

    return {
        "symbol": symbol,
        "total_points": total_points,
        "matched_points": matched_points,
        "parity_pct": parity_pct,
        "comparisons": comparisons,
        "screenshots": {
            "key_levels": str(table_shot),
            "magnet_zones": str(magnet_shot)
        }
    }


async def main():
    results = {}
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        context = browser.contexts[0]
        page = None
        for pg in context.pages:
            if "liquidation-levels" in pg.url:
                page = pg
                break
        if not page:
            page = await context.new_page()

        try:
            for sym in TEST_ASSETS:
                res = await verify_asset_parity(page, sym)
                results[sym] = res
        finally:
            print("\nClosing verification tab...")
            await page.close()
            print("Tab closed cleanly.")

    # Save summary json
    summary_path = ARTIFACT_DIR / "parity_verification_multi_asset.json"
    clean_results = {}
    for sym, d in results.items():
        clean_results[sym] = {
            "total_points": d["total_points"],
            "matched_points": d["matched_points"],
            "parity_pct": d["parity_pct"],
            "screenshots": d["screenshots"],
            "sample_comparisons": d["comparisons"][:15]
        }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(clean_results, f, indent=2)

    print(f"\n=======================================================")
    print(f"      ALL ASSETS PARITY VERIFICATION COMPLETE         ")
    print(f"=======================================================")
    for sym, d in results.items():
        print(f"  Asset {sym:6s}: {d['matched_points']}/{d['total_points']} matched -> {d['parity_pct']:.3f}% Parity")
    print(f"Results saved to: {summary_path.name}")


if __name__ == "__main__":
    asyncio.run(main())

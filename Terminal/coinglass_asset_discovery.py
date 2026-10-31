"""Comprehensive CoinGlass Asset Discovery Engine

Connects via Chrome DevTools Protocol to the live browser session and audits
all MT5 mutual candidate contracts for active liquidation models, discrete
strikes, key barriers, and sentiment data.
"""

import asyncio
import json
import logging
import sys
import urllib.request
import websockets

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("AssetDiscovery")

# Exhaustive candidates for non-crypto instruments
CANDIDATE_REGISTRY = [
    # --- Energy / Crude Oil ---
    {
        "mt5": "USWTI.p",
        "name": "Crude Oil WTI",
        "ex": "MEXC",
        "sym": "USOIL_USDT",
        "url_sym": "USOIL",
    },
    {
        "mt5": "USWTI.p",
        "name": "Crude Oil WTI",
        "ex": "Kraken",
        "sym": "PF_WTIOIL_USD",
        "url_sym": "WTIOIL",
    },
    {
        "mt5": "UKBRENT.p",
        "name": "Brent Crude Oil",
        "ex": "TradeXYZ",
        "sym": "BRENTOIL-USD",
        "url_sym": "BRENTOIL",
    },
    {
        "mt5": "UKBRENT.p",
        "name": "Brent Crude Oil",
        "ex": "MEXC",
        "sym": "UKOIL_USDT",
        "url_sym": "UKOIL",
    },
    {
        "mt5": "UKBRENT.p",
        "name": "Brent Crude Oil",
        "ex": "Lighter",
        "sym": "BRENTOIL-USDC",
        "url_sym": "BRENTOIL",
    },
    {
        "mt5": "GAS.p",
        "name": "Natural Gas",
        "ex": "Binance",
        "sym": "GASUSDT",
        "url_sym": "GAS",
    },
    # --- Equity Indices ---
    {
        "mt5": "SP500.p",
        "name": "S&P 500 Index",
        "ex": "TradeXYZ",
        "sym": "SP500-USD",
        "url_sym": "SP500",
    },
    {
        "mt5": "SP500.p",
        "name": "S&P 500 Index",
        "ex": "Bitget",
        "sym": "SP500USDT_UMCBL",
        "url_sym": "SP500",
    },
    {
        "mt5": "NAS100.p",
        "name": "Nasdaq 100",
        "ex": "MEXC",
        "sym": "NAS100_USDT",
        "url_sym": "NAS100",
    },
    {
        "mt5": "NAS100.p",
        "name": "Nasdaq 100",
        "ex": "Gate",
        "sym": "NAS100_USDT",
        "url_sym": "NAS100",
    },
    {
        "mt5": "NAS100.p",
        "name": "Nasdaq 100",
        "ex": "LBANK",
        "sym": "NAS100USDT",
        "url_sym": "NAS100",
    },
    {
        "mt5": "DJ30.p",
        "name": "Wall Street 30 / Dow",
        "ex": "MEXC",
        "sym": "US30_USDT",
        "url_sym": "US30",
    },
    {
        "mt5": "DJ30.p",
        "name": "Wall Street 30 / Dow",
        "ex": "Gate",
        "sym": "US30_USDT",
        "url_sym": "US30",
    },
    {
        "mt5": "DJ30.p",
        "name": "Wall Street 30 / Dow",
        "ex": "LBANK",
        "sym": "US30USDT",
        "url_sym": "US30",
    },
    {
        "mt5": "GER30.p",
        "name": "DAX 40 / Germany 30",
        "ex": "TradeXYZ",
        "sym": "DAX-USD",
        "url_sym": "DAX",
    },
    {
        "mt5": "GER30.p",
        "name": "DAX 40 / Germany 30",
        "ex": "MEXC",
        "sym": "GER30_USDT",
        "url_sym": "GER30",
    },
    {
        "mt5": "GER40.p",
        "name": "DAX 40 Cash",
        "ex": "TradeXYZ",
        "sym": "DAX-USD",
        "url_sym": "DAX",
    },
    {
        "mt5": "UK100.p",
        "name": "UK 100 / FTSE",
        "ex": "TradeXYZ",
        "sym": "FTSE-USD",
        "url_sym": "FTSE",
    },
    {
        "mt5": "JP225.p",
        "name": "Nikkei 225",
        "ex": "MEXC",
        "sym": "JP225_USDT",
        "url_sym": "JP225",
    },
    {
        "mt5": "AU200.p",
        "name": "Australia 200 / ASX",
        "ex": "TradeXYZ",
        "sym": "ASX-USD",
        "url_sym": "ASX",
    },
    # --- Metals & Commodities ---
    {
        "mt5": "XAUUSD.pi",
        "name": "Gold / USD",
        "ex": "Binance",
        "sym": "XAUUSDT",
        "url_sym": "XAU",
    },
    {
        "mt5": "XAUUSD.pi",
        "name": "Gold / USD",
        "ex": "MEXC",
        "sym": "XAU_USDT",
        "url_sym": "XAU",
    },
    {
        "mt5": "XAUUSD.pi",
        "name": "Gold / USD",
        "ex": "Gate",
        "sym": "XAU_USDT",
        "url_sym": "XAU",
    },
    {
        "mt5": "XAGUSD.pi",
        "name": "Silver / USD",
        "ex": "Binance",
        "sym": "XAGUSDT",
        "url_sym": "XAG",
    },
    {
        "mt5": "COPPER.p",
        "name": "Copper / USD",
        "ex": "Binance",
        "sym": "COPPERUSDT",
        "url_sym": "COPPER",
    },
    {
        "mt5": "XPTUSD.pi",
        "name": "Platinum / USD",
        "ex": "Binance",
        "sym": "PLATINUMUSDT",
        "url_sym": "PLATINUM",
    },
    {
        "mt5": "XPDUSD.pi",
        "name": "Palladium / USD",
        "ex": "Binance",
        "sym": "PALLADIUMUSDT",
        "url_sym": "PALLADIUM",
    },
    {
        "mt5": "ALUMINIUM.p",
        "name": "Aluminium",
        "ex": "Binance",
        "sym": "ALUMINIUMUSDT",
        "url_sym": "ALUMINIUM",
    },
    # --- Forex Majors ---
    {
        "mt5": "EURUSD.pi",
        "name": "Euro / USD",
        "ex": "Bitget",
        "sym": "EURUSDUSDT_UMCBL",
        "url_sym": "EURUSD",
    },
    {
        "mt5": "EURUSD.pi",
        "name": "Euro / USD",
        "ex": "Bybit",
        "sym": "EURUSDUSDT",
        "url_sym": "EURUSD",
    },
    {
        "mt5": "EURUSD.pi",
        "name": "Euro / USD",
        "ex": "Gate",
        "sym": "EURUSD_USDT",
        "url_sym": "EURUSD",
    },
    {
        "mt5": "EURUSD.pi",
        "name": "Euro / USD",
        "ex": "HTX",
        "sym": "EURUSD-USDT",
        "url_sym": "EURUSD",
    },
    {
        "mt5": "EURUSD.pi",
        "name": "Euro / USD",
        "ex": "Lighter",
        "sym": "EURUSD-USDC",
        "url_sym": "EURUSD",
    },
    {
        "mt5": "GBPUSD.pi",
        "name": "Pound / USD",
        "ex": "HTX",
        "sym": "GBPUSD-USDT",
        "url_sym": "GBPUSD",
    },
    {
        "mt5": "GBPUSD.pi",
        "name": "Pound / USD",
        "ex": "Bitget",
        "sym": "GBPUSDUSDT_UMCBL",
        "url_sym": "GBPUSD",
    },
    {
        "mt5": "GBPUSD.pi",
        "name": "Pound / USD",
        "ex": "Bybit",
        "sym": "GBPUSDUSDT",
        "url_sym": "GBPUSD",
    },
    {
        "mt5": "GBPUSD.pi",
        "name": "Pound / USD",
        "ex": "Lighter",
        "sym": "GBPUSD-USDC",
        "url_sym": "GBPUSD",
    },
    {
        "mt5": "USDJPY.pi",
        "name": "USD / Yen",
        "ex": "Bitget",
        "sym": "USDJPYUSDT_UMCBL",
        "url_sym": "USDJPY",
    },
    {
        "mt5": "USDJPY.pi",
        "name": "USD / Yen",
        "ex": "Lighter",
        "sym": "USDJPY-USDC",
        "url_sym": "USDJPY",
    },
    {
        "mt5": "USDJPY.pi",
        "name": "USD / Yen",
        "ex": "HTX",
        "sym": "USDJPY-USDT",
        "url_sym": "USDJPY",
    },
    {
        "mt5": "AUDUSD.pi",
        "name": "Aussie / USD",
        "ex": "Bitget",
        "sym": "AUDUSDUSDT_UMCBL",
        "url_sym": "AUDUSD",
    },
    {
        "mt5": "AUDUSD.pi",
        "name": "Aussie / USD",
        "ex": "Bybit",
        "sym": "AUDUSDUSDT",
        "url_sym": "AUDUSD",
    },
    {
        "mt5": "USDCAD.pi",
        "name": "USD / CAD",
        "ex": "Bitget",
        "sym": "USDCADUSDT_UMCBL",
        "url_sym": "USDCAD",
    },
    {
        "mt5": "USDCHF.pi",
        "name": "USD / Swiss",
        "ex": "Bitget",
        "sym": "USDCHFUSDT_UMCBL",
        "url_sym": "USDCHF",
    },
    {
        "mt5": "NZDUSD.pi",
        "name": "Kiwi / USD",
        "ex": "Bitget",
        "sym": "NZDUSDUSDT_UMCBL",
        "url_sym": "NZDUSD",
    },
]


def get_liquidation_tab_id():
  res = json.loads(
      urllib.request.urlopen("http://127.0.0.1:9222/json").read().decode("utf-8")
  )
  for t in res:
    if t.get("type") == "page" and "liquidation-levels" in t.get("url", ""):
      return t["id"]
  for t in res:
    if t.get("type") == "page":
      return t["id"]
  raise RuntimeError("No open browser tab found on http://127.0.0.1:9222")


async def audit_all_candidates():
  tab_id = get_liquidation_tab_id()
  uri = f"ws://127.0.0.1:9222/devtools/page/{tab_id}"
  logger.info(f"Connected to CDP tab: {tab_id}")

  async with websockets.connect(uri) as ws:
    results = []

    for c in CANDIDATE_REGISTRY:
      mt5_sym = c["mt5"]
      name = c["name"]
      ex = c["ex"]
      contract = c["sym"]
      url_sym = c["url_sym"]
      key = f"{mt5_sym} -> [{ex}] {contract}"

      logger.info(f"Auditing {key}...")

      # 1. Update localStorage
      set_js = f"""(() => {{
                let cur = JSON.parse(localStorage.getItem('liqLevelsPrefs') || '{{}}');
                cur.ex = '{ex}';
                cur.sym = '{contract}';
                localStorage.setItem('liqLevelsPrefs', JSON.stringify(cur));
            }})()"""
      await ws.send(
          json.dumps({
              "id": 1,
              "method": "Runtime.evaluate",
              "params": {"expression": set_js},
          })
      )
      while True:
        m = json.loads(await ws.recv())
        if m.get("id") == 1:
          break

      # 2. Navigate
      url = f"https://www.coinglass.com/liquidation-levels?symbol={url_sym}"
      await ws.send(
          json.dumps(
              {"id": 2, "method": "Page.navigate", "params": {"url": url}}
          )
      )
      while True:
        m = json.loads(await ws.recv())
        if m.get("id") == 2:
          break

      # 3. Wait for hydration
      await asyncio.sleep(3.5)

      # 4. Inspect Model, Table, and Text
      inspect_js = """(() => {
                let text = document.body.innerText;
                let noData = text.includes('No liquidation level data for this pair yet');
                
                const nextEl = document.getElementById('__next');
                const containerKey = nextEl ? Object.keys(nextEl).find(k => k.startsWith('__reactContainer$')) : null;
                const rootFiber = (nextEl && containerKey) ? nextEl[containerKey] : null;

                let targetFiber = null;
                function walk(f, depth = 0) {
                    if (!f || depth > 80 || targetFiber) return;
                    try {
                        if (f.memoizedProps && f.memoizedProps.model && f.memoizedProps.ex) {
                            targetFiber = f;
                            return;
                        }
                    } catch(e) {}
                    walk(f.child, depth + 1);
                    walk(f.sibling, depth);
                }
                if (rootFiber) walk(rootFiber);
                const p = targetFiber ? targetFiber.memoizedProps : null;
                const m = p ? p.model : null;
                
                let table = document.querySelector('table');
                let rows = table ? table.querySelectorAll('tbody tr').length : 0;
                
                // Key barrier sample
                let topBarriers = [];
                if (table) {
                    let trs = table.querySelectorAll('tbody tr');
                    for (let tr of Array.from(trs).slice(0, 3)) {
                        topBarriers.push(tr.innerText.replace(/\\t/g, ' | '));
                    }
                }
                
                return {
                    noDataText: noData,
                    hasModel: !!m,
                    ex: p ? p.ex : null,
                    sym: p ? p.sym : null,
                    curPrice: m ? m.curPrice : null,
                    nSeg: m ? m.nSeg : null,
                    tableRows: rows,
                    sampleBarriers: topBarriers
                };
            })()"""

      await ws.send(
          json.dumps({
              "id": 3,
              "method": "Runtime.evaluate",
              "params": {"expression": inspect_js, "returnByValue": True},
          })
      )
      while True:
        m = json.loads(await ws.recv())
        if m.get("id") == 3:
          val = m.get("result", {}).get("result", {}).get("value", {})
          has_model = val.get("hasModel", False)
          n_seg = val.get("nSeg")
          cur_px = val.get("curPrice")
          no_data = val.get("noDataText", False)
          table_rows = val.get("tableRows", 0)

          is_active = (
              has_model
              and (n_seg is not None and n_seg > 0)
              and (cur_px is not None and cur_px > 0)
              and not no_data
          )
          status = "ACTIVE" if is_active else "EMPTY_SHELL"

          logger.info(
              f"  [{status}] {key} => Px={cur_px}, nSeg={n_seg},"
              f" Rows={table_rows}"
          )

          results.append({
              "mt5_ticker": mt5_sym,
              "asset_name": name,
              "exchange": ex,
              "contract_symbol": contract,
              "url_symbol": url_sym,
              "status": status,
              "is_active": is_active,
              "cur_price": cur_px,
              "discrete_levels_count": n_seg,
              "key_barrier_rows": table_rows,
              "sample_barriers": val.get("sampleBarriers", []),
          })
          break

    # Save comprehensive results
    output_path = "Database/comprehensive_discovery_results.json"
    with open(output_path, "w", encoding="utf-8") as f:
      json.dump(results, f, indent=2)
    logger.info(
        f"Discovery scan complete! Audited {len(results)} candidate contracts."
    )
    logger.info(f"Results saved to {output_path}")

    active_count = sum(1 for r in results if r["is_active"])
    logger.info(f"Total Active Non-Crypto Contracts Found: {active_count}")


if __name__ == "__main__":
  asyncio.run(audit_all_candidates())

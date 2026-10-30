"""
CoinGlass Footprint Signal Dumper & Continuous Collector (Deep History Edition)
-------------------------------------------------------------------------------
Automates extraction of complete order-flow footprint signals across all 3 bar
models (15m Time, Volume, and Range bars) for all aggregated perpetual futures
contracts on CoinGlass via Chrome DevTools Protocol (CDP).

Requirements satisfied:
1. Deep historical extraction incorporating maybeOlder() paging (e.g. 100+ BTC signals).
2. Continuous cycle dumping into Database/coinglass_aggregated_futures_signals.parquet.
3. 2 minutes rest between consecutive dumping cycles.
4. Automated IP rotation via Cloudflare WARP CLI after every cycle.
"""

import sys
import os
import time
import json
import logging
import subprocess
from pathlib import Path
from datetime import datetime

import requests
import pandas as pd
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parent.parent
DB_DIR = ROOT_DIR / "Database"
OUTPUT_PARQUET = DB_DIR / "coinglass_aggregated_futures_signals.parquet"
WARP_CLI = Path(r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe")
CDP_URL = "http://127.0.0.1:9222"
CYCLE_REST_SECONDS = 120  # 2 minutes rest between cycles
CHUNK_SIZE = 20

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("SignalDumper")


def get_current_ip() -> str:
    """Fetch external public IP address."""
    try:
        res = requests.get("https://api.ipify.org", timeout=5)
        if res.status_code == 200:
            return res.text.strip()
    except Exception as e:
        logger.warning(f"Failed to fetch public IP: {e}")
    return "Unknown"


def rotate_cloudflare_ip():
    """Disconnect and reconnect Cloudflare WARP to rotate IP and re-key tunnel."""
    if not WARP_CLI.exists():
        logger.error(f"WARP CLI binary not found at: {WARP_CLI}")
        return

    old_ip = get_current_ip()
    logger.info(f"[Cloudflare WARP] Current outbound IP: {old_ip}. Initiating IP rotation...")

    try:
        subprocess.run([str(WARP_CLI), "disconnect"], capture_output=True, text=True, check=False)
        time.sleep(2)
        subprocess.run([str(WARP_CLI), "tunnel", "rotate-keys"], capture_output=True, text=True, check=False)
        time.sleep(1)
        subprocess.run([str(WARP_CLI), "connect"], capture_output=True, text=True, check=False)
        time.sleep(4)

        new_ip = get_current_ip()
        logger.info(f"[Cloudflare WARP] Rotation complete. New outbound IP: {new_ip} (Old: {old_ip})")
    except Exception as e:
        logger.error(f"[Cloudflare WARP] Error during IP rotation: {e}")


def get_coinglass_tab(browser_context):
    """Locate the existing CoinGlass footprint tab, or open a new one if missing."""
    for page in browser_context.pages:
        if "coinglass.com/footprint" in page.url:
            return page
    
    logger.info("CoinGlass footprint tab not found in active tabs. Navigating new page...")
    page = browser_context.new_page()
    page.goto("https://www.coinglass.com/footprint?symbol=ETH", wait_until="networkidle")
    time.sleep(3)
    return page


def ensure_terminal_ready(page):
    """Ensure terminal controller is attached to window.__cg_term."""
    init_script = """
    () => {
        if (window.__cg_term) return true;
        const nextEl = document.getElementById('__next');
        if (!nextEl) return false;
        const containerKey = Object.keys(nextEl).find(k => k.startsWith('__reactContainer$'));
        if (!containerKey) return false;
        const rootFiber = nextEl[containerKey];

        let term = null;
        function findTerm(fiber, depth = 0) {
            if (!fiber || depth > 80 || term) return;
            try {
                if (fiber.memoizedProps && fiber.memoizedProps.value && fiber.memoizedProps.value.term) {
                    term = fiber.memoizedProps.value.term;
                    return;
                }
            } catch (e) {}
            findTerm(fiber.child, depth + 1);
            findTerm(fiber.sibling, depth);
        }
        findTerm(rootFiber);
        if (term) {
            window.__cg_term = term;
            return true;
        }
        return false;
    }
    """
    ready = page.evaluate(init_script)
    if not ready:
        time.sleep(2)
        ready = page.evaluate(init_script)
    return ready


def extract_all_signals_deep(page) -> list:
    """Run in-page multi-symbol signal extraction with deep historical paging."""
    ensure_terminal_ready(page)

    symbols = page.evaluate("""
    () => {
        const term = window.__cg_term;
        const allPairs = term.pairs ? term.pairs() : [];
        return allPairs.filter(p => p.m && p.m.id === 'agg-perp').map(p => p.s);
    }
    """)

    if not symbols:
        raise RuntimeError("No aggregated perpetual futures symbols discovered in catalog.")

    logger.info(f"Targeting {len(symbols)} aggregated perpetual futures assets with deep history.")
    page.evaluate("() => { window.__allExtractedSignals = []; }")

    deep_chunk_fn = """
    async (symbolsChunk) => {
        const term = window.__cg_term;
        for (const sym of symbolsChunk) {
            await new Promise((resolve) => {
                const timeout = setTimeout(() => {
                    unsub();
                    resolve();
                }, 7000);

                let loadedOlder = false;

                const unsub = term.subscribe(async () => {
                    if (term.status === 'ready' && term.prefs.sym === sym && !term.D.err) {
                        if (!loadedOlder) {
                            loadedOlder = true;
                            try {
                                await term.maybeOlder();
                            } catch (e) {}
                        }

                        const sigSets = term.sigSets();
                        if (sigSets && sigSets.length === 3) {
                            clearTimeout(timeout);
                            unsub();
                            for (const set of sigSets) {
                                const X = set.X;
                                for (const s of set.sigs) {
                                    const text = term.sigText(s, X);
                                    window.__allExtractedSignals.push({
                                        symbol: sym,
                                        market: 'agg-perp',
                                        bar_type: set.bt,
                                        bar_label: set.label,
                                        time_ms: X && X.T ? X.T[s.i] : null,
                                        datetime_utc: X && X.T ? new Date(X.T[s.i]).toISOString() : null,
                                        sig_code: s.t,
                                        sig_name: term.sigName(s),
                                        side: s.side === 'buy' ? 'bullish' : 'bearish',
                                        price: s.p,
                                        title: text[0] || '',
                                        description: text[1] || ''
                                    });
                                }
                            }
                            resolve();
                        }
                    }
                });

                term.pickPair(sym, 'agg-perp');
            });
        }
        return window.__allExtractedSignals.length;
    }
    """

    total_chunks = (len(symbols) + CHUNK_SIZE - 1) // CHUNK_SIZE
    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i:i + CHUNK_SIZE]
        so_far = page.evaluate(deep_chunk_fn, chunk)
        chunk_num = i // CHUNK_SIZE + 1
        logger.info(f"Chunk {chunk_num}/{total_chunks} ({chunk[0]}..{chunk[-1]}): {so_far} total signals.")

    # Restore view back to ETH
    page.evaluate("() => window.__cg_term.pickPair('ETH', 'agg-perp')")

    all_signals = page.evaluate("() => window.__allExtractedSignals")
    return all_signals


def dump_to_parquet(signals: list):
    """Convert extracted records to a clean DataFrame and save to Parquet."""
    if not signals:
        logger.warning("Empty signal set received. Skipping file write.")
        return

    DB_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(signals)
    df["dumped_at_utc"] = datetime.utcnow().isoformat()

    df.to_parquet(OUTPUT_PARQUET, index=False, engine="pyarrow")
    logger.info(f"Parquet updated: {OUTPUT_PARQUET} ({len(df)} rows, {df['symbol'].nunique()} unique symbols)")


def run_continuous_collector():
    """Main execution loop: Dump -> Rotate Cloudflare IP -> 2 min rest -> Repeat."""
    logger.info("Initializing CoinGlass Footprint Continuous Signal Collector...")
    DB_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            logger.error(f"Cannot connect to CDP at {CDP_URL}: {e}")
            logger.error("Please ensure Edge/Chrome is running with: --remote-debugging-port=9222")
            sys.exit(1)

        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = get_coinglass_tab(context)
        logger.info(f"Connected to CoinGlass page: {page.title()}")

        cycle_count = 1
        while True:
            logger.info(f"========== STARTING DUMPING CYCLE #{cycle_count} ==========")
            start_time = time.time()

            try:
                signals = extract_all_signals_deep(page)
                dump_to_parquet(signals)
                elapsed = round(time.time() - start_time, 1)
                logger.info(f"Cycle #{cycle_count} completed in {elapsed}s.")
            except Exception as e:
                logger.error(f"Error during cycle #{cycle_count}: {e}", exc_info=True)

            # Step 2: Rotate Cloudflare IP after every cycle
            rotate_cloudflare_ip()

            # Step 1: 2 minutes rest before next cycle
            logger.info(f"Resting for {CYCLE_REST_SECONDS} seconds (2 minutes) before starting cycle #{cycle_count + 1}...")
            time.sleep(CYCLE_REST_SECONDS)
            cycle_count += 1


if __name__ == "__main__":
    run_continuous_collector()

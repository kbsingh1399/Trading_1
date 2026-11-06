from pathlib import Path

target_file = Path("1_coinglass_footprint_scraper.py")
text = target_file.read_text(encoding="utf-8")

idx1 = text.find("async def execute_footprint_sweep(ws) -> Tuple[int, int]:")
idx2 = text.find("async def run_sweep_once() -> bool:")
assert idx1 != -1 and idx2 != -1, "Function markers not found"

new_fn = """async def execute_footprint_sweep(ws) -> Tuple[int, int]:
    \"\"\"Sweeps target assets, harvests footprint signals and candles, and updates auction profiles.\"\"\"
    symbols, mt5_map = load_target_symbols()
    logger.info(f"Starting Footprint sweep across {len(symbols)} crypto assets...")
    total_signals = 0
    total_new = 0

    try:
        for idx, sym in enumerate(symbols):
            try:
                mt5_sym = mt5_map.get(sym, f"{sym}USD")
                res = await extract_symbol_footprint(ws, sym, mt5_sym)
                if res.get("error"):
                    logger.debug(f"[{sym}] Extraction notice: {res.get('error')}")
                    continue

                signals = res.get("signals", [])
                candles = res.get("candles", [])

                if candles:
                    dump_candles(sym, candles)

                if signals:
                    new_s = dump_signals(signals, candles=candles)
                    total_signals += len(signals)
                    total_new += len(new_s)
                    try:
                        latest_px = float(candles[-1].get("close")) if candles else None
                        update_asset_queue_and_evaluate(sym, new_s if new_s else signals, current_price=latest_px)
                    except Exception as eval_err:
                        logger.error(f"[{sym}] Trade evaluator error: {eval_err}")

            except Exception as e:
                logger.error(f"[{sym}] Sweep error: {e}")

            await asyncio.sleep(0.05)
    finally:
        # Update local auction levels atomically from all harvested candles
        try:
            update_all_auction_profiles()
            logger.info("Updated all local Auction Profile tables (Database/Auction_Levels/).")
        except Exception as e:
            logger.warning(f"Error updating auction profiles: {e}")

    logger.info(f"Footprint sweep complete: {total_signals} total signals harvested ({total_new} new).")
    return total_signals, total_new


"""

updated_text = text[:idx1] + new_fn + text[idx2:]
target_file.write_text(updated_text, encoding="utf-8")
print("execute_footprint_sweep cleanly replaced with try...finally block!")
